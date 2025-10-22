import os
import psycopg
import time
import logging
import asyncio
import json
from datetime import datetime
from contextlib import closing
from pathlib import Path
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Form, Request, status, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import re
import httpx
from dotenv import load_dotenv

# Импорт модуля интеграции с Битрикс24
from .bitrix24_integration import send_lead_to_bitrix24

# Подхватываем переменные окружения из .env
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
TEMPLATES_DIR = PROJECT_ROOT / "templates"

# PostgreSQL only
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL or not (DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")):
    raise ValueError("DATABASE_URL must be set to a PostgreSQL connection string")

# Настройка Jinja2 шаблонов
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

DATA_DIR.mkdir(parents=True, exist_ok=True)

# Флаг доступности БД
DB_AVAILABLE = True

def init_db() -> None:
    """Создаёт таблицы leads и events в PostgreSQL БД, если их нет."""
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=1) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS leads (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        name TEXT,
                        phone TEXT NOT NULL,
                        email TEXT,
                        message TEXT,
                        ip TEXT,
                        user_agent TEXT,
                        country TEXT,
                        city TEXT,
                        region TEXT,
                        social_source TEXT,
                        social_id TEXT,
                        social_data TEXT
                    );
                    
                    CREATE TABLE IF NOT EXISTS events (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        event_type TEXT NOT NULL,
                        page_url TEXT,
                        referrer TEXT,
                        session_id TEXT,
                        ip TEXT,
                        user_agent TEXT,
                        country TEXT,
                        city TEXT,
                        region TEXT,
                        social_source TEXT,
                        social_id TEXT,
                        social_data TEXT
                    )
                    """
                )
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

async def get_geo_by_ip(ip: str) -> Dict[str, Optional[str]]:
    """Получает геолокацию по IP через бесплатный API."""
    if not ip or ip in ("127.0.0.1", "localhost", "::1"):
        return {"country": None, "city": None, "region": None}
    
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(
                f"http://ip-api.com/json/{ip}?fields=status,country,regionName,city,query"
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    return {
                        "country": data.get("country"),
                        "city": data.get("city"),
                        "region": data.get("regionName")
                    }
    except Exception as e:
        logger.warning(f"Failed to get geo for IP {ip}: {e}")
    
    return {"country": None, "city": None, "region": None}

app = FastAPI(title="RostFerrum API", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["POST", "GET"],
    allow_headers=["*"]
)

# Rate limiting
RATE_WINDOW_SECONDS = 60
RATE_MAX_REQUESTS = 5
rate_bucket: dict[str, list[float]] = {}

def check_rate_limit(ip: str) -> bool:
    now = time.time()
    bucket = rate_bucket.setdefault(ip, [])
    bucket[:] = [ts for ts in bucket if now - ts < RATE_WINDOW_SECONDS]
    if len(bucket) >= RATE_MAX_REQUESTS:
        return False
    bucket.append(now)
    return True

# Функции для работы с товарами и категориями
def get_product_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    """Получает товар по slug из PostgreSQL БД"""
    if not DB_AVAILABLE:
        return None
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT product_id, sku, slug, sku, price, null, true, 
                           product_characs, null, null, category_id, null
                    FROM tr_products_raw 
                    WHERE slug = %s
                    """, (slug,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                
                return {
                    "id": row[0],
                    "sku": row[1],
                    "slug": row[2],
                    "name": row[3],
                    "price": float(row[4]) if row[4] else None,
                    "currency": row[5],
                    "in_stock": row[6],
                    "spec": row[7] if isinstance(row[7], (dict, list)) else {},
                    "short_desc": row[8],
                    "long_desc": row[9],
                    "category_id": row[10],
                    "h1": row[11] or row[3],  # используем meta_title или name
                    "image_url": "/assets/img/no-photo.png",
                }
    except Exception as e:
        logger.error(f"Ошибка при получении товара {slug}: {e}")
        return None

def get_category_by_id(category_id: int) -> Optional[Dict[str, Any]]:
    """Получает категорию по ID"""
    if not DB_AVAILABLE:
        return None
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, slug, null, parent_id
                    FROM tr_categories 
                    WHERE id = %s
                    """, (category_id,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                
                return {
                    "id": row[0],
                    "name": row[1],
                    "slug": row[2],
                    "description": row[3],  # seo_description
                    "parent_id": row[4],
                }
    except Exception as e:
        logger.error(f"Ошибка при получении категории {category_id}: {e}")
        return None

def get_related_products(category_id: int, exclude_product_id: int, limit: int = 6) -> list:
    """Получает похожие товары из той же категории"""
    if not DB_AVAILABLE:
        return []
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT product_id, sku, slug, sku, price, null, true, null
                    FROM tr_products_raw 
                    WHERE category_id = %s AND product_id != %s
                    ORDER BY sku 
                    LIMIT %s
                    """, (category_id, exclude_product_id, limit)
                )
                rows = cur.fetchall()
                return [
                    {
                        "id": row[0],
                        "sku": row[1],
                        "slug": row[2],
                        "name": row[3],
                        "price": float(row[4]) if row[4] else None,
                        "currency": row[5],
                        "in_stock": row[6],
                        "short_desc": row[7],
                        "image_url": "/assets/img/no-photo.png",
                    }
                    for row in rows
                ]
    except Exception as e:
        logger.error(f"Ошибка при получении похожих товаров: {e}")
        return []


# === ФУНКЦИИ ДЛЯ КАТАЛОГА ===

def get_root_categories(page: int = 1, per_page: int = 24) -> List[Dict[str, Any]]:
    """Получает корневые категории с пагинацией"""
    if not DB_AVAILABLE:
        return []
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                offset = (page - 1) * per_page
                cur.execute(
                    """
                    SELECT id, name, slug, null, null, null, null,
                           coalesce(null, '/assets/img/no-photo.png') as image_url,
                           coalesce(null, 0) as sort_val
                    FROM tr_categories 
                    WHERE parent_id IS NULL
                    ORDER BY name
                    LIMIT %s OFFSET %s
                    """, (per_page, offset)
                )
                
                return [
                    {
                        "id": row[0],
                        "name": row[1],
                        "slug": row[2],
                        "seo_title": row[3],
                        "seo_description": row[4],
                        "desc": row[4],  # для совместимости с шаблоном
                        "h1": row[5],
                        "intro_text": row[6],
                        "image_url": row[7],
                    }
                    for row in cur.fetchall()
                ]
    except Exception as e:
        logger.error(f"Ошибка при получении корневых категорий: {e}")
        return []


def get_root_categories_count() -> int:
    """Получает количество корневых категорий"""
    if not DB_AVAILABLE:
        return 0
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM tr_categories WHERE parent_id IS NULL"
                )
                return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка при подсчете корневых категорий: {e}")
        return 0


def get_category_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    """Получает категорию по slug"""
    if not DB_AVAILABLE:
        return None
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, slug, null, null, null, null, parent_id,
                           coalesce(null, '/assets/img/no-photo.png') as image_url
                    FROM tr_categories 
                    WHERE slug = %s
                    """, (slug,)
                )
                row = cur.fetchone()
                if not row:
                    return None
                
                return {
                    "id": row[0],
                    "name": row[1],
                    "slug": row[2],
                    "seo_title": row[3],
                    "description": row[4],  # seo_description
                    "h1": row[5],
                    "intro_text": row[6],
                    "parent_id": row[7],
                    "image_url": row[8],
                }
    except Exception as e:
        logger.error(f"Ошибка при получении категории {slug}: {e}")
        return None


def get_subcategories(category_id: int) -> List[Dict[str, Any]]:
    """Получает подкатегории для категории"""
    if not DB_AVAILABLE:
        return []
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, slug, null, null, null, null,
                           coalesce(null, '/assets/img/no-photo.png') as image_url,
                           coalesce(null, 0) as sort_val
                    FROM tr_categories 
                    WHERE parent_id = %s
                    ORDER BY name
                    """, (category_id,)
                )
                
                return [
                    {
                        "id": row[0],
                        "name": row[1],
                        "slug": row[2],
                        "seo_title": row[3],
                        "description": row[4],  # seo_description
                        "desc": row[4],  # для совместимости с шаблоном
                        "h1": row[5],
                        "intro_text": row[6],
                        "image_url": row[7],
                    }
                    for row in cur.fetchall()
                ]
    except Exception as e:
        logger.error(f"Ошибка при получении подкатегорий для {category_id}: {e}")
        return []


def get_products_by_category(category_id: int, page: int = 1, per_page: int = 24) -> List[Dict[str, Any]]:
    """Получает товары категории с пагинацией"""
    if not DB_AVAILABLE:
        return []
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                offset = (page - 1) * per_page
                cur.execute(
                    """
                    SELECT product_id, sku, slug, sku, price, null, true, product_characs, null, category_id
                    FROM tr_products_raw 
                    WHERE category_id = %s
                    ORDER BY sku
                    LIMIT %s OFFSET %s
                    """, (category_id, per_page, offset)
                )
                
                return [
                    {
                        "id": row[0],
                        "sku": row[1],
                        "slug": row[2],
                        "name": row[3],
                        "price": float(row[4]) if row[4] else None,
                        "currency": row[5],
                        "in_stock": row[6],
                        "spec": row[7] if isinstance(row[7], (dict, list)) else {},
                        "short_desc": row[8],
                        "category_id": row[9],
                        "image_url": "/assets/img/no-photo.png",
                    }
                    for row in cur.fetchall()
                ]
    except Exception as e:
        logger.error(f"Ошибка при получении товаров категории {category_id}: {e}")
        return []


def get_products_count_by_category(category_id: int) -> int:
    """Получает количество товаров в категории"""
    if not DB_AVAILABLE:
        return 0
        
    try:
        with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM tr_products_raw WHERE category_id = %s",
                    (category_id,)
                )
                return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Ошибка при подсчете товаров в категории {category_id}: {e}")
        return 0


def get_site_context() -> Dict[str, Any]:
    """Получает контекст сайта для шаблонов"""
    return {
        "base_url": os.getenv("SITE_BASE_URL", "https://rostferrum.ru"),
        "name": "РостФеррум",
        "phone": "+7 (927) 000-00-00",
        "email": "rostferrum@mail.ru",
        "address": "г. Сарапул, ул. Красная площадь, 4",
        "hours": "Пн–Пт 9:00–22:00",
    }


@app.on_event("startup")
def on_startup() -> None:
    global DB_AVAILABLE
    try:
        init_db()
        DB_AVAILABLE = True
        logger.info("Database initialized successfully")
    except Exception as e:
        DB_AVAILABLE = False
        logger.warning(f"Database unavailable on startup: {e}")

@app.get("/product/{slug}/", response_class=HTMLResponse)
async def product_page(request: Request, slug: str):
    """Страница товара (один коннект к БД на запрос)"""
    product = None
    category = None
    related: list = []
    if DB_AVAILABLE:
        try:
            with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    # Товар
                    cur.execute(
                        """
                        SELECT product_id, sku, slug, sku, price, null, true, 
                               product_characs, null, null, category_id, null
                        FROM tr_products_raw 
                        WHERE slug = %s
                        """,
                        (slug,)
                    )
                    row = cur.fetchone()
                    if row:
                        product = {
                            "id": row[0],
                            "sku": row[1],
                            "slug": row[2],
                            "name": row[3],
                            "price": float(row[4]) if row[4] else None,
                            "currency": row[5],
                            "in_stock": row[6],
                            "spec": row[7] if isinstance(row[7], (dict, list)) else {},
                            "short_desc": row[8],
                            "long_desc": row[9],
                            "category_id": row[10],
                            "h1": row[11] or row[3],
                            "image_url": "/assets/img/no-photo.png",
                        }
                    
                    if product and product.get("category_id"):
                        # Категория
                        cur.execute(
                            """
                            SELECT id, name, slug, null, parent_id
                            FROM tr_categories 
                            WHERE id = %s
                            """,
                            (product["category_id"],)
                        )
                        crow = cur.fetchone()
                        if crow:
                            category = {
                                "id": crow[0],
                                "name": crow[1],
                                "slug": crow[2],
                                "description": crow[3],
                                "parent_id": crow[4],
                            }
                        # Похожие
                        cur.execute(
                            """
                            SELECT product_id, sku, slug, sku, price, null, true, null
                            FROM tr_products_raw 
                            WHERE category_id = %s AND product_id != %s
                            ORDER BY sku 
                            LIMIT %s
                            """,
                            (product["category_id"], product["id"], 6)
                        )
                        rows = cur.fetchall()
                        related = [
                            {
                                "id": r[0],
                                "sku": r[1],
                                "slug": r[2],
                                "name": r[3],
                                "price": float(r[4]) if r[4] else None,
                                "currency": r[5],
                                "in_stock": r[6],
                                "short_desc": r[7],
                                "image_url": "/assets/img/no-photo.png",
                            }
                            for r in rows
                        ]
        except Exception as e:
            logger.error(f"Ошибка при построении страницы товара {slug}: {e}")
            # Помечаем БД как недоступную до рестарта
            globals()["DB_AVAILABLE"] = False
    
    if not product:
        # Возвращаем 404 из статики
        static_404_path = PROJECT_ROOT / "dist" / "404.html"
        if static_404_path.exists():
            with open(static_404_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read(), status_code=404)
        return HTMLResponse(content="<h1>404 - Товар не найден</h1>", status_code=404)
    
    # Breadcrumbs
    breadcrumbs = [{"name": "Главная", "url": "/"}]
    if category:
        breadcrumbs.append({"name": "Каталог", "url": "/catalog/"})
        breadcrumbs.append({"name": category["name"], "url": f"/catalog/{category['slug']}/"})
    breadcrumbs.append({"name": product["name"], "url": ""})
    
    context = {
        "request": request,
        "product": product,
        "category": category,
        "related": related,
        "breadcrumbs": breadcrumbs,
        "canonical_url": f"/product/{product['slug']}/",
        "site": {
            "name": "РостФеррум",
            "description": "Металлопрокат и трубопроводная арматура",
        },
    }
    return templates.TemplateResponse('product.html', context)

@app.post("/api/lead")
async def submit_lead(
    request: Request,
    name: Optional[str] = Form(default=None),
    phone: str = Form(...),
    email: Optional[str] = Form(default=None),
    message: Optional[str] = Form(default=None),
    hp_field: Optional[str] = Form(default=None),  # honeypot
    document: Optional[UploadFile] = File(default=None),
    social_source: Optional[str] = Form(default=None),
    social_id: Optional[str] = Form(default=None),
    social_data: Optional[str] = Form(default=None),
):
    global DB_AVAILABLE
    client_ip = request.client.host if request.client else ""
    user_agent = request.headers.get("user-agent", "")[:512]
    
    geo_data = await get_geo_by_ip(client_ip)

    # Honeypot
    if hp_field:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "error", "message": "Некорректная заявка"},
        )

    # Валидация телефона
    normalized_phone = "".join(ch for ch in phone if ch.isdigit() or ch == "+")
    if len(normalized_phone.replace("+", "").strip()) < 7:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"status": "error", "message": "Укажите корректный телефон"},
        )
    
    # Валидация email
    if email and not re.match(r'^[^@]+@[^@]+\.[^@]+$', email.strip()):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"status": "error", "message": "Укажите корректный email"},
        )

    # Rate limit
    if not check_rate_limit(client_ip):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"status": "error", "message": "Слишком много запросов, попробуйте позже"},
        )

    # Сохранение файла (если есть)
    file_path = None
    file_bytes: Optional[bytes] = None
    file_name: Optional[str] = None
    if document and document.filename:
        allowed_extensions = {
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', 
            '.jpg', '.jpeg', '.png', '.gif', '.txt'
        }
        file_extension = Path(document.filename).suffix.lower()
        
        if file_extension in allowed_extensions:
            uploads_dir = DATA_DIR / "uploads"
            uploads_dir.mkdir(exist_ok=True)
            
            timestamp = int(time.time())
            safe_filename = f"{timestamp}_{document.filename}"
            file_path = uploads_dir / safe_filename
            file_name = safe_filename
            
            try:
                content = await document.read()
                file_bytes = content
                with open(file_path, "wb") as f:
                    f.write(content)
                logger.info(f"File saved: {file_path}")
            except Exception as e:
                logger.error(f"Error saving file: {e}")
                file_path = None
        else:
            logger.warning(f"File type not allowed: {file_extension}")
    
    # Bitrix24 integration
    form_data = {
        'name': name,
        'phone': normalized_phone,
        'email': email.strip() if email else None,
        'message': message,
        'ip': client_ip,
        'user_agent': user_agent
    }
    
    bitrix_result = {'success': False}
    try:
        bitrix_result = await send_lead_to_bitrix24(
            form_data,
            str(file_path) if file_path else None,
            file_bytes=file_bytes,
            file_name=file_name
        )
        logger.info(f"Bitrix24 result: {bitrix_result}")
    except Exception as e:
        logger.error(f"Error sending to Bitrix24: {e}")
    
    # Запись в БД
    lead_id = None
    if DB_AVAILABLE:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO leads (created_at, name, phone, email, message, ip, user_agent,
                                         country, city, region, social_source, social_id, social_data)
                        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (name, normalized_phone, email.strip() if email else None, message, client_ip, user_agent,
                         geo_data["country"], geo_data["city"], geo_data["region"],
                         social_source, social_id, social_data),
                    )
                    lead_id = cur.fetchone()[0]
                    conn.commit()
        except Exception as e:
            DB_AVAILABLE = False
            logger.error(f"Failed to save lead to Postgres: {e}")
    else:
        logger.warning("Database write skipped (unavailable)")
    
    # Ответ
    response_data = {"status": "ok", "message": "Заявка принята, менеджер свяжется с вами"}
    
    if bitrix_result.get('success'):
        response_data['bitrix24_lead_id'] = bitrix_result.get('lead_id')
        if file_path and bitrix_result.get('file_attached'):
            response_data['file_status'] = 'uploaded'
        elif file_path:
            response_data['file_status'] = 'saved_locally'
    else:
        logger.warning(f"Bitrix24 integration failed: {bitrix_result}")
    
    return response_data

@app.post("/api/event")
async def track_event(
    request: Request,
    event_type: str = Form(...),
    page_url: Optional[str] = Form(default=None),
    referrer: Optional[str] = Form(default=None),
    session_id: Optional[str] = Form(default=None),
    social_source: Optional[str] = Form(default=None),
    social_id: Optional[str] = Form(default=None),
    social_data: Optional[str] = Form(default=None),
):
    """Фиксирует события пользователей на сайте."""
    global DB_AVAILABLE
    client_ip = request.client.host if request.client else ""
    user_agent = request.headers.get("user-agent", "")[:512]
    
    geo_data = await get_geo_by_ip(client_ip)
    
    if DB_AVAILABLE:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO events (created_at, event_type, page_url, referrer, session_id,
                                          ip, user_agent, country, city, region,
                                          social_source, social_id, social_data)
                        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (event_type, page_url, referrer, session_id, client_ip, user_agent,
                         geo_data["country"], geo_data["city"], geo_data["region"],
                         social_source, social_id, social_data),
                    )
                    conn.commit()
        except Exception as e:
            DB_AVAILABLE = False
            logger.error(f"Failed to save event to Postgres: {e}")
    else:
        logger.warning("Database write for event skipped (unavailable)")

    return {"status": "ok"}

# === КАТАЛОГ МАРШРУТЫ ===

@app.get("/catalog/", response_class=HTMLResponse)
async def catalog_index(request: Request, page: int = 1) -> HTMLResponse:
    """Главная страница каталога с пагинацией"""
    per_page = 24
    categories = get_root_categories(page=page, per_page=per_page)
    total_count = get_root_categories_count()
    
    # Пагинация
    total_pages = max(1, (total_count + per_page - 1) // per_page)
    prev_url = f"/catalog/page-{page-1}/" if page > 1 else None
    next_url = f"/catalog/page-{page+1}/" if page < total_pages else None
    
    pagination = {
        "total_pages": total_pages,
        "current_page": page,
        "prev_url": prev_url,
        "next_url": next_url,
    }
    
    # Хлебные крошки для каталога
    breadcrumbs = [
        {"name": "Главная", "url": "/"},
        {"name": "Каталог", "url": None}
    ]
    
    template = templates.get_template("catalog.html")
    context = {
        "request": request,
        "categories": categories,
        "pagination": pagination,
        "breadcrumbs": breadcrumbs,
        "site": get_site_context(),
        "year": datetime.now().year,
    }
    
    html = template.render(**context)
    return HTMLResponse(content=html)


@app.get("/catalog/page-{page}/", response_class=HTMLResponse)
async def catalog_index_paginated(request: Request, page: int) -> HTMLResponse:
    """Пагинированные страницы каталога"""
    return await catalog_index(request, page)


@app.get("/catalog/{category_slug}/", response_class=HTMLResponse)
async def category_page(request: Request, category_slug: str, page: int = 1) -> HTMLResponse:
    """Страница категории с товарами или подкатегориями (один коннект на запрос)"""
    category = None
    subcategories: List[Dict[str, Any]] = []
    products: List[Dict[str, Any]] = []
    total_count = 0
    if DB_AVAILABLE:
        try:
            with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    # Категория по slug
                    cur.execute(
                        """
                        SELECT id, name, slug, null, null, null, null, parent_id,
                               coalesce(null, '/assets/img/no-photo.png') as image_url
                        FROM tr_categories 
                        WHERE slug = %s
                        """,
                        (category_slug,)
                    )
                    row = cur.fetchone()
                    if row:
                        category = {
                            "id": row[0],
                            "name": row[1],
                            "slug": row[2],
                            "seo_title": row[3],
                            "description": row[4],
                            "h1": row[5],
                            "intro_text": row[6],
                            "parent_id": row[7],
                            "image_url": row[8],
                        }
                    
                    if category:
                        # Подкатегории
                        cur.execute(
                            """
                            SELECT id, name, slug, null, null, null, null,
                                   coalesce(null, '/assets/img/no-photo.png') as image_url,
                                   coalesce(null, 0) as sort_val
                            FROM tr_categories 
                            WHERE parent_id = %s
                            ORDER BY name
                            """,
                            (category["id"],)
                        )
                        subcategories = [
                            {
                                "id": r[0],
                                "name": r[1],
                                "slug": r[2],
                                "seo_title": r[3],
                                "description": r[4],
                                "desc": r[4],
                                "h1": r[5],
                                "intro_text": r[6],
                                "image_url": r[7],
                            }
                            for r in cur.fetchall()
                        ]
                        
                        if not subcategories:
                            # Товары категории с общим количеством (оконная функция)
                            per_page = 24
                            offset = (page - 1) * per_page
                            cur.execute(
                                """
                                SELECT product_id, sku, slug, sku, price, null, true, product_characs, null, category_id,
                                       COUNT(*) OVER() AS total_count
                                FROM tr_products_raw 
                                WHERE category_id = %s
                                ORDER BY sku
                                LIMIT %s OFFSET %s
                                """,
                                (category["id"], per_page, offset)
                            )
                            rows = cur.fetchall()
                            if rows:
                                total_count = rows[0][10]
                            products = [
                                {
                                    "id": r[0],
                                    "sku": r[1],
                                    "slug": r[2],
                                    "name": r[3],
                                    "price": float(r[4]) if r[4] else None,
                                    "currency": r[5],
                                    "in_stock": r[6],
                                    "spec": r[7] if isinstance(r[7], (dict, list)) else {},
                                    "short_desc": r[8],
                                    "category_id": r[9],
                                    "image_url": "/assets/img/no-photo.png",
                                }
                                for r in rows
                            ]
        except Exception as e:
            logger.error(f"Ошибка при построении страницы категории {category_slug}: {e}")
            globals()["DB_AVAILABLE"] = False
    
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")
    
    template = templates.get_template("category.html")
    
    if subcategories:
        meta_title = category.get("seo_title") or f"{category.get('name')} — {get_site_context()['name']}"
        meta_description = category.get("description") or f"Купить {category.get('name')} по ГОСТ. В наличии на складе. Счет за 1 час. Доставка по России. {get_site_context()['name']}"
        breadcrumbs = [
            {"name": "Главная", "url": "/"},
            {"name": "Каталог", "url": "/catalog/"},
            {"name": category.get("name"), "url": None}
        ]
        context = {
            "request": request,
            "category": category,
            "subcategories": subcategories,
            "products": [],
            "pagination": None,
            "breadcrumbs": breadcrumbs,
            "meta": {
                "title": meta_title,
                "description": meta_description,
            },
            "site": get_site_context(),
            "year": datetime.now().year,
        }
    else:
        per_page = 24
        total_pages = max(1, (total_count + per_page - 1) // per_page)
        base_url = f"/catalog/{category_slug}/"
        prev_url = f"{base_url}page-{page-1}/" if page > 1 else None
        next_url = f"{base_url}page-{page+1}/" if page < total_pages else None
        pagination = {
            "total_pages": total_pages,
            "current_page": page,
            "prev_url": prev_url,
            "next_url": next_url,
        }
        meta_title = category.get("seo_title") or f"{category.get('name')} — {get_site_context()['name']}"
        meta_description = category.get("description") or f"Купить {category.get('name')} по ГОСТ. В наличии на складе. Счет за 1 час. Доставка по России. {get_site_context()['name']}"
        breadcrumbs = [
            {"name": "Главная", "url": "/"},
            {"name": "Каталог", "url": "/catalog/"},
            {"name": category.get("name"), "url": None}
        ]
        context = {
            "request": request,
            "category": category,
            "subcategories": [],
            "products": products,
            "pagination": pagination,
            "breadcrumbs": breadcrumbs,
            "meta": {
                "title": meta_title,
                "description": meta_description,
            },
            "site": get_site_context(),
            "year": datetime.now().year,
        }
    html = template.render(**context)
    return HTMLResponse(content=html)


@app.get("/catalog/{category_slug}/page-{page}/", response_class=HTMLResponse)
async def category_page_paginated(request: Request, category_slug: str, page: int) -> HTMLResponse:
    """Пагинированные страницы категории"""
    return await category_page(request, category_slug, page)


# === SITEMAP ENDPOINTS ===

@app.get("/sitemap.xml", response_class=Response)
async def sitemap_index() -> Response:
    """Главный sitemap index"""
    base_url = os.getenv("SITE_BASE_URL", "https://rostferrum.ru")
    
    xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>{base_url}/sitemap-static.xml</loc>
    <lastmod>{datetime.now().strftime('%Y-%m-%d')}</lastmod>
  </sitemap>
  <sitemap>
    <loc>{base_url}/sitemap-categories.xml</loc>
    <lastmod>{datetime.now().strftime('%Y-%m-%d')}</lastmod>
  </sitemap>
  <sitemap>
    <loc>{base_url}/sitemap-products.xml</loc>
    <lastmod>{datetime.now().strftime('%Y-%m-%d')}</lastmod>
  </sitemap>
</sitemapindex>"""
    
    return Response(content=xml_content, media_type="application/xml")


@app.get("/sitemap-static.xml", response_class=Response)
async def sitemap_static() -> Response:
    """Sitemap для статических страниц"""
    base_url = os.getenv("SITE_BASE_URL", "https://rostferrum.ru")
    
    static_urls = ["/", "/about/", "/privacy/", "/terms/"]
    
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""
    
    for url in static_urls:
        xml_content += f"""
  <url>
    <loc>{base_url}{url}</loc>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
  </url>"""
    
    xml_content += "\n</urlset>"
    
    return Response(content=xml_content, media_type="application/xml")


@app.get("/sitemap-categories.xml", response_class=Response)
async def sitemap_categories() -> Response:
    """Sitemap для всех категорий"""
    base_url = os.getenv("SITE_BASE_URL", "https://rostferrum.ru")
    
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""
    
    # Добавляем главную страницу каталога
    xml_content += f"""
  <url>
    <loc>{base_url}/catalog/</loc>
    <changefreq>daily</changefreq>
    <priority>0.9</priority>
  </url>"""
    
    if DB_AVAILABLE:
        try:
            with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    # Все категории
                    cur.execute(
                        "SELECT slug FROM tr_categories ORDER BY slug"
                    )
                    for row in cur.fetchall():
                        slug = row[0]
                        xml_content += f"""
  <url>
    <loc>{base_url}/catalog/{slug}/</loc>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>"""
        except Exception as e:
            logger.error(f"Ошибка при генерации sitemap категорий: {e}")
    
    xml_content += "\n</urlset>"
    
    return Response(content=xml_content, media_type="application/xml")


@app.get("/sitemap-products.xml", response_class=Response)
async def sitemap_products() -> Response:
    """Sitemap для всех товаров"""
    base_url = os.getenv("SITE_BASE_URL", "https://rostferrum.ru")
    
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""
    
    if DB_AVAILABLE:
        try:
            with psycopg.connect(DATABASE_URL, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    # Все товары
                    cur.execute(
                        "SELECT slug FROM tr_products_raw ORDER BY slug"
                    )
                    for row in cur.fetchall():
                        slug = row[0]
                        xml_content += f"""
  <url>
    <loc>{base_url}/product/{slug}/</loc>
    <changefreq>weekly</changefreq>
    <priority>0.6</priority>
  </url>"""
        except Exception as e:
            logger.error(f"Ошибка при генерации sitemap товаров: {e}")
    
    xml_content += "\n</urlset>"
    
    return Response(content=xml_content, media_type="application/xml")


# Обслуживаем собранную статику из dist/
app.mount("/", StaticFiles(directory=str(PROJECT_ROOT / "dist"), html=True), name="static")
