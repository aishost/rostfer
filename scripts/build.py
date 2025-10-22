import os
import shutil
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys
import time
import argparse

from jinja2 import Environment, FileSystemLoader, select_autoescape, FileSystemBytecodeCache
from dotenv import load_dotenv, find_dotenv
import psycopg
# SQLite support removed - using PostgreSQL only
from urllib.parse import urlparse
from datetime import datetime, timezone
from typing import List, Dict, Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = PROJECT_ROOT / "templates"
DIST_DIR = PROJECT_ROOT / "dist"
ASSETS_DIR = PROJECT_ROOT / "assets"
JINJA_CACHE_DIR = PROJECT_ROOT / ".jinja_cache"
SITEMAP_DIR = DIST_DIR

# Загружаем переменные окружения
load_dotenv()


def get_categories_from_db() -> List[Dict[str, Any]]:
    """Получает категории из БД или возвращает заглушку при ошибке"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL не найден, используем заглушку для категорий")
        return [{"name": "Каталог", "slug": "", "desc": "Перейти в каталог", "image": "/assets/img/no-photo.png"}]
    
    try:
        with psycopg.connect(database_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT name, slug, COALESCE(null, '') as desc, 
                           COALESCE(null, '/assets/img/no-photo.png') as image
                    FROM tr_categories 
                    WHERE parent_id IS NULL
                    ORDER BY name 
                    LIMIT 6
                """)
                categories = []
                for row in cur.fetchall():
                    categories.append({
                        "name": row[0],
                        "slug": row[1] or "",
                        "desc": row[2] or "",
                        "image": row[3] or "/assets/img/no-photo.png"
                    })
                return categories if categories else [{"name": "Каталог", "slug": "", "desc": "Перейти в каталог", "image": "/assets/img/no-photo.png"}]
    except Exception as e:
        print(f"Ошибка при получении категорий из БД: {e}")
        return [{"name": "Каталог", "slug": "", "desc": "Перейти в каталог", "image": "/assets/img/no-photo.png"}]


def ensure_dist_folder() -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    JINJA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Создаём базовые файлы robots/sitemap заглушки если их нет
    (DIST_DIR).mkdir(parents=True, exist_ok=True)


def get_site_base_url() -> str:
    base = os.getenv("SITE_BASE_URL", "https://rostferrum.ru")
    # Удаляем завершающий слэш для корректной конкатенации
    return base[:-1] if base.endswith('/') else base


def iso_date_now() -> str:
    # Возвращаем дату в формате ISO с учётом UTC (timezone-aware)
    return datetime.now(timezone.utc).date().isoformat()


def write_robots_txt(hostname: str) -> None:
    base = get_site_base_url()
    lines = [
        "User-agent: *",
        "Disallow: /api/",
        "Disallow: /*.pdf$",
        "Clean-param: utm_source&utm_medium&utm_campaign&utm_term&utm_content&yclid&gclid&fbclid&from&openstat /",
        f"Host: {hostname}",
        f"Sitemap: {base}/sitemap.xml",
        "",
    ]
    write_if_changed(DIST_DIR / "robots.txt", "\n".join(lines))


def _sitemap_url(loc: str, lastmod: str | None = None) -> str:
    lastmod_xml = f"<lastmod>{lastmod}</lastmod>" if lastmod else ""
    return f"  <url>\n    <loc>{loc}</loc>\n{('    ' + lastmod_xml + '\n') if lastmod_xml else ''}  </url>"


def build_sitemaps(static_urls: list[str], category_urls: list[str], product_entries: list[tuple[str, str | None]]) -> None:
    base = get_site_base_url()
    today = iso_date_now()

    # static sitemap
    static_body = "\n".join([_sitemap_url(base + u, today) for u in static_urls])
    static_xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
        f"{static_body}\n"
        "</urlset>\n"
    )
    write_if_changed(SITEMAP_DIR / "sitemap-static.xml", static_xml)

    # categories sitemap
    cats_body = "\n".join([_sitemap_url(base + u, today) for u in category_urls])
    cats_xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
        f"{cats_body}\n"
        "</urlset>\n"
    )
    write_if_changed(SITEMAP_DIR / "sitemap-categories.xml", cats_xml)

    # products sitemap (с поддержкой image:image, если задано)
    items: list[str] = []
    for loc_path, image_url in product_entries:
        if image_url:
            item = (
                "  <url>\n"
                f"    <loc>{base + loc_path}</loc>\n"
                f"    <lastmod>{today}</lastmod>\n"
                "    <image:image>\n"
                f"      <image:loc>{base + image_url}</image:loc>\n"
                "    </image:image>\n"
                "  </url>"
            )
        else:
            item = _sitemap_url(base + loc_path, today)
        items.append(item)
    prods_xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\" xmlns:image=\"http://www.google.com/schemas/sitemap-image/1.1\">\n"
        + "\n".join(items) + "\n"
        "</urlset>\n"
    )
    write_if_changed(SITEMAP_DIR / "sitemap-products.xml", prods_xml)

    # sitemap index
    idx_items = [
        ("/sitemap-static.xml", today),
        ("/sitemap-categories.xml", today),
        ("/sitemap-products.xml", today),
    ]
    idx_xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
        + "\n".join([f"  <sitemap>\n    <loc>{base}{p}</loc>\n    <lastmod>{lm}</lastmod>\n  </sitemap>" for p, lm in idx_items]) + "\n"
        "</sitemapindex>\n"
    )
    write_if_changed(SITEMAP_DIR / "sitemap_index.xml", idx_xml)


def copy_assets() -> None:
    if not ASSETS_DIR.exists():
        return
    target = DIST_DIR / "assets"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(ASSETS_DIR, target)
    
    # Убеждаемся, что no-photo.png скопирован
    no_photo_src = PROJECT_ROOT / "data" / "uploads" / "no-photo.png"
    no_photo_dst = target / "img" / "no-photo.png"
    no_photo_dst.parent.mkdir(parents=True, exist_ok=True)
    
    if no_photo_src.exists():
        shutil.copyfile(no_photo_src, no_photo_dst)
    else:
        # Создаём минимальный плейсхолдер если исходника нет
        from PIL import Image
        img = Image.new("RGB", (300, 200), color=(240, 240, 240))
        img.save(no_photo_dst, format="PNG")


def ensure_root_favicons() -> None:
    """Кладёт только favicon.svg в корень dist и удаляет устаревшие фавиконки.

    Источник — assets/favicon.svg. Удаляем favicon.ico, favicon-32x32.png, apple-touch-icon.png
    из корня dist, чтобы не плодить альтернативы.
    """
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    # Копируем SVG в корень сайта
    src_svg = ASSETS_DIR / "favicon.svg"
    if src_svg.exists():
        try:
            shutil.copyfile(src_svg, DIST_DIR / "favicon.svg")
        except Exception as e:
            print(f"Не удалось скопировать favicon.svg: {e}")


def build_about(env: Environment) -> None:
    template = env.get_template("about.html")
    
    context = {
        "site": {
            "base_url": os.getenv("SITE_BASE_URL", "http://localhost:8000"),
            "name": "РостФеррум",
            "phone": "+7 (927) 000-00-00",
            "email": "rostferrum@mail.ru",
            "address": "г. Сарапул, ул. Красная площадь, 4",
            "hours": "Пн–Пт 9:00–22:00",
        },
        "company": {
            "full_name": "Общество с ограниченной ответственностью «РостФеррум»",
            "short_name": "ООО «РостФеррум»",
            "inn": "1800040283",
            "kpp": "180001001",
            "ogrn": "1251800011275",
            "legal_address": "427960, г. Сарапул, ул. Красная площадь, 4",
            "postal_address": "427960, г. Сарапул, ул. Красная площадь, 4",
            "director": "Байчурин Азат Ринатович"
        },
        "year": __import__("datetime").datetime.now().year,
    }
    
    html = template.render(**context)
    # Создаём папку about/ с файлом index.html для красивых ЧПУ
    about_dir = DIST_DIR / "about"
    about_dir.mkdir(parents=True, exist_ok=True)
    (about_dir / "index.html").write_text(html, encoding="utf-8-sig")


def build_privacy(env: Environment) -> None:
    template = env.get_template("privacy.html")
    context = {
        "site": get_site_context(),
        "year": __import__("datetime").datetime.now().year,
    }
    html = template.render(**context)
    target_dir = DIST_DIR / "privacy"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "index.html").write_text(html, encoding="utf-8-sig")


def build_terms(env: Environment) -> None:
    template = env.get_template("terms.html")
    context = {
        "site": get_site_context(),
        "year": __import__("datetime").datetime.now().year,
    }
    html = template.render(**context)
    target_dir = DIST_DIR / "terms"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "index.html").write_text(html, encoding="utf-8-sig")


def build_404(env: Environment) -> None:
    template = env.get_template("404.html")
    context = {"site": get_site_context()}
    html = template.render(**context)
    (DIST_DIR / "404.html").write_text(html, encoding="utf-8-sig")


def build_index(env: Environment) -> None:
    template = env.get_template("index.html")

    # Минимальные данные для главной по ТЗ
    context = {
        "site": {
            "base_url": os.getenv("SITE_BASE_URL", "http://localhost:8000"),
            "name": "РостФеррум",
            "phone": "+7 (927) 000-00-00",
            "email": "rostferrum@mail.ru",
            "address": "г. Сарапул, ул. Красная площадь, 4",
            "hours": "Пн–Пт 9:00–22:00",
        },
        "hero": {
            "title": "Металлопрокат с сертификатами и поставкой со склада",
            "subtitle": "Счёт в течение 1 часа, отсрочка до 30 дней, работа через ЭДО",
            "bullets": [
                {"icon": "shield-check", "text": "ГОСТ"},
                {"icon": "package-check", "text": "В наличии"},
                {"icon": "calendar-days", "text": "Отсрочка 30 дней"},
                {"icon": "clock", "text": "Счёт за 1 час"},
                {"icon": "file-check", "text": "ЭДО"},
            ],
        },
        # Получаем категории из БД или используем заглушку
        "categories": get_categories_from_db(),
        "advantages": [
            {"icon": "shield-check", "title": "ГОСТ и сертификаты", "text": "Вся продукция сертифицирована"},
            {"icon": "warehouse", "title": "Складской запас", "text": "Отгрузка со склада в день заказа"},
            {"icon": "calendar-days", "title": "Отсрочка 30 дней", "text": "Гибкие условия оплаты"},
            {"icon": "clock", "title": "Счёт за 1 час", "text": "Менеджеры готовят предложение быстро"},
            {"icon": "file-check", "title": "ЭДО", "text": "Полный цикл электронного документооборота"},
        ],
        "process": [
            {"step": 1, "title": "Заявка или звонок"},
            {"step": 2, "title": "Счёт за 1 час"},
            {"step": 3, "title": "Подписание через ЭДО"},
            {"step": 4, "title": "Оплата или отсрочка"},
            {"step": 5, "title": "Отгрузка со склада"},
        ],
        "faq": [
            {"q": "Как получить сертификаты?", "a": "Мы предоставляем копии сертификатов на продукцию по запросу."},
            {"q": "Как работает отсрочка?", "a": "Даем отсрочку до 30 дней для проверенных клиентов по договору."},
            {"q": "Подключение ЭДО?", "a": "Работаем с основными операторами ЭДО, подключаемся оперативно."},
            {"q": "Сроки отгрузки?", "a": "Как правило, в день оплаты со склада при наличии."},
        ],
        "year": __import__("datetime").datetime.now().year,
    }

    html = template.render(**context)
    # Главная как /index.html в корне
    write_if_changed(DIST_DIR / "index.html", html)
    # Пишем lastmod для главной


def write_if_changed(target_file: Path, content: str) -> bool:
    """Пишет файл, только если содержание изменилось. Возвращает True, если записано."""
    target_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        if target_file.exists():
            existing = target_file.read_text(encoding="utf-8-sig")
            if existing == content:
                return False
    except Exception:
        # На любых проблемах читаем/сравнения — перезаписываем
        pass
    target_file.write_text(content, encoding="utf-8-sig")
    return True


def render_to_dir(dir_path: Path, html: str) -> None:
    """Helper: пишет HTML как index.html в указанную директорию (write-if-changed)."""
    write_if_changed(dir_path / "index.html", html)


def create_env() -> Environment:
    """Создаёт Jinja окружение с bytecode cache."""
    disable_bcc = os.getenv("ROSTFERRUM_DISABLE_BCC") == "1"
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
        auto_reload=True,
    )
    if not disable_bcc:
        env.bytecode_cache = FileSystemBytecodeCache(directory=str(JINJA_CACHE_DIR))
    return env
def get_site_context() -> dict:
    return {
        "base_url": os.getenv("SITE_BASE_URL", "http://localhost:8000"),
        "name": "РостФеррум",
        "phone": "+7 (927) 000-00-00",
        "email": "rostferrum@mail.ru",
        "address": "г. Сарапул, ул. Красная площадь, 4",
        "hours": "Пн–Пт 9:00–22:00",
    }



def _worker_render_job(job: tuple[str, dict, str]) -> str:
    """Рендерит шаблон в подпроцессе и пишет index.html. Возвращает путь к записанному файлу."""
    template_name, context, out_dir = job
    try:
        env = create_env()
        template = env.get_template(template_name)
        html = template.render(**context)
        target = Path(out_dir) / "index.html"
        write_if_changed(target, html)
        return str(target)
    except Exception as e:
        # Явный лог ошибки конкретного задания, чтобы не терять исключения воркеров
        sys.stderr.write(f"\nRender error for {template_name} -> {out_dir}: {e}\n")
        return str(Path(out_dir) / "index.html")


def _print_progress(done: int, total: int, prefix: str = "Rendering") -> None:
    total = max(total, 1)
    width = 40
    ratio = min(max(done / total, 0.0), 1.0)
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    percent = int(ratio * 100)
    sys.stdout.write(f"\r{prefix} [{bar}] {percent}% ({done}/{total})")
    sys.stdout.flush()
    if done >= total:
        sys.stdout.write("\n")


def _category_context(env: Environment, category: dict, subcategories: list, products: list, pagination: dict | None) -> dict:
    breadcrumbs = [
        {"name": "Главная", "url": "/"},
        {"name": category.get("name"), "url": None},
    ]
    breadcrumbs_jsonld = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Главная", "item": "/"},
            {"@type": "ListItem", "position": 2, "name": category.get("name"), "item": f"/catalog/{category.get('slug')}/"},
        ],
    }
    # Определяем canonical с учётом пагинации
    base_url = f"/catalog/{category.get('slug')}/"
    current_page = (pagination or {}).get("current_page", 1)
    canonical_url = base_url if current_page == 1 else f"{base_url}page-{current_page}/"
    return {
        "site": get_site_context(),
        "category": category,
        "subcategories": subcategories,
        "products": products,
        "pagination": pagination,
        "breadcrumbs": breadcrumbs,
        "breadcrumbs_jsonld": breadcrumbs_jsonld,
        "canonical_url": canonical_url,
        "meta": {
            "title": f"{category.get('name')} — РостФеррум",
            "description": category.get("seo_description"),
        },
        "og": {
            "title": category.get("seo_title") or category.get("name"),
            "description": category.get("seo_description") or "",
            "image": category.get("image_url") or "/assets/img/placeholders/category.webp",
            "url": f"/catalog/{category.get('slug')}/",
        },
        "jsonld": None,
    }


def _product_context(env: Environment, product: dict, category: dict | None, related: list | None) -> dict:
    breadcrumbs = [{"name": "Главная", "url": "/"}]
    if category:
        breadcrumbs.append({"name": category.get("name"), "url": f"/catalog/{category.get('slug')}/"})
    breadcrumbs.append({"name": product.get("name"), "url": None})

    breadcrumbs_jsonld = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Главная", "item": "/"},
        ] + (
            [{"@type": "ListItem", "position": 2, "name": category.get("name"), "item": f"/catalog/{category.get('slug')}/"}] if category else []
        ) + [
            {"@type": "ListItem", "position": 3 if category else 2, "name": product.get("name"), "item": f"/product/{product.get('slug')}/"},
        ],
    }
    jsonld = {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": product.get("name"),
        "sku": product.get("sku"),
        "image": [product.get("image_url") or "/assets/img/placeholders/product.webp"],
        "offers": {
            "@type": "Offer",
            "price": product.get("price"),
            "priceCurrency": product.get("currency") or "RUB",
            "availability": "https://schema.org/InStock" if product.get("in_stock") else "https://schema.org/OutOfStock",
            "url": f"/product/{product.get('slug')}/",
        },
    }
    return {
        "site": get_site_context(),
        "product": product,
        "related": related or [],
        "breadcrumbs": breadcrumbs,
        "breadcrumbs_jsonld": breadcrumbs_jsonld,
        "canonical_url": f"/product/{product.get('slug')}/",
        "meta": {
            "title": product.get("meta_title") or f"{product.get('name')} — РостФеррум",
            "description": product.get("meta_description") or (product.get("short_desc") or ""),
        },
        "og": {
            "title": product.get("meta_title") or product.get("name"),
            "description": product.get("meta_description") or (product.get("short_desc") or ""),
            "image": product.get("image_url") or "/assets/img/placeholders/product.webp",
            "url": f"/product/{product.get('slug')}/",
        },
        "jsonld": jsonld,
    }


# Удалено - каталог теперь через FastAPI
def build_catalog_index_legacy(env: Environment, categories: list, page: int = 1, per_page: int = 24) -> None:
    template = env.get_template("catalog.html")
    breadcrumbs = [
        {"name": "Главная", "url": "/"},
        {"name": "Каталог", "url": None},
    ]
    breadcrumbs_jsonld = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Главная", "item": "/"},
            {"@type": "ListItem", "position": 2, "name": "Каталог", "item": "/catalog/"},
        ],
    }
    # Пагинация
    total_items = len(categories)
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    current_page = max(1, min(page, total_pages))
    start = (current_page - 1) * per_page
    end = start + per_page
    page_items = categories[start:end]

    pagination = None
    if total_pages > 1:
        base_url = "/catalog/"
        prev_url = f"{base_url}page-{current_page-1}/" if current_page > 1 else None
        next_url = f"{base_url}page-{current_page+1}/" if current_page < total_pages else None
        pagination = {
            "total_pages": total_pages,
            "current_page": current_page,
            "prev_url": prev_url,
            "next_url": next_url,
        }

    canonical = "/catalog/" if current_page == 1 else f"/catalog/page-{current_page}/"
    context = {
        "site": get_site_context(),
        "categories": page_items,
        "breadcrumbs": breadcrumbs,
        "breadcrumbs_jsonld": breadcrumbs_jsonld,
        "pagination": pagination,
        "canonical_url": canonical,
    }
    html = template.render(**context)
    target_dir = DIST_DIR / "catalog"
    if current_page == 1:
        render_to_dir(target_dir, html)
    else:
        render_to_dir(target_dir / f"page-{current_page}", html)


# Удалено - категории теперь через FastAPI  
def build_category_legacy(
    env: Environment,
    category: dict,
    subcategories: list,
    products: list,
    pagination: dict | None = None,
    page: int | None = None,
) -> None:
    template = env.get_template("category.html")
    breadcrumbs = [
        {"name": "Главная", "url": "/"},
        {"name": category.get("name"), "url": None},
    ]
    breadcrumbs_jsonld = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Главная", "item": "/"},
            {"@type": "ListItem", "position": 2, "name": category.get("name"), "item": f"/catalog/{category.get('slug')}/"},
        ],
    }
    context = {
        "site": get_site_context(),
        "category": category,
        "subcategories": subcategories,
        "products": products,
        "pagination": pagination,
        "breadcrumbs": breadcrumbs,
        "breadcrumbs_jsonld": breadcrumbs_jsonld,
        "canonical_url": f"/catalog/{category.get('slug')}/" if not pagination or pagination.get("current_page") == 1 else None,
        "meta": {
            "title": f"{category.get('name')} — РостФеррум",
            "description": category.get("seo_description"),
        },
        "og": {
            "title": category.get("seo_title") or category.get("name"),
            "description": category.get("seo_description") or "",
            "image": category.get("image_url") or "/assets/img/placeholders/category.webp",
            "url": f"/catalog/{category.get('slug')}/",
        },
        "jsonld": None,
    }
    html = template.render(**context)
    base_dir = DIST_DIR / "catalog" / category.get("slug")
    if page is None or page == 1:
        render_to_dir(base_dir, html)
    else:
        render_to_dir(base_dir / f"page-{page}", html)


# Удалено - товары теперь через FastAPI
def build_product_legacy(env: Environment, product: dict, category: dict | None = None, related: list | None = None) -> None:
    template = env.get_template("product.html")
    breadcrumbs = [
        {"name": "Главная", "url": "/"},
    ]
    if category:
        breadcrumbs.append({"name": category.get("name"), "url": f"/catalog/{category.get('slug')}/"})
    breadcrumbs.append({"name": product.get("name"), "url": None})

    breadcrumbs_jsonld = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Главная", "item": "/"},
        ] + (
            [{"@type": "ListItem", "position": 2, "name": category.get("name"), "item": f"/catalog/{category.get('slug')}/"}] if category else []
        ) + [
            {"@type": "ListItem", "position": 3 if category else 2, "name": product.get("name"), "item": f"/product/{product.get('slug')}/"},
        ],
    }
    jsonld = {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": product.get("name"),
        "sku": product.get("sku"),
        "image": [product.get("image_url") or "/assets/img/placeholders/product.webp"],
        "offers": {
            "@type": "Offer",
            "price": product.get("price"),
            "priceCurrency": product.get("currency") or "RUB",
            "availability": "https://schema.org/InStock" if product.get("in_stock") else "https://schema.org/OutOfStock",
            "url": f"/product/{product.get('slug')}/",
        },
    }
    context = {
        "site": get_site_context(),
        "product": product,
        "related": related or [],
        "breadcrumbs": breadcrumbs,
        "breadcrumbs_jsonld": breadcrumbs_jsonld,
        "canonical_url": f"/product/{product.get('slug')}/",
        "meta": {
            "title": product.get("meta_title") or f"{product.get('name')} — РостФеррум",
            "description": product.get("meta_description") or (product.get("short_desc") or ""),
        },
        "og": {
            "title": product.get("meta_title") or product.get("name"),
            "description": product.get("meta_description") or (product.get("short_desc") or ""),
            "image": product.get("image_url") or "/assets/img/placeholders/product.webp",
            "url": f"/product/{product.get('slug')}/",
        },
        "jsonld": jsonld,
    }
    html = template.render(**context)
    target_dir = DIST_DIR / "product" / product.get("slug")
    render_to_dir(target_dir, html)


def build_basic_sitemaps() -> None:
    """Генерирует только robots.txt - sitemap'ы теперь динамические через FastAPI"""
    base = get_site_base_url()
    hostname = urlparse(base).netloc or "rostferrum.ru"
    write_robots_txt(hostname)
    
    # Sitemap'ы больше не генерируются статически
    # Они доступны через FastAPI endpoints:
    # /sitemap.xml - главный index
    # /sitemap-static.xml - статические страницы  
    # /sitemap-categories.xml - все категории
    # /sitemap-products.xml - все товары


def main() -> None:
    parser = argparse.ArgumentParser(description="RostFerrum static site builder")
    parser.add_argument("--force-rebuild", action="store_true", help="Полная пересборка: очистить .jinja_cache и dist/catalog, dist/product")
    parser.add_argument("--no-bcc", action="store_true", help="Отключить Jinja bytecode cache (перекомпилировать шаблоны)")
    parser.add_argument("--serial", action="store_true", help="Рендерить без пула процессов (последовательно), для отладки/Windows")
    args = parser.parse_args()

    if args.no_bcc:
        os.environ["ROSTFERRUM_DISABLE_BCC"] = "1"
    if args.serial:
        os.environ["ROSTFERRUM_SERIAL"] = "1"

    if args.force_rebuild:
        # Чистим кэш шаблонов и релевантные директории в dist
        try:
            if JINJA_CACHE_DIR.exists():
                shutil.rmtree(JINJA_CACHE_DIR)
        except Exception as e:
            print(f"Не удалось удалить .jinja_cache: {e}")
        for p in [DIST_DIR / "catalog"]:
            try:
                if p.exists():
                    shutil.rmtree(p)
            except Exception as e:
                print(f"Не удалось очистить {p}: {e}")

    ensure_dist_folder()
    env = create_env()
    build_index(env)
    build_about(env)
    # Страницы политики/соглашения/404 (если шаблоны присутствуют)
    try:
        build_privacy(env)
    except Exception:
        pass
    try:
        build_terms(env)
    except Exception:
        pass
    try:
        build_404(env)
    except Exception:
        pass
    # Каталог теперь полностью обрабатывается через FastAPI
    # Генерируем только базовые robots.txt и sitemap index
    build_basic_sitemaps()
    copy_assets()
    # Обеспечиваем наличие фавиконок в корне сайта
    ensure_root_favicons()
    print("Build complete →", DIST_DIR)


if __name__ == "__main__":
    main()


