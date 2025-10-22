# pip install psycopg requests

import re
import time
import requests
import asyncio
import random
from html.parser import HTMLParser
from urllib.parse import urljoin
import httpx
from psycopg import connect
from dotenv import load_dotenv
import os
import psycopg
import csv
import json
 

# ---------- Настройки ----------
BASE_URL = "https://truboproduct.ru"  
HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
}
SLEEP_BETWEEN = 2.0       # сек между запросами (увеличено для обхода антибот защиты)
BATCH_SIZE = 500
# --------------------------------

# Асинхронные настройки
MAX_CONCURRENT_CATEGORIES = 60
MAX_CONCURRENT_PAGES_PER_CATEGORY = 10
REQUEST_TIMEOUT_SECONDS = 25.0
HTTPX_LIMITS = httpx.Limits(max_connections=200, max_keepalive_connections=100)
ASYNC_BASE_DELAY = 0.25

PRICE_RE = re.compile(r"(\d[\d\s\u00A0]*)(?:[.,](\d{2}))?")

def parse_price(text: str):
    if not text:
        return None
    t = text.replace('\u00A0', ' ')
    m = PRICE_RE.search(t)
    if not m:
        return None
    whole = m.group(1).replace(' ', '')
    cents = m.group(2) or "00"
    try:
        return float(f"{whole}.{cents}")
    except Exception:
        return None


class ListingParser(HTMLParser):
    """
    Парсит карточки из HTML листинга:
      <li class="listing-cards__item" data-product-id="..."> ... цена ... </li>
    Сохраняет [{'product_id':..., 'name':..., 'price':...}, ...]
    """
    def __init__(self):
        super().__init__()
        self.items = []
        self._in_item = False
        self._depth = 0
        self._cur = None
        self._text_buf = []
        self._in_price_block = False
        self._price_text_buf = []
        # Флаги для парсинга характеристик
        self._in_characs_list = False
        self._characs_depth = 0
        self._in_char_li = False
        self._char_li_buf = []
        # Флаги и буфер для краткого имени из ссылки
        self._in_name_link = False
        self._name_link_buf = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "li":
            cls = attrs_dict.get("class", "") or ""
            data_pid = (attrs_dict.get("data-product-id")
                        or attrs_dict.get("data-id")
                        or attrs_dict.get("data-productid"))
            if ("listing-cards__item" in cls) and data_pid:
                # Завершаем предыдущий товар если он был
                if self._in_item and self._cur:
                    self._finalize_item()
                    
                self._in_item = True
                self._depth = 1
                # Новая карточка с дополнительными полями
                self._cur = {
                    "product_id": str(data_pid).strip(),
                    "name": "",
                    "price": None,
                    "product_name": (attrs_dict.get("data-product-name") or "").strip(),
                }
                self._text_buf = []
                self._in_price_block = False
                self._price_text_buf = []
                return

        if self._in_item:
            self._depth += 1
            
            # Проверяем meta теги для имени товара
            if tag == "meta":
                itemprop = attrs_dict.get("itemprop")
                content = attrs_dict.get("content")
                if itemprop == "name" and content:
                    self._cur["name"] = content.strip()
            
            # Отслеживаем блоки с ценами
            cls = (attrs_dict.get("class") or "")
            if isinstance(cls, list):
                cls = " ".join(cls)
            if "price" in cls.lower():
                self._in_price_block = True

            # Захватываем первую картинку внутри карточки
            if tag == "img" and self._cur is not None:
                if "product_img" not in self._cur or not self._cur.get("product_img"):
                    img_src = (attrs_dict.get("data-src") or
                               attrs_dict.get("src") or
                               attrs_dict.get("data-original") or "").strip()
                    if img_src:
                        try:
                            self._cur["product_img"] = urljoin(BASE_URL, img_src)
                        except Exception:
                            self._cur["product_img"] = img_src

            # Отслеживаем список характеристик товара
            if tag == "ul":
                list_cls = (attrs_dict.get("class") or "")
                if isinstance(list_cls, list):
                    list_cls = " ".join(list_cls)
                if "listing-cards__list" in list_cls:
                    self._in_characs_list = True
                    self._characs_depth = 1
                    if self._cur is not None and "product_characs" not in self._cur:
                        self._cur["product_characs"] = []
                    return

            if getattr(self, "_in_characs_list", False):
                self._characs_depth += 1
                if tag == "li":
                    self._in_char_li = True
                    self._char_li_buf = []

            # Отслеживаем ссылку с кратким именем товара
            if tag == "a" and self._in_item:
                a_cls = (attrs_dict.get("class") or "")
                if isinstance(a_cls, list):
                    a_cls = " ".join(a_cls)
                # Ищем мобильную/основную ссылку карточки
                if "listing-cards__link" in a_cls:
                    self._in_name_link = True
                    self._name_link_buf = []
                    # Извлекаем slug из href
                    href = (attrs_dict.get("href") or "").strip()
                    if href and self._cur is not None and not self._cur.get("slug"):
                        path = href
                        # Если абсолютный URL — берём только path
                        if "://" in path:
                            try:
                                from urllib.parse import urlparse
                                path = urlparse(path).path
                            except Exception:
                                pass
                        s = path.strip("/")
                        if s.startswith("product/"):
                            s = s[len("product/"):]
                        s = s.strip("/")
                        if s:
                            self._cur["slug"] = s

    def handle_endtag(self, tag):
        # Закрытие элемента характеристики
        if getattr(self, "_in_char_li", False) and tag == "li":
            text = " ".join(getattr(self, "_char_li_buf", [])).strip()
            if self._cur is not None and text:
                # Сначала пытаемся разделить по ": " (двоеточие и пробел)
                if ": " in text:
                    ch, val = text.split(": ", 1)
                    ch, val = ch.strip(), val.strip()
                else:
                    # Фолбэк: разделяем по первому встреченному двоеточию/тире с любыми пробелами
                    parts = re.split(r"\s*[:\-–—]\s*", text, maxsplit=1)
                    if len(parts) == 2:
                        ch, val = parts[0].strip(), parts[1].strip()
                    else:
                        # Если разделитель не найден — не теряем данные
                        ch, val = "", text

                # Попытка убрать ведущие тире/маркеры и пробелы в начале у char
                # Если очищение ничего не дало, оставляем исходное значение
                ch_original = ch
                cleaned = re.sub(r"^[\s\u00A0]*[-–—•]+[\s\u00A0]*", "", ch)
                cleaned = cleaned.strip()
                ch = cleaned if cleaned else ch_original
                self._cur.setdefault("product_characs", []).append({"char": ch, "value": val})
            self._in_char_li = False
            self._char_li_buf = []

        # Закрытие списка характеристик
        if getattr(self, "_in_characs_list", False):
            self._characs_depth -= 1
            if self._characs_depth <= 0:
                self._in_characs_list = False

        # Закрытие ссылки с кратким именем
        if self._in_name_link and tag == "a":
            text = " ".join(self._name_link_buf).strip()
            if self._cur is not None and text:
                self._cur["product_name"] = text
            self._in_name_link = False
            self._name_link_buf = []

        if self._in_item:
            self._depth -= 1
            
            # Если выходим из блока цены
            cls = getattr(self, '_last_class', '')
            if "price" in cls.lower():
                self._in_price_block = False
                
            if self._depth == 0:
                # завершаем карточку
                self._finalize_item()

    def _finalize_item(self):
        """Завершение обработки текущего товара"""
        if not self._cur:
            return
            
        # Если имя не найдено в мета-теге, попробуем извлечь из текста
        if not self._cur["name"]:
            full_text = " ".join(self._text_buf).strip()
            for piece in re.split(r"\s{2,}|[|\n\r]", full_text):
                p = piece.strip()
                if p and not PRICE_RE.search(p) and len(p) >= 10:
                    self._cur["name"] = p
                    break

        # Фолбэк для product_name, если нет в data-атрибуте
        if not self._cur.get("product_name"):
            self._cur["product_name"] = self._cur.get("name", "")

        # Извлекаем цену из блоков цен или из всего текста
        price_text = " ".join(self._price_text_buf).strip()
        if not price_text:
            price_text = " ".join(self._text_buf).strip()
        
        price = parse_price(price_text)
        self._cur["price"] = price

        self.items.append(self._cur)
        
        # сбрасываем состояние
        self._in_item = False
        self._cur = None
        self._text_buf = []
        self._in_price_block = False
        self._price_text_buf = []

    def handle_data(self, data):
        if self._in_item and data:
            s = data.strip()
            if not s:
                return
            self._text_buf.append(s)
            
            # Если мы в блоке цены, сохраняем текст отдельно
            if self._in_price_block:
                self._price_text_buf.append(s)

            # Текст в текущем пункте характеристики
            if getattr(self, "_in_char_li", False):
                self._char_li_buf.append(s)

            # Текст внутри ссылки-краткого имени
            if self._in_name_link:
                self._name_link_buf.append(s)

    def close(self):
        # Завершаем последний товар если нужно
        if self._in_item and self._cur:
            self._finalize_item()
        super().close()


def extract_products_from_html(html: str):
    parser = ListingParser()
    parser.feed(html)
    parser.close()  # Важно! Завершаем парсинг
    # print(parser.items)
    # фильтр: нам нужны только те, где есть название и цена
    out = []
    for it in parser.items:
        name = it["name"]
        price = it["price"]
        if name and (price is not None):
            out.append(it)
    return out


def create_session():
    """Создает новую сессию с правильными настройками"""
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Настройки для более стабильной работы
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.packages.urllib3.util.retry.Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504, 404]
        )
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session


def build_proxy_url(addr: str, port: str, username: str | None, password: str | None) -> str:
    addr = (addr or "").strip()
    port = (port or "").strip()
    if username and password:
        return f"http://{username}:{password}@{addr}:{port}"
    return f"http://{addr}:{port}"


def load_proxies_from_db() -> list[str]:
    """Загружает список прокси из БД (только alive=true)."""
    proxies: list[str] = []
    try:
        with open_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT address, port, username, password
                    FROM proxy
                    WHERE alive IS TRUE
                    ORDER BY cnt ASC, id ASC
                    """
                )
                for addr, port, username, password in cur.fetchall():
                    try:
                        proxies.append(build_proxy_url(addr, port, username, password))
                    except Exception:
                        continue
    except Exception:
        pass
    return proxies


class ProxyRotator:
    def __init__(self, proxies: list[str]):
        self._proxies = proxies[:] if proxies else []
        random.shuffle(self._proxies)
        self._idx = 0

    def next(self) -> str | None:
        if not self._proxies:
            return None
        value = self._proxies[self._idx]
        self._idx = (self._idx + 1) % len(self._proxies)
        return value


class AsyncHttpClientPool:
    """Пул клиентов httpx.AsyncClient, по одному на прокси. Без прокси — общий клиент."""
    def __init__(self):
        self._clients: dict[str | None, httpx.AsyncClient] = {}

    async def get_client(self, proxy_url: str | None) -> httpx.AsyncClient:
        key = proxy_url or "__direct__"
        client = self._clients.get(key)
        if client is None:
            client = httpx.AsyncClient(
                http2=True,
                limits=HTTPX_LIMITS,
                timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
                proxies=proxy_url,
                headers=HEADERS,
            )
            self._clients[key] = client
        return client

    async def aclose(self):
        for client in self._clients.values():
            try:
                await client.aclose()
            except Exception:
                pass


async def async_fetch_listing(client: httpx.AsyncClient, slug: str, page: int) -> str | None:
    url = urljoin(BASE_URL, f"/catalog/{slug}/page__{page}/")
    backoff = ASYNC_BASE_DELAY
    for attempt in range(4):
        try:
            r = await client.get(url)
            if r.status_code == 200 and r.text:
                return r.text
            if r.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
                continue
            return None
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError, httpx.ConnectError):
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8.0)
            continue
        except Exception:
            return None
    return None


async def async_get_total_pages(client: httpx.AsyncClient, slug: str) -> int:
    html = await async_fetch_listing(client, slug, 1)
    return get_total_pages(html) if html else 0


async def process_page(slug: str, page: int, client_pool: AsyncHttpClientPool, proxy_rotator: ProxyRotator,
                       category_id: int, category_slug: str, progress: dict, progress_lock: asyncio.Lock):
    proxy_url = proxy_rotator.next()
    client = await client_pool.get_client(proxy_url)
    html = await async_fetch_listing(client, slug, page)
    if not html:
        return False
    items = extract_products_from_html(html)
    if not items:
        return False
    # Подготовка batch
    batch = []
    for prod in items:
        product_id_val = prod.get("product_id")
        try:
            product_id_val = int(product_id_val)
        except Exception:
            pass
        batch.append({
            "name": prod.get("name"),
            "price": prod.get("price"),
            "category_id": category_id,
            "category_slug": category_slug,
            "product_id": product_id_val,
            "product_name": prod.get("product_name"),
            "product_img": prod.get("product_img"),
            "product_characs": json.dumps(prod.get("product_characs") or [], ensure_ascii=False),
            "slug": prod.get("slug"),
        })

    # Запись в БД без блокировки event loop
    await asyncio.to_thread(upsert_products, batch)

    # Обновляем прогресс
    async with progress_lock:
        category_pages = progress.get("category_pages", {})
        category_pages[str(category_id)] = page + 1
        progress["category_pages"] = category_pages
        save_progress(progress)
    return True


async def process_category_async(category_id: int, slug: str, client_pool: AsyncHttpClientPool,
                                 proxy_rotator: ProxyRotator, progress: dict, progress_lock: asyncio.Lock,
                                 pages_concurrency: int = MAX_CONCURRENT_PAGES_PER_CATEGORY):
    # Начальная страница
    start_page = int(progress.get("category_pages", {}).get(str(category_id), 1))
    # Определяем общее число страниц
    client = await client_pool.get_client(proxy_rotator.next())
    total_pages = await async_get_total_pages(client, slug)
    if total_pages <= 0:
        return

    # Планируем страницы [start_page..total_pages]
    semaphore = asyncio.Semaphore(pages_concurrency)

    async def worker(page_number: int):
        async with semaphore:
            ok = await process_page(slug, page_number, client_pool, proxy_rotator, category_id, slug, progress, progress_lock)
            return ok

    tasks = [asyncio.create_task(worker(p)) for p in range(start_page, total_pages + 1)]
    for fut in asyncio.as_completed(tasks):
        try:
            await fut
        except Exception:
            # Ошибка страницы не должна валить категорию целиком
            continue

    # Категория завершена
    async with progress_lock:
        completed = set(progress.get("completed_categories", []))
        completed.add(category_id)
        progress["completed_categories"] = sorted(list(completed))
        cp = progress.get("category_pages", {})
        if str(category_id) in cp:
            del cp[str(category_id)]
        progress["category_pages"] = cp
        save_progress(progress)


async def parse_all_products_async():
    # Получаем список категорий
    with open_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""select c.id, c.slug
                            FROM tr_categories c
                            where c.is_leaf is true
                            and c.id in (select distinct category_id from tr_products_raw where slug is null)
                            order by 1""")
            categories = cur.fetchall()

    progress = load_progress()
    completed = set(progress.get("completed_categories", []))
    categories = [(cid, slug) for cid, slug in categories if cid not in completed]
    if not categories:
        print("Нет категорий для обработки")
        return

    proxies = load_proxies_from_db()
    proxy_rotator = ProxyRotator(proxies)
    client_pool = AsyncHttpClientPool()
    progress_lock = asyncio.Lock()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CATEGORIES)

    async def cat_worker(category_id: int, slug: str):
        async with semaphore:
            print(f"[async category {category_id}] {BASE_URL}/catalog/{slug}/")
            try:
                await process_category_async(category_id, slug, client_pool, proxy_rotator, progress, progress_lock)
                print(f"✓ Категория {slug} обработана (async)")
            except Exception as e:
                print(f"✗ Ошибка (async) категории {slug}: {e}")

    tasks = [asyncio.create_task(cat_worker(cid, slug)) for cid, slug in categories]
    await asyncio.gather(*tasks, return_exceptions=True)
    await client_pool.aclose()


def get_listing(session: requests.Session, slug: str, page: int) -> tuple[str | None, bool]:
    """
    Возвращает (html, should_recreate_session)
    should_recreate_session = True если нужно пересоздать сессию
    """
    url = urljoin(BASE_URL, f"/catalog/{slug}/page__{page}/")
    
    # Retry логика для обработки сетевых ошибок
    max_retries = 3
    should_recreate = False
    
    for attempt in range(max_retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            # print(r.url, r.status_code)
            if r.ok and r.text:
                return r.text, False
            elif r.status_code == 429:  # Too Many Requests
                print(f"Rate limit hit, waiting longer...")
                time.sleep(10)
                should_recreate = True
                continue
            else:
                print(f"HTTP {r.status_code} for {url}")
                return None, False
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout,
                requests.exceptions.RequestException) as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
            should_recreate = True
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Увеличиваем паузу с каждой попыткой
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"All attempts failed for {url}")
                return None, True
    
    return None, should_recreate


def get_total_pages(html: str) -> int:
    """Возвращает общее число страниц, извлекая максимальный номер из ссылок вида page__N.
    Если пагинация не найдена, возвращает 1.
    """
    if not html:
        return 0
    try:
        numbers = re.findall(r"page__\s*(\d+)", html)
        pages = [int(n) for n in numbers]
        return max(pages) if pages else 1
    except Exception:
        return 1


def count_products_on_page(html: str) -> int:
    """Считает количество карточек товаров на странице.
    Использует ListingParser и возвращает число найденных элементов,
    независимо от наличия цены/имени.
    """
    if not html:
        return 0
    parser = ListingParser()
    parser.feed(html)
    parser.close()
    return len(parser.items)


def get_category_pagination_stats(session: requests.Session, slug: str) -> tuple[int, int]:
    """Возвращает (cnt_pages, cnt_products_last_page) для категории.
    Загружает первую страницу, определяет число страниц. Если одна страница,
    считает товары на ней, иначе загружает последнюю и считает товары там.
    """
    # Первая страница
    html, should_recreate = get_listing(session, slug, 1)
    if should_recreate:
        session.close()
        session = create_session()
        time.sleep(5)
        html, _ = get_listing(session, slug, 1)

    if not html:
        return 0, 0

    cnt_pages = get_total_pages(html)
    if cnt_pages <= 1:
        return 1, count_products_on_page(html)

    # Последняя страница
    last_html, should_recreate_last = get_listing(session, slug, cnt_pages)
    if should_recreate_last:
        session.close()
        session = create_session()
        time.sleep(5)
        last_html, _ = get_listing(session, slug, cnt_pages)

    cnt_last = count_products_on_page(last_html) if last_html else 0
    return cnt_pages, cnt_last


def dump_category_pages_csv():
    """Проходит по всем записям в tr_categories и сохраняет CSV со столбцами:
    id, slug, cnt_pages, cnt_products_last_page в файл data/category_pages.csv
    """
    out_path = os.path.join("data", "category_pages.csv")

    with open_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, slug
                FROM tr_categories
                ORDER BY 1
            """)
            categories = cur.fetchall()

    session = create_session()
    try:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "slug", "cnt_pages", "cnt_products_last_page"])

            for cid, slug in categories:
                slug_str = (slug or "").strip()
                if not slug_str:
                    writer.writerow([cid, slug_str, 0, 0])
                    continue

                print(f"[category {cid}] {BASE_URL}/catalog/{slug_str}/")
                try:
                    cnt_pages, cnt_last = get_category_pagination_stats(session, slug_str)
                except Exception as e:
                    print(f"✗ Ошибка при обработке категории {slug_str}: {e}")
                    cnt_pages, cnt_last = 0, 0

                writer.writerow([cid, slug_str, cnt_pages, cnt_last])
                f.flush()
                time.sleep(1)
    finally:
        session.close()

def scrape_category(slug: str):
    """
    Идём по страницам, пока на следующей нет товаров.
    Автоматически пересоздает сессию при необходимости.
    """
    session = create_session()
    page = 1
    
    while True:
        print(f"category {slug}| page {page}")
        html, should_recreate = get_listing(session, slug, page)
        
        # Пересоздаем сессию если нужно
        if should_recreate:
            print("Пересоздаем сессию из-за сетевых проблем...")
            session.close()
            session = create_session()
            time.sleep(5)  # Дополнительная пауза после пересоздания
        
        # with open(f"data/{slug}_{page}.html", "w", encoding="utf-8") as f:
            # f.write(html)
        if not html:
            break
        items = extract_products_from_html(html)
        # print(items)
        if not items:
            break
        for it in items:
            yield it
        page += 1
        time.sleep(SLEEP_BETWEEN)
    
    session.close()


def open_db():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment variables")
    conn = psycopg.connect(database_url)
    conn.autocommit = True
    return conn

def upsert_products(rows_iterable):
    with open_db() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO tr_products_raw (
                    sku,
                    price,
                    category_id,
                    category_slug,
                    product_id,
                    product_name,
                    product_img,
                    product_characs,
                    slug
                )
                VALUES (
                    %(name)s,
                    %(price)s,
                    %(category_id)s,
                    %(category_slug)s,
                    %(product_id)s,
                    %(product_name)s,
                    %(product_img)s,
                    %(product_characs)s::jsonb,
                    %(slug)s
                )
                ON CONFLICT (sku) DO UPDATE SET
                    price = EXCLUDED.price,
                    category_id = EXCLUDED.category_id,
                    category_slug = EXCLUDED.category_slug,
                    product_id = EXCLUDED.product_id,
                    product_name = EXCLUDED.product_name,
                    product_img = EXCLUDED.product_img,
                    product_characs = EXCLUDED.product_characs,
                    slug = EXCLUDED.slug
                """,
                rows_iterable
            )
        conn.commit()


def parse_all_products():
    with open_db() as conn:
        # 1) берём листовые категории
        with conn.cursor() as cur:
            cur.execute("""select c.id, c.slug
                            FROM tr_categories c
                            where c.is_leaf is true
                            order by 1""")
            categories = cur.fetchall()

    progress = load_progress()
    completed = set(progress.get("completed_categories", []))
    category_pages = progress.get("category_pages", {})

    for cid, slug in categories:
        if cid in completed:
            continue
        print(f"[category {cid}] {BASE_URL}/catalog/{slug}/")
        start_page = int(category_pages.get(str(cid), 1))
        try:
            # Каждая категория — новая сессия
            for page, items in scrape_category_pages(slug, start_page=start_page):
                if not items:
                    break
                batch = []
                for prod in items:
                    product_id_val = prod.get("product_id")
                    try:
                        product_id_val = int(product_id_val)
                    except Exception:
                        pass
                    batch.append({
                        "name": prod.get("name"),
                        "price": prod.get("price"),
                        "category_id": cid,
                        "category_slug": slug,
                        "product_id": product_id_val,
                        "product_name": prod.get("product_name"),
                        "product_img": prod.get("product_img"),
            "product_characs": json.dumps(prod.get("product_characs") or [], ensure_ascii=False),
            "slug": prod.get("slug"),
                    })
                if batch:
                    upsert_products(batch)
                # Успешно записали страницу — обновляем прогресс
                category_pages[str(cid)] = page + 1
                progress["category_pages"] = category_pages
                save_progress(progress)
                time.sleep(SLEEP_BETWEEN)

            # Категория завершена
            completed.add(cid)
            progress["completed_categories"] = sorted(list(completed))
            if str(cid) in category_pages:
                del category_pages[str(cid)]
            progress["category_pages"] = category_pages
            save_progress(progress)
            print(f"✓ Категория {slug} обработана успешно")
            time.sleep(3)

        except Exception as e:
            print(f"✗ Ошибка при обработке категории {slug}: {e}")
            print("Продолжаем со следующей категорией...")
            time.sleep(5)
            continue

    print("Готово.")


def scrape_category_pages(slug: str, start_page: int = 1):
    """Генератор страниц категории: отдаёт (page, items) начиная с start_page."""
    session = create_session()
    page = max(1, int(start_page))
    try:
        while True:
            print(f"category {slug}| page {page}")
            html, should_recreate = get_listing(session, slug, page)
            if should_recreate:
                print("Пересоздаем сессию из-за сетевых проблем...")
                session.close()
                session = create_session()
                time.sleep(5)
            if not html:
                break
            items = extract_products_from_html(html)
            if not items:
                break
            yield page, items
            page += 1
            time.sleep(SLEEP_BETWEEN)
    finally:
        session.close()


def load_progress():
    """Загружает прогресс из файла, либо возвращает дефолтную структуру."""
    path = os.path.join("data", "parse_progress.json")
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {"completed_categories": [], "category_pages": {}}


def save_progress(progress: dict):
    """Сохраняет прогресс в файл."""
    path = os.path.join("data", "parse_progress.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = json.dumps(progress, ensure_ascii=False, indent=2)
    with open(path, "w", encoding="utf-8") as f:
        f.write(tmp)


def preview_first_category(limit: int = 20):
    """Печатает в консоль первые N товаров из первой листовой категории.
    Без записи в БД. Вывод в JSON по строке."""
    with open_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""select c.id, c.slug
                            FROM tr_categories c
                            where c.is_leaf is true
                            order by 1
                            limit 1""")
            row = cur.fetchone()
            if not row:
                print("Нет листовых категорий")
                return
            category_id, slug = row

    count = 0
    for prod in scrape_category(slug):
        out = {
            "product_id": int(prod.get("product_id")) if str(prod.get("product_id", "")).isdigit() else prod.get("product_id"),
            "name": prod.get("name"),
            "price": prod.get("price"),
            "product_name": prod.get("product_name"),
            "product_img": prod.get("product_img"),
            "product_characs": prod.get("product_characs"),
            "category_id": category_id,
            "category_slug": slug,
            "slug": prod.get("slug")
        }
        print(json.dumps(out, ensure_ascii=False))
        count += 1
        if count >= limit:
            break


async def parse_one_category_async(category_id: int):
    """Асинхронный запуск парсинга только для одной категории по id, без прокси."""
    # Находим slug категории
    with open_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, slug
                FROM tr_categories
                WHERE id = %s AND is_leaf IS TRUE
                """,
                (category_id,),
            )
            row = cur.fetchone()
            if not row:
                print("Категория не найдена или не является листовой")
                return
            cid, slug = row

    progress = load_progress()
    proxy_rotator = ProxyRotator([])  # пустой список => без прокси
    client_pool = AsyncHttpClientPool()
    progress_lock = asyncio.Lock()

    print(f"[async single category {cid}] {BASE_URL}/catalog/{slug}/")
    try:
        await process_category_async(cid, slug, client_pool, proxy_rotator, progress, progress_lock)
        print(f"✓ Категория {slug} обработана (async, no proxy)")
    finally:
        await client_pool.aclose()


SELECTED_CATEGORY_IDS: list[int] = [
    5662,687,]


async def parse_selected_categories_async(category_ids: list[int]):
    """Асинхронный запуск парсинга для набора категорий по id, без прокси."""
    if not category_ids:
        print("Список категорий пуст")
        return

    # Получаем id/slug только для листовых из переданного набора
    with open_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, slug
                FROM tr_categories
                WHERE is_leaf IS TRUE AND id = ANY(%s)
                ORDER BY 1
                """,
                (category_ids,),
            )
            categories = cur.fetchall()

    if not categories:
        print("Нет подходящих категорий в БД для обработки")
        return

    progress = load_progress()
    completed = set(progress.get("completed_categories", []))
    categories = [(cid, slug) for cid, slug in categories if cid not in completed]
    if not categories:
        print("Все указанные категории уже отмечены завершёнными")
        return

    proxy_rotator = ProxyRotator([])  # без прокси
    client_pool = AsyncHttpClientPool()
    progress_lock = asyncio.Lock()

    # Последовательно по категориям
    for cid, slug in categories:
        print(f"[async category {cid}] {BASE_URL}/catalog/{slug}/ (no proxy)")
        try:
            await process_category_async(cid, slug, client_pool, proxy_rotator, progress, progress_lock)
            print(f"✓ Категория {slug} обработана (async, no proxy)")
        except Exception as e:
            print(f"✗ Ошибка (async) категории {slug}: {e}")

    await client_pool.aclose()


if __name__ == "__main__":
    # dump_category_pages_csv()
    if SELECTED_CATEGORY_IDS:
        asyncio.run(parse_selected_categories_async(SELECTED_CATEGORY_IDS))
    else:
        # Основной режим: асинхронная запись с прогрессом возобновления и прокси
        asyncio.run(parse_all_products_async())
    # preview_first_category()