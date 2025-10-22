import os
import csv
import re
import shutil
from pathlib import Path
from typing import Optional, Tuple, Dict, List, Iterable
import hashlib

from dotenv import load_dotenv
from PIL import Image
import psycopg
from psycopg import sql


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
INBOX_DIR = DATA_DIR / "inbox"
UPLOADS_DIR = DATA_DIR / "uploads"
ASSETS_DIR = PROJECT_ROOT / "assets"
ASSETS_IMG_DIR = ASSETS_DIR / "img"
ASSETS_CAT_DIR = ASSETS_IMG_DIR / "categories"
ASSETS_PROD_DIR = ASSETS_IMG_DIR / "products"

PLACEHOLDER_SRC = UPLOADS_DIR / "no-photo.png"
PLACEHOLDER_DST = ASSETS_IMG_DIR / "no-photo.png"


def ensure_dirs() -> None:
    ASSETS_CAT_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS_PROD_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS_IMG_DIR.mkdir(parents=True, exist_ok=True)


def ensure_placeholder() -> None:
    ensure_dirs()
    if PLACEHOLDER_SRC.exists():
        shutil.copyfile(PLACEHOLDER_SRC, PLACEHOLDER_DST)
    else:
        # Если исходного плейсхолдера нет, создадим минимальную 1x1 PNG
        img = Image.new("RGB", (1, 1), color=(240, 240, 240))
        img.save(PLACEHOLDER_DST, format="PNG")


# Простейшая slug-генерация с транслитерацией RU→EN и нормализацией
RU_EN_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "i", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "",
    "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}


def translit_ru(text: str) -> str:
    result = []
    for ch in text.lower():
        result.append(RU_EN_MAP.get(ch, ch))
    return "".join(result)


def slugify(text: str) -> str:
    text = translit_ru(text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9\-\s_]", "", text)
    text = re.sub(r"[\s_]+", "-", text).strip("-")
    text = re.sub(r"-+", "-", text)
    return text or "item"


def open_db():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment variables")
    conn = psycopg.connect(database_url)
    conn.autocommit = True
    return conn


def ensure_unique_slug(cur: psycopg.Cursor, table: str, base_slug: str) -> str:
    slug = base_slug
    suffix = 2
    while True:
        cur.execute(f"select 1 from {table} where slug=%s limit 1", (slug,))
        if cur.fetchone() is None:
            return slug
        slug = f"{base_slug}-{suffix}"
        suffix += 1


def save_image_as_webp(src_path: Path, dst_path: Path, quality: int = 85) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    # Пропускаем перекодирование, если целевой файл не старее исходника
    try:
        if dst_path.exists() and dst_path.stat().st_mtime >= src_path.stat().st_mtime:
            return
    except Exception:
        pass
    with Image.open(src_path) as im:
        im.convert("RGB").save(dst_path, format="WEBP", quality=quality, method=4)


def process_category_image(category_name: str, slug: str) -> str:
    # Сначала ищем по названию категории, потом по slug
    search_patterns = [
        slugify(category_name),  # по транслиту названия
        category_name.lower().replace(" ", "-"),  # простая замена пробелов
        category_name.lower().replace(" ", "_"),  # с подчёркиваниями
        slug  # по итоговому slug (если кто-то угадал)
    ]
    
    for pattern in search_patterns:
        candidates = list((UPLOADS_DIR / "categories").glob(f"{pattern}.*"))
        if candidates:
            src = candidates[0]
            dst = ASSETS_CAT_DIR / f"{slug}.webp"
            try:
                save_image_as_webp(src, dst)
                return f"/assets/img/categories/{slug}.webp"
            except Exception:
                continue
    
    return "/assets/img/no-photo.png"


# Эта функция больше не используется, заменена на process_product_images
# def process_product_primary_image(sku: str) -> str:
#     sku_dir = UPLOADS_DIR / "products" / sku
#     if sku_dir.exists():
#         for src in sorted(sku_dir.iterdir()):
#             if src.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
#                 dst = ASSETS_PROD_DIR / sku / (src.stem + ".webp")
#                 try:
#                     save_image_as_webp(src, dst)
#                     return f"/assets/img/products/{sku}/{src.stem}.webp"
#                 except Exception:
#                     break
#     return "/assets/img/no-photo.png"


def upsert_category(cur: psycopg.Cursor, name: str, slug: Optional[str]) -> Tuple[int, str]:
    # Сначала проверяем, есть ли уже категория с таким названием
    cur.execute("select id, slug from categories where name=%s limit 1", (name,))
    existing = cur.fetchone()
    
    if existing:
        # Категория уже существует - просто активируем и возвращаем
        cat_id, existing_slug = existing
        cur.execute("update categories set is_active=true where id=%s", (cat_id,))
        return cat_id, existing_slug
    
    # Категории нет - создаём новую
    base_slug = slugify(name) if not slug else slugify(slug)
    unique_slug = ensure_unique_slug(cur, "categories", base_slug)
    img_url = process_category_image(name, unique_slug)
    
    cur.execute(
        """
        insert into categories(name, slug, image_url, is_active)
        values (%s, %s, %s, true)
        returning id, slug
        """,
        (name, unique_slug, img_url),
    )
    cat_id, cat_slug = cur.fetchone()
    
    return cat_id, cat_slug


def upsert_product(
    cur: psycopg.Cursor,
    sku: str,
    name: str,
    category_id: Optional[int],
    price: Optional[float],
    in_stock: bool,
    slug: Optional[str],
) -> Tuple[int, str, str]:
    base_slug = slugify(name) if not slug else slugify(slug)
    
    # Проверяем, изменился ли slug у существующего товара
    cur.execute("select id, slug from products where sku=%s limit 1", (sku,))
    existing = cur.fetchone()
    old_slug = existing[1] if existing else None
    
    unique_slug = ensure_unique_slug(cur, "products", base_slug)
    
    cur.execute(
        """
        insert into products(sku, slug, category_id, name, price, currency, in_stock, is_active)
        values (%s, %s, %s, %s, %s, 'RUB', %s, true)
        on conflict (sku) do update set
          slug=excluded.slug,
          category_id=excluded.category_id,
          name=excluded.name,
          price=excluded.price,
          in_stock=excluded.in_stock,
          is_active=true
        returning id, slug
        """,
        (sku, unique_slug, category_id, name, price, in_stock),
    )
    prod_id, prod_slug = cur.fetchone()
    
    # Создаём редирект если slug изменился
    if old_slug and old_slug != prod_slug:
        create_redirect(cur, "product", prod_id, old_slug, prod_slug)
    
    # Обрабатываем изображения товара
    process_product_images(cur, prod_id, sku)
    
    return prod_id, prod_slug, "/assets/img/no-photo.png"


def create_redirect(cur: psycopg.Cursor, entity_type: str, entity_id: int, old_slug: str, new_slug: str) -> None:
    """Создаёт запись о редиректе при изменении slug"""
    cur.execute(
        """
        insert into redirects(entity_type, entity_id, old_slug, new_slug)
        values (%s, %s, %s, %s)
        on conflict (entity_type, old_slug) do update set
          new_slug=excluded.new_slug,
          entity_id=excluded.entity_id
        """,
        (entity_type, entity_id, old_slug, new_slug)
    )


def process_product_images(cur: psycopg.Cursor, product_id: int, sku: str) -> None:
    """Обрабатывает все изображения для товара и записывает в product_images"""
    # Сначала удаляем старые записи для этого товара
    cur.execute("delete from product_images where product_id=%s", (product_id,))
    
    sku_dir = UPLOADS_DIR / "products" / sku
    if not sku_dir.exists():
        return
    
    image_files = []
    for src in sorted(sku_dir.iterdir()):
        if src.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
            dst = ASSETS_PROD_DIR / sku / (src.stem + ".webp")
            try:
                save_image_as_webp(src, dst)
                image_files.append({
                    "url": f"/assets/img/products/{sku}/{src.stem}.webp",
                    "alt": f"{sku} - изображение {src.stem}"
                })
            except Exception as e:
                print(f"Ошибка обработки изображения {src}: {e}")
    
    # Записываем изображения в БД
    for i, img in enumerate(image_files):
        cur.execute(
            """
            insert into product_images(product_id, url, alt, is_primary, sort_order)
            values (%s, %s, %s, %s, %s)
            """,
            (product_id, img["url"], img["alt"], i == 0, i)
        )


def parse_bool(val: str) -> bool:
    return str(val).strip().lower() in {"1", "true", "yes", "y", "да", "истина"}


def compute_product_hash(
    *,
    category_id: Optional[int],
    sku: str,
    name: str,
    price: Optional[float],
    in_stock: bool,
) -> str:
    """Стабильный хэш содержимого товара без учёта slug и изображений.
    Используется для определения, изменились ли значимые данные товара.
    """
    normalized = [
        str(category_id if category_id is not None else ""),
        sku.strip(),
        name.strip(),
        ("{:.2f}".format(price) if price is not None else ""),
        ("1" if in_stock else "0"),
    ]
    payload = "|".join(normalized)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_unique_product_slug(
    cur: psycopg.Cursor,
    base_slug: str,
    *,
    current_sku: Optional[str] = None,
    reserved_slugs: Optional[set] = None,
) -> str:
    """Возвращает уникальный slug для products с учётом БД и уже зарезервированных в батче.
    Если slug занят другим SKU, добавляет суффиксы -2, -3, ... пока не найдёт свободный.
    """
    if reserved_slugs is None:
        reserved_slugs = set()

    slug_candidate = base_slug
    suffix = 2
    while True:
        # Проверка на конфликты внутри текущего батча
        if slug_candidate in reserved_slugs:
            slug_candidate = f"{base_slug}-{suffix}"
            suffix += 1
            continue
        # Проверка в БД: slug свободен или принадлежит текущему SKU
        cur.execute("select sku from products where slug=%s limit 1", (slug_candidate,))
        row = cur.fetchone()
        if row is None or (current_sku is not None and row[0] == current_sku):
            reserved_slugs.add(slug_candidate)
            return slug_candidate
        slug_candidate = f"{base_slug}-{suffix}"
        suffix += 1


def fetch_existing_products(cur: psycopg.Cursor, skus: Iterable[str]) -> Dict[str, Tuple[int, str, Optional[int], str, Optional[float], bool]]:
    """Загружает существующие продукты по SKU одним запросом.
    Возвращает словарь sku -> (id, slug, category_id, name, price, in_stock)
    """
    result: Dict[str, Tuple[int, str, Optional[int], str, Optional[float], bool]] = {}
    if not skus:
        return result
    placeholders = ','.join(['%s'] * len(list(skus)))
    # Преобразуем в список снова, если итератор
    skus_list = list(skus)
    cur.execute(
        f"""
        select sku, id, slug, category_id, name, price, in_stock
        from products
        where sku in ({placeholders})
        """,
        skus_list,
    )
    for row in cur.fetchall():
        sku, pid, slug, cat_id, name, price, in_stock = row
        result[sku] = (pid, slug, cat_id, name, float(price) if price is not None else None, bool(in_stock))
    return result


def import_csv(path: Path) -> None:
    ensure_dirs()
    ensure_placeholder()
    
    # 0) Читаем CSV полностью
    with path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]
    if not rows:
        print("Empty CSV:", path)
        return

    # Собираем множества
    csv_skus: List[str] = []
    csv_categories: List[str] = []
    for row in rows:
        category_name = (row.get("category") or "").strip()
        sku = (row.get("sku") or "").strip()
        name = (row.get("name") or "").strip()
        if not sku or not name or not category_name:
            continue
        csv_skus.append(sku)
        csv_categories.append(category_name)

    # Убираем дубликаты, сохраняем порядок
    def dedup_keep_order(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for it in items:
            if it not in seen:
                seen.add(it)
                out.append(it)
        return out

    csv_skus = dedup_keep_order(csv_skus)
    csv_categories = dedup_keep_order(csv_categories)

    with open_db() as conn:
        with conn.cursor() as cur:
            # Буст транзакции
            try:
                cur.execute("SET LOCAL synchronous_commit TO OFF")
            except Exception:
                pass

            # 1) Гарантируем наличие категорий (по уникальным именам)
            category_name_to_id: Dict[str, int] = {}
            for cat_name in csv_categories:
                cat_id, _ = upsert_category(cur, cat_name, None)
                category_name_to_id[cat_name] = cat_id

            # 2) Предзагружаем существующие продукты по SKU
            existing_by_sku = fetch_existing_products(cur, csv_skus)

            # 3) Готовим батч upsert для изменившихся/новых
            upsert_values: List[tuple] = []
            slug_changes: Dict[str, str] = {}  # sku -> old_slug
            need_images_for_sku: set[str] = set()
            reserved_slugs: set[str] = set()

            unchanged_ids: List[int] = []

            for row in rows:
                category_name = (row.get("category") or "").strip()
                category_slug = (row.get("category_slug") or "").strip() or None
                sku = (row.get("sku") or "").strip()
                name = (row.get("name") or "").strip()
                price_raw = row.get("price")
                price = float(price_raw) if price_raw not in (None, "") else None
                in_stock = parse_bool(row.get("in_stock") or "true")
                product_slug = (row.get("product_slug") or "").strip() or None

                if not sku or not name or not category_name:
                    continue

                cat_id = category_name_to_id.get(category_name)
                if not cat_id:
                    continue

                desired_base_slug = slugify(product_slug) if product_slug else slugify(name)
                csv_hash = compute_product_hash(
                    category_id=cat_id,
                    sku=sku,
                    name=name,
                    price=price,
                    in_stock=in_stock,
                )

                existing = existing_by_sku.get(sku)
                if existing:
                    db_id, db_slug, db_cat_id, db_name, db_price, db_in_stock = existing
                    db_hash = compute_product_hash(
                        category_id=db_cat_id,
                        sku=sku,
                        name=db_name,
                        price=db_price,
                        in_stock=db_in_stock,
                    )
                    if (csv_hash == db_hash) and (db_slug == desired_base_slug):
                        unchanged_ids.append(db_id)
                        continue
                    # Если базовый slug совпадает с текущим — оставляем, иначе обеспечиваем уникальность
                    if db_slug == desired_base_slug:
                        final_slug = db_slug
                    else:
                        slug_changes[sku] = db_slug
                        final_slug = ensure_unique_product_slug(
                            cur, desired_base_slug, current_sku=sku, reserved_slugs=reserved_slugs
                        )
                else:
                    # Новый продукт
                    final_slug = ensure_unique_product_slug(
                        cur, desired_base_slug, current_sku=sku, reserved_slugs=reserved_slugs
                    )

                # Требуется upsert
                upsert_values.append((sku, final_slug, cat_id, name, price, in_stock))
                need_images_for_sku.add(sku)

            # 4) Массовая реактивация неизменившихся
            if unchanged_ids:
                placeholders = ','.join(['%s'] * len(unchanged_ids))
                cur.execute(
                    f"update products set is_active=true where id in ({placeholders}) and is_active=false",
                    unchanged_ids,
                )

            # 5) Массовый upsert изменённых/новых с возвратом id
            returned: List[tuple] = []
            if upsert_values:
                # Вставляем батчем без execute_values, формируем VALUES вручную безопасно
                values_sql_parts = []
                params: List[object] = []
                for (sku, desired_slug, cat_id, name, price, in_stock) in upsert_values:
                    values_sql_parts.append("(%s,%s,%s,%s,%s,'RUB',%s,true)")
                    params.extend([sku, desired_slug, cat_id, name, price, in_stock])
                values_sql = ",".join(values_sql_parts)
                sql_stmt = (
                    "insert into products (sku, slug, category_id, name, price, currency, in_stock, is_active) "
                    f"values {values_sql} on conflict (sku) do update set "
                    "slug=excluded.slug, category_id=excluded.category_id, name=excluded.name, "
                    "price=excluded.price, in_stock=excluded.in_stock, is_active=true "
                    "returning id, sku, slug"
                )
                cur.execute(sql_stmt, params)
                returned = cur.fetchall()

            # 6) Редиректы и изображения для изменённых/новых
            for pid, sku, new_slug in (returned or []):
                old_slug = slug_changes.get(sku)
                if old_slug and old_slug != new_slug:
                    create_redirect(cur, "product", pid, old_slug, new_slug)
                process_product_images(cur, pid, sku)

            # 7) Деактивируем товары, которых нет в CSV
            if csv_skus:
                placeholders = ','.join(['%s'] * len(csv_skus))
                cur.execute(
                    f"update products set is_active=false where sku not in ({placeholders}) and is_active=true",
                    list(csv_skus)
                )
                deactivated_products = cur.rowcount
                print(f"Деактивировано товаров: {deactivated_products}")

            # 8) Деактивируем категории, которых нет в CSV
            if csv_categories:
                placeholders = ','.join(['%s'] * len(csv_categories))
                cur.execute(
                    f"update categories set is_active=false where name not in ({placeholders}) and is_active=true",
                    list(csv_categories)
                )
                deactivated_categories = cur.rowcount
                print(f"Деактивировано категорий: {deactivated_categories}")


def run() -> None:
    # Обрабатываем все CSV в inbox
    if not INBOX_DIR.exists():
        print("No inbox directory:", INBOX_DIR)
        return
    for csv_file in sorted(INBOX_DIR.glob("*.csv")):
        print("Importing:", csv_file.name)
        import_csv(csv_file)
        # Архивируем/переносим
        archive_dir = DATA_DIR / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(csv_file), archive_dir / csv_file.name)
    print("ETL import complete.")


if __name__ == "__main__":
    run()


