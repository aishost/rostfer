# incremental_slug_fill.py
import os, re, time, csv
import psycopg  # pip install psycopg[binary]
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DATABASE_URL")
CSV_PATH = os.getenv("SLUG_CSV_PATH", os.path.join("data", "slug_offline.csv"))
CSV_PENDING_SUFFIX = os.getenv("SLUG_CSV_PENDING_SUFFIX", ".pending")
UPDATE_COMMIT_EVERY = int(os.getenv("UPDATE_COMMIT_EVERY", "1000"))
LOG_INTERVAL_SEC = float(os.getenv("LOG_INTERVAL_SEC", "2"))
FAST_UPDATE = os.getenv("FAST_UPDATE", "1") in {"1", "true", "yes", "y"}

RU_EN_MAP = {
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e",
    "ж":"zh","з":"z","и":"i","й":"i","к":"k","л":"l","м":"m",
    "н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u",
    "ф":"f","х":"h","ц":"c","ч":"ch","ш":"sh","щ":"sch","ъ":"",
    "ы":"y","ь":"", "э":"e","ю":"yu","я":"ya",
}

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def translit_ru(text: str) -> str:
    return "".join(RU_EN_MAP.get(ch, ch) for ch in text.lower())

def slugify(text: str) -> str:
    text = translit_ru(text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9\-\s_]", "", text)
    text = re.sub(r"[\s_]+", "-", text).strip("-")
    text = re.sub(r"-+", "-", text)
    return text or "item"

def ensure_slug_column(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE public.tr_products_raw
                ADD COLUMN IF NOT EXISTS slug text;
        """)
    conn.commit()

def fetch_existing_slugs(conn: psycopg.Connection) -> set[str]:
    log("Загружаю существующие slug...")
    reserved: set[str] = set()
    with conn.cursor(name="slug_stream") as cur:
        cur.itersize = 100_000
        cur.execute("SELECT slug FROM public.tr_products_raw WHERE slug IS NOT NULL;")
        while True:
            rows = cur.fetchmany(100_000)
            if not rows:
                break
            for (slug_val,) in rows:
                if slug_val:
                    reserved.add(slug_val)
    log(f"Резервов slug: {len(reserved):,}")
    return reserved

def export_slug_csv(conn: psycopg.Connection, csv_path: str) -> int:
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    reserved = fetch_existing_slugs(conn)
    count = 0
    t0 = time.time()
    log("Генерирую slug в Python и пишу CSV...")
    last = t0
    interval_count = 0
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sku", "slug"])  # заголовок
        with conn.cursor(name="sku_null_stream") as cur:
            cur.itersize = 100_000
            cur.execute("SELECT sku FROM public.tr_products_raw WHERE slug IS NULL;")
            while True:
                rows = cur.fetchmany(100_000)
                if not rows:
                    break
                for (sku,) in rows:
                    if not sku:
                        continue
                    base = slugify(sku)
                    attempt = 1
                    while True:
                        candidate = base if attempt == 1 else f"{base}-{attempt}"
                        if candidate not in reserved:
                            reserved.add(candidate)
                            writer.writerow([sku, candidate])
                            count += 1
                            interval_count += 1
                            now = time.time()
                            if now - last >= LOG_INTERVAL_SEC:
                                overall_rps = count / (now - t0) if now > t0 else 0
                                interval_rps = interval_count / (now - last) if now > last else 0
                                log(f"CSV: {count:,} записей, ~{int(overall_rps):,} rows/s (за {int(now-last)}s: {int(interval_rps):,}/s)")
                                last = now
                                interval_count = 0
                            break
                        attempt += 1
                if count and count % 100_000 == 0:
                    dt = time.time() - t0
                    rps = count / dt if dt > 0 else 0
                    log(f"CSV записано {count:,}, ~{int(rps):,} rows/s")
    log(f"CSV готов: {csv_path} (новых slug: {count:,})")
    return count

def apply_updates_from_csv(conn: psycopg.Connection, csv_path: str) -> int:
    if not os.path.exists(csv_path):
        log(f"CSV не найден: {csv_path}")
        return 0
    updated = 0
    t0 = time.time()
    log("Начинаю построчные UPDATE из CSV...")
    # Пишем файл с оставшимися (необновлёнными) строками
    pending_path = csv_path + CSV_PENDING_SUFFIX
    os.makedirs(os.path.dirname(pending_path), exist_ok=True)

    with open(csv_path, "r", encoding="utf-8", newline="") as fin, \
         open(pending_path, "w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames or ["sku", "slug"]
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        batch = 0
        last = t0
        interval_count = 0
        kept = 0

        with conn.cursor() as cur:
            cur.execute("SET LOCAL synchronous_commit = off;")
            for row in reader:
                sku = (row.get("sku") or "").strip()
                slug = (row.get("slug") or "").strip()
                if not sku or not slug:
                    # некорректная строка — оставим в pending на разбор
                    writer.writerow(row)
                    kept += 1
                    continue
                cur.execute(
                    """
                    UPDATE public.tr_products_raw
                    SET slug = %s
                    WHERE sku = %s AND slug IS NULL
                    """,
                    (slug, sku),
                )
                rc = cur.rowcount
                if rc == 1:
                    updated += 1  # успешно обновили — НЕ пишем назад в CSV
                else:
                    # не обновили (нет строки или slug уже установлен) — оставляем в pending
                    writer.writerow(row)
                    kept += 1

                interval_count += 1
                now = time.time()
                if now - last >= LOG_INTERVAL_SEC:
                    overall_rps = updated / (now - t0) if now > t0 else 0
                    interval_rps = interval_count / (now - last) if now > last else 0
                    log(f"UPDATE: ok {updated:,}, kept {kept:,}, ~{int(overall_rps):,} rows/s (за {int(now-last)}s: {int(interval_rps):,}/s)")
                    last = now
                    interval_count = 0

                batch += 1
                if batch >= UPDATE_COMMIT_EVERY:
                    conn.commit()
                    dt = time.time() - t0
                    rps = updated / dt if dt > 0 else 0
                    log(f"UPDATE применено: ok {updated:,}, kept {kept:,}, ~{int(rps):,} rows/s")
                    batch = 0
            if batch:
                conn.commit()

    # Атомарно заменяем оригинальный CSV остатком (только те, что не обновились)
    os.replace(pending_path, csv_path)
    log(f"Готово. Обновлено: {updated:,}")
    return updated

def fast_update_from_csv(conn: psycopg.Connection, csv_path: str) -> tuple[int, int]:
    """Очень быстрый путь: грузим CSV в UNLOGGED staging и обновляем JOIN'ом одной командой.
    Возвращает (updated_count, remaining_count). Генерирует pending CSV с остатком.
    """
    if not os.path.exists(csv_path):
        log(f"CSV не найден: {csv_path}")
        return 0, 0

    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute("SET LOCAL synchronous_commit = off;")
        cur.execute("DROP TABLE IF EXISTS public.slug_csv_stage;")
        cur.execute(
            """
            CREATE UNLOGGED TABLE public.slug_csv_stage (
                sku  text PRIMARY KEY,
                slug text NOT NULL
            );
            """
        )
        conn.commit()

    # COPY CSV -> staging (строго sku,slug; разделитель запятая; заголовок есть)
    log("Загрузка CSV в staging (COPY)...")
    loaded = 0
    with conn.cursor() as cur:
        # Стримим исходный CSV байт-в-байт: заголовок присутствует (sku,slug), разделитель запятая
        copy_sql = "COPY public.slug_csv_stage (sku, slug) FROM STDIN WITH (FORMAT csv, HEADER true)"
        with open(csv_path, "rb") as f:
            with cur.copy(copy_sql) as cp:
                while True:
                    chunk = f.read(4 * 1024 * 1024)
                    if not chunk:
                        break
                    cp.write(chunk)
                    loaded += chunk.count(b"\n")
                    if loaded and loaded % 200_000 == 0:
                        dt = time.time() - t0
                        rps = loaded / dt if dt > 0 else 0
                        log(f"COPY: ~{loaded:,} строк, ~{int(rps):,} rows/s")
        conn.commit()
    log(f"COPY завершён: {loaded:,} строк")

    with conn.cursor() as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS slug_csv_stage_sku_idx ON public.slug_csv_stage (sku);")
        conn.commit()

    # Массовый UPDATE
    log("Выполняю массовый UPDATE с JOIN...")
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH upd AS (
                UPDATE public.tr_products_raw t
                SET slug = s.slug
                FROM public.slug_csv_stage s
                WHERE t.sku = s.sku AND t.slug IS NULL
                RETURNING t.sku
            )
            SELECT COUNT(*) FROM upd;
            """
        )
        updated = int(cur.fetchone()[0])
        conn.commit()
    log(f"UPDATE завершён: обновлено {updated:,}")

    # Генерация pending CSV из оставшихся
    pending_path = csv_path + CSV_PENDING_SUFFIX
    remaining = 0
    with conn.cursor(name="rem_stream") as cur, open(pending_path, "w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["sku", "slug"])  # заголовок
        cur.itersize = 100_000
        cur.execute(
            """
            SELECT s.sku, s.slug
            FROM public.slug_csv_stage s
            LEFT JOIN public.tr_products_raw t ON t.sku = s.sku
            WHERE t.slug IS NULL
            ORDER BY s.sku
            """
        )
        while True:
            rows = cur.fetchmany(100_000)
            if not rows:
                break
            writer.writerows(rows)
            remaining += len(rows)
    os.replace(pending_path, csv_path)
    log(f"Осталось необновлённых: {remaining:,}. CSV обновлён (pending → source)")
    # По желанию: можно удалить staging
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.slug_csv_stage;")
        conn.commit()
    log(f"Готово. Быстрый апдейт: ok {updated:,}, kept {remaining:,}, заняло {(time.time()-t0):.1f}с")
    return updated, remaining

def run() -> None:
    t0 = time.time()
    processed = 0
    with psycopg.connect(DSN, autocommit=False) as conn:
        ensure_slug_column(conn)
        # Шаг 1: генерация CSV
        # cnt = export_slug_csv(conn, CSV_PATH)
        # Шаг 2: применение UPDATE
        if FAST_UPDATE:
            updated, remaining = fast_update_from_csv(conn, CSV_PATH)
            log(f"FAST_UPDATE: ok {updated:,}, kept {remaining:,}")
            processed = updated
            cnt = updated + remaining
        else:
            processed = apply_updates_from_csv(conn, CSV_PATH)
            # если нужно, считайте cnt отдельно из CSV
            cnt = processed
    log(f"Завершено. CSV всего: {cnt:,}, обновлено: {processed:,}. Заняло {(time.time()-t0):.1f}с")

if __name__ == "__main__":
    run()


