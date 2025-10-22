# build_unique_slugs.py
import os, re, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import psycopg  # psycopg v3  (pip install psycopg[binary])
from dotenv import load_dotenv
load_dotenv()

DSN = os.getenv("DATABASE_URL")
BATCH = 200_000  # сколько строк за раз стримим/копируем
WORK_MEM_MB = int(os.getenv("WORK_MEM_MB", "2048"))  # память сортировок на сессию
UPDATE_BATCH_SIZE = int(os.getenv("UPDATE_BATCH_SIZE", "10"))  # размер батча для UPDATE
DO_INDEX = os.getenv("DO_INDEX", "0") in {"1", "true", "yes", "y"}
PARALLEL_WORKERS = int(os.getenv("PARALLEL_WORKERS", "100"))  # количество параллельных воркеров

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

def run_parallel_updates(*, dsn: str, batch_size: int, work_mem_mb: int) -> int:
    total_updated_lock = threading.Lock()
    total_updated = {"n": 0}

    def worker(worker_idx: int):
        updated_local = 0
        with psycopg.connect(dsn, autocommit=False) as conn_w:
            while True:
                with conn_w.cursor() as curw:
                    # старт транзакции явно
                    curw.execute("BEGIN")
                    curw.execute("SET LOCAL synchronous_commit = off;")
                    curw.execute(f"SET LOCAL work_mem = '{work_mem_mb}MB';")
                    curw.execute(
                        """
                        WITH chunk AS (
                            SELECT ctid, sku, slug
                            FROM public.slug_final
                            ORDER BY sku
                            LIMIT %s
                            FOR UPDATE SKIP LOCKED
                        ), upd AS (
                            UPDATE public.tr_products_raw t
                            SET slug = chunk.slug
                            FROM chunk
                            WHERE t.sku = chunk.sku
                              AND t.slug IS DISTINCT FROM chunk.slug
                            RETURNING t.sku
                        ), del AS (
                            DELETE FROM public.slug_final f
                            USING chunk
                            WHERE f.ctid = chunk.ctid
                            RETURNING f.sku
                        )
                        SELECT (SELECT COUNT(*) FROM upd) AS updated,
                               (SELECT COUNT(*) FROM del) AS deleted;
                        """,
                        (batch_size,),
                    )
                    row = curw.fetchone()
                    updated_count = int(row[0]) if row else 0
                    deleted_count = int(row[1]) if row else 0
                    conn_w.commit()
                    updated_local += updated_count
                    if deleted_count == 0:
                        break
        if updated_local:
            with total_updated_lock:
                total_updated["n"] += updated_local
                log(f"[parallel] worker {worker_idx}: обновлено {updated_local:,}")

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = [executor.submit(worker, i) for i in range(PARALLEL_WORKERS)]
        for _ in as_completed(futures):
            pass
    return total_updated["n"]

def main():
    t0 = time.time()
    log("Старт генерации уникальных slug: подключение к БД и подготовка таблиц...")
    with psycopg.connect(DSN, autocommit=False) as conn:
        with conn.cursor() as cur:
            # 0) подготовка таблиц/столбца
            cur.execute("""
                ALTER TABLE public.tr_products_raw
                    ADD COLUMN IF NOT EXISTS slug text;
            """)
            # stage без WAL — быстрее, если ещё не создана
            cur.execute("""
                CREATE UNLOGGED TABLE IF NOT EXISTS public.slug_stage (
                    sku  text PRIMARY KEY,
                    base text NOT NULL
                );
            """)
            conn.commit()
            log("Таблица stage подготовлена (slug_stage), колонка slug гарантирована")

        # 1) потоково читаем SKU и грузим base-слуги в stage через COPY
        total = 0
        t_read = time.time()
        # Если slug_stage уже заполнена, не перезаполняем
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(1) FROM public.slug_stage;")
            stage_rows = cur.fetchone()[0]
        if stage_rows and stage_rows > 0:
            log(f"Найдено в slug_stage: {stage_rows:,} строк — используем существующие данные")
        else:
            log("Начинаю стриминг SKU в slug_stage через COPY...")
        with conn.cursor(name="sku_stream") as read_cur:
            read_cur.itersize = BATCH
            if not stage_rows:
                read_cur.execute("SELECT sku FROM public.tr_products_raw WHERE sku IS NOT NULL;")
            else:
                read_cur.execute("SELECT NULL WHERE FALSE;")  # пустой стрим

            while True:
                rows = read_cur.fetchmany(BATCH)
                if not rows:
                    break

                with conn.cursor() as copy_cur:
                    # COPY ... FROM STDIN (psycopg3): удобно писать по строкам
                    with copy_cur.copy(
                        "COPY public.slug_stage (sku, base) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', QUOTE E'\\b')"
                    ) as cp:
                        for (sku,) in rows:
                            if sku is None:
                                continue
                            base = slugify(sku)
                            # write_row сам экранирует CSV; используем таб в качестве разделителя
                            cp.write_row([sku, base])

                total += len(rows)
                elapsed = time.time() - t_read
                rps = total / elapsed if elapsed > 0 else 0
                print(f"[stage] staged {total:,} rows, ~{rps:,.0f} rows/s")
        # ВАЖНО: commit после завершения работы server-side курсора, чтобы его не закрывать транзакцией
        conn.commit()
        if not stage_rows:
            log("Стриминг зафиксирован (COMMIT)")
            log(f"Стриминг завершён: всего {total:,} SKU, заняло ~{(time.time()-t_read):.1f}с")

        # 2) индекс для быстрого join
        with conn.cursor() as cur:
            log("Создаю индекс slug_stage(sku) для ускорения join (если его ещё нет)...")
            cur.execute("CREATE INDEX IF NOT EXISTS slug_stage_sku_idx ON public.slug_stage (sku);")
            conn.commit()
            log("Индекс slug_stage(sku) создан/существует")

        # 3) Подготовка итоговых slug в отдельной финальной таблице для батч-обновления
        with conn.cursor() as cur:
            # Настройка памяти сортировок для сессии
            cur.execute(f"SET work_mem = '{WORK_MEM_MB}MB';")
            log(f"work_mem установлена на {WORK_MEM_MB}MB")

            # log("Строю таблицу slug_counts (base -> cnt) заново...")
            # cur.execute("DROP TABLE IF EXISTS public.slug_counts;")
            # cur.execute("""
            #     CREATE UNLOGGED TABLE public.slug_counts AS
            #     SELECT base, COUNT(*) AS cnt
            #     FROM public.slug_stage
            #     GROUP BY base;
            # """)
            # cur.execute("CREATE INDEX ON public.slug_counts (base);")
            # conn.commit()
            # log("slug_counts готова")

            log("Готовлю таблицу slug_final (sku, slug) с возобновлением...")
            cur.execute("""
                CREATE UNLOGGED TABLE IF NOT EXISTS public.slug_final (
                    sku  text PRIMARY KEY,
                    slug text NOT NULL
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS slug_final_sku_idx ON public.slug_final (sku);")
            conn.commit()

            # Уникальные base: вставляем без оконки, только недостающие
            # log("Вставляю уникальные base в slug_final (без оконки)...")
            # cur.execute("""
            #     INSERT INTO public.slug_final (sku, slug)
            #     SELECT s.sku, s.base
            #     FROM public.slug_stage s
            #     JOIN public.slug_counts c USING (base)
            #     WHERE c.cnt = 1
            #       AND NOT EXISTS (
            #             SELECT 1 FROM public.slug_final f WHERE f.sku = s.sku
            #       );
            # """)
            # ins1 = cur.rowcount
            # conn.commit()
            # log(f"Добавлено уникальных: {ins1:,}")

            # Дубликаты: оконка только для cnt>1 и только недостающие
            # log("Вставляю дубликаты base в slug_final (с суффиксами)...")
            # cur.execute("""
            #     INSERT INTO public.slug_final (sku, slug)
            #     SELECT s.sku,
            #            CASE WHEN rn = 1 THEN s.base ELSE s.base || '-' || rn END AS slug
            #     FROM (
            #         SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.base ORDER BY s.sku) AS rn
            #         FROM public.slug_stage s
            #         JOIN public.slug_counts c USING (base)
            #         WHERE c.cnt > 1
            #     ) s
            #     WHERE NOT EXISTS (
            #         SELECT 1 FROM public.slug_final f WHERE f.sku = s.sku
            #     );
            # """)
            # ins2 = cur.rowcount
            # conn.commit()
            # log(f"Добавлено с суффиксами: {ins2:,}")

        # 4) Обновление основной таблицы: параллельное или последовательное (возобновляемое)
        print("[update] updating main table with unique slugs (batch/parallel mode)...")
        t_upd_all = time.time()
        if PARALLEL_WORKERS > 1:
            updated_total = run_parallel_updates(dsn=DSN, batch_size=UPDATE_BATCH_SIZE, work_mem_mb=WORK_MEM_MB)
        else:
            updated_total = 0
            while True:
                with conn.cursor() as cur:
                    # память сортировок для текущей транзакции
                    cur.execute(f"SET LOCAL work_mem = '{WORK_MEM_MB}MB';")
                    # берем батч для обновления
                    cur.execute("""
                        WITH chunk AS (
                            SELECT sku, slug
                            FROM public.slug_final
                            ORDER BY sku
                            LIMIT %s
                        ), upd AS (
                            UPDATE public.tr_products_raw t
                            SET slug = c.slug
                            FROM chunk c
                            WHERE t.sku = c.sku
                              AND t.slug IS DISTINCT FROM c.slug
                            RETURNING t.sku
                        )
                        SELECT * FROM upd;
                    """, (UPDATE_BATCH_SIZE,))
                    updated_rows = cur.fetchall()
                    batch_updated = len(updated_rows)
                    updated_total += batch_updated

                    # Удаляем обработанные из slug_final
                    cur.execute("""
                        WITH chunk AS (
                            SELECT sku
                            FROM public.slug_final
                            ORDER BY sku
                            LIMIT %s
                        )
                        DELETE FROM public.slug_final f
                        USING chunk c
                        WHERE f.sku = c.sku;
                    """, (UPDATE_BATCH_SIZE,))

                    conn.commit()

                # Прогресс и выход
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM public.slug_final;")
                    remain = cur.fetchone()[0]
                elapsed = time.time() - t_upd_all
                speed = (updated_total / elapsed) if elapsed > 0 else 0
                log(f"[batch-update] +{batch_updated:,} (итого {updated_total:,}), осталось {remain:,}, ~{int(speed):,} rows/s")
                if remain == 0:
                    break
        log(f"Обновление завершено за {(time.time()-t_upd_all):.1f}с, обновлено {updated_total:,}")

        # 5) По просьбе — не удаляем slug_stage, оставляем для анализа/повторного запуска
        log("slug_stage оставлена без изменений (не удаляем по просьбе пользователя)")

    # 6) Опционально: уникальный индекс и NOT NULL (может быть долгим)
    if DO_INDEX:
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                # Анализ статистики
                log("VACUUM ANALYZE tr_products_raw...")
                cur.execute("VACUUM ANALYZE public.tr_products_raw;")
                log("VACUUM ANALYZE завершён")

                # Уникальный индекс без простоя записи
                log("Создаю UNIQUE INDEX CONCURRENTLY на tr_products_raw.slug...")
                cur.execute("""
                    CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS tr_products_raw_slug_uidx
                    ON public.tr_products_raw(slug);
                """)
                log("Уникальный индекс создан/проверен")
                # Запрет NULL после заполнения
                log("Устанавливаю NOT NULL для столбца slug...")
                cur.execute("""
                    ALTER TABLE public.tr_products_raw
                    ALTER COLUMN slug SET NOT NULL;
                """)
                log("NOT NULL установлен")

    print(f"Done in {(time.time()-t0):.1f}s")

if __name__ == "__main__":
    main()
