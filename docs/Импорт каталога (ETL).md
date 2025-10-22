# Импорт каталога (ETL): CSV → PostgreSQL → SSG

Документ описывает подготовку данных каталога, правила изображений и запуск импорта.

## 1) Подготовка
- Переменная БД в `.env`: `DATABASE_URL` (например: `postgresql://user:password@localhost:5432/rostferrum`)
- Установить зависимости: `pip install -r requirements.txt` (нужен Pillow).

## 2) Директории
- Вход CSV: `data/inbox/*.csv`
- Архив: `data/archive/`
- Изображения (вход):
  - Категории: `data/uploads/categories/<название-категории>.(jpg|jpeg|png|webp)` (ищется по разным вариантам названия)
  - Товары: `data/uploads/products/<sku>/*.(jpg|jpeg|png|webp)`
  - Плейсхолдер: `data/uploads/no-photo.png`
- Изображения (выход):
  - Категории: `assets/img/categories/<slug>.webp`
  - Товары: `assets/img/products/<sku>/<name>.webp`
  - Плейсхолдер: `assets/img/no-photo.png`

## 3) Формат CSV (MVP)
Обязательные поля: `category`, `sku`, `name`.
Опциональные: `category_slug`, `price`, `in_stock`, `product_slug`.

Пример:
```
category,category_slug,sku,name,price,in_stock,product_slug
Арматура,,SKU-001,Арматура 12мм A500C,59990,true,
Арматура,armatura,SKU-002,Арматура 10мм A500C,49990,1,armatura-10-a500c
```

## 4) Правила slug
- Транслит RU→EN, `[a-z0-9-]`, схлопывание пробелов в дефис, обрезка по краям.
- Уникализация: `-2`, `-3`, … при коллизиях.
- Источник: категория — `category_slug` или `category`; товар — `product_slug` или `name`.

## 5) Изображения

### Категории  
Поиск изображения по названию категории (несколько вариантов):
1. `data/uploads/categories/арматура.*` → транслит → `armatura.*`
2. `data/uploads/categories/арматура.*` → замена пробелов → `арматура.*` 
3. `data/uploads/categories/арматура_*.*` → с подчёркиваниями
4. `data/uploads/categories/<итоговый-slug>.*` → по финальному slug

Результат: WebP → `assets/img/categories/<slug>.webp`

### Товары
Все файлы в `data/uploads/products/<sku>/` → WebP → `assets/img/products/<sku>/<basename>.webp`  
Первое изображение помечается как основное (`is_primary=true`)

### Fallback
Нет исходника → `/assets/img/no-photo.png`

## 6) Запуск ETL
Windows (PowerShell):
```
.\venv\Scripts\python.exe scripts\etl_catalog.py
```
Linux/macOS:
```
venv/bin/python scripts/etl_catalog.py
```

### Что делает ETL:
1. **Импорт данных**: создание/обновление категорий и товаров в PostgreSQL
2. **Генерация slug**: автоматическая транслитерация и уникализация URL
3. **Обработка изображений**: конвертация всех изображений товаров в WebP, запись в таблицу `product_images`
4. **Создание редиректов**: отслеживание изменений slug и создание записей в таблице `redirects`
5. **Деактивация**: установка `is_active=false` для товаров/категорий, отсутствующих в новом CSV
6. **Архивирование**: перенос обработанных CSV в `data/archive/`

## 7) Сборка сайта
```
.\venv\Scripts\python.exe scripts\build.py
```
URL без `.html` (директории с `index.html`).

## 8) Проверка
- Файлы в `assets/img/categories/` и `assets/img/products/` появились.
- Страницы в `dist/`: `/<category-slug>/`, `/product/<product-slug>/`.
- Плейсхолдер доступен: `/assets/img/no-photo.png`.
- В БД заполнены таблицы: `categories`, `products`, `product_images`, `redirects`.
- Товары/категории отсутствующие в CSV помечены как `is_active=false`.

## 9) Таблицы БД после импорта
- **categories**: категории с slug, изображениями и SEO-полями
- **products**: товары с характеристиками и связью с категориями  
- **product_images**: все изображения товаров с указанием основного
- **redirects**: история изменений slug для 301-редиректов

---
Соответствует `scripts/etl_catalog.py` и `scripts/build.py`.
