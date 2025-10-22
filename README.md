# РостФеррум — статический сайт + API форм + Битрикс24 CRM

Корпоративный сайт компании РостФеррум с интеграцией в CRM Битрикс24.

## 🚀 Быстрый старт

### 1. Установка зависимостей

```bash
# Создание виртуального окружения
python -m venv .venv

# Активация (Windows)
.venv\Scripts\activate

# Активация (Linux/Mac)
source .venv/bin/activate

# Установка пакетов
pip install -r requirements.txt
```

### 2. Настройка конфигурации

Создайте файл `.env` на основе примера:

```bash
# Битрикс24 интеграция
BITRIX24_WEBHOOK_URL=https://rostferrum.bitrix24.ru/rest/1/1q4w7kljo6pwr111
BITRIX24_RESPONSIBLE_USER_ID=1

# База данных (выберите одно)
# SQLite (по умолчанию)
# DATABASE_URL=sqlite:///data/catalog.db
# PostgreSQL
# DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DBNAME

# Настройки API
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=True
```

### 3. Сборка сайта

```bash
# Генерация статических файлов
python scripts/build.py
```

### 4. Запуск сервера

```bash
# Запуск API сервера
uvicorn scripts.api:app --reload --host 0.0.0.0 --port 8000
```

Сайт будет доступен по адресу: http://localhost:8000

## 🔧 Интеграция с Битрикс24

### Настройка

1. **Создайте входящий вебхук в Битрикс24:**
   - Разработчикам → Другое → Входящий вебхук
   - Права: CRM, Диск, Пользователи

2. **Укажите URL вебхука в .env файле**

3. **Протестируйте интеграцию:**
   ```bash
   python scripts/test_bitrix24.py
   ```

Подробная инструкция: `docs/Bitrix24 интеграция.md`

## 📁 Структура проекта

```
rostferrum/
├── templates/          # HTML шаблоны (Jinja2)
├── assets/            # CSS, JS, изображения
├── scripts/           # Python скрипты
│   ├── build.py       # Генератор статики
│   ├── api.py         # FastAPI сервер
│   └── bitrix24_integration.py  # Интеграция CRM
├── dist/              # Собранный сайт
├── data/              # База данных и загрузки
└── docs/              # Документация
```

## 🎯 Функции

- ✅ Статическая генерация сайта
- ✅ Форма обратной связи с валидацией
- ✅ Загрузка документов
- ✅ Интеграция с Битрикс24 CRM
- ✅ Антиспам защита (honeypot, rate limiting)
- ✅ Адаптивный дизайн
- ✅ SEO-оптимизация

## 📖 Документация

- [Техническое задание](docs/ТЗ%20сайт.md)
- [Структура лендинга](docs/Лендинг.md)
- [Прогресс разработки](docs/Прогресс%20разработки.md)
- [Следующие задачи](docs/Следующие%20задачи.md)
- [Интеграция с Битрикс24](docs/Bitrix24%20интеграция.md)

## 🛠 Разработка

### Команды

```bash
# Пересборка при изменениях
python scripts/build.py

# Запуск с автоперезагрузкой
uvicorn scripts.api:app --reload

# Тестирование Битрикс24
python scripts/test_bitrix24.py
```

## ☸️ Прод: деплой на VPS (Ubuntu, без Docker)

### Предпосылки
- На сервере установлены: `nginx`, `python3`, `python3-venv`, `git`, `ufw` (по желанию), PostgreSQL.
- Создан пользователь для деплоя (например, `deploy`) и директория проекта: `/var/www/rostferrum`.
- В PostgreSQL создана БД и пользователь, переменная `DATABASE_URL` прописана в `.env`.

### Первичная подготовка сервера (однократно)
1. Скопируйте systemd unit и запустите сервис:
   ```bash
   sudo mkdir -p /var/www/rostferrum
   sudo cp -r infra/systemd/rostferrum.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable rostferrum.service
   ```
2. Установите Nginx и конфиг:
   ```bash
   sudo apt update && sudo apt install -y nginx python3-venv
   sudo cp infra/nginx.conf /etc/nginx/sites-available/rostferrum.conf
   sudo ln -sf /etc/nginx/sites-available/rostferrum.conf /etc/nginx/sites-enabled/rostferrum.conf
   sudo nginx -t && sudo systemctl reload nginx
   ```
3. TLS (опционально):
   ```bash
   sudo apt install -y certbot python3-certbot-nginx
   sudo certbot --nginx -d example.com -d www.example.com
   ```

### CI/CD (GitHub Actions)
Файл `/.github/workflows/deploy.yml`:
- На push в `main` собирает статику (`dist/`)
- Копирует файлы на сервер через `rsync`
- Ставит зависимости в venv на сервере и делает `reload/restart` systemd

Необходимые Secrets в репозитории:
- `SSH_HOST`, `SSH_USER`, `SSH_KEY` (private key PEM)
- `DEPLOY_PATH` (например, `/var/www/rostferrum`)

### Прод-сервис
Сервис запускается через `gunicorn` c `uvicorn.workers.UvicornWorker`, слушает `127.0.0.1:8000`, а `nginx` проксирует домен на приложение.

### Переменные окружения

| Переменная | Описание | Пример |
|------------|----------|--------|
| `BITRIX24_WEBHOOK_URL` | URL вебхука Битрикс24 | `https://domain.bitrix24.ru/rest/1/code/` |
| `BITRIX24_RESPONSIBLE_USER_ID` | ID ответственного в CRM | `1` |
| `DATABASE_URL` | URL БД (SQLite/PostgreSQL) | `postgresql://user:pass@localhost:5432/rostferrum` |
| `DEBUG` | Режим отладки | `True` |

---

**Версия:** 1.1  
**Дата:** Сентябрь 2025