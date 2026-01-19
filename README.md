# ComfyUI-TelegramBot

> Управление ComfyUI прямо из Telegram: редактирование workflow, запуск генераций, галерея и Mini App визуализация.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![ComfyUI Compatible](https://img.shields.io/badge/ComfyUI-Compatible-green.svg)](https://github.com/comfyanonymous/ComfyUI)

## Возможности

- Управление workflow: загрузка/сохранение, редактирование нод, шаблоны.
- Запуск и мониторинг генераций: прогресс, превью, очередь.
- Галерея и история запусков.
- Mini App (WebApp) визуализация графа в Telegram.

## Требования

- Python 3.10+
- Установленный ComfyUI (HTTP/WS доступен)
- Токен бота от @BotFather

## Установка

```bash
git clone https://github.com/VitalikDen0/ComfyUI-TelegramBot.git
cd ComfyUI-TelegramBot
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt
```

## Настройка (.env)

```env
BOT_TOKEN=telegram_bot_token
COMFYUI_HOST=http://127.0.0.1:8000
COMFYUI_WS=ws://127.0.0.1:8000/ws

DATA_DIR=data
OUTPUT_DIR=Output
COMFYUI_SHARED_OUTPUT_DIR=Output
PERSISTENCE_FILE=bot_state.pkl
CHECK_COMFY_RUNNING=false
LOG_BOOT_DEBUG_SECONDS=30            # если LOG_LEVEL не DEBUG, держать DEBUG первые N секунд
WEBAPP_URL=https://your-miniapp-url  # для кнопки Mini App
WEBAPP_API_HOST=0.0.0.0             # хост для API Mini App (по умолчанию 0.0.0.0)
WEBAPP_API_PORT=8081                # порт API Mini App
WEBAPP_API_ENABLED=true             # выключите при внешнем API
WEBAPP_SERVE_ENABLED=false          # если true и собран webapp/dist — раздаём Mini App статикой
WEBAPP_SERVE_PATH=webapp/dist       # путь к собранному фронту Mini App
```

## Запуск

```bash
python bot.py
```

В Telegram: отправьте `/start` боту.

## Mini App (WebApp)

- Нужен публичный HTTPS URL фронта (WEBAPP_URL). Telegram не хостит фронт сам.
- Укажите URL в .env и в BotFather (Configure Mini App / Web App URL).
- В клавиатуре появится кнопка «📊 Визуализация (Mini App)» (web_app).
- Для локального теста: `npm run dev` в `webapp` + туннель (ngrok/cloudflared) и подставить выданный https-URL.

## Быстрый сценарий

1. `/start` — открыть меню.
2. Импортировать или создать workflow.
3. Настроить ноды.
4. Запустить генерацию и следить за прогрессом.
5. Смотреть результаты в галерее.

## Полезное

- `CHECK_COMFY_RUNNING=true` — автопроверка/автозапуск ComfyUI (см. comfy_manager.py).
- Если `WEBAPP_URL` пуст, кнопка Mini App скрыта.

## Что уже сделано

- Управление workflow в Telegram (загрузка, редактирование, запуск, экспорт).
- Мониторинг прогресса генераций с превью и управлением очередью.
- Галерея и история запусков.
- Кнопка Mini App (WebApp) при наличии `WEBAPP_URL`.

## В планах

- Улучшения Mini App: более удобная визуализация и управление с мобильных.
- Пакетная генерация и очередь нескольких workflow.
- Больше шаблонов и улучшенный поиск по нодам.

## Известные проблемы

- Mini App требует публичный HTTPS (без него кнопка скрыта или не откроется).
- Если ComfyUI недоступен по `COMFYUI_HOST/WS`, запуск или превью не сработают.

## Лицензия

MIT — см. [LICENSE](LICENSE)
