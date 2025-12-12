# ComfyUI Telegram WebApp

Минимальный веб-клиент для визуализации workflow в Telegram WebApp.

## Быстрый старт

```bash
npm install
npm run dev -- --host
```

Переменные:

- `VITE_API_BASE` — базовый URL бота (например, `http://localhost:8080`), по умолчанию используется относительный `/api`.

## Что реализовано

- Загрузка workflow по `sid` (`/api/workflow/{sid}`)
- Отрисовка графа в React Flow (read-only)
- Тёмная тема под Telegram WebApp
- Интеграция с Telegram WebApp SDK (ready/expand)

## TODO

- Авто-раскладка координат
- Отображение метаданных нод
- Редактирование и отправка изменений
