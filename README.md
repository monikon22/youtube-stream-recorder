# YouTube Stream Recorder & Publisher

Автоматизированная система для записи прямых трансляций, их обработки (наложение вотермарок) и публикации в Telegram каналы. Проект построен на микросервисной архитектуре с использованием Docker.

## Возможности

- **Мультиплатформенность:** Поддержка YouTube, Twitch, Kick и других сервисов (через `yt-dlp`).
- **Микросервисная архитектура:**
  - `Recorder`: Мониторинг и запись потока в сыром формате (`.ts`).
  - `Post-processor`: Конвертация в `.mp4`, создание двух версий (с вотермаркой и без), сохранение метаданных.
  - `Publisher`: Публикация видео в Telegram каналы.
- **Двойная публикация:**
  - Видео с вотермаркой -> в основной канал.
  - Оригинал (без вотермарки) -> в приватный/премиум канал.
- **Telegram Bot API:** Использование локального сервера Telegram Bot API для загрузки больших файлов (до 2000 МБ).
- **Гибкая сегментация:** Деление записи по времени (например, каждые 30 минут) или по размеру файла (например, 2 ГБ).
- **MongoDB:** Хранение полной истории стримов, сегментов и очереди публикации.

## Структура проекта

- `recorder/` - Сервис записи (Python + FFmpeg).
- `postprocessor/` - Сервис обработки видео (Python + FFmpeg).
- `publisher/` - Сервис отправки в Telegram (Python).
- `docker-compose.yml` - Описание инфраструктуры.
- `config.json` - Единый файл конфигурации.

## Установка и настройка

### 1. Предварительные требования

- Docker и Docker Compose.
- Аккаунт Telegram (для получения `api_id` и `api_hash`).
- Созданный Telegram бот и два канала (основной и премиум).

### 2. Настройка Telegram API

Для работы с большими файлами используется локальный сервер Telegram Bot API.

1.  Перейдите на [my.telegram.org](https://my.telegram.org).
2.  Получите `API_ID` и `API_HASH`.
3.  Откройте `docker-compose.yml` и в сервисе `telegram-api` замените значения:
    ```yaml
    environment:
      - TELEGRAM_API_ID=ВАШ_ID
      - TELEGRAM_API_HASH=ВАШ_HASH
    ```

### 3. Конфигурация (config.json)

Создайте или отредактируйте файл `config.json`:

```json
{
  "channels": [
    {
      "name": "Channel Name",
      "url": "https://www.youtube.com/@channel",
      "platform": "youtube"
    }
  ],
  "settings": {
    "check_interval": 60,
    "segment_time": "00:30:00", // Деление по времени (если segment_size не задан)
    "segment_size": "2G", // Деление по размеру (приоритетнее)
    "watermark_path": "/app/watermark.png",
    "watermark_position": "bottom-right",
    "output_path": "/app/recordings"
  },
  "telegram": {
    "bot_token": "YOUR_BOT_TOKEN",
    "channel_id": "@public_channel", // Канал для видео с вотермаркой
    "channel_id_original": "@private_channel", // Канал для оригиналов
    "api_url": "http://telegram-api:8081",
    "message_template": "<b>{title}</b>\nЧасть {sequence_number}",
    "message_template_original": "<b>{title}</b> (Original)\nЧасть {sequence_number}"
  },
  "mongodb": {
    "uri": "mongodb://mongo:27017/",
    "db_name": "stream_recorder",
    "collection": "streams"
  }
}
```

### 4. Запуск

```bash
docker-compose up -d --build
```

## Как это работает

1.  **Recorder** проверяет каналы. Если стрим идет, он начинает писать его в `.ts` файлы (сегменты).
2.  **Post-processor** следит за папкой записей. Когда сегмент дописан:
    - Создает копию без изменений (`_orig.mp4`).
    - Создает версию с наложенным `watermark.png` (`.mp4`).
    - Записывает информацию в MongoDB (коллекция `streams`).
    - Создает задачи в очереди публикации (коллекция `publish_queue`).
3.  **Publisher** берет задачи из очереди и отправляет видео в соответствующие Telegram каналы через локальный API сервер.

## Управление

Для управления рекордером (остановка/возобновление записи конкретных каналов) можно подключиться к контейнеру:

```bash
docker attach stream_recorder
```

Команды: `list`, `stop <name>`, `resume <name>`, `quit`.
