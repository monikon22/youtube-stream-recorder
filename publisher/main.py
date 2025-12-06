import json
import os
import time
import logging
import sys
import requests
from pymongo import MongoClient

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class Publisher:
    def __init__(self, config_path='config.json'):
        self.load_config(config_path)
        self.setup_db()

    def load_config(self, path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            self.telegram_config = self.config.get('telegram', {})
            self.mongo_config = self.config.get('mongodb', {})
            logger.info("Конфигурация загружена.")
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
            self.telegram_config = {}
            self.mongo_config = {}

    def setup_db(self):
        try:
            self.client = MongoClient(self.mongo_config.get('uri'), serverSelectionTimeoutMS=2000)
            self.db = self.client[self.mongo_config.get('db_name')]
            self.publish_queue = self.db['publish_queue']
            # Проверка подключения
            self.client.server_info()
            logger.info("Успешное подключение к MongoDB")
        except Exception as e:
            logger.error(f"Ошибка подключения к MongoDB: {e}")
            self.publish_queue = None

    def format_message(self, template, info, sequence_number):
        # Создаем словарь для форматирования, объединяя info и доп. поля
        format_data = info.copy()
        format_data['sequence_number'] = sequence_number
        
        # Безопасное форматирование (если ключа нет в info, оставляем плейсхолдер или пустоту)
        # Используем format_map с defaultdict или кастомным классом, чтобы не падать
        class SafeDict(dict):
            def __missing__(self, key):
                return '{' + key + '}'
        
        return template.format_map(SafeDict(format_data))

    def send_video(self, file_path, caption, target_type='watermarked'):
        bot_token = self.telegram_config.get('bot_token')
        api_url = self.telegram_config.get('api_url')
        
        # Выбор канала в зависимости от типа
        if target_type == 'original':
            channel_id = self.telegram_config.get('channel_id_original')
        else:
            channel_id = self.telegram_config.get('channel_id')
            
        if not bot_token or not channel_id or not api_url:
            logger.error(f"Не настроен Telegram для {target_type} (токен, канал или api_url)")
            return False

        url = f"{api_url}/bot{bot_token}/sendVideo"
        
        try:
            with open(file_path, 'rb') as video_file:
                files = {'video': video_file}
                data = {
                    'chat_id': channel_id,
                    'caption': caption,
                    'parse_mode': 'HTML',
                    'supports_streaming': True
                }
                response = requests.post(url, files=files, data=data)
                
                if response.status_code == 200:
                    logger.info(f"Видео успешно отправлено ({target_type}): {file_path}")
                    return True
                else:
                    logger.error(f"Ошибка отправки видео ({target_type}): {response.text}")
                    return False
        except Exception as e:
            logger.error(f"Ошибка при отправке запроса: {e}")
            return False

    def run(self):
        logger.info("Запущен процесс публикации...")
        while True:
            if self.publish_queue is None:
                time.sleep(10)
                self.setup_db()
                continue

            try:
                # Ищем задачи со статусом pending
                task = self.publish_queue.find_one_and_update(
                    {'status': 'pending'},
                    {'$set': {'status': 'processing'}}
                )
                
                if task:
                    logger.info(f"Обработка задачи публикации: {task.get('stream_id')} #{task.get('sequence_number')}")
                    
                    file_path = task.get('file_path')
                    info = task.get('info', {})
                    sequence_number = task.get('sequence_number')
                    target_type = task.get('target_type', 'watermarked')
                    
                    # Выбор шаблона
                    if target_type == 'original':
                        template = self.telegram_config.get('message_template_original', 'Original Part {sequence_number}')
                    else:
                        template = self.telegram_config.get('message_template', 'Part {sequence_number}')
                        
                    caption = self.format_message(template, info, sequence_number)
                    
                    if os.path.exists(file_path):
                        success = self.send_video(file_path, caption, target_type)
                        if success:
                            self.publish_queue.update_one(
                                {'_id': task['_id']},
                                {'$set': {'status': 'completed', 'published_at': time.time()}}
                            )
                        else:
                            # Возвращаем в очередь или помечаем ошибкой
                            self.publish_queue.update_one(
                                {'_id': task['_id']},
                                {'$set': {'status': 'failed', 'error': 'Send failed'}}
                            )
                    else:
                        logger.error(f"Файл не найден: {file_path}")
                        self.publish_queue.update_one(
                            {'_id': task['_id']},
                             {'$set': {'status': 'failed', 'error': 'File not found'}}
                        )
                else:
                    time.sleep(5) # Нет задач
                    
            except Exception as e:
                logger.error(f"Ошибка в цикле публикации: {e}")
                time.sleep(5)

if __name__ == "__main__":
    publisher = Publisher()
    try:
        publisher.run()
    except KeyboardInterrupt:
        logger.info("Остановка скрипта...")
