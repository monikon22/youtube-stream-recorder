import json
import os
import time
import subprocess
import logging
import sys
import glob
import datetime
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

class PostProcessor:
    def __init__(self, config_path='config.json'):
        self.load_config(config_path)
        self.setup_db()

    def load_config(self, path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            self.settings = self.config.get('settings', {})
            self.mongo_config = self.config.get('mongodb', {})
            logger.info("Конфигурация загружена.")
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
            self.settings = {}
            self.mongo_config = {}

    def setup_db(self):
        try:
            self.client = MongoClient(self.mongo_config.get('uri'), serverSelectionTimeoutMS=2000)
            self.db = self.client[self.mongo_config.get('db_name')]
            self.collection = self.db[self.mongo_config.get('collection')]
            self.publish_queue = self.db['publish_queue']
            # Проверка подключения
            self.client.server_info()
            logger.info("Успешное подключение к MongoDB")
        except Exception as e:
            logger.error(f"Ошибка подключения к MongoDB: {e}")
            self.collection = None
            self.publish_queue = None

    def process_segment(self, ts_file):
        """Обработка одного сегмента: создание версии без вотермарки и с вотермаркой"""
        try:
            mp4_file = ts_file.replace('.ts', '.mp4')
            mp4_file_orig = ts_file.replace('.ts', '_orig.mp4')
            
            if os.path.exists(mp4_file) and os.path.exists(mp4_file_orig):
                return # Уже обработан

            logger.info(f"Обработка сегмента: {ts_file}")
            
            # 1. Создаем оригинальную версию (без вотермарки)
            cmd_orig = ['ffmpeg', '-y', '-i', ts_file, '-c', 'copy', mp4_file_orig]
            subprocess.run(cmd_orig, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            logger.info(f"Оригинальный сегмент создан: {mp4_file_orig}")

            # 2. Создаем версию с вотермаркой (используем оригинал как источник, чтобы не читать TS дважды, или TS)
            # Используем TS как источник, так надежнее
            watermark_path = self.settings.get('watermark_path')
            if sys.platform == 'win32' and watermark_path.startswith('/app'):
                watermark_path = 'watermark.png'
            
            watermark_pos = self.settings.get('watermark_position', 'bottom-right')
            overlay_positions = {
                'top-left': '10:10',
                'top-right': 'main_w-overlay_w-10:10',
                'bottom-left': '10:main_h-overlay_h-10',
                'bottom-right': 'main_w-overlay_w-10:main_h-overlay_h-10'
            }
            overlay_cmd = overlay_positions.get(watermark_pos, overlay_positions['bottom-right'])

            cmd = ['ffmpeg', '-y', '-i', ts_file]
            
            if watermark_path and os.path.exists(watermark_path):
                cmd.extend([
                    '-i', watermark_path,
                    '-filter_complex', f'overlay={overlay_cmd}',
                    '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
                    '-c:a', 'aac'
                ])
            else:
                # Если вотермарки нет, просто копируем (будет дубликат оригинала, но с другим именем)
                cmd.extend(['-c', 'copy'])
            
            cmd.append(mp4_file)
            
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            logger.info(f"Сегмент с вотермаркой создан: {mp4_file}")
            
            # Удаляем исходный .ts файл после успешной конвертации обоих файлов
            if os.path.exists(mp4_file) and os.path.exists(mp4_file_orig):
                os.remove(ts_file)
            
            # Обновление БД
            self.update_db(mp4_file, mp4_file_orig)
            
        except Exception as e:
            logger.error(f"Ошибка обработки сегмента {ts_file}: {e}")

    def update_db(self, mp4_file, mp4_file_orig):
        if self.collection is None:
            return

        try:
            # Получаем директорию файла для поиска info.json
            file_dir = os.path.dirname(mp4_file)
            info_json_path = os.path.join(file_dir, 'info.json')
            
            stream_info = {}
            if os.path.exists(info_json_path):
                with open(info_json_path, 'r', encoding='utf-8') as f:
                    stream_info = json.load(f)
            
            stream_id = stream_info.get('id')
            if not stream_id:
                logger.warning(f"Не найден stream_id для {mp4_file}")
                return

            # Определяем номер сегмента из имени файла (video_001.mp4 -> 1)
            filename = os.path.basename(mp4_file)
            try:
                index_str = filename.replace('video_', '').replace('.mp4', '')
                sequence_number = int(index_str) + 1
            except:
                sequence_number = 1

            # Метаданные для обновления
            metadata = {
                'stream_id': stream_id,
                'stream_title': stream_info.get('title'),
                'uploader': stream_info.get('uploader'),
                'description': stream_info.get('description'),
                'start_time': datetime.datetime.now(),
                'file_path': file_dir
            }
            
            # Обновляем основной документ стрима
            self.collection.update_one(
                {'stream_id': stream_id},
                {
                    '$set': metadata,
                    '$setOnInsert': {'segments': {}, 'segments_original': {}}
                },
                upsert=True
            )
            
            # Добавляем сегменты в списки
            segment_key = str(sequence_number)
            self.collection.update_one(
                {'stream_id': stream_id},
                {
                    '$set': {
                        f'segments.{segment_key}': mp4_file,
                        f'segments_original.{segment_key}': mp4_file_orig
                    }
                }
            )
            
            logger.info(f"БД обновлена для стрима {stream_id}, сегмент {sequence_number}")
            
            # Отправка уведомлений в очередь публикации
            if self.publish_queue is not None:
                # Задача для видео с вотермаркой (основной канал)
                queue_item_wm = {
                    'stream_id': stream_id,
                    'sequence_number': sequence_number,
                    'file_path': mp4_file,
                    'info': stream_info,
                    'created_at': datetime.datetime.now(),
                    'status': 'pending',
                    'target_type': 'watermarked'
                }
                self.publish_queue.insert_one(queue_item_wm)
                
                # Задача для оригинального видео (премиум канал)
                queue_item_orig = {
                    'stream_id': stream_id,
                    'sequence_number': sequence_number,
                    'file_path': mp4_file_orig,
                    'info': stream_info,
                    'created_at': datetime.datetime.now(),
                    'status': 'pending',
                    'target_type': 'original'
                }
                self.publish_queue.insert_one(queue_item_orig)
                
                logger.info(f"Добавлено в очередь публикации: {stream_id} #{sequence_number} (2 tasks)")

        except Exception as e:
            logger.error(f"Ошибка обновления БД: {e}")


    def run(self):
        """Фоновый процесс для обработки завершенных сегментов"""
        logger.info("Запущен процесс постобработки (watermark)...")
        while True:
            try:
                base_path = self.settings.get('output_path', '/app/recordings')
                if sys.platform == 'win32' and base_path.startswith('/app'):
                    base_path = 'recordings'
                
                if not os.path.exists(base_path):
                    time.sleep(10)
                    continue

                # Ищем все .ts файлы рекурсивно
                ts_files = glob.glob(os.path.join(base_path, '**', '*.ts'), recursive=True)
                
                # Группируем файлы по папкам
                files_by_dir = {}
                for f in ts_files:
                    d = os.path.dirname(f)
                    if d not in files_by_dir:
                        files_by_dir[d] = []
                    files_by_dir[d].append(f)
                
                for d, files in files_by_dir.items():
                    # Сортируем файлы по имени (video_000.ts, video_001.ts ...)
                    files.sort()
                    
                    # Обрабатываем все, кроме последнего (он может еще писаться)
                    # Если файлов > 1, то все кроме последнего точно готовы
                    # Если файл 1, проверяем время модификации
                    
                    for i, f in enumerate(files):
                        is_last = (i == len(files) - 1)
                        
                        should_process = False
                        if not is_last:
                            should_process = True
                        else:
                            # Проверяем, не устарел ли последний файл (может запись упала)
                            try:
                                mtime = os.path.getmtime(f)
                                if time.time() - mtime > 60: # 5 минут без изменений
                                    should_process = True
                            except OSError:
                                pass
                        
                        if should_process:
                            self.process_segment(f)
                            
            except Exception as e:
                logger.error(f"Ошибка в цикле постобработки: {e}")
            
            time.sleep(10) # Проверка каждые 10 секунд

if __name__ == "__main__":
    processor = PostProcessor()
    try:
        processor.run()
    except KeyboardInterrupt:
        logger.info("Остановка скрипта...")
