import json
import os
import time
import subprocess
import datetime
import logging
import sys
import threading
import schedule
import yt_dlp

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class StreamRecorder:
    def __init__(self, config_path='config.json'):
        self.load_config(config_path)
        self.active_recordings = {} # Словарь для отслеживания активных процессов записи
        self.stopped_manually = set() # Множество каналов, остановленных вручную

    def load_config(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.channels = self.config.get('channels', [])
        self.settings = self.config.get('settings', {})

    def get_stream_info(self, channel_url):
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': True,
        }
        
        # Поддержка cookies.txt для yt-dlp
        cookies_file = self.settings.get('cookies_file')
        if cookies_file:
            cookies_path = cookies_file
            # Если путь относительный, добавляем префикс /app или текущий путь
            if not os.path.isabs(cookies_path):
                if sys.platform == 'win32':
                    cookies_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', cookies_file)
                else:
                    cookies_path = f'/app/{cookies_file}'
            
            if os.path.exists(cookies_path):
                ydl_opts['cookiefile'] = cookies_path
                logger.info(f"Используется файл cookies: {cookies_path}")
            else:
                logger.warning(f"Файл cookies не найден: {cookies_path}")
        
        # Если это YouTube канал, попробуем найти live
        if 'youtube.com' in channel_url or 'youtu.be' in channel_url:
            if '/watch' not in channel_url and '/live' not in channel_url:
                # Это похоже на ссылку на канал, добавляем /live для поиска текущего стрима
                if not channel_url.endswith('/'):
                    channel_url += '/'
                channel_url += 'live'

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(channel_url, download=False)
                if not info:
                    return None
                
                # Если вернулся плейлист (канал), ищем live в записях
                if 'entries' in info:
                    for entry in info['entries']:
                        if not entry: continue
                        if entry.get('is_live') or entry.get('was_live'):
                            return entry
                    return None

                # Проверка, идет ли стрим (is_live для YouTube, Twitch и т.д.)
                if info.get('is_live') or info.get('was_live'): # was_live иногда возвращается для текущих стримов
                    return info
                return None
            except Exception as e:
                logger.error(f"Ошибка при получении информации о канале {channel_url}: {e}")
                return None

    def parse_size(self, size_str):
        if not size_str: return None
        units = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
        size_str = size_str.upper()
        for unit, multiplier in units.items():
            if size_str.endswith(unit):
                try:
                    return int(float(size_str[:-len(unit)]) * multiplier)
                except:
                    return None
        try:
            return int(size_str)
        except:
            return None

    def stream_writer(self, process, path_template, max_size):
        file_index = 0
        current_file = None
        current_size = 0
        
        try:
            while True:
                chunk = process.stdout.read(1024*1024) # 1MB chunks
                if not chunk:
                    break
                
                if current_file is None:
                    filename = path_template % file_index
                    current_file = open(filename, 'wb')
                    current_size = 0
                    logger.info(f"Начало записи сегмента (по размеру): {filename}")
                
                current_file.write(chunk)
                current_size += len(chunk)
                
                if max_size and current_size >= max_size:
                    current_file.close()
                    current_file = None
                    file_index += 1
                    
        except Exception as e:
            logger.error(f"Ошибка записи потока: {e}")
        finally:
            if current_file:
                current_file.close()

    def start_recording(self, channel, stream_info):
        channel_name = channel['name']
        
        if channel_name in self.stopped_manually:
            logger.info(f"Канал {channel_name} был остановлен вручную. Пропуск.")
            return

        channel_url = channel['url']
        
        if channel_name in self.active_recordings:
            # Проверяем, жив ли процесс (или поток)
            proc = self.active_recordings[channel_name]
            is_alive = False
            if isinstance(proc, subprocess.Popen):
                if proc.poll() is None: is_alive = True
            elif isinstance(proc, dict) and 'process' in proc:
                if proc['process'].poll() is None: is_alive = True
            
            if is_alive:
                logger.info(f"Запись канала {channel_name} уже идет.")
                return
            else:
                logger.info(f"Предыдущая запись {channel_name} завершилась. Начинаем новую.")
                del self.active_recordings[channel_name]

        logger.info(f"Обнаружен прямой эфир на канале: {channel_name}")

        # Подготовка путей
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        session_time = datetime.datetime.now().strftime('%H-%M-%S')
        
        base_output_path = self.settings.get('output_path', '/app/recordings')
        if sys.platform == 'win32' and base_output_path.startswith('/app'):
             base_output_path = 'recordings'

        channel_path = os.path.join(base_output_path, channel_name, date_str, session_time)
        os.makedirs(channel_path, exist_ok=True)

        # Метаданные для БД (будут использованы пост-процессором)
        # metadata = { ... } # Перенесено в postprocessor
        
        try:
            info_json_path = os.path.join(channel_path, 'info.json')
            with open(info_json_path, 'w', encoding='utf-8') as f:
                json.dump(stream_info, f, ensure_ascii=False, indent=4, default=str)
            logger.info(f"Info JSON сохранен в {info_json_path}")
        except Exception as e:
            logger.error(f"Ошибка сохранения info.json: {e}")

        stream_url = stream_info.get('url')
        
        # Проверяем настройки сегментации
        segment_size_str = self.settings.get('segment_size')
        segment_size_bytes = self.parse_size(segment_size_str)
        
        output_filename_template = os.path.join(channel_path, f"video_%03d.ts")

        ffmpeg_cmd = ['ffmpeg', '-y', '-i', stream_url]
        
        http_headers = stream_info.get('http_headers', {})
        if http_headers and 'User-Agent' in http_headers:
            ffmpeg_cmd.insert(1, '-user_agent')
            ffmpeg_cmd.insert(2, http_headers['User-Agent'])

        if segment_size_bytes:
            # Режим разделения по размеру
            # ffmpeg пишет в stdout -> Python читает и пишет в файлы
            ffmpeg_cmd.extend(['-c', 'copy', '-f', 'mpegts', '-'])
            
            logger.info(f"Запуск записи (split by size: {segment_size_str}) для {channel_name}...")
            try:
                p = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                
                # Запускаем поток записи
                writer_thread = threading.Thread(
                    target=self.stream_writer,
                    args=(p, output_filename_template, segment_size_bytes),
                    daemon=True
                )
                writer_thread.start()
                
                self.active_recordings[channel_name] = {'process': p, 'thread': writer_thread}
                
            except Exception as e:
                logger.error(f"Не удалось запустить процесс записи: {e}")
        else:
            # Режим разделения по времени (стандартный ffmpeg segment)
            segment_time = self.settings.get('segment_time', '00:30:00')
            ffmpeg_cmd.extend([
                '-c', 'copy',
                '-f', 'segment',
                '-segment_time', str(segment_time),
                '-reset_timestamps', '1',
                '-strftime', '0',
                output_filename_template
            ])

            logger.info(f"Запуск записи (split by time) для {channel_name}...")
            try:
                p = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.active_recordings[channel_name] = p 
            except Exception as e:
                logger.error(f"Не удалось запустить процесс записи: {e}")

    def stop_recording(self, channel_name):
        if channel_name in self.active_recordings:
            logger.info(f"Остановка записи канала: {channel_name}")
            obj = self.active_recordings[channel_name]
            
            proc = None
            if isinstance(obj, subprocess.Popen):
                proc = obj
            elif isinstance(obj, dict):
                proc = obj.get('process')
            
            if proc:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                
            del self.active_recordings[channel_name]
            self.stopped_manually.add(channel_name)
            logger.info(f"Запись {channel_name} остановлена.")
        else:
            logger.warning(f"Канал {channel_name} сейчас не записывается.")

    def command_listener(self):
        logger.info("Запущен слушатель команд. Доступные команды: stop <channel_name>, list, quit, resume <channel_name>")
        while True:
            try:
                cmd_input = input()
                parts = cmd_input.strip().split(' ', 1)
                cmd = parts[0].lower()
                
                if cmd == 'stop':
                    if len(parts) > 1:
                        self.stop_recording(parts[1])
                    else:
                        logger.warning("Использование: stop <channel_name>")
                elif cmd == 'resume':
                    if len(parts) > 1:
                        channel_name = parts[1]
                        if channel_name in self.stopped_manually:
                            self.stopped_manually.remove(channel_name)
                            logger.info(f"Канал {channel_name} удален из списка остановленных. Запись начнется при следующей проверке.")
                        else:
                            logger.warning(f"Канал {channel_name} не был остановлен вручную.")
                    else:
                        logger.warning("Использование: resume <channel_name>")
                elif cmd == 'list':
                    logger.info(f"Активные записи: {list(self.active_recordings.keys())}")
                    logger.info(f"Остановленные вручную: {list(self.stopped_manually)}")
                elif cmd == 'quit':
                    logger.info("Завершение работы...")
                    for name in list(self.active_recordings.keys()):
                        self.stop_recording(name)
                    os._exit(0)
            except EOFError:
                break
            except Exception as e:
                logger.error(f"Ошибка при обработке команды: {e}")

    def check_channels(self):
        logger.info("Проверка каналов...")
        for channel in self.channels:
            try:
                info = self.get_stream_info(channel['url'])
                if info:
                    self.start_recording(channel, info)
                else:
                    # Если стрим не идет, но процесс висит - проверим, жив ли он
                    if channel['name'] in self.active_recordings:
                        obj = self.active_recordings[channel['name']]
                        proc = None
                        if isinstance(obj, subprocess.Popen):
                            proc = obj
                        elif isinstance(obj, dict):
                            proc = obj.get('process')
                            
                        if proc and proc.poll() is not None:
                            logger.info(f"Стрим {channel['name']} закончился. Процесс завершен.")
                            del self.active_recordings[channel['name']]
            except Exception as e:
                logger.error(f"Ошибка при проверке канала {channel['name']}: {e}")

    def run(self):
        # Запуск слушателя команд в отдельном потоке
        cmd_thread = threading.Thread(target=self.command_listener, daemon=True)
        cmd_thread.start()

        interval = self.settings.get('check_interval', 60)
        schedule.every(interval).seconds.do(self.check_channels)
        
        # Первая проверка сразу
        self.check_channels()
        
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    recorder = StreamRecorder()
    try:
        recorder.run()
    except KeyboardInterrupt:
        logger.info("Остановка скрипта...")
