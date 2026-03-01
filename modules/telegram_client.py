import os
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto
from telethon.errors import FloodWaitError, SessionPasswordNeededError
import logging
from telethon.tl.functions.messages import GetForumTopicsRequest
from modules.task_queue import TaskQueue

class TelegramClientManager:
    def __init__(self):
        self.client = TelegramClient(
            os.getenv('SESSION_FILE', 'session_name.session'),
            int(os.getenv('TELEGRAM_API_ID')),
            os.getenv('TELEGRAM_API_HASH')
        )
        self.logger = logging.getLogger(__name__)
        self.active_topics = {}  # Для отслеживания активных топиков и таймеров
        self.wait_time = int(os.getenv('WAIT_TIME', '300'))  # 5 минут по умолчанию
        self.task_queue = TaskQueue(max_workers=2, telegram_client=self)  # Передаем себя в очередь
    
    async def connect(self):
        """Connect to Telegram and authenticate if needed."""
        try:
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                self.logger.info("Sending code request...")
                await self.client.send_code_request(os.getenv('TELEGRAM_PHONE_NUMBER'))
                
                code = input("Enter the code you received: ")
                
                try:
                    await self.client.sign_in(os.getenv('TELEGRAM_PHONE_NUMBER'), code)
                except SessionPasswordNeededError:
                    self.logger.info("Two-factor authentication is enabled")
                    
                    # Try to get password from environment variable first
                    password = os.getenv('TELEGRAM_2FA_PASSWORD')
                    if not password:
                        password = input("Enter your 2FA password: ")
                    
                    await self.client.sign_in(password=password)
                
            self.logger.info("Successfully connected to Telegram")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Telegram: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Telegram."""
        await self.client.disconnect()
    
    async def get_chat_topics(self) -> List[Dict[str, Any]]:
        """Get 10 latest topics containing UA or UB from the target chat."""
        try:
            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            collected_topics = []
            
            offset_date = 0
            offset_id = 0
            offset_topic = 0
            
            self.logger.info("Searching for 1 latest UA/UB topics...")
            
            while len(collected_topics) < 1:
                result = await self.client(GetForumTopicsRequest(
                    peer=chat,
                    q='',
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_topic=offset_topic,
                    limit=100
                ))
                
                if not getattr(result, 'topics', []):
                    break
                
                for topic in result.topics:
                    # Переводим в верхний регистр
                    title_upper = topic.title.upper()
                    
                    # ИЩЕМ БЕЗ ДЕФИСА! Просто 'UA' или 'UB'
                    if ('UA' in title_upper or 'UB' in title_upper) and ('TSS' not in title_upper and 'TSSR' not in title_upper):
                        topic_info = {
                            'topic_id': topic.id,
                            'title': topic.title,
                            'date': getattr(topic, 'date', None) 
                        }
                        collected_topics.append(topic_info)
                        # Пишем в лог, что мы нашли подходящий топик!
                        self.logger.info(f"✅ Найден топик: {topic.title} (ID: {topic.id})")
                        
                        if len(collected_topics) == 1:
                            break
                            
                if len(collected_topics) == 1:
                    break
                
                if getattr(result, 'messages', []):
                    last_msg = result.messages[-1]
                    offset_date = int(last_msg.date.timestamp())
                    offset_id = last_msg.id
                    if result.topics:
                        offset_topic = result.topics[-1].id
                else:
                    break
            
            self.logger.info(f"Found {len(collected_topics)} matching topics in chat")
            return collected_topics
            
        except Exception as e:
            self.logger.error(f"Failed to get topics: {e}")
            return []
    
    def _create_topic_folder(self, topic_id: int) -> str:
        """Create folder for topic downloads."""
        download_dir = os.path.join(os.getenv('DOWNLOADS_DIR', 'downloads'), f"topic_{topic_id}")
        os.makedirs(download_dir, exist_ok=True)
        self.logger.info(f"Создана папка для топика: {download_dir}")
        return download_dir
    
    def _save_message_to_log(self, topic_dir: str, message_data: Dict[str, Any]) -> None:
        """Save message text to log file with proper formatting."""
        if not message_data.get('text'):
            return
            
        log_file = os.path.join(topic_dir, 'messages_log.txt')
        timestamp = message_data['date'].strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] | [{message_data['id']}] | {message_data['text']}\n{'='*50}\n"
        
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
        
        self.logger.info(f"Найдено сообщение с текстом (ID: {message_data['id']}), сохранено.")
    
    async def download_topic_content(self, topic_id: int, message_limit: int = 100) -> Dict[str, int]:
        """
        Download all text messages and photos from a specific Telegram topic.
        
        Args:
            topic_id: ID of the topic to download from
            message_limit: Maximum number of messages to process (default: 100)
            
        Returns:
            Dict with counts: {'text_messages': X, 'photos': Y}
        """
        try:
            # Step 1: Create topic folder
            topic_dir = self._create_topic_folder(topic_id)
            
            # Step 2: Get messages from topic
            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            photos_count = 0
            text_count = 0
            
            self.logger.info(f"Начинаю выгрузку топика {topic_id}...")
            
            # Step 3: Process messages
            async for message in self.client.iter_messages(
                chat,
                reply_to=topic_id,
                limit=message_limit
            ):
                message_data = {
                    'id': message.id,
                    'date': message.date,
                    'text': message.text,
                    'media_group_id': getattr(message, 'grouped_id', None),
                    'has_media': bool(message.media),
                    'media_type': None
                }
                
                # Step 4: Handle text content
                if message.text:
                    self._save_message_to_log(topic_dir, message_data)
                    text_count += 1
                
                # Step 5: Handle media content
                if message.media and isinstance(message.media, MessageMediaPhoto):
                    self.logger.info(f"Скачивается фотография (ID: {message.id})...")
                    
                    # Download photo with proper naming
                    file_name = f"photo_{message.id}.jpg"
                    file_path = os.path.join(topic_dir, file_name)
                    
                    try:
                        downloaded_path = await self.client.download_media(
                            message,
                            file=file_path
                        )
                        
                        if downloaded_path:
                            photos_count += 1
                            self.logger.info(f"Фотография успешно скачана: {downloaded_path}")
                        
                        # Rate limiting between downloads
                        await asyncio.sleep(0.5)
                        
                    except FloodWaitError as e:
                        self.logger.warning(f"Flood wait: {e.seconds} секунд")
                        await asyncio.sleep(e.seconds)
                        # Retry download after wait
                        downloaded_path = await self.client.download_media(
                            message,
                            file=file_path
                        )
                        if downloaded_path:
                            photos_count += 1
                
                # Small delay between processing messages
                await asyncio.sleep(0.2)
            
            # Step 6: Final summary
            summary = f"Загрузка топика завершена. Всего скачано {photos_count} фото и {text_count} текстовых сообщений."
            self.logger.info(summary)
            
            return {
                'text_messages': text_count,
                'photos': photos_count
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка при выгрузке топика {topic_id}: {e}")
            return {'text_messages': 0, 'photos': 0}
    
    async def get_topic_title_by_id(self, topic_id: int) -> Optional[str]:
        """Получить название топика по его ID."""
        try:
            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            # Получаем топик по ID
            topics_result = await self.client(GetForumTopicsRequest(
                peer=chat,
                q='',
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=100
            ))
            
            for topic in topics_result.topics:
                if topic.id == topic_id:
                    return topic.title
            
            return None
        except Exception as e:
            self.logger.error(f"Ошибка при получении названия топика {topic_id}: {e}")
            return None
    
    def should_process_topic(self, topic_title: str) -> bool:
        """Проверить, нужно ли обрабатывать этот топик."""
        if not topic_title:
            return False
        
        title_upper = topic_title.upper()
        # Ищем UA или UB, но исключаем TSS/TSSR
        return ('UA' in title_upper or 'UB' in title_upper) and ('TSS' not in title_upper and 'TSSR' not in title_upper)
    
    async def reset_topic_timer(self, topic_id: int, topic_title: str):
        """Сбросить таймер для топика или создать новый."""
        # Если уже есть активный таймер, отменяем его
        if topic_id in self.active_topics:
            self.active_topics[topic_id]['last_message_time'] = datetime.now()
            self.logger.info(f"🔄 Таймер для топика {topic_title} (ID: {topic_id}) сброшен")
            return
        
        # Создаем новую запись об активном топике
        self.active_topics[topic_id] = {
            'last_message_time': datetime.now(),
            'timer_task': None,
            'message_count': 1,
            'title': topic_title
        }
        
        self.logger.info(f"🆕 Активность в топике {topic_title} (ID: {topic_id}), жду завершения загрузки...")
        
        # Создаем задачу таймера
        task = asyncio.create_task(self._topic_timer_handler(topic_id))
        self.active_topics[topic_id]['timer_task'] = task
    
    async def _topic_timer_handler(self, topic_id: int):
        """Обработчик таймера для топика."""
        try:
            await asyncio.sleep(self.wait_time)
            
            # Проверяем, что топик все еще активен
            if topic_id not in self.active_topics:
                return
            
            # Проверяем, не было ли новых сообщений
            last_message_time = self.active_topics[topic_id]['last_message_time']
            time_since_last = datetime.now() - last_message_time
            
            if time_since_last >= timedelta(seconds=self.wait_time):
                # Таймер истек, начинаем обработку
                topic_title = self.active_topics[topic_id]['title']
                self.logger.info(f"⏰ Таймер для топика {topic_title} (ID: {topic_id}) истек, начинаю сбор данных")
                
                # Удаляем из активных и добавляем в очередь
                del self.active_topics[topic_id]
                await self.add_topic_to_queue(topic_id)
            else:
                # Были новые сообщения, сбрасываем таймер
                await self.reset_topic_timer(topic_id, self.active_topics[topic_id]['title'])
                
        except asyncio.CancelledError:
            self.logger.debug(f"Таймер для топика {topic_id} отменен")
        except Exception as e:
            self.logger.error(f"Ошибка в обработчике таймера топика {topic_id}: {e}")
    
        
    async def add_topic_to_queue(self, topic_id: int):
        """Добавить топик в очередь обработки вместо немедленной обработки."""
        try:
            # Получаем название топика
            topic_title = await self.get_topic_title_by_id(topic_id)
            if not topic_title:
                self.logger.error(f"Не удалось получить название топика {topic_id}")
                return
            
            # Добавляем в очередь на обработку
            await self.task_queue.add_topic_task(topic_id, topic_title)
            
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении топика {topic_id} в очередь: {e}")
    
    def setup_event_handler(self, process_callback=None):
        """Настроить обработчик событий для новых сообщений."""
        
        @self.client.on(events.NewMessage(chats=int(os.getenv('TARGET_CHAT_ID'))))
        async def new_message_handler(event):
            try:
                message = event.message
                
                # Пропускаем сообщения без reply_to (не в топиках)
                if not message.reply_to:
                    return
                
                topic_id = message.reply_to.reply_to_msg_id
                if not topic_id:
                    return
                
                # Получаем название топика для фильтрации
                topic_title = await self.get_topic_title_by_id(topic_id)
                if not topic_title:
                    return
                
                # Проверяем, нужно ли обрабатывать этот топик
                if not self.should_process_topic(topic_title):
                    self.logger.debug(f"🚫 Топик '{topic_title}' игнорируется (не UA/UB)")
                    return
                
                self.logger.debug(f"📨 Новое сообщение в топике '{topic_title}' (ID: {topic_id})")
                
                # Сбрасываем или создаем таймер
                await self.reset_topic_timer(topic_id, topic_title)
                
            except Exception as e:
                self.logger.error(f"Ошибка в обработчике новых сообщений: {e}")
        
        self.logger.info("🎯 Event handler настроен для отслеживания новых сообщений")
    
    async def start_autonomous_mode(self):
        """Запустить автономный режим работы."""
        self.logger.info("🚀 Запускаю автономный режим работы...")
        self.setup_event_handler()
        
        # Обновляем очередь с текущим event loop
        self.task_queue.main_loop = asyncio.get_running_loop()
        
        # Запускаем Consumer для обработки очереди в фоновом режиме
        asyncio.create_task(self.task_queue.start_consumer())
        
        try:
            self.logger.info("👂 Начинаю слушать сообщения 24/7...")
            self.logger.info(f"⚙️ Очередь обработки запущена с {self.task_queue.max_workers} воркерами")
            
            # Запускаем Telegram клиент
            await self.client.run_until_disconnected()
            
        except KeyboardInterrupt:
            self.logger.info("⏹️ Остановка по запросу пользователя")
        except Exception as e:
            self.logger.error(f"Ошибка в автономном режиме: {e}")
        finally:
            # Останавливаем очередь
            await self.task_queue.shutdown()
            await self.disconnect()
