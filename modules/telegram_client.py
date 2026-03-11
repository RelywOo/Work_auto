import os
import asyncio
import getpass
import functools
from collections import OrderedDict
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto
from telethon.errors import FloodWaitError, SessionPasswordNeededError
import logging
from telethon.tl.functions.messages import GetForumTopicsRequest
from modules.task_queue import TaskQueue
from modules.database import DatabaseManager
import time

# Configuration constants
DEFAULT_WAIT_TIME = 300          # seconds to wait for topic inactivity before processing
DEFAULT_MAX_WORKERS = 2          # number of parallel processing workers
TOPIC_CACHE_TTL = 3600           # seconds to cache topic titles (1 hour)
DEFAULT_PHOTO_LIMIT = 100        # max messages to scan for photos
HEARTBEAT_INTERVAL = 30          # seconds between heartbeat file updates
DEFAULT_HEALTHCHECK_INTERVAL = 3600  # seconds between Telegram healthcheck reports
MAX_TOPIC_CACHE_SIZE = 500           # max entries in topic title cache
MAX_HEALTHCHECK_FAILURES = 5         # consecutive healthcheck failures before alert

def with_retry(max_retries=3, base_delay=2):
    """Decorator for retrying async Telegram API calls on errors (including FloodWait)."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            logger = logging.getLogger(__name__)
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except FloodWaitError as e:
                    wait_time = e.seconds
                    logger.warning(f"FloodWaitError in {func.__name__}: waiting {wait_time}s (attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                except Exception as e:
                    logger.warning(f"{type(e).__name__} in {func.__name__} (attempt {attempt+1}/{max_retries}): {e}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(base_delay * (2 ** attempt))
            raise Exception(f"Max retries ({max_retries}) exceeded for {func.__name__}")
        return wrapper
    return decorator

class TelegramClientManager:
    def __init__(self) -> None:
        self.client = TelegramClient(
            os.getenv('SESSION_FILE', 'session_name.session'),
            int(os.getenv('TELEGRAM_API_ID')),
            os.getenv('TELEGRAM_API_HASH')
        )
        self.logger = logging.getLogger(__name__)
        self.active_topics = {}  # Track active topics and their timers
        self._active_topics_lock = asyncio.Lock()  # Lock for atomic access to active_topics
        self.wait_time = int(os.getenv('WAIT_TIME', str(DEFAULT_WAIT_TIME)))
        max_workers = int(os.getenv('MAX_WORKERS', str(DEFAULT_MAX_WORKERS)))
        self.task_queue = TaskQueue(max_workers=max_workers, telegram_client=self)
        self.db = DatabaseManager(os.getenv('DB_PATH', 'bot_memory.db'))
        self._topic_cache: OrderedDict = OrderedDict()  # topic_id -> (title, expiry_time)

    async def connect(self) -> None:
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
                    password = getpass.getpass("Enter your 2FA password: ")
                    await self.client.sign_in(password=password)

            self.logger.info("Successfully connected to Telegram")

        except Exception as e:
            self.logger.error(f"Failed to connect to Telegram: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Telegram."""
        await self.client.disconnect()

    @with_retry(max_retries=3)
    async def get_chat_topics(self, limit: int = 1) -> List[Dict[str, Any]]:
        """Get latest topics containing UA or UB from the target chat."""
        try:
            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            collected_topics = []

            offset_date = 0
            offset_id = 0
            offset_topic = 0

            self.logger.info(f"Searching for {limit} latest UA/UB topics...")

            while len(collected_topics) < limit:
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
                    title_upper = topic.title.upper()

                    # Match 'UA' or 'UB' but exclude TSS/TSSR topics
                    if ('UA' in title_upper or 'UB' in title_upper) and ('TSS' not in title_upper and 'TSSR' not in title_upper):
                        topic_info = {
                            'topic_id': topic.id,
                            'title': topic.title,
                            'date': getattr(topic, 'date', None)
                        }
                        collected_topics.append(topic_info)
                        self.logger.info(f"✅ Found topic: {topic.title} (ID: {topic.id})")

                        if len(collected_topics) == limit:
                            break

                if len(collected_topics) == limit:
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
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        download_dir = os.path.join(base_dir, os.getenv('DOWNLOADS_DIR', 'downloads'), f"topic_{topic_id}")
        os.makedirs(download_dir, exist_ok=True)
        self.logger.info(f"Created topic folder: {download_dir}")
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

        self.logger.info(f"Text message found (ID: {message_data['id']}), saved to log.")

    async def download_topic_content(self, topic_id: int, photo_message_limit: int = 100) -> Dict[str, int]:
        """Download all text messages and photos from a specific Telegram topic.

        Args:
            topic_id: ID of the topic to download from.
            photo_message_limit: Maximum number of messages to process for photo downloads.

        Returns:
            Dict with counts: {'text_messages': X, 'photos': Y}
        """
        try:
            topic_dir = self._create_topic_folder(topic_id)
            last_msg_id = self.db.get_last_msg_id(topic_id)
            
            # Fetch topic title early to ensure the topic exists in the DB before messages are saved
            topic_title = await self.get_topic_title_by_id(topic_id)
            title = topic_title or f"Topic {topic_id}"
            
            # If it's a new topic, create the record now to satisfy FK constraints in processed_messages
            if last_msg_id is None:
                self.db.update_topic(topic_id, title, 0)
                self.logger.info(f"🆕 New topic {topic_id} ('{title}') initialized in DB")

            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            photos_count = 0
            text_count = 0
            max_message_id = 0

            self.logger.info(f"Starting topic {topic_id} download... (last_msg_id: {last_msg_id})")

            iter_params = {
                'entity': chat,
                'reply_to': topic_id
            }

            if last_msg_id is not None:
                iter_params['min_id'] = last_msg_id
                self.logger.info(f"Using min_id={last_msg_id} to download only new messages")

            # Optimization: fetch all processed IDs for this topic in one query
            processed_msg_ids = self.db.get_processed_message_ids(topic_id)

            messages_iterated = 0
            async for message in self.client.iter_messages(**iter_params):
                messages_iterated += 1
                message_data = {
                    'id': message.id,
                    'date': message.date,
                    'text': message.text,
                    'media_group_id': getattr(message, 'grouped_id', None),
                    'has_media': bool(message.media),
                    'media_type': None
                }

                if message.id > max_message_id:
                    max_message_id = message.id

                # Handle text content
                if message.text:
                    if message.id in processed_msg_ids:
                        self.logger.info(f"Text message {message.id} already processed, skipping")
                        continue

                    self._save_message_to_log(topic_dir, message_data)
                    text_count += 1
                    self.db.save_message(message.id, topic_id, 'text')
                    processed_msg_ids.add(message.id)

                # Handle media content
                if messages_iterated <= photo_message_limit and message.media and isinstance(message.media, MessageMediaPhoto):
                    if message.id in processed_msg_ids:
                        self.logger.info(f"Message {message.id} already processed, skipping")
                        continue

                    self.logger.info(f"Downloading photo (ID: {message.id})...")

                    file_name = f"photo_{message.id}.jpg"
                    file_path = os.path.join(topic_dir, file_name)

                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            downloaded_path = await self.client.download_media(
                                message,
                                file=file_path
                            )

                            if downloaded_path:
                                photos_count += 1
                                self.logger.info(f"Photo downloaded successfully: {downloaded_path}")
                                self.db.save_message(message.id, topic_id, 'photo', downloaded_path)
                                processed_msg_ids.add(message.id)

                            await asyncio.sleep(0.5)
                            break

                        except FloodWaitError as e:
                            self.logger.warning(f"Flood wait (attempt {attempt+1}/{max_retries}): {e.seconds}s")
                            await asyncio.sleep(e.seconds)
                            if attempt == max_retries - 1:
                                self.logger.error(f"Max retries exceeded downloading photo: {message.id}")

                await asyncio.sleep(0.2)

            if max_message_id > 0:
                self.db.update_topic(topic_id, title, max_message_id)
                self.logger.info(f"DB updated: topic_id={topic_id}, last_msg_id={max_message_id}")

            self.logger.info(f"Topic download complete. Downloaded {photos_count} photos and {text_count} text messages.")

            # DEBUG: check DB visibility from the same thread that wrote the data
            debug_count = len(self.db.get_unprocessed_files(topic_id))
            self.logger.info(f"DEBUG [same thread]: unprocessed files in DB right after download = {debug_count}")

            return {
                'text_messages': text_count,
                'photos': photos_count
            }

        except Exception as e:
            self.logger.error(f"Error downloading topic {topic_id}: {e}")
            raise

    @with_retry(max_retries=3)
    async def get_topic_title_by_id(self, topic_id: int) -> Optional[str]:
        """Get topic title by its ID, with caching."""
        now = time.time()

        # Check cache
        if topic_id in self._topic_cache:
            title, expiry = self._topic_cache[topic_id]
            if now < expiry:
                self._topic_cache.move_to_end(topic_id)
                return title
            else:
                del self._topic_cache[topic_id]

        try:
            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            topics_result = await self.client(GetForumTopicsRequest(
                peer=chat,
                q='',
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=100
            ))

            # Cache all fetched topics for 1 hour (3600 seconds)
            for topic in topics_result.topics:
                self._topic_cache[topic.id] = (topic.title, now + TOPIC_CACHE_TTL)
                self._topic_cache.move_to_end(topic.id)

            # Evict oldest entries if cache exceeds max size
            while len(self._topic_cache) > MAX_TOPIC_CACHE_SIZE:
                self._topic_cache.popitem(last=False)

            if topic_id in self._topic_cache:
                return self._topic_cache[topic_id][0]

            return None
        except Exception as e:
            self.logger.error(f"Error getting topic title for {topic_id}: {e}")
            return None

    @with_retry(max_retries=3)
    async def find_main_topic_for_vdo(self, site_id: str) -> Optional[int]:
        """Find the main topic (without VDO/TSS/TSSR) for the given site_id."""
        if not site_id:
            return None

        try:
            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            result = await self.client(GetForumTopicsRequest(
                peer=chat,
                q=site_id,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=100
            ))

            if not getattr(result, 'topics', []):
                return None

            for topic in result.topics:
                title_upper = topic.title.upper()
                if site_id.upper() in title_upper and 'VDO' not in title_upper and 'ВДО' not in title_upper and 'TSS' not in title_upper and 'TSSR' not in title_upper:
                    self.logger.info(f"✅ Found main topic for VDO (site {site_id}): {topic.title} (ID: {topic.id})")
                    return topic.id

            return None
        except Exception as e:
            self.logger.error(f"Error searching main topic for VDO (site_id={site_id}): {e}")
            return None

    @with_retry(max_retries=3)
    async def find_vdo_topic_for_main(self, site_id: str) -> Optional[int]:
        """Find the VDO topic (with VDO/ВДО) for the given site_id."""
        if not site_id:
            return None

        try:
            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            result = await self.client(GetForumTopicsRequest(
                peer=chat,
                q=site_id,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=100
            ))

            if not getattr(result, 'topics', []):
                return None

            for topic in result.topics:
                title_upper = topic.title.upper()
                if site_id.upper() in title_upper and ('VDO' in title_upper or 'ВДО' in title_upper) and 'TSS' not in title_upper and 'TSSR' not in title_upper:
                    self.logger.info(f"✅ Found VDO twin for main topic (site {site_id}): {topic.title} (ID: {topic.id})")
                    return topic.id

            return None
        except Exception as e:
            self.logger.error(f"Error searching VDO twin for main topic (site_id={site_id}): {e}")
            return None

    @with_retry(max_retries=3)
    async def download_topic_photos_to_dir(self, topic_id: int, target_dir: str, photo_message_limit: int = 100) -> int:
        """Download all photos from a topic directly to a specified directory.

        Args:
            topic_id: ID of the topic to download photos from.
            target_dir: Directory to save photos to.
            photo_message_limit: Maximum number of messages to scan.

        Returns:
            Number of photos downloaded.
        """
        os.makedirs(target_dir, exist_ok=True)
        chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
        photos_count = 0
        messages_iterated = 0

        async for message in self.client.iter_messages(entity=chat, reply_to=topic_id):
            messages_iterated += 1
            if messages_iterated > photo_message_limit:
                break

            if message.media and isinstance(message.media, MessageMediaPhoto):
                file_name = f"main_photo_{message.id}.jpg"
                file_path = os.path.join(target_dir, file_name)

                try:
                    downloaded_path = await self.client.download_media(message, file=file_path)
                    if downloaded_path:
                        photos_count += 1
                        self.logger.info(f"📸 Main topic photo downloaded: {file_name}")
                    await asyncio.sleep(0.5)
                except FloodWaitError as e:
                    self.logger.warning(f"FloodWait downloading main topic photo: {e.seconds}s")
                    await asyncio.sleep(e.seconds)

            await asyncio.sleep(0.2)

        self.logger.info(f"📸 Downloaded {photos_count} photos from main topic {topic_id}")
        return photos_count

    @with_retry(max_retries=3)
    async def get_topic_text_log(self, topic_id: int) -> str:
        """Download text messages from the specified topic, chronologically sorted."""
        try:
            chat = await self.client.get_entity(int(os.getenv('TARGET_CHAT_ID')))
            messages_text = []

            # reverse=True fetches older messages first
            async for message in self.client.iter_messages(entity=chat, reply_to=topic_id, reverse=True):
                if message.text:
                    timestamp = message.date.strftime('%Y-%m-%d %H:%M:%S')
                    log_entry = f"[{timestamp}] | [{message.id}] | {message.text}\n{'='*50}\n"
                    messages_text.append(log_entry)

            self.logger.info(f"Collected {len(messages_text)} text messages from topic {topic_id}")
            return "".join(messages_text)

        except Exception as e:
            self.logger.error(f"Error collecting text messages from topic {topic_id}: {e}")
            return ""

    def should_process_topic(self, topic_title: str) -> bool:
        """Check whether this topic should be processed (matches UA/UB, not TSS/TSSR)."""
        if not topic_title:
            return False

        title_upper = topic_title.upper()
        return ('UA' in title_upper or 'UB' in title_upper) and ('TSS' not in title_upper and 'TSSR' not in title_upper)

    async def reset_topic_timer(self, topic_id: int, topic_title: str) -> None:
        """Reset or create a timer for the given topic."""
        async with self._active_topics_lock:
            if topic_id in self.active_topics:
                self.active_topics[topic_id]['last_message_time'] = datetime.now()
                self.logger.info(f"🔄 Timer reset for topic {topic_title} (ID: {topic_id})")
                return

            self.active_topics[topic_id] = {
                'last_message_time': datetime.now(),
                'timer_task': None,
                'message_count': 1,
                'title': topic_title
            }

            self.logger.info(f"🆕 Activity in topic {topic_title} (ID: {topic_id}), waiting for upload to finish...")

            task = asyncio.create_task(self._topic_timer_handler(topic_id))
            self.active_topics[topic_id]['timer_task'] = task

    async def _topic_timer_handler(self, topic_id: int) -> None:
        """Timer handler for a topic — waits for inactivity, then queues processing."""
        try:
            while True:
                async with self._active_topics_lock:
                    if topic_id not in self.active_topics:
                        return

                    last_message_time = self.active_topics[topic_id]['last_message_time']
                    time_since_last = datetime.now() - last_message_time
                    wait_duration = timedelta(seconds=self.wait_time)

                    if time_since_last >= wait_duration:
                        topic_title = self.active_topics[topic_id]['title']
                        self.logger.info(f"⏰ Timer expired for topic {topic_title} (ID: {topic_id}), starting data collection")

                        # Atomically remove from active and add to queue
                        del self.active_topics[topic_id]
                        await self.add_topic_to_queue(topic_id, topic_title=topic_title)
                        break
                    else:
                        sleep_time = (wait_duration - time_since_last).total_seconds()

                # Wait remaining time outside the lock
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            self.logger.debug(f"Timer cancelled for topic {topic_id}")
        except Exception as e:
            self.logger.error(f"Error in timer handler for topic {topic_id}: {e}")


    async def add_topic_to_queue(self, topic_id: int, topic_title: Optional[str] = None) -> None:
        """Add topic to the processing queue instead of processing immediately."""
        try:
            if not topic_title:
                topic_title = await self.get_topic_title_by_id(topic_id)
            if not topic_title:
                self.logger.error(f"Could not get title for topic {topic_id}")
                return

            await self.task_queue.add_topic_task(topic_id, topic_title)

        except Exception as e:
            self.logger.error(f"Error adding topic {topic_id} to queue: {e}")

    async def run_startup_recovery(self) -> None:
        """Recover unfinished tasks on bot startup."""
        try:
            self.logger.info("🔄 Starting crash recovery procedure...")

            recovery_topics = set()

            # AI recovery: find topics with unprocessed files
            unprocessed_topics = self.db.get_topics_with_unprocessed_files()
            if unprocessed_topics:
                recovery_topics.update(unprocessed_topics)
                self.logger.info(f"🔍 Found {len(unprocessed_topics)} topics with unprocessed AI files: {unprocessed_topics}")
            else:
                self.logger.info("✅ No topics with unprocessed AI files found")

            # Download recovery: check recent active topics
            recent_topics = await self.get_chat_topics(limit=5)
            if recent_topics:
                recent_topic_ids = [topic['topic_id'] for topic in recent_topics]
                recovery_topics.update(recent_topic_ids)
                self.logger.info(f"🔍 Checking {len(recent_topics)} recent active topic(s) for missed messages: {recent_topic_ids}")
            else:
                self.logger.info("✅ No recent active topics found")

            if recovery_topics:
                self.logger.info(f"📋 Adding {len(recovery_topics)} topics to recovery queue: {list(recovery_topics)}")
                for topic_id in recovery_topics:
                    await self.add_topic_to_queue(topic_id)
            else:
                self.logger.info("✅ No topics to recover")

            self.logger.info("🎉 Recovery procedure completed")

        except Exception as e:
            self.logger.error(f"❌ Error in recovery procedure: {e}")

    def setup_event_handler(self, process_callback: Any = None) -> None:
        """Set up the event handler for new messages."""

        @self.client.on(events.NewMessage(chats=int(os.getenv('TARGET_CHAT_ID'))))
        async def new_message_handler(event):
            try:
                message = event.message

                # Skip messages not in topics (no reply_to)
                if not message.reply_to:
                    return

                topic_id = message.reply_to.reply_to_msg_id
                if not topic_id:
                    return

                topic_title = await self.get_topic_title_by_id(topic_id)
                if not topic_title:
                    return

                if not self.should_process_topic(topic_title):
                    self.logger.debug(f"🚫 Topic '{topic_title}' ignored (not UA/UB)")
                    return

                self.logger.debug(f"📨 New message in topic '{topic_title}' (ID: {topic_id})")

                await self.reset_topic_timer(topic_id, topic_title)

            except Exception as e:
                self.logger.exception("Critical error in Telegram new_message_handler")
                try:
                    # User-facing alert (Russian)
                    await self.send_message_with_retry('me', f"🚨 Ошибка в `new_message_handler`:\n`{str(e)}`")
                except Exception as alert_error:
                    self.logger.error(f"Failed to send error alert: {alert_error}")

        self.logger.info("🎯 Event handler configured for tracking new messages")

    def _write_heartbeat(self) -> None:
        """Write a marker file for Docker HEALTHCHECK."""
        try:
            heartbeat_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'healthcheck')
            with open(heartbeat_path, 'w') as f:
                f.write(str(asyncio.get_event_loop().time()))
        except Exception:  # nosec B110 - heartbeat failure is non-critical
            pass

    async def _healthcheck_loop(self) -> None:
        """Periodically send healthcheck messages and update the heartbeat file."""
        healthcheck_interval = int(os.getenv('HEALTHCHECK_INTERVAL', str(DEFAULT_HEALTHCHECK_INTERVAL)))
        heartbeat_interval = HEARTBEAT_INTERVAL
        time_since_last_report = 0
        consecutive_failures = 0
        while True:
            try:
                await asyncio.sleep(heartbeat_interval)
                self._write_heartbeat()
                time_since_last_report += heartbeat_interval

                # Send Telegram report at the configured interval
                if time_since_last_report >= healthcheck_interval:
                    time_since_last_report = 0
                    topics = self.db.get_all_topics()
                    count = len(topics) if topics else 0
                    metrics = self.task_queue.get_metrics_summary()
                    # User-facing message (Russian)
                    message = f"🟢 Бот жив. Топиков в БД: {count}.\n📊 {metrics}"
                    self.logger.info(f"Sending healthcheck: {message}")
                    await self.send_message_with_retry('me', message)
                    consecutive_failures = 0
            except asyncio.CancelledError:
                self.logger.debug("Healthcheck loop cancelled")
                break
            except Exception as e:
                consecutive_failures += 1
                self.logger.error(f"Error in healthcheck loop ({consecutive_failures}/{MAX_HEALTHCHECK_FAILURES}): {e}")
                if consecutive_failures >= MAX_HEALTHCHECK_FAILURES:
                    self.logger.critical(
                        f"Healthcheck failed {consecutive_failures} times in a row! "
                        "Bot may be in a broken state."
                    )
                    try:
                        await self.send_message_with_retry(
                            'me',
                            f"🚨 Healthcheck не работает уже {consecutive_failures} раз подряд! Проверьте бота."
                        )
                    except Exception as alert_err:
                        self.logger.debug(f"Failed to send healthcheck alert: {alert_err}")
                    consecutive_failures = 0
                await asyncio.sleep(60)

    async def start_autonomous_mode(self) -> None:
        """Start autonomous 24/7 operation mode."""
        self.logger.info("🚀 Starting autonomous mode...")
        self.setup_event_handler()

        # Set the event loop on the task queue
        self.task_queue.main_loop = asyncio.get_running_loop()

        # Start the consumer for queue processing in background
        asyncio.create_task(self.task_queue.start_consumer())

        # Start healthcheck loop
        healthcheck_task = asyncio.create_task(self._healthcheck_loop())

        try:
            await self.run_startup_recovery()
            self.logger.info("🔄 Recovery complete, switching to message listening mode...")

            self.logger.info("👂 Listening for messages 24/7...")
            self.logger.info(f"⚙️ Processing queue running with {self.task_queue.max_workers} workers")

            await self.client.run_until_disconnected()

        except KeyboardInterrupt:
            self.logger.info("⏹️ Stopped by user request")
        except Exception as e:
            self.logger.error(f"Error in autonomous mode: {e}")
        finally:
            healthcheck_task.cancel()
            await self.task_queue.shutdown()
            await self.disconnect()

    @with_retry(max_retries=3)
    async def send_message_with_retry(self, entity, message, **kwargs):
        """Send a message with retry logic (FloodWait)."""
        return await self.client.send_message(entity, message, **kwargs)

    @with_retry(max_retries=3)
    async def send_file_with_retry(self, entity, file, **kwargs):
        """Send a file with retry logic (FloodWait)."""
        return await self.client.send_file(entity, file, **kwargs)
