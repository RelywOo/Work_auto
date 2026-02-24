import os
import asyncio
import time
from typing import List, Dict, Any, Optional, Tuple
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChatAdminRequiredError, SessionPasswordNeededError
from datetime import datetime, timedelta 
import logging
from telethon.tl.functions.messages import GetForumTopicsRequest

class TelegramClientManager:
    def __init__(self):
        self.client = TelegramClient(
            os.getenv('SESSION_FILE', 'session_name.session'),
            int(os.getenv('TELEGRAM_API_ID')),
            os.getenv('TELEGRAM_API_HASH')
        )
        self.logger = logging.getLogger(__name__)
    
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
            
            self.logger.info("Searching for 10 latest UA/UB topics...")
            
            while len(collected_topics) < 7:
                from telethon.tl.functions.messages import GetForumTopicsRequest
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
                    if 'UA' in title_upper or 'UB' in title_upper:
                        topic_info = {
                            'topic_id': topic.id,
                            'title': topic.title,
                            'date': getattr(topic, 'date', None) 
                        }
                        collected_topics.append(topic_info)
                        # Пишем в лог, что мы нашли подходящий топик!
                        self.logger.info(f"✅ Найден топик: {topic.title} (ID: {topic.id})")
                        
                        if len(collected_topics) == 7:
                            break
                            
                if len(collected_topics) == 7:
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
    
    async def get_topic_messages(self, topic_id: int, days_back: int = None) -> List[Dict[str, Any]]:
        """Get messages from a specific topic within the specified time range."""
        if days_back is None:
            days_back = int(os.getenv('DAYS_TO_PROCESS', '7'))
        
        try:
            chat = await self.client.get_entity(os.getenv('TARGET_CHAT_ID'))
            topic_entity = await self.client.get_entity(os.getenv('TARGET_CHAT_ID'))
            
            # Get messages from the topic
            messages = []
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            async for message in self.client.iter_messages(
                chat,
                reply_to=topic_id,
                offset_date=cutoff_date
            ):
                if message.date < cutoff_date:
                    break
                
                message_data = {
                    'id': message.id,
                    'date': message.date,
                    'text': message.text,
                    'media_group_id': getattr(message, 'grouped_id', None),
                    'has_media': bool(message.media),
                    'media_type': None
                }
                
                # Determine media type
                if message.media:
                    if isinstance(message.media, MessageMediaPhoto):
                        message_data['media_type'] = 'photo'
                    elif isinstance(message.media, MessageMediaDocument):
                        message_data['media_type'] = 'document'
                
                messages.append(message_data)
                
                # Rate limiting
                await asyncio.sleep(float(os.getenv('RATE_LIMIT_DELAY', '1.0')))
            
            self.logger.info(f"Retrieved {len(messages)} messages from topic {topic_id}")
            return messages
            
        except FloodWaitError as e:
            self.logger.warning(f"Flood wait: {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await self.get_topic_messages(topic_id, days_back)
        except Exception as e:
            self.logger.error(f"Failed to get messages from topic {topic_id}: {e}")
            return []
    
    async def download_media(self, message_id: int, topic_id: int) -> List[str]:
        """Download media files from a message."""
        try:
            chat = await self.client.get_entity(os.getenv('TARGET_CHAT_ID'))
            message = await self.client.get_messages(chat, ids=message_id)
            
            if not message.media:
                return []
            
            # Create download directory for this topic
            download_dir = os.path.join(os.getenv('DOWNLOADS_DIR', 'downloads'), f"topic_{topic_id}")
            os.makedirs(download_dir, exist_ok=True)
            
            downloaded_files = []
            
            if message.media:
                file_name = f"msg_{message_id}_{int(time.time())}"
                file_path = await self.client.download_media(
                    message,
                    file=os.path.join(download_dir, file_name)
                )
                
                if file_path:
                    downloaded_files.append(file_path)
                    self.logger.info(f"Downloaded: {file_path}")
            
            await asyncio.sleep(float(os.getenv('RATE_LIMIT_DELAY', '1.0')))
            return downloaded_files
            
        except Exception as e:
            self.logger.error(f"Failed to download media from message {message_id}: {e}")
            return []
    
    async def get_topic_by_site_id(self, site_id: str) -> Optional[int]:
        """Find topic ID by site ID in topic title."""
        try:
            topics = await self.get_chat_topics()
            
            for topic in topics:
                if site_id.lower() in topic['title'].lower():
                    return topic['topic_id']
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to find topic for site ID {site_id}: {e}")
            return None
    
    def extract_site_id_from_topic(self, topic_title: str) -> Optional[str]:
        """Extract site ID from topic title."""
        # Simple extraction - look for patterns like "SITE123", "BS-456", etc.
        import re
        
        patterns = [
            r'(?:SITE|BS|BASE|STATION)[\s-]*([A-Z0-9]{3,})',
            r'([A-Z]{2,4}\d{3,})',
            r'(\d{4,})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, topic_title, re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        return None
