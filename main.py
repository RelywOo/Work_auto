import asyncio
import logging
import os
from dotenv import load_dotenv

from modules.telegram_client import TelegramClientManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def main():
    """Main entry point for Telegram authentication and topic listing."""
    logger.info("Starting Telegram authentication...")
    
    try:
        # Initialize Telegram client
        telegram_client = TelegramClientManager()
        
        # Connect to Telegram
        await telegram_client.connect()
        
        logger.info("Successfully connected to Telegram!")
        
        # Get target chat ID from environment
        target_chat_id = os.getenv('TARGET_CHAT_ID')
        if not target_chat_id:
            logger.error("TARGET_CHAT_ID not found in environment variables")
            return
        
        logger.info(f"Fetching latest topics from chat {target_chat_id}...")
        
        # Get topics from the supergroup
        topics = await telegram_client.get_chat_topics()
        
        if not topics:
            logger.warning("No topics found in the chat")
            return
        
        # Sort topics by date (most recent first) and take latest 1
        topics.sort(key=lambda x: x['date'], reverse=True)
        latest_topics = topics[:1]
        
        print('\n' + '='*60)
        print('📋 LATEST 1 TOPIC FROM SUPERGROUP')
        print('='*60)
        
        for i, topic in enumerate(latest_topics, 1):
            print(f"\n{i}. 📌 Topic ID: {topic['topic_id']}")
            print(f"   📝 Title: {topic['title']}")
            print(f"   📅 Created: {topic['date']}")
        
        print('\n' + '='*60)
        print('Total topics found: ' + str(len(topics)))
        print('Showing latest 1 topic')
        print('='*60)
        
        logger.info("Successfully retrieved and displayed topics")
        
        # Automatically download content from the latest topic
        if latest_topics:
            latest_topic = latest_topics[0]
            topic_id = latest_topic['topic_id']
            topic_title = latest_topic['title']
            
            print(f"\n🚀 НАЧИНАЮ ВЫГРУЗКУ КОНТЕНТА ИЗ ТОПИКА:")
            print(f"📌 Топик: {topic_title} (ID: {topic_id})")
            print('='*60)
            
            # Download content from the topic
            result = await telegram_client.download_topic_content(
                topic_id=topic_id,
                message_limit=50  # Ограничим для первого теста
            )
            
            print(f"\n✅ ВЫГРУЗКА ЗАВЕРШЕНА!")
            print(f"📄 Текстовых сообщений: {result['text_messages']}")
            print(f"📸 Фотографий: {result['photos']}")
            print(f"📁 Папка с файлами: downloads/topic_{topic_id}")
            print('='*60)
            
            logger.info(f"Topic download completed: {result}")
        else:
            logger.warning("No topics available for download")
            return
        
        # Disconnect after completion
        logger.info("Work completed. Disconnecting...")
        await telegram_client.disconnect()
            
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
