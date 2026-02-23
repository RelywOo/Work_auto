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
        
        # Sort topics by date (most recent first) and take latest 7
        topics.sort(key=lambda x: x['date'], reverse=True)
        latest_topics = topics[:7]
        
        print(f"\n{'='*60}")
        print(f"📋 LATEST 7 TOPICS FROM SUPERGROUP")
        print(f"{'='*60}")
        
        for i, topic in enumerate(latest_topics, 1):
            print(f"\n{i}. 📌 Topic ID: {topic['topic_id']}")
            print(f"   📝 Title: {topic['title']}")
            print(f"   📅 Created: {topic['date']}")
        
        print(f"\n{'='*60}")
        print(f"Total topics found: {len(topics)}")
        print(f"Showing latest 7 topics")
        print(f"{'='*60}")
        
        logger.info("Successfully retrieved and displayed topics")
        
        # Keep the connection open for testing
        print("\nPress Ctrl+C to disconnect...")
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Disconnecting...")
            await telegram_client.disconnect()
            
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
