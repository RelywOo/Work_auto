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
    """Main entry point for Telegram bot - autonomous mode only."""
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
    
    # Запускаем автономный режим 24/7
    logger.info("🚀 Запускаю автономный режим 24/7...")
    logger.info(f"🎯 Отслеживаю чат {target_chat_id}")
    logger.info(f"⏰ Время ожидания загрузки: {telegram_client.wait_time} секунд")
    logger.info("🔍 Фильтрую только UA/UB топики (без TSS/TSSR)")
    
    try:
        await telegram_client.start_autonomous_mode()
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
