import asyncio
import logging
import os
import signal
from dotenv import load_dotenv

from modules.telegram_client import TelegramClientManager

# Load environment variables
load_dotenv()

import logging.handlers

# Configure logging with absolute paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(os.path.join(LOG_DIR, 'bot.log'), maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def validate_env():
    """Проверяет наличие и корректность обязательных переменных окружения."""
    required_vars = [
        'TELEGRAM_API_ID', 
        'TELEGRAM_API_HASH', 
        'TARGET_CHAT_ID',
        'GEMINI_API_KEY'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        error_msg = f"КРИТИЧЕСКАЯ ОШИБКА: Отсутствуют обязательные переменные: {', '.join(missing_vars)}. Проверьте .env файл."
        logger.error(error_msg)
        raise EnvironmentError(error_msg)
        
    try:
        int(os.getenv('TELEGRAM_API_ID'))
    except (TypeError, ValueError):
        error_msg = "КРИТИЧЕСКАЯ ОШИБКА: TELEGRAM_API_ID должен быть целым числом."
        logger.error(error_msg)
        raise EnvironmentError(error_msg)
        
    try:
        int(os.getenv('TARGET_CHAT_ID'))
    except (TypeError, ValueError):
        error_msg = "КРИТИЧЕСКАЯ ОШИБКА: TARGET_CHAT_ID должен быть целым числом."
        logger.error(error_msg)
        raise EnvironmentError(error_msg)

async def main():
    """Main entry point for Telegram bot - autonomous mode only."""
    # Валидация переменных окружения ДО инициализации
    validate_env()
    
    # Initialize Telegram client
    telegram_client = TelegramClientManager()
    
    # Setup graceful shutdown handlers
    loop = asyncio.get_running_loop()
    
    def handle_signal(sig):
        logger.info(f"🛑 Получен сигнал {sig}. Начинаю graceful shutdown...")
        if telegram_client.client.is_connected():
            asyncio.create_task(telegram_client.client.disconnect())

    try:
        if os.name == 'nt':
            signal.signal(signal.SIGINT, lambda s, f: loop.call_soon_threadsafe(handle_signal, s))
            signal.signal(signal.SIGTERM, lambda s, f: loop.call_soon_threadsafe(handle_signal, s))
        else:
            loop.add_signal_handler(signal.SIGINT, lambda: handle_signal(signal.SIGINT))
            loop.add_signal_handler(signal.SIGTERM, lambda: handle_signal(signal.SIGTERM))
    except Exception as e:
        logger.warning(f"Signals setup failed: {e}")
        
    # Connect to Telegram
    await telegram_client.connect()
    
    logger.info("Successfully connected to Telegram!")
    
    target_chat_id = os.getenv('TARGET_CHAT_ID')
    
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
