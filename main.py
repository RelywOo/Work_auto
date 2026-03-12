import asyncio
import logging
import logging.handlers
import os
import signal
from dotenv import load_dotenv

from modules.telegram_client import TelegramClientManager

# Load environment variables
load_dotenv()

# Configure logging with absolute paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            os.path.join(LOG_DIR, "bot.log"),
            maxBytes=5 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def validate_env() -> None:
    """Validate presence and correctness of required environment variables."""
    required_vars = [
        "TELEGRAM_API_ID",
        "TELEGRAM_API_HASH",
        "TARGET_CHAT_ID",
        "GEMINI_API_KEY",
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        error_msg = f"CRITICAL: Missing required env variables: {', '.join(missing_vars)}. Check your .env file."
        logger.error(error_msg)
        raise EnvironmentError(error_msg)

    try:
        int(os.getenv("TELEGRAM_API_ID"))
    except (TypeError, ValueError):
        error_msg = "CRITICAL: TELEGRAM_API_ID must be an integer."
        logger.error(error_msg)
        raise EnvironmentError(error_msg)

    try:
        int(os.getenv("TARGET_CHAT_ID"))
    except (TypeError, ValueError):
        error_msg = "CRITICAL: TARGET_CHAT_ID must be an integer."
        logger.error(error_msg)
        raise EnvironmentError(error_msg)

    # Validate WAIT_TIME if provided
    wait_time_str = os.getenv("WAIT_TIME")
    if wait_time_str is not None:
        try:
            wait_time = int(wait_time_str)
            if wait_time < 10 or wait_time > 3600:
                error_msg = "CRITICAL: WAIT_TIME must be between 10 and 3600 seconds."
                logger.error(error_msg)
                raise EnvironmentError(error_msg)
        except ValueError:
            error_msg = "CRITICAL: WAIT_TIME must be an integer."
            logger.error(error_msg)
            raise EnvironmentError(error_msg)

    # Validate MAX_WORKERS if provided
    max_workers_str = os.getenv("MAX_WORKERS")
    if max_workers_str is not None:
        try:
            max_workers = int(max_workers_str)
            if max_workers < 1 or max_workers > 8:
                error_msg = "CRITICAL: MAX_WORKERS must be between 1 and 8."
                logger.error(error_msg)
                raise EnvironmentError(error_msg)
        except ValueError:
            error_msg = "CRITICAL: MAX_WORKERS must be an integer."
            logger.error(error_msg)
            raise EnvironmentError(error_msg)


async def main() -> None:
    """Main entry point for Telegram bot - autonomous mode only."""
    # Validate env variables before initialization
    validate_env()

    telegram_client = TelegramClientManager()

    # Setup graceful shutdown handlers
    loop = asyncio.get_running_loop()

    def handle_signal(sig):
        logger.info(f"🛑 Received signal {sig}. Starting graceful shutdown...")
        if telegram_client.client.is_connected():
            asyncio.create_task(telegram_client.client.disconnect())

    try:
        if os.name == "nt":
            signal.signal(
                signal.SIGINT, lambda s, f: loop.call_soon_threadsafe(handle_signal, s)
            )
            signal.signal(
                signal.SIGTERM, lambda s, f: loop.call_soon_threadsafe(handle_signal, s)
            )
        else:
            loop.add_signal_handler(signal.SIGINT, lambda: handle_signal(signal.SIGINT))
            loop.add_signal_handler(
                signal.SIGTERM, lambda: handle_signal(signal.SIGTERM)
            )
    except Exception as e:
        logger.warning(f"Signals setup failed: {e}")

    await telegram_client.connect()

    logger.info("Successfully connected to Telegram!")

    target_chat_id = os.getenv("TARGET_CHAT_ID")

    # Start autonomous 24/7 mode
    logger.info("🚀 Starting autonomous 24/7 mode...")
    logger.info(f"🎯 Monitoring chat {target_chat_id}")
    logger.info(f"⏰ Upload wait time: {telegram_client.wait_time} seconds")
    logger.info("🔍 Filtering UA/UB topics only (excluding TSS/TSSR)")

    try:
        await telegram_client.start_autonomous_mode()
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
