import asyncio
import logging
import os
from dotenv import load_dotenv

from modules.telegram_client import TelegramClientManager
from modules.ai_processor import AIProcessor, organize_files_by_analysis

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
            
            print("\n🚀 НАЧИНАЮ ВЫГРУЗКУ КОНТЕНТА ИЗ ТОПИКА:")
            print(f"📌 Топик: {topic_title} (ID: {topic_id})")
            print('='*60)
            
            # Download content from the topic
            result = await telegram_client.download_topic_content(
                topic_id=topic_id,
                message_limit=50  # Ограничим для первого теста
            )
            
            print("\n✅ ВЫГРУЗКА ЗАВЕРШЕНА!")
            print("📄 Текстовых сообщений:", result['text_messages'])
            print("📸 Фотографий:", result['photos'])
            print(f"📁 Папка с файлами: downloads/topic_{topic_id}")
            print('='*60)
            
            logger.info(f"Topic download completed: {result}")
            
            # Запускаем ИИ-обработку если есть фотографии
            if result['photos'] > 0:
                print("\n🤖 НАЧИНАЮ ИИ-АНАЛИЗ...")
                print('='*60)
                
                try:
                    # Инициализируем ИИ-процессор
                    ai_processor = AIProcessor()
                    
                    # Путь к папке с файлами топика
                    topic_dir = os.path.join('downloads', f'topic_{topic_id}')
                    log_file_path = os.path.join(topic_dir, 'messages_log.txt')
                    
                    # Шаг 1: Извлекаем списки оборудования из лога
                    print("📋 Извлекаю списки оборудования из лога...")
                    equipment_lists = ai_processor.extract_equipment_lists(log_file_path)
                    
                    print("🔧 Найдено оборудования:")
                    print(f"   Демонтаж: {len(equipment_lists['demontaj'])} единиц")
                    print(f"   Монтаж: {len(equipment_lists['montaj'])} единиц")
                    
                    if equipment_lists['demontaj']:
                        print(f"   Список демонтажа: {', '.join(equipment_lists['demontaj'])}")
                    
                    # Шаг 2: Анализируем фотографии
                    if equipment_lists['demontaj']:
                        print("\n📸 Анализирую фотографии...")
                        analysis_results = ai_processor.process_photos_batch(
                            topic_dir, 
                            equipment_lists['demontaj'],
                            delay_seconds=2
                        )
                        
                        # Шаг 3: Организуем файлы
                        print("\n📁 Организую файлы по папкам...")
                        stats = organize_files_by_analysis(topic_dir, analysis_results)
                        
                        print("\n✅ ИИ-АНАЛИЗ ЗАВЕРШЕН!")
                        print("📊 Статистика:")
                        print(f"   Демонтаж: {stats['demontaj']} фото")
                        print(f"   Монтаж: {stats['montaj']} фото")
                        print(f"   Другое: {stats['other']} фото")
                        print('='*60)
                    else:
                        print("⚠️ Список демонтажа пуст, анализ фотографий не требуется")
                        
                except Exception as e:
                    logger.error(f"Ошибка при ИИ-обработке: {e}")
                    print(f"❌ Ошибка ИИ-обработки: {e}")
            else:
                print("⚠️ Фотографий для анализа не найдено")
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
