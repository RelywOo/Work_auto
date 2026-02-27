import asyncio
import logging
import os
import re
import shutil
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

def extract_site_id(topic_title):
    """Извлекает чистый ID сайта из названия топика с помощью regex."""
    match = re.search(r'(UA|UB)\d+', topic_title)
    if match:
        return match.group(0)
    return None

def create_readme_file(topic_dir, equipment_lists, text_messages):
    """Создает файл README.txt с отчетом о демонтаже/монтаже."""
    readme_path = os.path.join(topic_dir, 'README.txt')
    
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write("ОТЧЕТ О РАБОТАХ ПО САЙТУ\n")
        f.write("=" * 40 + "\n\n")
        
        if equipment_lists['demontaj']:
            f.write("ОБОРУДОВАНИЕ НА ДЕМОНТАЖ:\n")
            for item in equipment_lists['demontaj']:
                f.write(f"- {item}\n")
            f.write("\n")
        
        if equipment_lists['montaj']:
            f.write("ОБОРУДОВАНИЕ НА МОНТАЖ:\n")
            for item in equipment_lists['montaj']:
                f.write(f"- {item}\n")
            f.write("\n")
        
        if text_messages:
            f.write("ПОЛНЫЙ ТЕКСТ СООБЩЕНИЯ:\n")
            f.write("-" * 30 + "\n")
            for msg in text_messages:
                f.write(f"{msg}\n")
    
    return readme_path

def create_zip_report(site_id, source_folder, output_folder):
    """Создает ZIP-архив с фото из папки Demontaj и README.txt."""
    # Создаем выходную папку если ее нет
    os.makedirs(output_folder, exist_ok=True)
    
    # Путь к архиву
    zip_path = os.path.join(output_folder, f"{site_id}")
    
    # Временная папка для содержимого архива
    temp_folder = os.path.join(source_folder, "temp_zip_content")
    os.makedirs(temp_folder, exist_ok=True)
    
    try:
        # Копируем папку Demontaj если она существует
        demontaj_folder = os.path.join(source_folder, "Demontaj")
        if os.path.exists(demontaj_folder):
            shutil.copytree(demontaj_folder, os.path.join(temp_folder, "Demontaj"))
        
        # Копируем README.txt если он существует
        readme_path = os.path.join(source_folder, "README.txt")
        if os.path.exists(readme_path):
            shutil.copy2(readme_path, temp_folder)
        
        # Создаем ZIP-архив
        shutil.make_archive(zip_path, 'zip', temp_folder)
        
        # Удаляем временную папку
        shutil.rmtree(temp_folder)
        
        logger.info(f"ZIP-архив создан: {zip_path}.zip")
        return f"{zip_path}.zip"
        
    except Exception as e:
        # Удаляем временную папку в случае ошибки
        if os.path.exists(temp_folder):
            shutil.rmtree(temp_folder)
        logger.error(f"Ошибка при создании ZIP-архива: {e}")
        raise

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
            
            # Извлекаем чистый ID сайта из названия топика
            site_id = extract_site_id(topic_title)
            if not site_id:
                logger.error(f"Не удалось извлечь ID сайта из названия: {topic_title}")
                return
            
            print("\n🚀 НАЧИНАЮ ВЫГРУЗКУ КОНТЕНТА ИЗ ТОПИКА:")
            print(f"📌 Топик: {topic_title} (ID: {topic_id})")
            print(f"🏷️ ID сайта: {site_id}")
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
                    
                    # Создаем README.txt файл с отчетом
                    print("\n📝 Создаю README.txt с отчетом...")
                    
                    # Читаем реальные текстовые сообщения из лога
                    text_messages = []
                    try:
                        with open(log_file_path, 'r', encoding='utf-8') as f:
                            text_messages = f.readlines()
                    except Exception as e:
                        logger.warning(f"Не удалось прочитать текстовые сообщения: {e}")
                    
                    readme_path = create_readme_file(topic_dir, equipment_lists, text_messages)
                    print(f"✅ README.txt создан: {readme_path}")
                    
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
                        
                        # Создаем ZIP-архив с результатами
                        print("\n📦 Создаю ZIP-архив с отчетом...")
                        output_folder = "output"
                        zip_path = create_zip_report(site_id, topic_dir, output_folder)
                        print(f"✅ ZIP-архив создан: {zip_path}")
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
