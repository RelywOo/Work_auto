import os
import re
import shutil
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

def extract_site_id(topic_title: str) -> str:
    """Извлекает чистый ID сайта из названия топика с помощью regex."""
    match = re.search(r'(UA|UB)\d+', topic_title)
    if match:
        return match.group(0)
    return f"topic_{topic_title}" if topic_title else "unknown"

def create_readme_file(topic_dir: str, equipment_lists: Dict[str, List[str]], text_messages: List[str]) -> str:
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
                f.write(f"{msg}")
    
    return readme_path

def create_zip_report(site_id: str, source_folder: str, output_folder: str) -> str:
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
