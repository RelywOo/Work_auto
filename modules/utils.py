import os
import re
import shutil
import logging
import time
import stat
from typing import Dict, List

logger = logging.getLogger(__name__)

def extract_site_id(topic_title: str) -> str:
    """Извлекает чистый ID сайта из названия топика с помощью regex."""
    match = re.search(r'(UA|UB)\d+', topic_title)
    if match:
        return match.group(0)
    return f"topic_{topic_title}" if topic_title else "unknown"

def safe_rmtree(path: str, retries: int = 5, delay: float = 1.0):
    """
    Robust folder deletion with retries and permission handling (for Windows WinError 5).
    """
    if not os.path.exists(path):
        return

    def on_error(func, path, exc_info):
        """Error handler for shutil.rmtree to change file permissions."""
        os.chmod(path, stat.S_IWRITE)
        func(path)

    for i in range(retries):
        try:
            # Attempt to delete the directory tree
            shutil.rmtree(path, onerror=on_error)
            logger.info(f"✅ Успешно удалена папка: {path}")
            return
        except Exception as e:
            if i < retries - 1:
                logger.warning(f"⚠️ Попытка {i+1} удалить {path} не удалась ({e}). Ждем {delay}с...")
                time.sleep(delay)
            else:
                logger.error(f"❌ Не удалось окончательно удалить папку {path} после {retries} попыток: {e}")

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

def create_zip_report(site_id: str, source_folder: str, output_folder: str, topic_title: str) -> str:
    """Создает ZIP-архив с фото из папки Demontaj и README.txt."""
    # Создаем выходную папку если ее нет
    os.makedirs(output_folder, exist_ok=True)
    
    # Решение: Добавлять суффикс в зависимости от типа топика
    topic_title_upper = topic_title.upper()
    suffix = "_VDO" if "VDO" in topic_title_upper or "ВДО" in topic_title_upper else "_MAIN"
    
    # Путь к архиву
    zip_path = os.path.join(output_folder, f"{site_id}{suffix}")
    
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
        safe_rmtree(temp_folder)
        
        logger.info(f"ZIP-архив создан: {zip_path}.zip")
        return f"{zip_path}.zip"
        
    except Exception as e:
        # Удаляем временную папку в случае ошибки
        if os.path.exists(temp_folder):
            safe_rmtree(temp_folder)
        logger.error(f"Ошибка при создании ZIP-архива: {e}")
        raise
