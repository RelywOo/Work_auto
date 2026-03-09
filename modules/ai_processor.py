import os
import json
import time
import logging
import shutil
from typing import Dict, List, Any
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted
from PIL import Image

class AIProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.api_key = os.getenv('GEMINI_API_KEY')
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        genai.configure(api_key=self.api_key)
        self.text_model = genai.GenerativeModel('gemini-flash-latest')
        self.vision_model = genai.GenerativeModel('gemini-flash-latest')
        
        self.logger.info("AI Processor initialized with Gemini API")
    
    def _generate_with_retry(self, model, *args, **kwargs):
        """Обертка для вызова API с retry при Rate Limits (429)."""
        max_retries = 5
        base_delay = 2.0
        for attempt in range(max_retries):
            try:
                return model.generate_content(*args, **kwargs)
            except ResourceExhausted as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Rate limit exceeded after {max_retries} retries: {e}")
                    raise
                delay = base_delay * (2 ** attempt)
                self.logger.warning(f"Rate limit hit. Retrying in {delay}s (Attempt {attempt + 1}/{max_retries})...")
                time.sleep(delay)
    def extract_equipment_lists(self, log_file_path: str) -> Dict[str, List[str]]:
        """
        Извлекает списки оборудования для монтажа и демонтажа из лога сообщений.
        
        Args:
            log_file_path: Путь к файлу messages_log.txt
            
        Returns:
            Dict с ключами 'demontaj' и 'montaj', содержащий списки оборудования
        """
        try:
            # Читаем лог файл
            with open(log_file_path, 'r', encoding='utf-8') as f:
                log_content = f.read()
            
            # Промпт для извлечения списков оборудования
            prompt = """Ты — технический ассистент. Проанализируй предоставленный лог сообщений из Telegram-чата инженеров. 
            Найди сообщение, в котором перечисляется оборудование для монтажа (Montaj) и демонтажа (Demontaj). 
            Извлеки эти данные и верни СТРОГО в формате JSON. 
            Пример ответа: {"demontaj": ["Anten T1003M6R011", "DCDU12B"], "montaj": ["RRU 5516"]}. 
            Если списков нет, верни пустые массивы."""
            
            # Отправляем запрос к Gemini с retry
            response = self._generate_with_retry(
                self.text_model,
                prompt + "\n\nЛог сообщений:\n" + log_content,
                generation_config=genai.types.GenerationConfig(
                    response_mime_type="application/json",
                    temperature=0.1,
                )
            )
            
            # Парсим JSON ответ
            result = json.loads(response.text)
            
            # Валидация структуры
            if not isinstance(result, dict):
                raise ValueError("Invalid response format")
            
            if 'demontaj' not in result:
                result['demontaj'] = []
            if 'montaj' not in result:
                result['montaj'] = []
            
            # Убедимся, что это списки строк
            result['demontaj'] = [str(item).strip() for item in result['demontaj'] if item]
            result['montaj'] = [str(item).strip() for item in result['montaj'] if item]
            
            self.logger.info(f"Извлечено списков: демонтаж - {len(result['demontaj'])}, монтаж - {len(result['montaj'])}")
            return result
            
        except FileNotFoundError:
            self.logger.error(f"Лог файл не найден: {log_file_path}")
            return {"demontaj": [], "montaj": []}
        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка парсинга JSON ответа: {e}")
            return {"demontaj": [], "montaj": []}
        except Exception as e:
            self.logger.error(f"Ошибка при извлечении списков оборудования: {e}")
            return {"demontaj": [], "montaj": []}
    
    def analyze_photo(self, photo_path: str, demontaj_list: List[str], montaj_list: List[str]) -> Dict[str, Any]:
        """
        Анализирует фотографию для определения демонтированного оборудования.
        
        Args:
            photo_path: Путь к файлу фотографии
            demontaj_list: Список оборудования для демонтажа
            montaj_list: Список оборудования для монтажа
            
        Returns:
            Dict с результатами анализа: is_demontaj, equipment_found, reason
        """
        try:
            # Проверяем существование файла
            if not os.path.exists(photo_path):
                raise FileNotFoundError(f"Фото файл не найден: {photo_path}")
            
            demontaj_str = ", ".join(demontaj_list) if demontaj_list else "Нет списка"
            montaj_str = ", ".join(montaj_list) if montaj_list else "Нет списка"
            
            prompt = f"""Ты — строгий технический аудитор телеком-оборудования. 
            Твоя задача — определить, относится ли это фото к ДЕМОНТАЖУ (снятое старое оборудование) или к МОНТАЖУ/ДРУГОМУ.
            
            Список ДЕМОНТАЖА (ищем это): [{demontaj_str}]
            Список МОНТАЖА (это новое оборудование, игнорируй его): [{montaj_str}]
            
            ПРАВИЛА ИСКЛЮЧЕНИЯ (КРИТИЧНО ВАЖНО):
            1. Внимательно читай номера моделей на шильдиках (например, RRU 3971 vs RRU 5527). Не перепутай старую модель с новой.
            2. Пересечения: Если тип оборудования (например, DCDU или Кабель) есть В ОБОИХ списках, смотри на ВНЕШНИЙ ВИД:
               - НОВОЕ: В картонной коробке, в заводской пленке, пенопласте, с чистыми разъемами/заглушками -> ЭТО МОНТАЖ (is_demontaj: false).
               - СТАРОЕ: Грязное, ржавое, в пыли, валяется на земле, обрезаны провода/коннекторы -> ЭТО ДЕМОНТАЖ (is_demontaj: true).
            3. Если это просто панорама вышки — отвечай is_demontaj: false.
            4. Не выдумывай модели. Если текст на шильдике не читается кристально четко, не пытайся угадать его под список демонтажа.
            
            Ответь строго в формате JSON: {{"is_demontaj": true/false, "equipment_found": "Название из списка демонтажа или null", "reason": "Детально объясни по визуальным признакам, почему это старое/новое"}}."""
            
            # Загружаем изображение через контекстный менеджер для гарантии освобождения файла
            with Image.open(photo_path) as image:
                try:
                    # Отправляем запрос к Gemini Vision API с retry
                    response = self._generate_with_retry(
                        self.vision_model,
                        [prompt, image],    
                        generation_config=genai.types.GenerationConfig(
                            response_mime_type="application/json",
                            temperature=0.1, # Понизили температуру для большей строгости
                        )
                    )
                    
                    # Парсим JSON ответ
                    result = json.loads(response.text)
                    
                    # Валидация и нормализация
                    if not isinstance(result, dict):
                        raise ValueError("Invalid response format")
                    
                    # Убедимся, что все поля присутствуют
                    result.setdefault('is_demontaj', False)
                    result.setdefault('equipment_found', None)
                    result.setdefault('reason', 'Не удалось определить')
                    
                    # Логируем результат
                    equipment_name = result['equipment_found'] or 'не определено'
                    self.logger.info(f"Анализ фото {os.path.basename(photo_path)}: демонтаж={result['is_demontaj']}, оборудование={equipment_name}. Причина: {result.get('reason')}")
                    
                    return result
                    
                except Exception as e:
                    self.logger.error(f"Ошибка при анализе содержимого изображения {photo_path}: {e}")
                    raise
            
        except FileNotFoundError as e:
            self.logger.error(str(e))
            return {"is_demontaj": False, "equipment_found": None, "reason": "Файл не найден"}
        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка парсинга JSON ответа: {e}")
            return {"is_demontaj": False, "equipment_found": None, "reason": "Ошибка анализа"}
        except Exception as e:
            self.logger.error(f"Ошибка при анализе фотографии {photo_path}: {e}")
            return {"is_demontaj": False, "equipment_found": None, "reason": "Техническая ошибка"}
    

