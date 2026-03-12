import os
import json
import asyncio
import logging
from typing import Dict, List, Any
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted
from PIL import Image

# AI retry configuration
AI_MAX_RETRIES = 5
AI_BASE_DELAY = 2.0  # seconds, doubles each retry (exponential backoff)
AI_TIMEOUT = 120.0   # seconds, max wait for a single API call

class AIProcessor:
    def __init__(self) -> None:
        """Initialize AIProcessor with Gemini API models."""
        self.logger = logging.getLogger(__name__)
        self.api_key = os.getenv('GEMINI_API_KEY')
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        genai.configure(api_key=self.api_key)
        model_name = os.getenv('GEMINI_MODEL', 'gemini-flash-latest')
        self.text_model = genai.GenerativeModel(model_name)
        self.vision_model = genai.GenerativeModel(model_name)
        
        self.logger.info("AI Processor initialized with Gemini API")
    
    async def _generate_with_retry(self, model: Any, *args: Any, **kwargs: Any) -> Any:
        """Wrapper for API calls with retry on Rate Limits (429) and timeouts.

        Uses asyncio.sleep() instead of time.sleep() to avoid blocking
        the event loop during retry waits.
        """
        max_retries = AI_MAX_RETRIES
        base_delay = AI_BASE_DELAY
        # Fix PRR [P0-4]: Deprecated asyncio.get_event_loop()
        loop = asyncio.get_running_loop()
        for attempt in range(max_retries):
            try:
                # Fix PRR [P0-3]: No timeout on AI calls
                task = loop.run_in_executor(
                    None, lambda: model.generate_content(*args, **kwargs)
                )
                return await asyncio.wait_for(task, timeout=AI_TIMEOUT)
            except ResourceExhausted as e:
                # Handle rate limiting (HTTP 429) from Google API
                if attempt == max_retries - 1:
                    self.logger.error(f"Rate limit exceeded after {max_retries} retries: {e}")
                    raise
                delay = base_delay * (2 ** attempt)
                self.logger.warning(f"Rate limit hit. Retrying in {delay}s (Attempt {attempt + 1}/{max_retries})...")
                await asyncio.sleep(delay)
            except asyncio.TimeoutError:
                # Handle infinite hanging requests
                if attempt == max_retries - 1:
                    self.logger.error(f"AI timeout ({AI_TIMEOUT}s) exceeded after {max_retries} retries")
                    raise
                delay = base_delay * (2 ** attempt)
                self.logger.warning(f"AI call timed out after {AI_TIMEOUT}s. Retrying in {delay}s (Attempt {attempt + 1}/{max_retries})...")
                await asyncio.sleep(delay)
    async def extract_equipment_lists(self, log_file_path: str) -> Dict[str, List[str]]:
        """Extract equipment lists for installation/removal from a message log.

        Args:
            log_file_path: Path to messages_log.txt file.

        Returns:
            Dict with 'demontaj' and 'montaj' keys containing equipment lists.
        """
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                log_content = f.read()
            prompt = """Ты — технический ассистент. Проанализируй предоставленный лог сообщений из Telegram-чата инженеров. 
            Найди сообщение, в котором перечисляется оборудование для монтажа (Montaj) и демонтажа (Demontaj). 
            Извлеки эти данные и верни СТРОГО в формате JSON. 
            Пример ответа: {"demontaj": ["Anten T1003M6R011", "DCDU12B"], "montaj": ["RRU 5516"]}. 
            Если списков нет, верни пустые массивы."""
            
            # Send request to Gemini with retry
            response = await self._generate_with_retry(
                self.text_model,
                prompt + "\n\nЛог сообщений:\n" + log_content,
                generation_config=genai.types.GenerationConfig(
                    response_mime_type="application/json",
                    temperature=0.1,
                )
            )
            
            # Parse JSON response
            result = json.loads(response.text)

            # Validate structure
            if not isinstance(result, dict):
                raise ValueError("Invalid response format")
            
            if 'demontaj' not in result:
                result['demontaj'] = []
            if 'montaj' not in result:
                result['montaj'] = []
            
            # Ensure values are lists of strings
            result['demontaj'] = [str(item).strip() for item in result['demontaj'] if item]
            result['montaj'] = [str(item).strip() for item in result['montaj'] if item]
            
            self.logger.info(f"Extracted lists: demontaj={len(result['demontaj'])}, montaj={len(result['montaj'])}")
            return result
            
        except FileNotFoundError:
            self.logger.error(f"Log file not found: {log_file_path}")
            return {"demontaj": [], "montaj": []}
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parse error: {e}")
            return {"demontaj": [], "montaj": []}
        except Exception as e:
            self.logger.error(f"Error extracting equipment lists: {e}")
            return {"demontaj": [], "montaj": []}
    
    async def analyze_photo(self, photo_path: str, demontaj_list: List[str], montaj_list: List[str]) -> Dict[str, Any]:
        """Analyze a photo to determine if it shows decommissioned equipment.

        Args:
            photo_path: Path to the photo file.
            demontaj_list: List of equipment scheduled for removal.
            montaj_list: List of equipment scheduled for installation.

        Returns:
            Dict with analysis results: is_demontaj, equipment_found, reason.
        """
        try:
            if not os.path.exists(photo_path):
                raise FileNotFoundError(f"Photo file not found: {photo_path}")
            
            demontaj_str = ", ".join(demontaj_list) if demontaj_list else "Нет списка"
            montaj_str = ", ".join(montaj_list) if montaj_list else "Нет списка"
            
            prompt = f"""Ты — строгий технический аудитор телеком-оборудования. 
            Твоя задача — определить, относится ли это фото к ДЕМОНТАЖУ (снятое старое оборудование) или к МОНТАЖУ/ДРУГОМУ.
            
            Список ДЕМОНТАЖА (ищем это): [{demontaj_str}]
            Список МОНТАЖА (это новое оборудование, игнорируй его): [{montaj_str}]
            
            ПРАВИЛА КЛАССИФИКАЦИИ (КРИТИЧНО ВАЖНО):
            
            1. ВИЗУАЛЬНЫЕ ПРИЗНАКИ ИМЕЮТ НАИВЫСШИЙ ПРИОРИТЕТ.
               Если оборудование имеет ЯВНЫЕ визуальные признаки старого/демонтированного — классифицируй как ДЕМОНТАЖ (is_demontaj: true), ДАЖЕ ЕСЛИ точная модель не найдена в списке демонтажа.
               Признаки ДЕМОНТАЖА (старое оборудование):
               - Грязное, ржавое, в пыли, потёртое, с царапинами
               - Лежит на земле/асфальте/бетоне без упаковки
               - Отслаивающиеся или выцветшие наклейки/шильдики
               - Оборудование в руках инженера без заводской упаковки на фоне улицы
               - Обрезаны провода/коннекторы
               - Наличие желтой наклейки 'CAUTION' с текстом 'The shield layer shall be grounded...' и синим круглым логотипом зажима (это внутренний стикер старых RRU, признак ДЕМОНТАЖА).
               Признаки МОНТАЖА (новое оборудование):
               - В картонной коробке, заводской плёнке, пенопласте
               - Чистые разъемы/заглушки, новые бирки
               - Жёлтые маркировочные бирки на кабелях, новые пластиковые стяжки
               - Смонтировано на мачте/в стойке, кабели подключены и промаркированы
               - Горящие LED-индикаторы (оборудование в работе)
            
            2. НЕЧЁТКОЕ СОПОСТАВЛЕНИЕ МОДЕЛЕЙ (Fuzzy Matching).
               Модели на шильдике могут ОТЛИЧАТЬСЯ от списка на пару символов, суффиксов или ревизий. Это НОРМАЛЬНО.
               Примеры эквивалентных моделей:
               - ATR4518R6V01 ≈ ATR451602v01 (одна серия ATR4518/ATR4516)
               - RRU5526et ≈ RRU 5526 (суффикс 'et' — вариант исполнения)
               - TDT-172718DEH-65Fv03 ≈ TDT-172718DEI-65Fv03 (одна буква отличается)
               ПРАВИЛО: Сравнивай по ОСНОВЕ модели (первые значимые символы серии), а не посимвольно. Если основа совпадает — считай оборудование найденным в списке.
            
            3. Пересечения: Если тип оборудования (например, DCDU или Кабель) есть В ОБОИХ списках, определяй по ВНЕШНЕМУ ВИДУ (см. правило 1).
            
            4. Если это просто панорама вышки без крупного плана конкретного оборудования — отвечай is_demontaj: false.
            
            5. Внимательно различай СЕРИИ моделей (например, RRU 3xxx — старая серия, RRU 5xxx — новая). Не перепутай старую модель с новой.
            
            6. В поле equipment_found записывай ТОЧНО то, что прочитано на шильдике. Не подгоняй текст под список.
            
            Ответь строго в формате JSON: {{"is_demontaj": true/false, "equipment_found": "Что прочитано на шильдике или визуально определено, либо null", "reason": "Детально объясни: какие визуальные признаки и/или совпадение модели привели к решению"}}."""
            
            # Open image via context manager to ensure file handle is released
            with Image.open(photo_path) as image:
                try:
                    # Send request to Gemini Vision API with retry
                    response = await self._generate_with_retry(
                        self.vision_model,
                        [prompt, image],
                        generation_config=genai.types.GenerationConfig(
                            response_mime_type="application/json",
                            temperature=0.1,  # Low temperature for stricter classification
                        )
                    )
                    
                    # Parse JSON response
                    result = json.loads(response.text)

                    # Validate and normalize
                    if not isinstance(result, dict):
                        raise ValueError("Invalid response format")
                    
                    # Ensure all required fields are present
                    result.setdefault('is_demontaj', False)
                    result.setdefault('equipment_found', None)
                    result.setdefault('reason', 'Не удалось определить')
                    
                    equipment_name = result['equipment_found'] or 'unknown'
                    self.logger.info(f"Photo analysis {os.path.basename(photo_path)}: demontaj={result['is_demontaj']}, equipment={equipment_name}. Reason: {result.get('reason')}")
                    
                    return result
                    
                except Exception as e:
                    self.logger.error(f"Error analyzing image content {photo_path}: {e}")
                    raise
            
        except FileNotFoundError as e:
            self.logger.error(str(e))
            return {"is_demontaj": False, "equipment_found": None, "reason": "Файл не найден"}
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parse error for photo response: {e}")
            return {"is_demontaj": False, "equipment_found": None, "reason": "Ошибка анализа"}
        except Exception as e:
            self.logger.error(f"Error analyzing photo {photo_path}: {e}")
            return {"is_demontaj": False, "equipment_found": None, "reason": "Техническая ошибка"}
    

