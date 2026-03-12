import os
import json
import asyncio
import logging
from typing import Dict, List, Any, Tuple
import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted
from PIL import Image

# AI retry configuration
AI_MAX_RETRIES = 5
AI_BASE_DELAY = 2.0  # seconds, doubles each retry (exponential backoff)
AI_TIMEOUT = 120.0  # seconds, max wait for a single API call


class AIProcessor:
    def __init__(self) -> None:
        """Initialize AIProcessor with Gemini API models."""
        self.logger = logging.getLogger(__name__)
        self.api_key = os.getenv("GEMINI_API_KEY")

        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")

        genai.configure(api_key=self.api_key)
        model_name = os.getenv("GEMINI_MODEL", "gemini-flash-latest")
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
                    self.logger.error(
                        f"Rate limit exceeded after {max_retries} retries: {e}"
                    )
                    raise
                delay = base_delay * (2**attempt)
                self.logger.warning(
                    f"Rate limit hit. Retrying in {delay}s (Attempt {attempt + 1}/{max_retries})..."
                )
                await asyncio.sleep(delay)
            except asyncio.TimeoutError:
                # Handle infinite hanging requests
                if attempt == max_retries - 1:
                    self.logger.error(
                        f"AI timeout ({AI_TIMEOUT}s) exceeded after {max_retries} retries"
                    )
                    raise
                delay = base_delay * (2**attempt)
                self.logger.warning(
                    f"AI call timed out after {AI_TIMEOUT}s. Retrying in {delay}s (Attempt {attempt + 1}/{max_retries})..."
                )
                await asyncio.sleep(delay)

    async def extract_equipment_lists(self, log_file_path: str) -> Dict[str, List[str]]:
        """Extract equipment lists for installation/removal from a message log.

        Args:
            log_file_path: Path to messages_log.txt file.

        Returns:
            Dict with 'demontaj' and 'montaj' keys containing equipment lists.
        """
        try:
            with open(log_file_path, "r", encoding="utf-8") as f:
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
                ),
            )

            # Parse JSON response
            result = json.loads(response.text)

            # Validate structure
            if not isinstance(result, dict):
                raise ValueError("Invalid response format")

            if "demontaj" not in result:
                result["demontaj"] = []
            if "montaj" not in result:
                result["montaj"] = []

            # Ensure values are lists of strings
            result["demontaj"] = [
                str(item).strip() for item in result["demontaj"] if item
            ]
            result["montaj"] = [str(item).strip() for item in result["montaj"] if item]

            self.logger.info(
                f"Extracted lists: demontaj={len(result['demontaj'])}, montaj={len(result['montaj'])}"
            )
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

    async def analyze_photo(
        self, photo_path: str, demontaj_list: List[str], montaj_list: List[str]
    ) -> Dict[str, Any]:
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
            
            ПРАВИЛА КЛАССИФИКАЦИИ (КРИТИЧНО ВАЖНО — применяй СТРОГО В УКАЗАННОМ ПОРЯДКЕ):

            1. СОВПАДЕНИЕ С КОНКРЕТНЫМ СПИСКОМ — НАИВЫСШИЙ ПРИОРИТЕТ.
               Если модель оборудования на шильдике найдена в списке ДЕМОНТАЖА (с учётом нечёткого сопоставления) — это ДЕМОНТАЖ (is_demontaj: true), НЕЗАВИСИМО от внешнего вида оборудования. Демонтируемое оборудование может выглядеть чистым и новым — это нормально.
               Аналогично: если модель найдена ТОЛЬКО в списке МОНТАЖА — это МОНТАЖ (is_demontaj: false).

            2. ПЕРЕСЕЧЕНИЯ: Если оборудование (или его тип, например DCDU, Кабель) есть В ОБОИХ списках — определяй по ВНЕШНЕМУ ВИДУ (см. правило 4).

            3. НЕЧЁТКОЕ СОПОСТАВЛЕНИЕ МОДЕЛЕЙ (Fuzzy Matching).
               Модели на шильдике могут ОТЛИЧАТЬСЯ от списка на пару символов, суффиксов или ревизий. Это НОРМАЛЬНО.
               Примеры эквивалентных моделей:
               - ATR4518R6V01 ≈ ATR451602v01 (одна серия ATR4518/ATR4516)
               - RRU5526et ≈ RRU 5526 (суффикс 'et' — вариант исполнения)
               - TDT-172718DEH-65Fv03 ≈ TDT-172718DEI-65Fv03 (одна буква отличается)
               ПРАВИЛО: Сравнивай по ОСНОВЕ модели (первые значимые символы серии), а не посимвольно. Если основа совпадает — считай оборудование найденным в списке.

            4. ВИЗУАЛЬНЫЕ ПРИЗНАКИ (используй когда модель НЕ найдена ни в одном списке, ИЛИ найдена в обоих).
               Если модель не удалось прочитать или она не найдена ни в одном списке, определяй по внешнему виду:
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

            5. Если это просто панорама вышки без крупного плана конкретного оборудования — отвечай is_demontaj: false.

            6. Внимательно различай СЕРИИ моделей (например, RRU 3xxx — старая серия, RRU 5xxx — новая). Не перепутай старую модель с новой.

            7. В поле equipment_found записывай ТОЧНО то, что прочитано на шильдике. Не подгоняй текст под список.
            
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
                        ),
                    )

                    # Parse JSON response
                    result = json.loads(response.text)

                    # Validate and normalize
                    if not isinstance(result, dict):
                        raise ValueError("Invalid response format")

                    # Ensure all required fields are present
                    result.setdefault("is_demontaj", False)
                    result.setdefault("equipment_found", None)
                    result.setdefault("reason", "Не удалось определить")

                    equipment_name = result["equipment_found"] or "unknown"
                    classification = "DEMONTAJ" if result["is_demontaj"] else "MONTAJ"
                    self.logger.info(
                        f"📷 {os.path.basename(photo_path)} → {classification} | equipment: {equipment_name}"
                    )
                    self.logger.info(f"   └─ Reason: {result.get('reason')}")

                    return result

                except Exception as e:
                    self.logger.error(
                        f"Error analyzing image content {photo_path}: {e}"
                    )
                    raise

        except FileNotFoundError as e:
            self.logger.error(str(e))
            return {
                "is_demontaj": False,
                "equipment_found": None,
                "reason": "Файл не найден",
            }
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parse error for photo response: {e}")
            return {
                "is_demontaj": False,
                "equipment_found": None,
                "reason": "Ошибка анализа",
            }
        except Exception as e:
            self.logger.error(f"Error analyzing photo {photo_path}: {e}")
            return {
                "is_demontaj": False,
                "equipment_found": None,
                "reason": "Техническая ошибка",
            }

    async def reanalyze_with_context(
        self,
        photo_path: str,
        demontaj_list: List[str],
        montaj_list: List[str],
        neighbor_classifications: List[str],
    ) -> Dict[str, Any]:
        """Re-analyze a photo with neighbor context for outlier correction.

        Args:
            photo_path: Path to the photo file.
            demontaj_list: List of equipment scheduled for removal.
            montaj_list: List of equipment scheduled for installation.
            neighbor_classifications: List of neighbor results, e.g. ['demontaj', 'demontaj', '?', 'demontaj'].
                                      '?' marks the current photo's position.
        """
        context_str = ", ".join(neighbor_classifications)
        demontaj_str = ", ".join(demontaj_list) if demontaj_list else "Нет списка"
        montaj_str = ", ".join(montaj_list) if montaj_list else "Нет списка"

        prompt = f"""Ты — строгий технический аудитор телеком-оборудования.
            Это ПОВТОРНЫЙ анализ фото, которое было помечено как подозрительное, потому что его классификация
            противоречит серии соседних фото.

            Список ДЕМОНТАЖА: [{demontaj_str}]
            Список МОНТАЖА: [{montaj_str}]

            КОНТЕКСТ СЕРИИ (НАИВЫСШИЙ ПРИОРИТЕТ):
            Инженеры фотографируют оборудование сериями — сначала монтаж, потом демонтаж.
            Соседние фото в серии классифицированы так: [{context_str}].
            Знак '?' — это текущее фото.

            ПРАВИЛО ПРИОРИТЕТА ВТОРОГО ПРОХОДА (КРИТИЧНО ВАЖНО):
            По умолчанию ДОВЕРЯЙ КОНТЕКСТУ СЕРИИ. Если все соседи классифицированы одинаково,
            то это фото скорее всего относится к тому же типу.

            Ты можешь НЕ согласиться с контекстом ТОЛЬКО при выполнении ВСЕХ условий одновременно:
            1. На фото есть НЕОПРОВЕРЖИМЫЕ визуальные признаки противоположного типа
               (например, заводская упаковка/коробка среди серии демонтажа, или грязь/ржавчина/оборудование на земле среди серии монтажа)
            2. Модель оборудования найдена ТОЛЬКО в противоположном списке и ОТСУТСТВУЕТ в списке,
               совпадающем с контекстом серии

            ВАЖНО: Если оборудование (или его тип, например DCDU) есть в ОБОИХ списках — ВСЕГДА соглашайся с контекстом серии.
            Если есть хоть малейшие сомнения — СОГЛАШАЙСЯ С КОНТЕКСТОМ СЕРИИ.

            Визуальные признаки (для справки):
            Признаки ДЕМОНТАЖА: грязное, ржавое, потёртое, на земле без упаковки, обрезаны провода, наклейка CAUTION.
            Признаки МОНТАЖА: в заводской упаковке, чистое, новые бирки/стяжки, смонтировано на мачте, горящие LED.

            Нечёткое сопоставление моделей: сравнивай по ОСНОВЕ модели, не посимвольно (ATR4518 ≈ ATR4516, RRU5526et ≈ RRU 5526).

            В поле equipment_found записывай ТОЧНО то, что прочитано на шильдике. Не подгоняй текст под список.

            Ответь строго в формате JSON: {{"is_demontaj": true/false, "equipment_found": "Что прочитано на шильдике или визуально определено, либо null", "reason": "Детально объясни: почему согласился или не согласился с контекстом серии"}}."""

        try:
            if not os.path.exists(photo_path):
                raise FileNotFoundError(f"Photo file not found: {photo_path}")

            with Image.open(photo_path) as image:
                response = await self._generate_with_retry(
                    self.vision_model,
                    [prompt, image],
                    generation_config=genai.types.GenerationConfig(
                        response_mime_type="application/json",
                        temperature=0.1,
                    ),
                )
                result = json.loads(response.text)

                if not isinstance(result, dict):
                    raise ValueError("Invalid response format")

                result.setdefault("is_demontaj", False)
                result.setdefault("equipment_found", None)
                result.setdefault("reason", "Не удалось определить")

                equipment_name = result["equipment_found"] or "unknown"
                classification = "DEMONTAJ" if result["is_demontaj"] else "MONTAJ"
                self.logger.info(
                    f"🔄 Re-analysis {os.path.basename(photo_path)} → {classification} "
                    f"| equipment: {equipment_name} | context: [{context_str}]"
                )
                self.logger.info(f"   └─ Reason: {result.get('reason')}")
                return result

        except FileNotFoundError as e:
            self.logger.error(str(e))
            return {
                "is_demontaj": False,
                "equipment_found": None,
                "reason": "Файл не найден",
            }
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parse error for re-analysis response: {e}")
            return {
                "is_demontaj": False,
                "equipment_found": None,
                "reason": "Ошибка анализа",
            }
        except Exception as e:
            self.logger.error(f"Error re-analyzing photo {photo_path}: {e}")
            return {
                "is_demontaj": False,
                "equipment_found": None,
                "reason": "Техническая ошибка",
            }


def find_outliers(classifications: List[str]) -> List[int]:
    """Find suspicious outlier photos that break the series pattern.

    Engineers photograph equipment in series: first montaj, then demontaj.
    A photo that breaks the series pattern is likely a classification error.

    Args:
        classifications: List of 'demontaj'/'montaj' strings in chronological order.

    Returns:
        List of indices of suspicious photos to re-analyze.
    """
    if len(classifications) < 3:
        return []

    # Build segments: [(value, start_index, length), ...]
    segments: List[Tuple[str, int, int]] = []
    current = classifications[0]
    start = 0
    for i in range(1, len(classifications)):
        if classifications[i] != current:
            segments.append((current, start, i - start))
            current = classifications[i]
            start = i
    segments.append((current, start, len(classifications) - start))

    # Check for chaotic pattern using transition rate.
    # A clean series with outliers has few transitions relative to photo count
    # (e.g., 7 transitions in 96 photos = 0.07). A truly chaotic pattern like
    # [M,D,M,D,M,D,M] has a high rate (6/7 = 0.86). Threshold 0.3 separates them.
    num_transitions = len(segments) - 1
    transition_rate = num_transitions / len(classifications)
    if transition_rate > 0.3:
        return []

    # Find short segments (1-3) surrounded by opposite type with enough mass
    outlier_indices: List[int] = []
    for seg_idx, (value, start_idx, length) in enumerate(segments):
        if length > 3:
            continue

        # Calculate surrounding opposite mass
        left_mass = 0
        for j in range(seg_idx - 1, -1, -1):
            if segments[j][0] != value:
                left_mass += segments[j][2]
            else:
                break

        right_mass = 0
        for j in range(seg_idx + 1, len(segments)):
            if segments[j][0] != value:
                right_mass += segments[j][2]
            else:
                break

        total_surrounding = left_mass + right_mass
        if total_surrounding >= 5:
            for i in range(start_idx, start_idx + length):
                outlier_indices.append(i)

    return outlier_indices
