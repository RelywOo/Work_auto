import pytest
import json
import os
from unittest.mock import patch, MagicMock, AsyncMock
from google.api_core.exceptions import ResourceExhausted
from PIL import Image

# Устанавливаем env до импорта AIProcessor
os.environ['GEMINI_API_KEY'] = 'test_key_for_tests'


@pytest.fixture
def ai_processor():
    """Создаёт AIProcessor с замоканным Gemini API."""
    with patch('modules.ai_processor.genai') as mock_genai:
        mock_genai.GenerativeModel.return_value = MagicMock()
        mock_genai.types.GenerationConfig = MagicMock()
        from modules.ai_processor import AIProcessor
        processor = AIProcessor()
    return processor


@pytest.fixture
def mock_response():
    """Фабрика для создания mock-ответов Gemini API."""
    def _make(text: str):
        resp = MagicMock()
        resp.text = text
        return resp
    return _make


@pytest.fixture
def sample_log_file(tmp_path):
    """Создаёт временный лог-файл с текстом сообщений."""
    log_file = tmp_path / "messages_log.txt"
    log_file.write_text(
        "Инженер: Список демонтажа: Anten T1003, DCDU12B\n"
        "Инженер: Список монтажа: RRU 5516\n",
        encoding='utf-8'
    )
    return str(log_file)


@pytest.fixture
def sample_photo(tmp_path):
    """Создаёт минимальное тестовое изображение."""
    photo_path = tmp_path / "test_photo.jpg"
    img = Image.new('RGB', (10, 10), color='red')
    img.save(str(photo_path))
    return str(photo_path)


# ===========================================================================
# _generate_with_retry
# ===========================================================================

@pytest.mark.asyncio
async def test_generate_with_retry_success(ai_processor, mock_response):
    """Успешный вызов API с первой попытки."""
    expected = mock_response('{"result": "ok"}')
    ai_processor.text_model.generate_content = MagicMock(return_value=expected)

    result = await ai_processor._generate_with_retry(
        ai_processor.text_model, "test prompt"
    )
    assert result.text == '{"result": "ok"}'


@pytest.mark.asyncio
async def test_generate_with_retry_rate_limit_then_success(ai_processor, mock_response):
    """Rate limit на первых двух попытках, успех на третьей."""
    expected = mock_response('{"ok": true}')
    ai_processor.text_model.generate_content = MagicMock(
        side_effect=[
            ResourceExhausted("429"),
            ResourceExhausted("429"),
            expected,
        ]
    )

    with patch('asyncio.sleep', new_callable=AsyncMock):
        result = await ai_processor._generate_with_retry(
            ai_processor.text_model, "test"
        )
    assert result.text == '{"ok": true}'


@pytest.mark.asyncio
async def test_generate_with_retry_all_retries_exhausted(ai_processor):
    """Rate limit на всех 5 попытках — должен пробросить исключение."""
    ai_processor.text_model.generate_content = MagicMock(
        side_effect=ResourceExhausted("429")
    )

    with patch('asyncio.sleep', new_callable=AsyncMock):
        with pytest.raises(ResourceExhausted):
            await ai_processor._generate_with_retry(
                ai_processor.text_model, "test"
            )


# ===========================================================================
# extract_equipment_lists
# ===========================================================================

@pytest.mark.asyncio
async def test_extract_equipment_lists_valid_json(ai_processor, mock_response, sample_log_file):
    """Корректный JSON ответ от Gemini."""
    response_json = json.dumps({
        "demontaj": ["Anten T1003", "DCDU12B"],
        "montaj": ["RRU 5516"]
    })
    ai_processor._generate_with_retry = AsyncMock(return_value=mock_response(response_json))

    result = await ai_processor.extract_equipment_lists(sample_log_file)

    assert result['demontaj'] == ["Anten T1003", "DCDU12B"]
    assert result['montaj'] == ["RRU 5516"]


@pytest.mark.asyncio
async def test_extract_equipment_lists_missing_keys(ai_processor, mock_response, sample_log_file):
    """JSON без ключа montaj — должен подставить пустой список."""
    response_json = json.dumps({"demontaj": ["Switch 1"]})
    ai_processor._generate_with_retry = AsyncMock(return_value=mock_response(response_json))

    result = await ai_processor.extract_equipment_lists(sample_log_file)

    assert result['demontaj'] == ["Switch 1"]
    assert result['montaj'] == []


@pytest.mark.asyncio
async def test_extract_equipment_lists_invalid_json(ai_processor, mock_response, sample_log_file):
    """Невалидный JSON — должен вернуть пустые списки."""
    ai_processor._generate_with_retry = AsyncMock(
        return_value=mock_response("это не JSON {{{}}")
    )

    result = await ai_processor.extract_equipment_lists(sample_log_file)

    assert result == {"demontaj": [], "montaj": []}


@pytest.mark.asyncio
async def test_extract_equipment_lists_empty_response(ai_processor, mock_response, sample_log_file):
    """Пустой JSON объект — должен вернуть пустые списки."""
    ai_processor._generate_with_retry = AsyncMock(return_value=mock_response("{}"))

    result = await ai_processor.extract_equipment_lists(sample_log_file)

    assert result == {"demontaj": [], "montaj": []}


@pytest.mark.asyncio
async def test_extract_equipment_lists_file_not_found(ai_processor):
    """Несуществующий файл — должен вернуть пустые списки."""
    result = await ai_processor.extract_equipment_lists("/nonexistent/path.txt")

    assert result == {"demontaj": [], "montaj": []}


@pytest.mark.asyncio
async def test_extract_equipment_lists_rate_limit(ai_processor, sample_log_file):
    """ResourceExhausted пробрасывается — должен вернуть пустые списки."""
    ai_processor._generate_with_retry = AsyncMock(side_effect=ResourceExhausted("429"))

    result = await ai_processor.extract_equipment_lists(sample_log_file)

    assert result == {"demontaj": [], "montaj": []}


# ===========================================================================
# analyze_photo
# ===========================================================================

@pytest.mark.asyncio
async def test_analyze_photo_demontaj(ai_processor, mock_response, sample_photo):
    """Фото определено как демонтаж."""
    response_json = json.dumps({
        "is_demontaj": True,
        "equipment_found": "DCDU12B",
        "reason": "Старое оборудование, ржавчина"
    })
    ai_processor._generate_with_retry = AsyncMock(return_value=mock_response(response_json))

    result = await ai_processor.analyze_photo(sample_photo, ["DCDU12B"], ["RRU 5516"])

    assert result['is_demontaj'] is True
    assert result['equipment_found'] == "DCDU12B"


@pytest.mark.asyncio
async def test_analyze_photo_montaj(ai_processor, mock_response, sample_photo):
    """Фото определено как монтаж (не демонтаж)."""
    response_json = json.dumps({
        "is_demontaj": False,
        "equipment_found": None,
        "reason": "Новое оборудование в заводской упаковке"
    })
    ai_processor._generate_with_retry = AsyncMock(return_value=mock_response(response_json))

    result = await ai_processor.analyze_photo(sample_photo, ["DCDU12B"], ["RRU 5516"])

    assert result['is_demontaj'] is False


@pytest.mark.asyncio
async def test_analyze_photo_missing_fields(ai_processor, mock_response, sample_photo):
    """Ответ без некоторых полей — должны подставиться defaults."""
    ai_processor._generate_with_retry = AsyncMock(return_value=mock_response("{}"))

    result = await ai_processor.analyze_photo(sample_photo, [], [])

    assert result['is_demontaj'] is False
    assert result['equipment_found'] is None
    assert result['reason'] == 'Не удалось определить'


@pytest.mark.asyncio
async def test_analyze_photo_invalid_json(ai_processor, mock_response, sample_photo):
    """Невалидный JSON ответ — graceful fallback."""
    ai_processor._generate_with_retry = AsyncMock(
        return_value=mock_response("not json at all")
    )

    result = await ai_processor.analyze_photo(sample_photo, [], [])

    assert result['is_demontaj'] is False
    assert result['reason'] == "Ошибка анализа"


@pytest.mark.asyncio
async def test_analyze_photo_file_not_found(ai_processor):
    """Несуществующее фото — graceful fallback."""
    result = await ai_processor.analyze_photo("/nonexistent/photo.jpg", [], [])

    assert result['is_demontaj'] is False
    assert result['reason'] == "Файл не найден"


@pytest.mark.asyncio
async def test_analyze_photo_api_error(ai_processor, sample_photo):
    """Ошибка API — graceful fallback."""
    ai_processor._generate_with_retry = AsyncMock(
        side_effect=Exception("Network error")
    )

    result = await ai_processor.analyze_photo(sample_photo, ["A"], ["B"])

    assert result['is_demontaj'] is False
    assert result['reason'] == "Техническая ошибка"
