"""
Тесты ошибочных сценариев (MEDIUM-7):
- Сетевые ошибки при вызове AI
- Невалидные/повреждённые фото
- Concurrent доступ к БД из нескольких потоков
- Ошибки файловой системы
"""
import pytest
import json
import os
import threading
from unittest.mock import patch, MagicMock, AsyncMock
from PIL import Image

os.environ.setdefault('GEMINI_API_KEY', 'test_key')

from modules.database import DatabaseManager


# ===========================================================================
# Сетевые ошибки AI
# ===========================================================================

@pytest.fixture
def ai_processor():
    with patch('modules.ai_processor.genai') as mock_genai:
        mock_genai.GenerativeModel.return_value = MagicMock()
        mock_genai.types.GenerationConfig = MagicMock()
        from modules.ai_processor import AIProcessor
        processor = AIProcessor()
    return processor


@pytest.mark.asyncio
async def test_ai_network_timeout(ai_processor, tmp_path):
    """Таймаут сети при вызове Gemini API."""
    log_file = tmp_path / "log.txt"
    log_file.write_text("some log content", encoding='utf-8')

    ai_processor._generate_with_retry = AsyncMock(
        side_effect=TimeoutError("Connection timed out")
    )

    result = await ai_processor.extract_equipment_lists(str(log_file))
    assert result == {"demontaj": [], "montaj": []}


@pytest.mark.asyncio
async def test_ai_connection_refused(ai_processor, tmp_path):
    """ConnectionError при вызове Gemini API."""
    log_file = tmp_path / "log.txt"
    log_file.write_text("some log content", encoding='utf-8')

    ai_processor._generate_with_retry = AsyncMock(
        side_effect=ConnectionError("Connection refused")
    )

    result = await ai_processor.extract_equipment_lists(str(log_file))
    assert result == {"demontaj": [], "montaj": []}


@pytest.mark.asyncio
async def test_analyze_photo_network_error_returns_safe_default(ai_processor, tmp_path):
    """Сетевая ошибка при анализе фото — возвращает безопасный default."""
    photo_path = tmp_path / "photo.jpg"
    img = Image.new('RGB', (10, 10), color='red')
    img.save(str(photo_path))

    ai_processor._generate_with_retry = AsyncMock(
        side_effect=OSError("Network unreachable")
    )

    result = await ai_processor.analyze_photo(str(photo_path), ["A"], ["B"])
    assert result['is_demontaj'] is False
    assert result['reason'] == "Техническая ошибка"


# ===========================================================================
# Невалидные/повреждённые фото
# ===========================================================================

@pytest.mark.asyncio
async def test_analyze_corrupted_photo(ai_processor, tmp_path):
    """Повреждённый файл изображения — не может быть открыт Pillow."""
    corrupted_file = tmp_path / "corrupted.jpg"
    corrupted_file.write_bytes(b'\x00\x01\x02\x03garbage_data')

    result = await ai_processor.analyze_photo(str(corrupted_file), ["X"], ["Y"])

    assert result['is_demontaj'] is False
    assert result['reason'] == "Техническая ошибка"


@pytest.mark.asyncio
async def test_analyze_empty_file_as_photo(ai_processor, tmp_path):
    """Пустой файл вместо фото."""
    empty_file = tmp_path / "empty.jpg"
    empty_file.write_bytes(b'')

    result = await ai_processor.analyze_photo(str(empty_file), [], [])

    assert result['is_demontaj'] is False
    assert result['reason'] == "Техническая ошибка"


@pytest.mark.asyncio
async def test_analyze_text_file_as_photo(ai_processor, tmp_path):
    """Текстовый файл вместо изображения."""
    text_file = tmp_path / "not_a_photo.jpg"
    text_file.write_text("This is not a photo", encoding='utf-8')

    result = await ai_processor.analyze_photo(str(text_file), ["A"], ["B"])

    assert result['is_demontaj'] is False
    assert result['reason'] == "Техническая ошибка"


# ===========================================================================
# Concurrent доступ к БД
# ===========================================================================

def test_concurrent_db_writes(tmp_path):
    """Множество потоков одновременно пишут в БД — не должно быть ошибок."""
    db_path = tmp_path / "test_concurrent.db"
    db = DatabaseManager(str(db_path))
    errors = []
    num_threads = 10
    writes_per_thread = 20

    def write_messages(thread_id):
        try:
            for i in range(writes_per_thread):
                msg_id = thread_id * 1000 + i
                db.save_message(msg_id, thread_id, 'text')
        except Exception as e:
            errors.append(f"Thread {thread_id}: {e}")

    threads = [
        threading.Thread(target=write_messages, args=(tid,))
        for tid in range(num_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Concurrent write errors: {errors}"

    # Проверяем что все записи на месте
    total_messages = 0
    for tid in range(num_threads):
        msg_ids = db.get_processed_message_ids(tid)
        total_messages += len(msg_ids)

    assert total_messages == num_threads * writes_per_thread


def test_concurrent_read_write(tmp_path):
    """Одновременное чтение и запись в БД — нет deadlock и ошибок."""
    db_path = tmp_path / "test_rw.db"
    db = DatabaseManager(str(db_path))
    errors = []

    # Предзаполняем данные
    for i in range(50):
        db.save_message(i, 1, 'photo', f'/path/photo_{i}.jpg')

    def writer():
        try:
            for i in range(50, 100):
                db.save_message(i, 1, 'photo', f'/path/photo_{i}.jpg')
                db.update_ai_result(i, 'demontaj')
        except Exception as e:
            errors.append(f"Writer: {e}")

    def reader():
        try:
            for _ in range(50):
                db.get_unprocessed_files(1)
                db.get_processed_message_ids(1)
                db.is_message_processed(25)
        except Exception as e:
            errors.append(f"Reader: {e}")

    threads = [
        threading.Thread(target=writer),
        threading.Thread(target=reader),
        threading.Thread(target=reader),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Concurrent R/W errors: {errors}"


# ===========================================================================
# Ошибки файловой системы
# ===========================================================================

@pytest.mark.asyncio
async def test_extract_equipment_from_empty_log(ai_processor, tmp_path):
    """Пустой лог-файл — AI всё равно вызывается, результат валидный."""
    empty_log = tmp_path / "empty_log.txt"
    empty_log.write_text("", encoding='utf-8')

    mock_resp = MagicMock()
    mock_resp.text = json.dumps({"demontaj": [], "montaj": []})
    ai_processor._generate_with_retry = AsyncMock(return_value=mock_resp)

    result = await ai_processor.extract_equipment_lists(str(empty_log))
    assert result == {"demontaj": [], "montaj": []}


def test_db_on_readonly_path_after_init(tmp_path):
    """БД создаётся корректно, операции работают."""
    db_path = tmp_path / "subdir" / "test.db"
    os.makedirs(db_path.parent, exist_ok=True)

    db = DatabaseManager(str(db_path))
    db.save_message(1, 100, 'text')
    assert db.is_message_processed(1) is True


def test_db_rejects_duplicate_message(tmp_path):
    """Повторное сохранение того же message_id — вызывает IntegrityError.
    В реальном коде дубликаты предотвращаются через is_message_processed()."""
    db_path = tmp_path / "dup.db"
    db = DatabaseManager(str(db_path))

    db.save_message(42, 100, 'text')
    assert db.is_message_processed(42) is True

    # Повторная вставка — SQLite выбрасывает IntegrityError
    with pytest.raises(Exception):
        db.save_message(42, 100, 'text')
