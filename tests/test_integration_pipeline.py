"""
Интеграционные тесты для TaskQueue pipeline.
Тестируют полный цикл: download -> AI analysis -> packaging -> send & cleanup.
Все внешние зависимости (Telegram, Gemini) замоканы.
"""
import pytest
import asyncio
import json
import os
import shutil
import zipfile
from unittest.mock import MagicMock, AsyncMock, patch
from PIL import Image

os.environ.setdefault('GEMINI_API_KEY', 'test_key')
os.environ.setdefault('TELEGRAM_API_ID', '12345')
os.environ.setdefault('TELEGRAM_API_HASH', 'test_hash')
os.environ.setdefault('TARGET_CHAT_ID', '-100123456')

from modules.task_queue import TaskQueue


def _create_test_photo(path: str):
    """Создаёт минимальное тестовое изображение."""
    img = Image.new('RGB', (10, 10), color='blue')
    img.save(path)


def _make_mock_response(text: str):
    """Создаёт mock-ответ Gemini API."""
    resp = MagicMock()
    resp.text = text
    return resp


@pytest.fixture
def integration_env(tmp_path):
    """Подготовка полного окружения для интеграционного теста."""
    downloads_dir = tmp_path / "downloads"
    output_dir = tmp_path / "output"
    topic_dir = downloads_dir / "topic_100"
    downloads_dir.mkdir()
    output_dir.mkdir()
    topic_dir.mkdir()

    log_file = topic_dir / "messages_log.txt"
    log_file.write_text(
        "Инженер: Demontaj: DCDU12B, Anten T1003\n"
        "Инженер: Montaj: RRU 5516\n",
        encoding='utf-8'
    )

    for i in range(3):
        _create_test_photo(str(topic_dir / f"photo_{i}.jpg"))

    mock_tg = MagicMock()
    mock_tg.db = MagicMock()
    mock_tg.db.get_last_msg_id.return_value = None
    mock_tg.db.get_unprocessed_files.return_value = [
        (10, str(topic_dir / "photo_0.jpg")),
        (11, str(topic_dir / "photo_1.jpg")),
        (12, str(topic_dir / "photo_2.jpg")),
    ]
    mock_tg.db.update_ai_result = MagicMock()
    mock_tg.db.clear_topic_state = MagicMock()
    mock_tg.download_topic_content = AsyncMock(return_value={
        'photos': 3, 'text_messages': 5
    })
    mock_tg.send_message_with_retry = AsyncMock()

    async def _backup_zip_on_send(*args, **kwargs):
        """Save a backup copy of ZIP before pipeline deletes it."""
        file_path = kwargs.get('file') or (args[1] if len(args) > 1 else None)
        if file_path and os.path.exists(file_path):
            shutil.copy2(file_path, str(tmp_path / "backup.zip"))

    mock_tg.send_file_with_retry = AsyncMock(side_effect=_backup_zip_on_send)
    mock_tg.processed_msg_ids = set()

    # main_loop будет установлен внутри теста
    task_queue = TaskQueue(max_workers=1, telegram_client=mock_tg, main_loop=None)

    return {
        'task_queue': task_queue,
        'mock_tg': mock_tg,
        'tmp_path': tmp_path,
        'downloads_dir': downloads_dir,
        'output_dir': output_dir,
        'topic_dir': topic_dir,
    }


# ===========================================================================
# Интеграционный тест: полный happy-path pipeline
# ===========================================================================

@pytest.mark.asyncio
async def test_full_pipeline_happy_path(integration_env):
    """Полный цикл обработки: download -> AI -> package -> send -> cleanup."""
    env = integration_env
    tq = env['task_queue']
    tq.main_loop = asyncio.get_running_loop()

    equipment_json = json.dumps({
        "demontaj": ["DCDU12B", "Anten T1003"],
        "montaj": ["RRU 5516"]
    })
    photo_results = [
        json.dumps({"is_demontaj": True, "equipment_found": "DCDU12B", "reason": "Старое"}),
        json.dumps({"is_demontaj": True, "equipment_found": "Anten T1003", "reason": "Ржавое"}),
        json.dumps({"is_demontaj": False, "equipment_found": "RRU 5516", "reason": "Новое"}),
    ]
    call_count = {'n': 0}

    async def mock_generate_with_retry(self, model, *args, **kwargs):
        result = call_count['n']
        call_count['n'] += 1
        if result == 0:
            return _make_mock_response(equipment_json)
        else:
            return _make_mock_response(photo_results[result - 1])

    with patch('modules.ai_processor.genai') as mock_genai, \
         patch.dict(os.environ, {'DOWNLOADS_DIR': 'downloads'}), \
         patch('modules.task_queue.BASE_DIR', str(env['tmp_path'])):

        mock_genai.GenerativeModel.return_value = MagicMock()
        mock_genai.types.GenerationConfig = MagicMock()

        with patch('modules.ai_processor.AIProcessor._generate_with_retry', mock_generate_with_retry):
            task_data = {
                'topic_id': 100,
                'topic_title': 'UA123 Обслуговування',
                'priority': 1,
                'status': 'processing',
            }

            await asyncio.to_thread(tq._process_topic_heavy, task_data)

    # AI-результаты сохранены в БД
    assert env['mock_tg'].db.update_ai_result.call_count == 3

    # ZIP отправлен
    env['mock_tg'].send_file_with_retry.assert_called_once()
    send_call_args = env['mock_tg'].send_file_with_retry.call_args
    zip_path = send_call_args.kwargs.get('file') or send_call_args[1].get('file')
    assert zip_path.endswith('.zip')

    # ZIP содержал README (проверяем через копию, сохранённую до cleanup)
    backup_zip = env['tmp_path'] / "backup.zip"
    assert backup_zip.exists(), "Backup ZIP should have been saved by send side_effect"
    with zipfile.ZipFile(str(backup_zip), 'r') as zf:
        names = zf.namelist()
        assert "README.txt" in names


# ===========================================================================
# Интеграционный тест: AI не находит списки → should_stop + cleanup
# ===========================================================================

@pytest.mark.asyncio
async def test_pipeline_should_stop_when_no_equipment_lists(integration_env):
    """Если AI не нашёл списки — обработка останавливается, cleanup выполняется."""
    env = integration_env
    tq = env['task_queue']
    tq.main_loop = asyncio.get_running_loop()

    empty_json = json.dumps({"demontaj": [], "montaj": []})

    async def mock_generate_with_retry(self, model, *args, **kwargs):
        return _make_mock_response(empty_json)

    with patch('modules.ai_processor.genai') as mock_genai, \
         patch.dict(os.environ, {'DOWNLOADS_DIR': 'downloads'}), \
         patch('modules.task_queue.BASE_DIR', str(env['tmp_path'])):

        mock_genai.GenerativeModel.return_value = MagicMock()
        mock_genai.types.GenerationConfig = MagicMock()

        with patch('modules.ai_processor.AIProcessor._generate_with_retry', mock_generate_with_retry):
            task_data = {
                'topic_id': 100,
                'topic_title': 'UA123 Обслуговування',
                'priority': 1,
                'status': 'processing',
            }

            await asyncio.to_thread(tq._process_topic_heavy, task_data)

    # Уведомление об ошибке отправлено
    env['mock_tg'].send_message_with_retry.assert_called()

    # БД очищена
    env['mock_tg'].db.clear_topic_state.assert_called_once_with(100)

    # ZIP НЕ отправлялся
    env['mock_tg'].send_file_with_retry.assert_not_called()


# ===========================================================================
# Интеграционный тест: пустой топик
# ===========================================================================

@pytest.mark.asyncio
async def test_pipeline_empty_topic(integration_env):
    """Если 0 фото и 0 сообщений — ничего не обрабатывается."""
    env = integration_env
    tq = env['task_queue']
    tq.main_loop = asyncio.get_running_loop()

    env['mock_tg'].download_topic_content = AsyncMock(return_value={
        'photos': 0, 'text_messages': 0
    })
    env['mock_tg'].db.get_unprocessed_files.return_value = []

    with patch('modules.task_queue.BASE_DIR', str(env['tmp_path'])):
        task_data = {
            'topic_id': 100,
            'topic_title': 'UA123 Test',
            'priority': 1,
            'status': 'processing',
        }

        await asyncio.to_thread(tq._process_topic_heavy, task_data)

    env['mock_tg'].db.update_ai_result.assert_not_called()
    env['mock_tg'].send_file_with_retry.assert_not_called()


# ===========================================================================
# Интеграционный тест: worker берёт задачу из очереди
# ===========================================================================

@pytest.mark.asyncio
async def test_worker_processes_queued_task(integration_env):
    """Worker достаёт задачу из очереди и вызывает _process_topic_heavy."""
    env = integration_env
    tq = env['task_queue']

    tq._process_topic_heavy = MagicMock()
    env['mock_tg'].db.get_last_msg_id.return_value = None
    env['mock_tg'].db.get_unprocessed_files.return_value = [(10, '/fake/photo.jpg')]

    await tq.add_topic_task(100, "UA123 Test")

    worker_task = asyncio.create_task(tq._worker("TestWorker"))
    await asyncio.sleep(0.2)
    worker_task.cancel()

    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    tq._process_topic_heavy.assert_called_once()
    call_args = tq._process_topic_heavy.call_args[0][0]
    assert call_args['topic_id'] == 100


# ===========================================================================
# Тест: graceful shutdown ждёт завершения текущей задачи
# ===========================================================================

@pytest.mark.asyncio
async def test_graceful_shutdown_waits_for_worker(integration_env):
    """shutdown() дожидается завершения текущей задачи воркера без cancel."""
    env = integration_env
    tq = env['task_queue']
    tq.main_loop = asyncio.get_running_loop()

    processing_started = asyncio.Event()
    processing_finished = asyncio.Event()

    original_process = tq._process_topic_heavy

    def slow_process(task_data):
        # Signal that processing has started
        tq.main_loop.call_soon_threadsafe(processing_started.set)
        import time
        time.sleep(0.5)  # Simulate work
        tq.main_loop.call_soon_threadsafe(processing_finished.set)

    tq._process_topic_heavy = MagicMock(side_effect=slow_process)
    env['mock_tg'].db.get_last_msg_id.return_value = None
    env['mock_tg'].db.get_unprocessed_files.return_value = [(10, '/fake/photo.jpg')]

    await tq.add_topic_task(100, "UA123 Test")

    # Start worker
    worker_task = asyncio.create_task(tq._worker("TestWorker"))
    tq.active_tasks.add(worker_task)
    worker_task.add_done_callback(tq.active_tasks.discard)

    # Wait for processing to start
    await processing_started.wait()

    # Call shutdown — should wait for worker to finish, not cancel immediately
    await tq.shutdown()

    # Worker should have finished its task gracefully
    assert processing_finished.is_set(), "Worker task should have completed before shutdown returned"
    tq._process_topic_heavy.assert_called_once()


# ===========================================================================
# Тест: shutdown принудительно cancel()-ит воркеров после таймаута
# ===========================================================================

@pytest.mark.asyncio
async def test_shutdown_force_cancels_after_timeout(integration_env):
    """shutdown() принудительно cancel()-ит воркеров, застрявших дольше таймаута."""
    import modules.task_queue as tq_module

    env = integration_env
    tq = env['task_queue']
    tq.main_loop = asyncio.get_running_loop()

    stuck_started = asyncio.Event()

    def stuck_process(task_data):
        # Signal that processing has started, then block "forever"
        tq.main_loop.call_soon_threadsafe(stuck_started.set)
        import time
        time.sleep(30)  # Simulate a stuck task (much longer than timeout)

    tq._process_topic_heavy = MagicMock(side_effect=stuck_process)
    env['mock_tg'].db.get_last_msg_id.return_value = None
    env['mock_tg'].db.get_unprocessed_files.return_value = [(10, '/fake/photo.jpg')]

    await tq.add_topic_task(100, "UA123 Test")

    worker_task = asyncio.create_task(tq._worker("TestWorker"))
    tq.active_tasks.add(worker_task)
    worker_task.add_done_callback(tq.active_tasks.discard)

    await stuck_started.wait()

    # Use a very short timeout so the test doesn't block
    original_timeout = tq_module.GRACEFUL_SHUTDOWN_TIMEOUT
    tq_module.GRACEFUL_SHUTDOWN_TIMEOUT = 1  # 1 second timeout
    try:
        await tq.shutdown()
    finally:
        tq_module.GRACEFUL_SHUTDOWN_TIMEOUT = original_timeout

    # Worker should have been force-cancelled
    assert worker_task.done(), "Worker task should be done after shutdown (force-cancelled)"
