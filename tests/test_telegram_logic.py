import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import os
import asyncio
import time

# We need to set environment variables before importing TelegramClientManager
# otherwise it might fail on __init__ with missing int(TELEGRAM_API_ID)
os.environ["TELEGRAM_API_ID"] = "12345"
os.environ["TELEGRAM_API_HASH"] = "test_hash"
os.environ["TARGET_CHAT_ID"] = "-100123456"
os.environ["DOWNLOADS_DIR"] = "test_downloads"
os.environ["MAX_WORKERS"] = "2"

from modules.telegram_client import TelegramClientManager


@pytest.fixture
def mock_client_manager():
    # Patch the telethon TelegramClient, DatabaseManager, and TaskQueue
    with (
        patch("modules.telegram_client.TelegramClient", new_callable=MagicMock),
        patch("modules.telegram_client.DatabaseManager", new_callable=MagicMock),
        patch("modules.telegram_client.TaskQueue", new_callable=MagicMock),
    ):
        manager = TelegramClientManager()
        yield manager


def test_should_process_topic(mock_client_manager):
    """Тестирование логики фильтрации топиков should_process_topic()."""
    # Yes cases
    assert mock_client_manager.should_process_topic("UA123 Some issue") is True
    assert mock_client_manager.should_process_topic("UB456 Another issue") is True
    assert mock_client_manager.should_process_topic("issue with UB789") is True
    assert mock_client_manager.should_process_topic("ua001 lowercase") is True
    assert mock_client_manager.should_process_topic("ub007 lowercase") is True

    # No cases (TSS / TSSR / missing UA or UB)
    assert mock_client_manager.should_process_topic("UA123 TSS Issue") is False
    assert mock_client_manager.should_process_topic("UA123 TSSR Issue") is False
    assert mock_client_manager.should_process_topic("General Discussion") is False
    assert mock_client_manager.should_process_topic("VDO 123 Issue") is False

    # Edge cases
    assert mock_client_manager.should_process_topic("") is False
    assert mock_client_manager.should_process_topic(None) is False


@pytest.mark.asyncio
async def test_reset_topic_timer(mock_client_manager):
    """Тестирование работы таймеров топиков (reset_topic_timer)."""
    # Reduce wait time for tests but keep it robust for Windows timing
    mock_client_manager.wait_time = 0.2

    # Mock add_topic_to_queue to observe if it's called
    mock_client_manager.add_topic_to_queue = AsyncMock()

    # Initial call sets the timer
    topic_id = 999
    topic_title = "UA123 Text"

    await mock_client_manager.reset_topic_timer(topic_id, topic_title)

    assert topic_id in mock_client_manager.active_topics
    assert mock_client_manager.active_topics[topic_id]["title"] == topic_title

    # Store initial time to check update later
    first_time = mock_client_manager.active_topics[topic_id]["last_message_time"]

    # Wait shortly and reset the timer again to simulate another message
    await asyncio.sleep(0.05)
    await mock_client_manager.reset_topic_timer(topic_id, topic_title)

    second_time = mock_client_manager.active_topics[topic_id]["last_message_time"]
    assert second_time > first_time

    # Wait for the timer to expire
    await asyncio.sleep(0.3)

    # After expiration, the topic should be removed from active lists
    # and moved to the processing queue.
    assert topic_id not in mock_client_manager.active_topics
    mock_client_manager.add_topic_to_queue.assert_called_once_with(
        topic_id, topic_title=topic_title
    )


@pytest.mark.asyncio
async def test_get_topic_title_by_id_cache(mock_client_manager):
    """Тест проверки логики кэша в get_topic_title_by_id."""

    # Pre-populate the cache manually to verify it bypasses network calls
    topic_id = 555
    cached_title = "Cached Title UA123"

    mock_client_manager._topic_cache[topic_id] = (cached_title, time.time() + 3600)

    title = await mock_client_manager.get_topic_title_by_id(topic_id)

    # Result should come directly from cache
    assert title == cached_title
    mock_client_manager.client.assert_not_called()  # No API call

    # Test expiration
    mock_client_manager._topic_cache[topic_id] = (
        cached_title,
        time.time() - 100,
    )  # Expired

    # Setting up the mock client to return a mocked result
    mock_result = MagicMock()
    mock_topic = MagicMock()
    mock_topic.id = topic_id
    mock_topic.title = "Fetched Title UA456"
    mock_result.topics = [mock_topic]

    # Because client is called like await self.client(GetForumTopicsRequest(...))
    mock_client_manager.client = AsyncMock(return_value=mock_result)
    mock_client_manager.client.get_entity = AsyncMock()

    title_after_expiry = await mock_client_manager.get_topic_title_by_id(topic_id)

    assert title_after_expiry == "Fetched Title UA456"
    assert mock_client_manager._topic_cache[topic_id][0] == "Fetched Title UA456"
