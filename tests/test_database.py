import pytest
from modules.database import DatabaseManager

@pytest.fixture
def db_manager(tmp_path):
    # Use a temporary file for testing
    db_path = tmp_path / "test_bot_memory.db"
    db = DatabaseManager(str(db_path))
    yield db

def test_init_db(db_manager):
    """Проверка создания таблиц."""
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Check processed_topics table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='processed_topics'")
        assert cursor.fetchone() is not None

        # Check processed_messages table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='processed_messages'")
        assert cursor.fetchone() is not None

        # Check columns in processed_messages
        cursor.execute("PRAGMA table_info(processed_messages)")
        columns = [row[1] for row in cursor.fetchall()]
        assert 'message_type' in columns
        assert 'ai_processed' in columns
        assert 'ai_result' in columns

def test_update_and_get_topic(db_manager):
    """Тест сохранения и получения информации о топике."""
    # The topic does not exist yet
    assert db_manager.get_last_msg_id(100) is None

    # Add new topic
    db_manager.update_topic(100, "UA123 Test", 500)
    
    # Retrieve last_msg_id
    assert db_manager.get_last_msg_id(100) == 500

    # Update existing topic
    db_manager.update_topic(100, "UA123 Test Updated", 600)
    assert db_manager.get_last_msg_id(100) == 600

def test_save_and_check_messages(db_manager):
    """Тест сохранения информации об обработанных сообщениях."""
    # Ensure initial state
    assert db_manager.is_message_processed(10) is False
    assert db_manager.get_processed_message_ids(100) == set()

    # Create parent topic first (FK constraint)
    db_manager.update_topic(100, "Test Topic", 0)

    # Save a text message
    db_manager.save_message(10, 100, 'text')
    
    # Check if processed
    assert db_manager.is_message_processed(10) is True

    # Save a photo message
    db_manager.save_message(11, 100, 'photo', '/path/to/photo.jpg')
    
    assert db_manager.is_message_processed(11) is True

    # Get processed message ids for topic 100
    msg_ids = db_manager.get_processed_message_ids(100)
    assert msg_ids == {10, 11}

    # Get unprocessed files
    unprocessed = db_manager.get_unprocessed_files(100)
    assert len(unprocessed) == 1
    assert unprocessed[0][0] == 11
    assert unprocessed[0][1] == '/path/to/photo.jpg'

    # Update AI result
    db_manager.update_ai_result(11, "some ai result")
    
    # Verify unprocessed files is now empty for topic 100
    unprocessed_after = db_manager.get_unprocessed_files(100)
    assert len(unprocessed_after) == 0

def test_get_topics_with_unprocessed_files(db_manager):
    """Тест получения топиков с необработанными файлами."""
    assert db_manager.get_topics_with_unprocessed_files() == []

    # Create parent topics first (FK constraint)
    db_manager.update_topic(200, "Topic 200", 0)
    db_manager.update_topic(300, "Topic 300", 0)

    # Add processed text and a photo for topic 200
    db_manager.save_message(1, 200, 'text')
    db_manager.save_message(2, 200, 'photo', '/path/to/img1.jpg')

    # Add a photo for topic 300
    db_manager.save_message(3, 300, 'photo', '/path/to/img2.jpg')

    topics = db_manager.get_topics_with_unprocessed_files()
    assert set(topics) == {200, 300}

    # Process all files for topic 200
    db_manager.update_ai_result(2, "done")
    
    topics_after = db_manager.get_topics_with_unprocessed_files()
    assert topics_after == [300]
