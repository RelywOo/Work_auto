import sqlite3
import logging
from typing import Optional, List, Tuple, Set

from contextlib import contextmanager


class DatabaseManager:
    def __init__(self, db_path: str = "bot_memory.db"):
        """Initialize database manager and create tables if needed."""
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.init_db()

    @contextmanager
    def _get_connection(self):
        """Yield a per-call SQLite connection with WAL mode enabled."""
        # Open a separate connection per call.
        # SQLite in WAL mode (enabled below) handles concurrent access
        # from multiple threads (readers/writers) on its own.
        with sqlite3.connect(self.db_path, timeout=20.0) as conn:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA foreign_keys = ON")
            except Exception as e:
                self.logger.warning(f"Failed to set PRAGMAs: {e}")
            yield conn

    def init_db(self) -> None:
        """Create DB file and tables (processed_topics, processed_messages) if they don't exist."""
        with self._get_connection() as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_topics (
                    topic_id INTEGER PRIMARY KEY,
                    title TEXT,
                    last_msg_id INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_messages (
                    message_id INTEGER PRIMARY KEY,
                    topic_id INTEGER,
                    message_type TEXT NOT NULL, -- 'text' or 'photo'
                    file_path TEXT, -- Nullable for text messages
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (topic_id) REFERENCES processed_topics(topic_id) ON DELETE CASCADE
                )
            """)

            # Index to speed up per-topic queries (get_unprocessed_files, get_processed_message_ids)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_topic ON processed_messages(topic_id)
            """)

            # Migration for existing DBs: add message_type column if missing
            try:
                cursor.execute("PRAGMA table_info(processed_messages)")
                columns = [column[1] for column in cursor.fetchall()]

                if "message_type" not in columns:
                    cursor.execute(
                        "ALTER TABLE processed_messages ADD COLUMN message_type TEXT NOT NULL DEFAULT 'photo'"
                    )
                    self.logger.info("Added message_type column to processed_messages")

                # Update existing records without message_type to 'photo'
                cursor.execute(
                    "UPDATE processed_messages SET message_type = 'photo' WHERE message_type IS NULL"
                )

                # Migration for AI processing: add ai_processed and ai_result columns if missing
                if "ai_processed" not in columns:
                    cursor.execute(
                        "ALTER TABLE processed_messages ADD COLUMN ai_processed INTEGER DEFAULT 0"
                    )
                    self.logger.info("Added ai_processed column to processed_messages")

                if "ai_result" not in columns:
                    cursor.execute(
                        "ALTER TABLE processed_messages ADD COLUMN ai_result TEXT DEFAULT NULL"
                    )
                    self.logger.info("Added ai_result column to processed_messages")

            except Exception as e:
                self.logger.error(f"DB migration error: {e}")

            conn.commit()

    def get_last_msg_id(self, topic_id: int) -> Optional[int]:
        """Return last_msg_id for a topic, or None if the topic is new."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_msg_id FROM processed_topics WHERE topic_id = ?",
                (topic_id,),
            )
            result = cursor.fetchone()
            return result[0] if result else None

    def update_topic(self, topic_id: int, title: str, last_msg_id: int) -> None:
        """Insert or update a topic record with the latest last_msg_id."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO processed_topics (topic_id, title, last_msg_id, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(topic_id) DO UPDATE SET
                    title = excluded.title,
                    last_msg_id = excluded.last_msg_id,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (topic_id, title, last_msg_id),
            )
            conn.commit()

    def is_message_processed(self, message_id: int) -> bool:
        """Check whether a message has already been processed (downloaded)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT message_id FROM processed_messages WHERE message_id = ?",
                (message_id,),
            )
            result = cursor.fetchone()
            return result is not None

    def get_processed_message_ids(self, topic_id: int) -> Set[int]:
        """Return a set of all processed message IDs for the given topic."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT message_id FROM processed_messages WHERE topic_id = ?",
                (topic_id,),
            )
            result = cursor.fetchall()
            return {row[0] for row in result}

    def save_message(
        self,
        message_id: int,
        topic_id: int,
        message_type: str,
        file_path: Optional[str] = None,
    ) -> None:
        """Save a downloaded file or text message record to the database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO processed_messages (message_id, topic_id, message_type, file_path)
                VALUES (?, ?, ?, ?)
            """,
                (message_id, topic_id, message_type, file_path),
            )
            conn.commit()

    def get_unprocessed_files(self, topic_id: int) -> List[Tuple[int, str]]:
        """Return list of files not yet processed by AI for the given topic."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT message_id, file_path FROM processed_messages
                WHERE topic_id = ? AND ai_processed = 0 AND file_path IS NOT NULL
                ORDER BY message_id ASC
            """,
                (topic_id,),
            )
            return cursor.fetchall()

    def update_ai_result(self, message_id: int, result: str) -> None:
        """Update the AI processing result for a message."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE processed_messages 
                SET ai_processed = 1, ai_result = ? 
                WHERE message_id = ?
            """,
                (result, message_id),
            )
            conn.commit()

    def get_all_topics(self) -> List[Tuple]:
        """Return all processed topics ordered by updated_at DESC."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT topic_id, title, last_msg_id, updated_at FROM processed_topics ORDER BY updated_at DESC"
            )
            return cursor.fetchall()

    def get_topics_with_unprocessed_files(self) -> List[int]:
        """Return list of topic_ids that have downloaded but not yet AI-processed files."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT topic_id FROM processed_messages WHERE ai_processed = 0 AND file_path IS NOT NULL"
            )
            result = cursor.fetchall()
            return [row[0] for row in result]

    def clear_topic_state(self, topic_id: int) -> None:
        """Remove all records for a topic and its messages from the DB.
        Used when processing is stopped early (e.g. no equipment lists found)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM processed_messages WHERE topic_id = ?", (topic_id,)
            )
            cursor.execute(
                "DELETE FROM processed_topics WHERE topic_id = ?", (topic_id,)
            )
            conn.commit()
