import sqlite3
import os
import logging
from datetime import datetime
from typing import Optional

class DatabaseManager:
    def __init__(self, db_path: str = "bot_memory.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.init_db()
    
    def init_db(self):
        """Создает файл БД и таблицы processed_topics и processed_messages, если их нет"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_topics (
                    topic_id INTEGER PRIMARY KEY,
                    title TEXT,
                    last_msg_id INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_messages (
                    message_id INTEGER PRIMARY KEY,
                    topic_id INTEGER,
                    message_type TEXT NOT NULL, -- 'text' or 'photo'
                    file_path TEXT, -- Nullable for text messages
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Миграция для существующих БД: добавляем колонку message_type если её нет
            try:
                cursor.execute("PRAGMA table_info(processed_messages)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'message_type' not in columns:
                    cursor.execute("ALTER TABLE processed_messages ADD COLUMN message_type TEXT NOT NULL DEFAULT 'photo'")
                    self.logger.info("Добавлена колонка message_type в таблицу processed_messages")
                
                # Обновляем существующие записи без message_type на 'photo'
                cursor.execute("UPDATE processed_messages SET message_type = 'photo' WHERE message_type IS NULL")
                
                # Миграция для ИИ-обработки: добавляем колонки ai_processed и ai_result если их нет
                if 'ai_processed' not in columns:
                    cursor.execute("ALTER TABLE processed_messages ADD COLUMN ai_processed INTEGER DEFAULT 0")
                    self.logger.info("Добавлена колонка ai_processed в таблицу processed_messages")
                
                if 'ai_result' not in columns:
                    cursor.execute("ALTER TABLE processed_messages ADD COLUMN ai_result TEXT DEFAULT NULL")
                    self.logger.info("Добавлена колонка ai_result в таблицу processed_messages")
                
            except Exception as e:
                self.logger.error(f"Ошибка при миграции БД: {e}")
            
            conn.commit()
    
    def get_last_msg_id(self, topic_id: int) -> Optional[int]:
        """Возвращает last_msg_id для топика, либо None, если топик новый"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_msg_id FROM processed_topics WHERE topic_id = ?",
                (topic_id,)
            )
            result = cursor.fetchone()
            return result[0] if result else None
    
    def update_topic(self, topic_id: int, title: str, last_msg_id: int):
        """Добавляет новый топик в базу или обновляет last_msg_id у существующего"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO processed_topics 
                (topic_id, title, last_msg_id, updated_at) 
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (topic_id, title, last_msg_id))
            conn.commit()
    
    def get_topic_info(self, topic_id: int) -> Optional[tuple]:
        """Возвращает полную информацию о топике"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT topic_id, title, last_msg_id, updated_at FROM processed_topics WHERE topic_id = ?",
                (topic_id,)
            )
            return cursor.fetchone()
    
    def get_all_topics(self) -> list:
        """Возвращает все обработанные топики"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT topic_id, title, last_msg_id, updated_at FROM processed_topics ORDER BY updated_at DESC"
            )
            return cursor.fetchall()
    
    def is_message_processed(self, message_id: int) -> bool:
        """Проверяет, было ли сообщение уже обработано (скачано)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT message_id FROM processed_messages WHERE message_id = ?",
                (message_id,)
            )
            result = cursor.fetchone()
            return result is not None
    
    def save_message(self, message_id: int, topic_id: int, message_type: str, file_path: str = None):
        """Сохраняет информацию о скачанном файле или текстовом сообщении в базу данных"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO processed_messages (message_id, topic_id, message_type, file_path)
                VALUES (?, ?, ?, ?)
            ''', (message_id, topic_id, message_type, file_path))
            conn.commit()
    
    def get_unprocessed_files(self, topic_id: int) -> list[tuple]:
        """Возвращает список необработанных ИИ файлов для топика"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT message_id, file_path FROM processed_messages 
                WHERE topic_id = ? AND ai_processed = 0 AND file_path IS NOT NULL
            ''', (topic_id,))
            return cursor.fetchall()
    
    def update_ai_result(self, message_id: int, result: str):
        """Обновляет результат ИИ-обработки для сообщения"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE processed_messages 
                SET ai_processed = 1, ai_result = ? 
                WHERE message_id = ?
            ''', (result, message_id))
            conn.commit()
