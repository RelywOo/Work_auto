import asyncio
import logging
from typing import Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os
from modules.utils import extract_site_id, create_readme_file, create_zip_report

class TaskQueue:
    """Асинхронная очередь задач для обработки топиков (Producer-Consumer архитектура)."""
    
    def __init__(self, max_workers: int = 2, telegram_client=None, main_loop=None):
        self.queue = asyncio.Queue()
        self.logger = logging.getLogger(__name__)
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks = set()
        self.processed_topics = set()
        self.telegram_client = telegram_client  # ← Сохраняем активный клиент
        self.main_loop = main_loop  # ← Сохраняем основной event loop
        
    async def add_topic_task(self, topic_id: int, topic_title: str, priority: int = 1):
        """Добавить задачу на обработку топика в очередь."""
        task_data = {
            'topic_id': topic_id,
            'topic_title': topic_title,
            'priority': priority,
            'added_time': datetime.now(),
            'status': 'queued'
        }
        
        await self.queue.put(task_data)
        self.logger.info(f"📋 Топик {topic_title} (ID: {topic_id}) добавлен в очередь обработки")
        
    async def start_consumer(self):
        """Запустить Consumer задач для обработки очереди."""
        self.logger.info(f"🚀 Запускаю Consumer задач ({self.max_workers} воркеров)...")
        
        # Запускаем несколько воркеров для параллельной обработки
        tasks = [
            asyncio.create_task(self._worker(f"Worker-{i+1}"))
            for i in range(self.max_workers)
        ]
        
        # Ждем завершения всех воркеров
        await asyncio.gather(*tasks)
        
    async def _worker(self, worker_name: str):
        """Рабочий процесс для обработки задач из очереди."""
        self.logger.info(f"👷 {worker_name} готов к обработке задач")
        
        while True:
            try:
                # Получаем задачу из очереди (блокирующая операция)
                task_data = await self.queue.get()
                
                # Проверяем, не обрабатывали ли уже этот топик
                topic_id = task_data['topic_id']
                if topic_id in self.processed_topics:
                    self.logger.warning(f"⚠️ Топик {topic_id} уже был обработан, пропускаем")
                    self.queue.task_done()
                    continue
                
                # Помечаем как обрабатываемый
                task_data['status'] = 'processing'
                task_data['worker'] = worker_name
                task_data['start_time'] = datetime.now()
                
                self.logger.info(f"🔨 {worker_name} начинает обработку топика {task_data['topic_title']} (ID: {topic_id})")
                
                # Обрабатываем в отдельном потоке, чтобы не блокировать event loop
                try:
                    await asyncio.to_thread(self._process_topic_heavy, task_data)
                    task_data['status'] = 'completed'
                    task_data['end_time'] = datetime.now()
                    
                    processing_time = (task_data['end_time'] - task_data['start_time']).total_seconds()
                    self.logger.info(f"✅ {worker_name} завершил обработку топика {topic_id} за {processing_time:.1f} сек")
                    
                except Exception as e:
                    task_data['status'] = 'failed'
                    task_data['error'] = str(e)
                    task_data['end_time'] = datetime.now()
                    self.logger.error(f"❌ {worker_name} не смог обработать топик {topic_id}: {e}")
                
                finally:
                    # Помечаем как обработанный
                    self.processed_topics.add(topic_id)
                    self.queue.task_done()
                    
            except asyncio.CancelledError:
                self.logger.info(f"⏹️ {worker_name} останавливается")
                break
            except Exception as e:
                self.logger.error(f"🚨 {worker_name} столкнулся с ошибкой: {e}")
                await asyncio.sleep(1)  # Пауза перед следующей попыткой
                
    def _process_topic_heavy(self, task_data: Dict[str, Any]):
        """Тяжелая обработка топика в отдельном потоке (ИИ, архивация и т.д.)."""
        topic_id = task_data['topic_id']
        topic_title = task_data['topic_title']
        
        try:
            # Импортируем здесь для избежания circular imports
            from modules.ai_processor import AIProcessor, organize_files_by_analysis
            
            # Используем существующий активный клиент
            telegram_client = self.telegram_client
            if not telegram_client:
                raise Exception("Telegram клиент не передан в очередь!")
            
            # Используем run_coroutine_threadsafe для выполнения в основном loop
            future = asyncio.run_coroutine_threadsafe(
                self.telegram_client.download_topic_content(topic_id, message_limit=100),
                self.main_loop
            )
            download_result = future.result(timeout=300)  # 5 минут таймаут
            
            if download_result['photos'] > 0 or download_result['text_messages'] > 0:
                # Извлекаем ID сайта
                site_id = extract_site_id(topic_title)
                
                # Запускаем ИИ-обработку
                ai_processor = AIProcessor()
                topic_dir = os.path.join('downloads', f'topic_{topic_id}')
                log_file_path = os.path.join(topic_dir, 'messages_log.txt')
                
                # Извлекаем списки оборудования
                equipment_lists = ai_processor.extract_equipment_lists(log_file_path)
                
                # Создаем README
                text_messages = []
                try:
                    with open(log_file_path, 'r', encoding='utf-8') as f:
                        text_messages = f.readlines()
                except Exception:
                    pass
                
                # Создаем README.txt
                readme_path = create_readme_file(topic_dir, equipment_lists, text_messages)
                self.logger.info(f"📝 README.txt создан: {readme_path}")
                
                # Анализируем фотографии
                if equipment_lists['demontaj']:
                    analysis_results = ai_processor.process_photos_batch(
                        topic_dir, 
                        equipment_lists['demontaj'],
                        equipment_lists['montaj'],
                        delay_seconds=2
                    )
                    
                    # Организуем файлы
                    organize_files_by_analysis(topic_dir, analysis_results)
                    
                    # Создаем ZIP-архив
                    output_folder = "output"
                    zip_path = create_zip_report(site_id, topic_dir, output_folder)
                    self.logger.info(f"📦 ZIP-архив создан: {zip_path}")
                
            else:
                self.logger.warning(f"Топик {topic_id} пуст, нечего обрабатывать")
                    
        except Exception as e:
            raise Exception(f"Ошибка при обработке топика {topic_id}: {e}")
    
    async def get_queue_status(self) -> Dict[str, Any]:
        """Получить статус очереди и активных задач."""
        return {
            'queue_size': self.queue.qsize(),
            'active_tasks': len(self.active_tasks),
            'processed_topics': len(self.processed_topics),
            'max_workers': self.max_workers
        }
    
    async def shutdown(self):
        """Корректно завершить работу очереди."""
        self.logger.info("🛑 Завершаю работу очереди задач...")
        
        # Отменяем все активные задачи
        for task in self.active_tasks:
            task.cancel()
        
        # Ждем завершения
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks, return_exceptions=True)
        
        # Закрываем executor
        self.executor.shutdown(wait=True)
        self.logger.info("✅ Очередь задач остановлена")
