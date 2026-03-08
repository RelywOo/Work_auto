import asyncio
import logging
from typing import Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os
import shutil
from modules.utils import extract_site_id, create_readme_file, create_zip_report, safe_rmtree

class TaskQueue:
    """Асинхронная очередь задач для обработки топиков (Producer-Consumer архитектура)."""
    
    def __init__(self, max_workers: int = 2, telegram_client=None, main_loop=None):
        self.queue = asyncio.Queue()
        self.logger = logging.getLogger(__name__)
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks = set()
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
                
                # Проверяем, есть ли новые сообщения для этого топика
                topic_id = task_data['topic_id']
                last_msg_id = self.telegram_client.db.get_last_msg_id(topic_id)
                
                # Проверяем, есть ли файлы для ИИ-обработки
                unprocessed_files = self.telegram_client.db.get_unprocessed_files(topic_id)
                has_ai_files = len(unprocessed_files) > 0
                
                # Определяем, нужно ли обрабатывать топик
                should_process = False
                if last_msg_id is None:
                    self.logger.info(f"🆕 Новый топик {topic_id}, начинаем обработку")
                    should_process = True
                elif has_ai_files:
                    self.logger.info(f"🤖 Топик {topic_id} имеет {len(unprocessed_files)} необработанных ИИ файлов")
                    should_process = True
                else:
                    self.logger.info(f"ℹ️ Топик {topic_id} уже обработан, пропускаем")
                
                # Если не нужно обрабатывать, помечаем задачу как завершенную и продолжаем
                if not should_process:
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
                
                # База данных теперь отслеживает обработанные топики через last_msg_id
                # Дополнительное помечение не требуется
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
            from modules.ai_processor import AIProcessor 
            
            # Используем существующий активный клиент
            telegram_client = self.telegram_client
            if not telegram_client:
                raise Exception("Telegram клиент не передан в очередь!")
            
            # Используем run_coroutine_threadsafe для выполнения в основном loop
            future = asyncio.run_coroutine_threadsafe(
                self.telegram_client.download_topic_content(topic_id, photo_message_limit=100),
                self.main_loop
            )
            download_result = future.result(timeout=300)  # 5 минут таймаут
            
            # Проверяем, есть ли файлы для ИИ-обработки (независимо от новых скачиваний)
            unprocessed_files = telegram_client.db.get_unprocessed_files(topic_id)
            
            if download_result['photos'] > 0 or download_result['text_messages'] > 0 or unprocessed_files:
                # Извлекаем ID сайта
                site_id = extract_site_id(topic_title)
                
                # Детекция VDO-топика
                topic_title_upper = topic_title.upper()
                is_vdo = 'VDO' in topic_title_upper or 'ВДО' in topic_title_upper
                
                demontaj_count = 0
                montaj_count = 0
                unknown_count = 0
                
                # === ИНТЕГРАЦИЯ ИИ-ОБРАБОТКИ И СОРТИРОВКИ ===
                try:
                    # Инициализируем ИИ-процессор
                    from modules.ai_processor import AIProcessor
                    ai_processor = AIProcessor()
                    
                    if unprocessed_files:
                        self.logger.info(f"🤖 Найдено {len(unprocessed_files)} файлов для ИИ-обработки")
                        
                        # Извлекаем списки оборудования из лога
                        topic_dir = os.path.join('downloads', f'topic_{topic_id}')
                        log_file_path = os.path.join(topic_dir, 'messages_log.txt')
                        equipment_lists = {'montaj': [], 'demontaj': []}  # Списки по умолчанию
                        
                        if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > 0:
                            # Если текст есть - парсим как обычно
                            equipment_lists = ai_processor.extract_equipment_lists(log_file_path)
                            
                        has_montaj = bool(equipment_lists.get('montaj'))
                        has_demontaj = bool(equipment_lists.get('demontaj'))
                        
                        # Fallback for VDO topics
                        if is_vdo and not (has_montaj or has_demontaj):
                            self.logger.info(f"🔍 В VDO топике не нашли списков (или лог пуст). Ищем основной топик для сайта {site_id}...")
                            try:
                                main_topic_future = asyncio.run_coroutine_threadsafe(
                                    telegram_client.find_main_topic_for_vdo(site_id),
                                    self.main_loop
                                )
                                main_topic_id = main_topic_future.result(timeout=30)
                                if main_topic_id:
                                    self.logger.info(f"Основной топик найден (ID: {main_topic_id}). Скачиваем текстовые логи...")
                                    text_log_future = asyncio.run_coroutine_threadsafe(
                                        telegram_client.get_topic_text_log(main_topic_id),
                                        self.main_loop
                                    )
                                    text_log = text_log_future.result(timeout=120)
                                    if text_log:
                                        with open(log_file_path, 'w', encoding='utf-8') as f:
                                            f.write(text_log)
                                        self.logger.info(f"Текстовые логи перезаписаны из основного топика. Повторный запуск извлечения...")
                                        equipment_lists = ai_processor.extract_equipment_lists(log_file_path)
                                        has_montaj = bool(equipment_lists.get('montaj'))
                                        has_demontaj = bool(equipment_lists.get('demontaj'))
                            except Exception as e:
                                self.logger.error(f"❌ Ошибка при поиске/загрузке основного топика: {e}")

                        # === ВАЛИДАЦИЯ СПИСКОВ ===
                        if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > 0:
                            if not is_vdo:
                                if not has_montaj and not has_demontaj:
                                    error_msg = f"🚨 Внимание! В топике '{topic_title}' не найдены списки монтажа/демонтажа. Обработка остановлена. Пожалуйста, проверьте топик вручную."
                                    self.logger.warning(error_msg)
                                    
                                    try:
                                        self.logger.info("Попытка отправить уведомление в Telegram")
                                        asyncio.run_coroutine_threadsafe(
                                            telegram_client.client.send_message('me', error_msg),
                                            self.main_loop
                                        ).result(timeout=10)
                                        self.logger.info("Уведомление успешно отправлено в Telegram.")
                                    except Exception as e:
                                        self.logger.error(f"Не удалось отправить уведомление в Telegram: {e}")
                                        
                                    return
                                else:
                                    self.logger.info(f"✅ Списки оборудования найдены, продолжаем обработку")
                            else:
                                if not has_montaj and not has_demontaj:
                                    error_msg = f"🚨 Внимание! (VDO топик) В топике '{topic_title}' не найдены списки монтажа/демонтажа. Пожалуйста, проверьте топик вручную."
                                    self.logger.warning(error_msg)
                                    
                                    try:
                                        self.logger.info("Попытка отправить уведомление в Telegram (VDO)")
                                        asyncio.run_coroutine_threadsafe(
                                            telegram_client.client.send_message('me', error_msg),
                                            self.main_loop
                                        ).result(timeout=10)
                                        self.logger.info("Уведомление успешно отправлено в Telegram.")
                                    except Exception as e:
                                        self.logger.error(f"Не удалось отправить уведомление в Telegram: {e}")
                        else:
                            if is_vdo:
                                self.logger.info(f"ℹ️ VDO топик без текстового отчета. Продолжаем работу.")
                                
                                error_msg = f"🚨 Внимание! (VDO топик) В топике '{topic_title}' отсутствует текстовый лог с списками оборудования даже после поиска. Пожалуйста, проверьте топик вручную."
                                self.logger.warning(error_msg)
                                
                                try:
                                    self.logger.info("Попытка отправить уведомление в Telegram (VDO)")
                                    asyncio.run_coroutine_threadsafe(
                                        telegram_client.client.send_message('me', error_msg),
                                        self.main_loop
                                    ).result(timeout=10)
                                    self.logger.info("Уведомление успешно отправлено в Telegram.")
                                except Exception as e:
                                    self.logger.error(f"Не удалось отправить уведомление в Telegram: {e}")
                                
                                # Добавляем "заглушку" для ИИ, чтобы он всё равно искал демонтаж
                                equipment_lists['demontaj'] = ['Любое старое/демонтированное оборудование VDO']
                            else:
                                error_msg = f"🚨 Внимание! В топике '{topic_title}' отсутствует текстовый лог с списками оборудования. Обработка остановлена. Пожалуйста, проверьте топик вручную."
                                self.logger.warning(error_msg)
                                
                                try:
                                    self.logger.info("Попытка отправить уведомление в Telegram")
                                    asyncio.run_coroutine_threadsafe(
                                        telegram_client.client.send_message('me', error_msg),
                                        self.main_loop
                                    ).result(timeout=10)
                                    self.logger.info("Уведомление успешно отправлено в Telegram.")
                                except Exception as e:
                                    self.logger.error(f"Не удалось отправить уведомление в Telegram: {e}")
                                    
                                return
                        
                        # Создаем папки для сортировки
                        montaj_dir = os.path.join(topic_dir, 'Montaj')
                        demontaj_dir = os.path.join(topic_dir, 'Demontaj')
                        unknown_dir = os.path.join(topic_dir, 'Unknown')
                        
                        os.makedirs(montaj_dir, exist_ok=True)
                        os.makedirs(demontaj_dir, exist_ok=True)
                        os.makedirs(unknown_dir, exist_ok=True)
                        
                        # Обрабатываем каждый файл
                        for message_id, file_path in unprocessed_files:
                            try:
                                # Вариант А: Если это VDO, сразу в демонтаж без ИИ
                                if is_vdo:
                                    ai_result = 'demontaj'
                                    target_dir = demontaj_dir
                                    self.logger.info(f"🚀 VDO топик: файл {os.path.basename(file_path)} сразу направлен в демонтаж")
                                else:
                                    # Анализируем фото через ИИ
                                    analysis_result = ai_processor.analyze_photo(
                                        file_path, 
                                        equipment_lists['demontaj'], 
                                        equipment_lists['montaj']
                                    )
                                    
                                    # Определяем результат
                                    if analysis_result['is_demontaj']:
                                        ai_result = 'demontaj'
                                        target_dir = demontaj_dir
                                    elif analysis_result['equipment_found']:
                                        ai_result = 'montaj'
                                        target_dir = montaj_dir
                                    else:
                                        ai_result = 'unknown'
                                        target_dir = unknown_dir
                                
                                # СНАЧАЛА физически перемещаем файл
                                filename = os.path.basename(file_path)
                                target_path = os.path.join(target_dir, filename)
                                
                                try:
                                    shutil.move(file_path, target_path)
                                    self.logger.info(f"📁 Файл {filename} перемещен в {os.path.basename(target_dir)}")
                                    
                                    # ПОТОМ обновляем результат в БД (только если перемещение успешно)
                                    telegram_client.db.update_ai_result(message_id, ai_result)
                                    self.logger.info(f"🤖 Файл {filename} классифицирован как '{ai_result}' и записан в БД")
                                    
                                except Exception as move_error:
                                    self.logger.error(f"❌ Ошибка перемещения файла {filename}: {move_error}")
                                    # Не обновляем БД, оставляем файл для повторной обработки
                                    raise
                                
                            except Exception as e:
                                self.logger.error(f"❌ Ошибка при обработке файла {file_path}: {e}")
                                # Не обновляем ai_result, оставляем 0 для повторной обработки
                        
                        # Подсчитываем статистику по отсортированным файлам
                        demontaj_count = len(os.listdir(demontaj_dir)) if os.path.exists(demontaj_dir) else 0
                        montaj_count = len(os.listdir(montaj_dir)) if os.path.exists(montaj_dir) else 0
                        unknown_count = len(os.listdir(unknown_dir)) if os.path.exists(unknown_dir) else 0
                        
                        self.logger.info(f"📊 Организация файлов: Демонтаж={demontaj_count}, Монтаж={montaj_count}, Другое={unknown_count}")
                        self.logger.info(f"✅ ИИ-обработка топика {topic_id} завершена")
                    else:
                        self.logger.info(f"ℹ️ Нет файлов для ИИ-обработки в топике {topic_id}")
                        
                except Exception as e:
                    self.logger.error(f"🚨 Критическая ошибка ИИ-обработки топика {topic_id}: {e}")
                    # Продолжаем выполнение, но логируем ошибку
                
                # === СУЩЕСТВУЮЩАЯ ЛОГИКА ===
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
                
                # Создаем ZIP-архив
                output_folder = "output"
                zip_path = create_zip_report(site_id, topic_dir, output_folder, topic_title)
                self.logger.info(f"📦 ZIP-архив создан: {zip_path}")
                
                # === Отправка архива в Telegram ===
                try:
                    vdo_info = "VDO" if is_vdo else "не VDO"
                    summary = f"Сайт {site_id} ({vdo_info}) обработан✅. "
                    if demontaj_count > 0 or montaj_count > 0 or unknown_count > 0:
                        total = demontaj_count + montaj_count + unknown_count
                        recog_pct = int(((demontaj_count + montaj_count) / total) * 100) if total > 0 else 0
                        summary += f"Найдено {demontaj_count} фото демонтажа, {montaj_count} монтажа. Распознано {recog_pct}% оборудования."
                    else:
                        summary += "Новых фото для обработки не было."
                        
                    self.logger.info(f"📤 Отправляем ZIP-архив в Telegram ({zip_path})...")
                    asyncio.run_coroutine_threadsafe(
                        telegram_client.client.send_file('me', file=zip_path, caption=summary),
                        self.main_loop
                    ).result(timeout=60)
                    self.logger.info("✅ ZIP-архив успешно отправлен.")
                except Exception as tg_error:
                    self.logger.error(f"❌ Ошибка отправки ZIP-архива в Telegram: {tg_error}")
                
                # === ТОТАЛЬНАЯ ЗАЧИСТКА (Garbage Collection) ===
                try:
                    self.logger.info(f"🧹 Начинаю удаление временной папки со всеми фото: {topic_dir}")
                    # Используем безопасное удаление с ретраями для Windows
                    safe_rmtree(topic_dir)
                    self.logger.info(f"✅ Зачистка завершена. На диске остался только архив: {zip_path}")
                except Exception as cleanup_error:
                    # Если вдруг какой-то файл занят системой и не удаляется, 
                    # скрипт не упадет, а просто запишет ошибку в лог
                    self.logger.error(f"⚠️ Ошибка при удалении папки {topic_dir}: {cleanup_error}")
                
            else:
                self.logger.warning(f"Топик {topic_id} пуст, нечего обрабатывать")
                    
        except Exception as e:
            raise Exception(f"Ошибка при обработке топика {topic_id}: {e}")
    
    async def get_queue_status(self) -> Dict[str, Any]:
        """Получить статус очереди и активных задач."""
        return {
            'queue_size': self.queue.qsize(),
            'active_tasks': len(self.active_tasks),
            # 'processed_topics': len(self.processed_topics),  # Убрали, теперь используем БД
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
