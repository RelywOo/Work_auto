import asyncio
import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os
import shutil
from modules.utils import extract_site_id, create_readme_file, create_zip_report, safe_rmtree

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Timeout constants (seconds)
DOWNLOAD_TIMEOUT = 300       # max wait for topic download
NOTIFICATION_TIMEOUT = 10    # max wait for Telegram notification send
SEND_FILE_TIMEOUT = 60       # max wait for ZIP file upload
VDO_SEARCH_TIMEOUT = 30      # max wait for VDO main topic search
VDO_LOG_TIMEOUT = 120        # max wait for VDO text log download
PHOTO_MESSAGE_LIMIT = 100    # max messages to scan for photos

class TaskQueue:
    """Async task queue for topic processing (Producer-Consumer architecture)."""

    def __init__(self, max_workers: int = 2, telegram_client: Any = None, main_loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.queue = asyncio.Queue()
        self.logger = logging.getLogger(__name__)
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks = set()
        self.telegram_client = telegram_client
        self.main_loop = main_loop

        # Metrics
        self.metrics = {
            'topics_processed': 0,
            'topics_failed': 0,
            'total_processing_time': 0.0,
            'ai_errors': 0,
        }

    async def add_topic_task(self, topic_id: int, topic_title: str, priority: int = 1) -> None:
        """Add a topic processing task to the queue."""
        task_data = {
            'topic_id': topic_id,
            'topic_title': topic_title,
            'priority': priority,
            'added_time': datetime.now(),
            'status': 'queued'
        }

        await self.queue.put(task_data)
        self.logger.info(f"📋 Topic {topic_title} (ID: {topic_id}) added to processing queue")

    async def start_consumer(self) -> None:
        """Start the task consumer to process the queue."""
        self.logger.info(f"🚀 Starting task consumer ({self.max_workers} workers)...")

        # Launch multiple workers for parallel processing
        for i in range(self.max_workers):
            task = asyncio.create_task(self._worker(f"Worker-{i+1}"))
            self.active_tasks.add(task)
            task.add_done_callback(self.active_tasks.discard)

        await asyncio.gather(*self.active_tasks)

    async def _worker(self, worker_name: str) -> None:
        """Worker process that consumes tasks from the queue."""
        self.logger.info(f"👷 {worker_name} ready for processing")

        while True:
            try:
                task_data = await self.queue.get()

                topic_id = task_data['topic_id']
                last_msg_id = self.telegram_client.db.get_last_msg_id(topic_id)

                unprocessed_files = self.telegram_client.db.get_unprocessed_files(topic_id)
                has_ai_files = len(unprocessed_files) > 0

                # Determine whether the topic needs processing
                should_process = False
                if last_msg_id is None:
                    self.logger.info(f"🆕 New topic {topic_id}, starting processing")
                    should_process = True
                elif has_ai_files:
                    self.logger.info(f"🤖 Topic {topic_id} has {len(unprocessed_files)} unprocessed AI files")
                    should_process = True
                else:
                    self.logger.info(f"ℹ️ Topic {topic_id} already processed, skipping")

                if not should_process:
                    self.queue.task_done()
                    continue

                task_data['status'] = 'processing'
                task_data['worker'] = worker_name
                task_data['start_time'] = datetime.now()

                self.logger.info(f"🔨 {worker_name} starting topic {task_data['topic_title']} (ID: {topic_id})")

                # Process in a separate thread to avoid blocking the event loop
                try:
                    await asyncio.to_thread(self._process_topic_heavy, task_data)
                    task_data['status'] = 'completed'
                    task_data['end_time'] = datetime.now()

                    processing_time = (task_data['end_time'] - task_data['start_time']).total_seconds()
                    self.metrics['topics_processed'] += 1
                    self.metrics['total_processing_time'] += processing_time
                    self.logger.info(f"✅ {worker_name} finished topic {topic_id} in {processing_time:.1f}s")

                except Exception as e:
                    task_data['status'] = 'failed'
                    task_data['error'] = str(e)
                    task_data['end_time'] = datetime.now()
                    self.metrics['topics_failed'] += 1
                    self.logger.error(f"❌ {worker_name} failed to process topic {topic_id}: {e}")

                # DB tracks processed topics via last_msg_id, no extra marking needed
                self.queue.task_done()

            except asyncio.CancelledError:
                self.logger.info(f"⏹️ {worker_name} stopping")
                break
            except Exception as e:
                self.logger.error(f"🚨 {worker_name} encountered an error: {e}")
                await asyncio.sleep(1)

    def _send_notification(self, message: str, context_info: str = "") -> None:
        """Send a notification to Telegram from a background thread."""
        try:
            log_ctx = f" ({context_info})" if context_info else ""
            self.logger.info(f"Sending Telegram notification{log_ctx}")
            asyncio.run_coroutine_threadsafe(
                self.telegram_client.send_message_with_retry('me', message),
                self.main_loop
            ).result(timeout=NOTIFICATION_TIMEOUT)
            self.logger.info("Telegram notification sent successfully.")
        except Exception as e:
            self.logger.error(f"Failed to send Telegram notification: {e}")

    def _download_phase(self, topic_id: int) -> Tuple[Dict[str, int], List[Tuple[int, str]]]:
        """Download phase: fetch topic content and return unprocessed files."""
        future = asyncio.run_coroutine_threadsafe(
            self.telegram_client.download_topic_content(topic_id, photo_message_limit=PHOTO_MESSAGE_LIMIT),
            self.main_loop
        )
        download_result = future.result(timeout=DOWNLOAD_TIMEOUT)
        unprocessed_files = self.telegram_client.db.get_unprocessed_files(topic_id)
        return download_result, unprocessed_files

    def _ai_analysis_phase(self, topic_id: int, topic_title: str, is_vdo: bool, site_id: str, unprocessed_files: List[Tuple[int, str]], topic_dir: str, log_file_path: str) -> Tuple[int, int, int, Dict[str, List[str]], bool]:
        """AI analysis phase: classify photos and sort into categories."""
        demontaj_count, montaj_count, unknown_count = 0, 0, 0
        equipment_lists = {'montaj': [], 'demontaj': []}
        should_stop = False

        try:
            from modules.ai_processor import AIProcessor
            ai_processor = AIProcessor()

            if unprocessed_files:
                self.logger.info(f"🤖 Found {len(unprocessed_files)} files for AI processing")

                if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > 0:
                    equipment_lists = asyncio.run_coroutine_threadsafe(
                        ai_processor.extract_equipment_lists(log_file_path),
                        self.main_loop
                    ).result()

                has_montaj = bool(equipment_lists.get('montaj'))
                has_demontaj = bool(equipment_lists.get('demontaj'))

                if is_vdo and not (has_montaj or has_demontaj):
                    self.logger.info(f"🔍 No lists found in VDO topic (or log empty). Searching main topic for site {site_id}...")
                    try:
                        main_topic_future = asyncio.run_coroutine_threadsafe(
                            self.telegram_client.find_main_topic_for_vdo(site_id),
                            self.main_loop
                        )
                        main_topic_id = main_topic_future.result(timeout=VDO_SEARCH_TIMEOUT)
                        if main_topic_id:
                            self.logger.info(f"Main topic found (ID: {main_topic_id}). Downloading text logs...")
                            text_log_future = asyncio.run_coroutine_threadsafe(
                                self.telegram_client.get_topic_text_log(main_topic_id),
                                self.main_loop
                            )
                            text_log = text_log_future.result(timeout=VDO_LOG_TIMEOUT)
                            if text_log:
                                with open(log_file_path, 'w', encoding='utf-8') as f:
                                    f.write(text_log)
                                self.logger.info("Text logs overwritten from main topic. Re-running extraction...")
                                equipment_lists = asyncio.run_coroutine_threadsafe(
                                    ai_processor.extract_equipment_lists(log_file_path),
                                    self.main_loop
                                ).result()
                                has_montaj = bool(equipment_lists.get('montaj'))
                                has_demontaj = bool(equipment_lists.get('demontaj'))
                    except Exception as e:
                        self.logger.error(f"❌ Error searching/downloading main topic: {e}")

                if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > 0:
                    if not is_vdo:
                        if not has_montaj and not has_demontaj:
                            # User-facing notification (Russian)
                            error_msg = f"🚨 Внимание! В топике '{topic_title}' не найдены списки монтажа/демонтажа. Обработка остановлена. Пожалуйста, проверьте топик вручную."
                            self.logger.warning(error_msg)
                            self._send_notification(error_msg)
                            return 0, 0, 0, equipment_lists, True
                        else:
                            self.logger.info("✅ Equipment lists found, continuing processing")
                    else:
                        if not has_montaj and not has_demontaj:
                            error_msg = f"🚨 Внимание! (VDO топик) В топике '{topic_title}' не найдены списки монтажа/демонтажа. Пожалуйста, проверьте топик вручную."
                            self.logger.warning(error_msg)
                            self._send_notification(error_msg, "VDO")
                else:
                    if is_vdo:
                        self.logger.info("ℹ️ VDO topic without text report. Continuing.")
                        error_msg = f"🚨 Внимание! (VDO топик) В топике '{topic_title}' отсутствует текстовый лог с списками оборудования даже после поиска. Пожалуйста, проверьте топик вручную."
                        self.logger.warning(error_msg)
                        self._send_notification(error_msg, "VDO")
                        equipment_lists['demontaj'] = ['Любое старое/демонтированное оборудование VDO']
                    else:
                        error_msg = f"🚨 Внимание! В топике '{topic_title}' отсутствует текстовый лог с списками оборудования. Обработка остановлена. Пожалуйста, проверьте топик вручную."
                        self.logger.warning(error_msg)
                        self._send_notification(error_msg)
                        return 0, 0, 0, equipment_lists, True

                montaj_dir = os.path.join(topic_dir, 'Montaj')
                demontaj_dir = os.path.join(topic_dir, 'Demontaj')
                unknown_dir = os.path.join(topic_dir, 'Unknown')

                os.makedirs(montaj_dir, exist_ok=True)
                os.makedirs(demontaj_dir, exist_ok=True)
                os.makedirs(unknown_dir, exist_ok=True)

                for message_id, file_path in unprocessed_files:
                    try:
                        if is_vdo:
                            ai_result = 'demontaj'
                            target_dir = demontaj_dir
                            self.logger.info(f"🚀 VDO topic: file {os.path.basename(file_path)} routed to demontaj")
                        else:
                            analysis_result = asyncio.run_coroutine_threadsafe(
                                ai_processor.analyze_photo(
                                    file_path,
                                    equipment_lists['demontaj'],
                                    equipment_lists['montaj']
                                ),
                                self.main_loop
                            ).result()
                            if analysis_result['is_demontaj']:
                                ai_result = 'demontaj'
                                target_dir = demontaj_dir
                            elif analysis_result['equipment_found']:
                                ai_result = 'montaj'
                                target_dir = montaj_dir
                            else:
                                ai_result = 'unknown'
                                target_dir = unknown_dir

                        filename = os.path.basename(file_path)
                        target_path = os.path.join(target_dir, filename)

                        try:
                            # Copy first, update DB, then remove original to avoid data loss on crash
                            shutil.copy2(file_path, target_path)
                            self.logger.info(f"📁 File {filename} copied to {os.path.basename(target_dir)}")

                            self.telegram_client.db.update_ai_result(message_id, ai_result)
                            self.logger.info(f"🤖 File {filename} classified as '{ai_result}' and saved to DB")

                            # Remove original only after successful DB update
                            if os.path.exists(file_path):
                                os.remove(file_path)

                        except Exception as move_error:
                            self.logger.error(f"❌ Error processing file {filename}: {move_error}")
                            raise

                    except Exception as e:
                        self.logger.error(f"❌ Error processing file {file_path}: {e}")

                demontaj_count = len(os.listdir(demontaj_dir)) if os.path.exists(demontaj_dir) else 0
                montaj_count = len(os.listdir(montaj_dir)) if os.path.exists(montaj_dir) else 0
                unknown_count = len(os.listdir(unknown_dir)) if os.path.exists(unknown_dir) else 0

                self.logger.info(f"📊 File organization: Demontaj={demontaj_count}, Montaj={montaj_count}, Unknown={unknown_count}")
                self.logger.info(f"✅ AI processing of topic {topic_id} completed")
            else:
                self.logger.info(f"ℹ️ No files for AI processing in topic {topic_id}")

        except Exception as e:
            self.metrics['ai_errors'] += 1
            self.logger.error(f"🚨 Critical AI processing error for topic {topic_id}: {e}")

        return demontaj_count, montaj_count, unknown_count, equipment_lists, should_stop

    def _packaging_phase(self, topic_id: int, topic_title: str, site_id: str, topic_dir: str, log_file_path: str, equipment_lists: Dict[str, List[str]]) -> str:
        """Packaging phase: create README and ZIP archive."""
        text_messages = []
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                text_messages = f.readlines()
        except Exception:  # nosec B110 - log file is optional for README
            pass

        readme_path = create_readme_file(topic_dir, equipment_lists, text_messages)
        self.logger.info(f"📝 README.txt created: {readme_path}")

        output_folder = os.path.join(BASE_DIR, "output")
        zip_path = create_zip_report(site_id, topic_dir, output_folder, topic_title)
        self.logger.info(f"📦 ZIP archive created: {zip_path}")
        return zip_path

    def _send_and_cleanup_phase(self, topic_id: int, topic_title: str, site_id: str, is_vdo: bool, demontaj_count: int, montaj_count: int, unknown_count: int, zip_path: str, topic_dir: str) -> None:
        """Send notifications and clean up temporary files."""
        try:
            # User-facing summary (Russian)
            vdo_info = "VDO" if is_vdo else "не VDO"
            summary = f"Сайт {site_id} ({vdo_info}) обработан✅. "
            if demontaj_count > 0 or montaj_count > 0 or unknown_count > 0:
                total = demontaj_count + montaj_count + unknown_count
                recog_pct = int(((demontaj_count + montaj_count) / total) * 100) if total > 0 else 0
                summary += f"Найдено {demontaj_count} фото демонтажа, {montaj_count} монтажа. Распознано {recog_pct}% оборудования."
            else:
                summary += "Новых фото для обработки не было."

            self.logger.info(f"📤 Sending ZIP archive to Telegram ({zip_path})...")
            asyncio.run_coroutine_threadsafe(
                self.telegram_client.send_file_with_retry('me', file=zip_path, caption=summary),
                self.main_loop
            ).result(timeout=SEND_FILE_TIMEOUT)
            self.logger.info("✅ ZIP archive sent successfully.")
        except Exception as tg_error:
            self.logger.error(f"❌ Error sending ZIP archive to Telegram: {tg_error}")

        try:
            self.logger.info(f"🧹 Deleting temporary folder: {topic_dir}")
            safe_rmtree(topic_dir)
            self.logger.info(f"✅ Cleanup done. Only archive remains: {zip_path}")
        except Exception as cleanup_error:
            self.logger.error(f"⚠️ Error deleting folder {topic_dir}: {cleanup_error}")

    def _process_topic_heavy(self, task_data: Dict[str, Any]) -> None:
        """Heavy topic processing in a separate thread (AI, archiving, etc.)."""
        topic_id = task_data['topic_id']
        topic_title = task_data['topic_title']

        try:
            if not getattr(self, 'telegram_client', None):
                raise Exception("Telegram client not provided to the queue!")

            download_result, unprocessed_files = self._download_phase(topic_id)

            if download_result['photos'] > 0 or download_result['text_messages'] > 0 or unprocessed_files:
                site_id = extract_site_id(topic_title)
                topic_title_upper = topic_title.upper()
                is_vdo = 'VDO' in topic_title_upper or 'ВДО' in topic_title_upper

                topic_dir = os.path.join(BASE_DIR, os.getenv('DOWNLOADS_DIR', 'downloads'), f'topic_{topic_id}')
                log_file_path = os.path.join(topic_dir, 'messages_log.txt')

                demontaj_count, montaj_count, unknown_count, equipment_lists, should_stop = self._ai_analysis_phase(
                    topic_id, topic_title, is_vdo, site_id, unprocessed_files, topic_dir, log_file_path
                )

                if should_stop:
                    try:
                        self.logger.info(f"🧹 Cleaning up folder on early stop: {topic_dir}")
                        safe_rmtree(topic_dir)

                        # Clear the DB state for this topic
                        self.telegram_client.db.clear_topic_state(topic_id)

                        # Remove from in-memory cache if present
                        for msg_id, file_path in unprocessed_files:
                            if hasattr(self.telegram_client, 'processed_msg_ids'):
                                if msg_id in self.telegram_client.processed_msg_ids:
                                    self.telegram_client.processed_msg_ids.remove(msg_id)

                        self.logger.info(f"💾 DB and cache cleared: bot forgot topic {topic_id}")
                    except Exception as cleanup_error:
                        self.logger.error(f"⚠️ Error during cleanup/DB reset for {topic_dir}: {cleanup_error}")
                    return

                zip_path = self._packaging_phase(topic_id, topic_title, site_id, topic_dir, log_file_path, equipment_lists)

                self._send_and_cleanup_phase(topic_id, topic_title, site_id, is_vdo, demontaj_count, montaj_count, unknown_count, zip_path, topic_dir)
            else:
                self.logger.warning(f"Topic {topic_id} is empty, nothing to process")

        except Exception as e:
            raise Exception(f"Error processing topic {topic_id}: {e}")

    def get_metrics_summary(self) -> str:
        """Return a formatted string with current metrics."""
        m = self.metrics
        avg_time = (m['total_processing_time'] / m['topics_processed']) if m['topics_processed'] > 0 else 0.0
        return (
            f"Обработано: {m['topics_processed']}, "
            f"Ошибок: {m['topics_failed']}, "
            f"AI ошибок: {m['ai_errors']}, "
            f"Среднее время: {avg_time:.1f}s, "
            f"В очереди: {self.queue.qsize()}"
        )

    async def shutdown(self) -> None:
        """Gracefully shut down the task queue."""
        self.logger.info("🛑 Shutting down task queue...")

        # Copy the set since done-callbacks may modify active_tasks during iteration
        tasks_to_cancel = list(self.active_tasks)
        self.logger.info(f"📋 Cancelling {len(tasks_to_cancel)} active workers...")

        for task in tasks_to_cancel:
            task.cancel()

        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        self.executor.shutdown(wait=True)
        self.logger.info("✅ Task queue stopped")
