import asyncio
import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import threading
from modules.utils import extract_site_id, create_readme_file, create_zip_report, safe_rmtree

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Timeout constants (seconds)
DOWNLOAD_TIMEOUT = 300       # max wait for topic download
NOTIFICATION_TIMEOUT = 10    # max wait for Telegram notification send
SEND_FILE_TIMEOUT_BASE = 60  # base wait for ZIP file upload
SEND_FILE_TIMEOUT_PER_MB = 15  # extra seconds per MB of file size
VDO_SEARCH_TIMEOUT = 30      # max wait for VDO main topic search
VDO_LOG_TIMEOUT = 120        # max wait for VDO text log download
GRACEFUL_SHUTDOWN_TIMEOUT = 60  # max wait for workers to finish before force-cancel
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
        self._shutting_down = asyncio.Event()

        # Metrics lock to prevent race conditions from concurrent updates
        self._metrics_lock = threading.Lock()

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

        while not self._shutting_down.is_set():
            try:
                # Use wait_for to periodically re-check the shutdown flag
                try:
                    task_data = await asyncio.wait_for(
                        self.queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                topic_id = task_data['topic_id']
                loop = asyncio.get_running_loop()
                last_msg_id = await loop.run_in_executor(
                    self.executor, self.telegram_client.db.get_last_msg_id, topic_id
                )

                unprocessed_files = await loop.run_in_executor(
                    self.executor, self.telegram_client.db.get_unprocessed_files, topic_id
                )
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
                    await loop.run_in_executor(self.executor, self._process_topic_heavy, task_data)
                    task_data['status'] = 'completed'
                    task_data['end_time'] = datetime.now()

                    processing_time = (task_data['end_time'] - task_data['start_time']).total_seconds()
                    with self._metrics_lock:
                        self.metrics['topics_processed'] += 1
                        self.metrics['total_processing_time'] += processing_time
                    self.logger.info(f"✅ {worker_name} finished topic {topic_id} in {processing_time:.1f}s")

                except Exception as e:
                    task_data['status'] = 'failed'
                    task_data['error'] = str(e)
                    task_data['end_time'] = datetime.now()
                    with self._metrics_lock:
                        self.metrics['topics_failed'] += 1
                    self.logger.error(f"❌ {worker_name} failed to process topic {topic_id}: {e}")

                # DB tracks processed topics via last_msg_id, no extra marking needed
                self.queue.task_done()

            except asyncio.CancelledError:
                self.logger.info(f"⏹️ {worker_name} cancelled")
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
        self.logger.info(f"DEBUG [worker thread]: unprocessed files after download = {len(unprocessed_files)}, topic_id = {topic_id}")
        return download_result, unprocessed_files

    def _ai_analysis_phase(self, topic_id: int, topic_title: str, is_vdo: bool, site_id: str, unprocessed_files: List[Tuple[int, str]], topic_dir: str, log_file_path: str) -> Tuple[int, int, int, Dict[str, List[str]], bool, bool, bool]:
        """AI analysis phase: classify photos and sort into categories."""
        demontaj_count, montaj_count, unknown_count = 0, 0, 0
        equipment_lists = {'montaj': [], 'demontaj': []}
        should_stop = False
        has_vdo_twin = False
        vdo_twin_topic_id = None
        missing_text_log = False

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

                # For VDO: always search for main topic (photos + text logs)
                vdo_main_topic_id = None
                if is_vdo:
                    self.logger.info(f"🔍 VDO topic: searching main topic for site {site_id}...")
                    try:
                        main_topic_future = asyncio.run_coroutine_threadsafe(
                            self.telegram_client.find_main_topic_for_vdo(site_id),
                            self.main_loop
                        )
                        vdo_main_topic_id = main_topic_future.result(timeout=VDO_SEARCH_TIMEOUT)
                        if vdo_main_topic_id:
                            self.logger.info(f"Main topic found (ID: {vdo_main_topic_id}). Downloading text logs...")
                            text_log_future = asyncio.run_coroutine_threadsafe(
                                self.telegram_client.get_topic_text_log(vdo_main_topic_id),
                                self.main_loop
                            )
                            text_log = text_log_future.result(timeout=VDO_LOG_TIMEOUT)
                            if text_log:
                                # Save main topic log to a temp file for extraction
                                main_log_path = log_file_path + '.main_topic'
                                with open(main_log_path, 'w', encoding='utf-8') as f:
                                    f.write(text_log)
                                self.logger.info("Extracting equipment lists from main topic...")
                                main_lists = asyncio.run_coroutine_threadsafe(
                                    ai_processor.extract_equipment_lists(main_log_path),
                                    self.main_loop
                                ).result()
                                # Clean up temp file
                                if os.path.exists(main_log_path):
                                    os.remove(main_log_path)
                                # Merge: add items from main topic that aren't already in VDO lists
                                for item in main_lists.get('demontaj', []):
                                    if item not in equipment_lists['demontaj']:
                                        equipment_lists['demontaj'].append(item)
                                for item in main_lists.get('montaj', []):
                                    if item not in equipment_lists['montaj']:
                                        equipment_lists['montaj'].append(item)
                                has_montaj = bool(equipment_lists.get('montaj'))
                                has_demontaj = bool(equipment_lists.get('demontaj'))
                                self.logger.info(f"✅ Merged equipment lists: demontaj={len(equipment_lists['demontaj'])}, montaj={len(equipment_lists['montaj'])}")
                        else:
                            self.logger.warning(f"⚠️ Main topic not found for site {site_id}")
                    except Exception as e:
                        self.logger.error(f"❌ Error searching/downloading main topic: {e}")

                # For non-VDO: check if a VDO twin exists for this site
                if not is_vdo:
                    self.logger.info(f"🔍 Checking for VDO twin for site {site_id}...")
                    try:
                        vdo_twin_future = asyncio.run_coroutine_threadsafe(
                            self.telegram_client.find_vdo_topic_for_main(site_id),
                            self.main_loop
                        )
                        vdo_twin_topic_id = vdo_twin_future.result(timeout=VDO_SEARCH_TIMEOUT)
                        if vdo_twin_topic_id:
                            has_vdo_twin = True
                            self.logger.info(f"🔗 VDO twin found (ID: {vdo_twin_topic_id}). Downloading text logs...")
                            text_log_future = asyncio.run_coroutine_threadsafe(
                                self.telegram_client.get_topic_text_log(vdo_twin_topic_id),
                                self.main_loop
                            )
                            text_log = text_log_future.result(timeout=VDO_LOG_TIMEOUT)
                            if text_log:
                                vdo_log_path = log_file_path + '.vdo_twin'
                                with open(vdo_log_path, 'w', encoding='utf-8') as f:
                                    f.write(text_log)
                                self.logger.info("Extracting equipment lists from VDO twin...")
                                vdo_lists = asyncio.run_coroutine_threadsafe(
                                    ai_processor.extract_equipment_lists(vdo_log_path),
                                    self.main_loop
                                ).result()
                                if os.path.exists(vdo_log_path):
                                    os.remove(vdo_log_path)
                                for item in vdo_lists.get('demontaj', []):
                                    if item not in equipment_lists['demontaj']:
                                        equipment_lists['demontaj'].append(item)
                                for item in vdo_lists.get('montaj', []):
                                    if item not in equipment_lists['montaj']:
                                        equipment_lists['montaj'].append(item)
                                has_montaj = bool(equipment_lists.get('montaj'))
                                has_demontaj = bool(equipment_lists.get('demontaj'))
                                self.logger.info(f"✅ Merged equipment lists from VDO twin: demontaj={len(equipment_lists['demontaj'])}, montaj={len(equipment_lists['montaj'])}")
                        else:
                            self.logger.info(f"ℹ️ No VDO twin found for site {site_id}")
                    except Exception as e:
                        self.logger.error(f"❌ Error searching/downloading VDO twin: {e}")

                if os.path.exists(log_file_path) and os.path.getsize(log_file_path) > 0:
                    if is_vdo or has_vdo_twin:
                        # VDO / VDO-twin: warn in log only, don't stop
                        if not has_montaj and not has_demontaj:
                            missing_text_log = True
                            self.logger.warning(f"⚠️ Equipment lists empty for VDO-path topic '{topic_title}', continuing")
                        else:
                            self.logger.info("✅ Equipment lists found, continuing processing")
                    else:
                        # Standalone non-VDO: missing lists = stop
                        if not has_montaj and not has_demontaj:
                            error_msg = f"🚨 Внимание! В топике '{topic_title}' не найдены списки монтажа/демонтажа. Обработка остановлена. Пожалуйста, проверьте топик вручную."
                            self.logger.warning(error_msg)
                            self._send_notification(error_msg)
                            return 0, 0, 0, equipment_lists, True, has_vdo_twin, missing_text_log
                        else:
                            self.logger.info("✅ Equipment lists found, continuing processing")
                else:
                    if is_vdo or has_vdo_twin:
                        # VDO / VDO-twin: no text log is OK, continue silently
                        missing_text_log = True
                        self.logger.info(f"ℹ️ VDO-path topic without text report. Continuing.")
                        if is_vdo:
                            equipment_lists['demontaj'] = ['Любое старое/демонтированное оборудование VDO']
                    else:
                        error_msg = f"🚨 Внимание! В топике '{topic_title}' отсутствует текстовый лог с списками оборудования. Обработка остановлена. Пожалуйста, проверьте топик вручную."
                        self.logger.warning(error_msg)
                        self._send_notification(error_msg)
                        return 0, 0, 0, equipment_lists, True, has_vdo_twin, missing_text_log

                montaj_dir = os.path.join(topic_dir, 'Montaj')
                demontaj_dir = os.path.join(topic_dir, 'Demontaj')

                os.makedirs(montaj_dir, exist_ok=True)
                os.makedirs(demontaj_dir, exist_ok=True)

                # For VDO: download photos from main topic directly to Montaj
                if is_vdo and vdo_main_topic_id:
                    try:
                        main_photos = asyncio.run_coroutine_threadsafe(
                            self.telegram_client.download_topic_photos_to_dir(
                                vdo_main_topic_id, montaj_dir
                            ),
                            self.main_loop
                        ).result(timeout=DOWNLOAD_TIMEOUT)
                        self.logger.info(f"📸 Downloaded {main_photos} montaj photos from main topic")
                    except Exception as e:
                        self.logger.error(f"❌ Error downloading photos from main topic: {e}")

                # For main topic with VDO twin: download VDO twin photos to Demontaj
                if not is_vdo and has_vdo_twin and vdo_twin_topic_id:
                    try:
                        vdo_photos = asyncio.run_coroutine_threadsafe(
                            self.telegram_client.download_topic_photos_to_dir(
                                vdo_twin_topic_id, demontaj_dir
                            ),
                            self.main_loop
                        ).result(timeout=DOWNLOAD_TIMEOUT)
                        self.logger.info(f"📸 Downloaded {vdo_photos} demontaj photos from VDO twin")
                    except Exception as e:
                        self.logger.error(f"❌ Error downloading photos from VDO twin: {e}")

                for message_id, file_path in unprocessed_files:
                    try:
                        if is_vdo:
                            ai_result = 'demontaj'
                            target_dir = demontaj_dir
                            self.logger.info(f"🚀 VDO topic: file {os.path.basename(file_path)} routed to demontaj")
                        elif has_vdo_twin:
                            ai_result = 'montaj'
                            target_dir = montaj_dir
                            self.logger.info(f"🔗 Main topic with VDO twin: file {os.path.basename(file_path)} routed to montaj")
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
                            else:
                                ai_result = 'montaj'
                                target_dir = montaj_dir

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
                unknown_count = 0

                self.logger.info(f"📊 File organization: Demontaj={demontaj_count}, Montaj={montaj_count}, Unknown={unknown_count}")
                self.logger.info(f"✅ AI processing of topic {topic_id} completed")

                if demontaj_count == 0 and not should_stop:
                    error_msg = f"🚨 Внимание! В топике '{topic_title}' не найдено ни одного фото демонтажа. Обработка остановлена."
                    self.logger.warning(error_msg)
                    self._send_notification(error_msg)
                    should_stop = True
            else:
                self.logger.info(f"ℹ️ No files for AI processing in topic {topic_id}")

        except Exception as e:
            with self._metrics_lock:
                self.metrics['ai_errors'] += 1
            self.logger.error(f"🚨 Critical AI processing error for topic {topic_id}: {e}")

        return demontaj_count, montaj_count, unknown_count, equipment_lists, should_stop, has_vdo_twin, missing_text_log

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

    def _send_and_cleanup_phase(self, topic_id: int, topic_title: str, site_id: str, is_vdo: bool, demontaj_count: int, montaj_count: int, unknown_count: int, zip_path: str, topic_dir: str, has_vdo_twin: bool = False, missing_text_log: bool = False) -> None:
        """Send notifications and clean up temporary files."""
        try:
            # User-facing summary (Russian)
            vdo_info = "VDO" if is_vdo or has_vdo_twin else "не VDO"
            summary = f"Сайт {site_id} ({vdo_info}) обработан✅. "
            if demontaj_count > 0 or montaj_count > 0 or unknown_count > 0:
                total = demontaj_count + montaj_count + unknown_count
                recog_pct = int(((demontaj_count + montaj_count) / total) * 100) if total > 0 else 0
                summary += f"Найдено {demontaj_count} фото демонтажа, {montaj_count} монтажа. Распознано {recog_pct}% оборудования."
            else:
                summary += "Новых фото для обработки не было."
            if missing_text_log:
                summary += "\n⚠️ Текстовые логи с списками оборудования не найдены."

            file_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
            send_timeout = SEND_FILE_TIMEOUT_BASE + int(file_size_mb * SEND_FILE_TIMEOUT_PER_MB)
            self.logger.info(f"📤 Sending ZIP archive to Telegram ({zip_path}, {file_size_mb:.1f} MB, timeout={send_timeout}s)...")
            asyncio.run_coroutine_threadsafe(
                self.telegram_client.send_file_with_retry('me', file=zip_path, caption=summary),
                self.main_loop
            ).result(timeout=send_timeout)
            self.logger.info("✅ ZIP archive sent successfully.")
            zip_sent = True
        except Exception as tg_error:
            zip_sent = False
            self.logger.error(f"❌ Error sending ZIP archive to Telegram: {tg_error}")

        try:
            self.logger.info(f"🧹 Deleting temporary folder: {topic_dir}")
            safe_rmtree(topic_dir)

            if zip_sent and os.path.exists(zip_path):
                os.remove(zip_path)
                self.logger.info(f"✅ Cleanup done. ZIP archive deleted: {zip_path}")
            elif not zip_sent:
                self.logger.warning(f"⚠️ ZIP archive kept (send failed): {zip_path}")
        except Exception as cleanup_error:
            self.logger.error(f"⚠️ Error during cleanup for {topic_dir}: {cleanup_error}")

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

                demontaj_count, montaj_count, unknown_count, equipment_lists, should_stop, has_vdo_twin, missing_text_log = self._ai_analysis_phase(
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

                # Idempotency: check if ZIP already exists from a previous crashed run
                output_folder = os.path.join(BASE_DIR, "output")
                suffix = "_VDO" if is_vdo else "_MAIN"
                existing_zip = os.path.join(output_folder, f"{site_id}{suffix}.zip")
                if os.path.exists(existing_zip):
                    self.logger.info(f"♻️ Found existing ZIP from previous run: {existing_zip}")
                    zip_path = existing_zip
                else:
                    zip_path = self._packaging_phase(topic_id, topic_title, site_id, topic_dir, log_file_path, equipment_lists)

                self._send_and_cleanup_phase(topic_id, topic_title, site_id, is_vdo, demontaj_count, montaj_count, unknown_count, zip_path, topic_dir, has_vdo_twin, missing_text_log)
            else:
                self.logger.warning(f"Topic {topic_id} is empty, nothing to process")

        except Exception as e:
            raise Exception(f"Error processing topic {topic_id}: {e}")

    def get_metrics_summary(self) -> str:
        """Return a formatted string with current metrics."""
        with self._metrics_lock:
            m = dict(self.metrics)

        avg_time = (m['total_processing_time'] / m['topics_processed']) if m['topics_processed'] > 0 else 0.0
        return (
            f"Обработано: {m['topics_processed']}, "
            f"Ошибок: {m['topics_failed']}, "
            f"AI ошибок: {m['ai_errors']}, "
            f"Среднее время: {avg_time:.1f}s, "
            f"В очереди: {self.queue.qsize()}"
        )

    async def shutdown(self) -> None:
        """Gracefully shut down the task queue with drain support.

        Phase 1: Signal workers to stop accepting new tasks and drain the
                 pending queue so no new work is picked up.
        Phase 2: Wait for in-flight tasks to finish (up to GRACEFUL_SHUTDOWN_TIMEOUT).
        Phase 3: Force-cancel any workers that didn't finish in time.
        """
        self.logger.info("🛑 Shutting down task queue...")

        # Phase 1: Signal workers to stop after current task
        self._shutting_down.set()

        # Drain pending queue so workers don't pick up new tasks
        drained = 0
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
                drained += 1
            except asyncio.QueueEmpty:
                break
        if drained:
            self.logger.info(f"🗑️ Drained {drained} pending task(s) from queue")

        # Phase 2: Wait for in-flight tasks to finish (with timeout)
        tasks_snapshot = list(self.active_tasks)
        if tasks_snapshot:
            self.logger.info(
                f"⏳ Waiting up to {GRACEFUL_SHUTDOWN_TIMEOUT}s "
                f"for {len(tasks_snapshot)} worker(s) to finish current tasks..."
            )
            done, pending = await asyncio.wait(
                tasks_snapshot,
                timeout=GRACEFUL_SHUTDOWN_TIMEOUT,
            )
            if done:
                self.logger.info(f"✅ {len(done)} worker(s) finished gracefully")

            # Phase 3: Force-cancel any that didn't finish in time
            if pending:
                self.logger.warning(
                    f"⚠️ {len(pending)} worker(s) still running after "
                    f"{GRACEFUL_SHUTDOWN_TIMEOUT}s timeout, force-cancelling..."
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        self.executor.shutdown(wait=True)
        self.logger.info("✅ Task queue stopped")
