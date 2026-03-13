# Telegram Bot for Automated Report Processing

A smart Telegram bot for automated processing of field crew reports using AI-powered photo analysis and file organization.

## Features

- **24/7 Autonomous Mode** — runs continuously and processes new reports automatically
- **Smart Timer** — waits for the crew to finish uploading all files (configurable timeout)
- **Smart Filtering** — processes only UA/UB topics, ignores administrative and TSS/TSSR ones
- **Two-Stage AI Analysis** — initial photo classification + outlier re-analysis using series context
- **VDO Support** — automatic detection of VDO topics and equipment list merging with the main topic
- **Crash Recovery** — restores unfinished tasks from the database on restart
- **Producer-Consumer Architecture** — parallel processing of multiple topics via a task queue
- **Monitoring** — healthcheck system with periodic status reports to Telegram

## Installation

1. Install dependencies:

```bash
pip install -r requirements.txt
```

1. Copy `.env.example` to `.env` and fill in the environment variables:

| Variable | Required | Description | Default |
| --- | --- | --- | --- |
| `TELEGRAM_API_ID` | yes | API ID from [my.telegram.org](https://my.telegram.org) | — |
| `TELEGRAM_API_HASH` | yes | API Hash from Telegram | — |
| `TELEGRAM_PHONE_NUMBER` | yes | Phone number for authentication | — |
| `TARGET_CHAT_ID` | yes | Target supergroup ID | — |
| `GEMINI_API_KEY` | yes | API key from [Google AI Studio](https://aistudio.google.com) | — |
| `WAIT_TIME` | no | Upload wait time (seconds) | `300` |
| `MAX_WORKERS` | no | Number of parallel workers (1–8) | `2` |
| `GEMINI_MODEL` | no | Gemini model for analysis | `gemini-flash-latest` |
| `DB_PATH` | no | Path to SQLite database | `bot_memory.db` |
| `HEALTHCHECK_INTERVAL` | no | Telegram report interval (seconds) | `3600` |
| `DOWNLOADS_DIR` | no | Temporary downloads directory | `downloads` |
| `SESSION_FILE` | no | Telethon session file | `session_name.session` |

## Running

### Docker (recommended for production)

```bash
# Build and start in the background
docker-compose up -d --build

# View logs
docker logs -f work_auto_bot

# Stop
docker-compose down
```

> On the first run you may need to start the container interactively (`docker-compose run -it bot`) to enter the Telegram verification code or 2FA password. The session is persisted for subsequent runs.

Docker container includes:

- Health checks every 60 seconds
- Memory limit 1 GB, CPU limit 1.0
- Automatic restart (`unless-stopped`)
- Volume mounts for data persistence
- Timezone `Europe/Kiev`

### Local

```bash
python main.py
```

### Systemd Service (Linux)

A ready-to-use `work_auto.service` file is provided for systemd deployment.

## How It Works

1. The bot continuously listens for messages in UA/UB topics
2. When new activity is detected, it starts a timer (`WAIT_TIME`)
3. If new files arrive — the timer resets
4. Once the timer expires, the bot downloads everything and starts AI analysis
5. **Stage 1** — classify each photo (dismantling / installation / other)
6. **Stage 2** — detect outliers in the series and re-analyze them using neighboring photo context
7. Creates a ZIP archive with sorted files and a README report
8. For VDO topics — automatically finds the paired topic and merges data

## Topic Filtering

**Processes** topics with "UA" or "UB" in the title:

- `UA4571 Демонтаж/Монтаж`
- `UB9999 Оборудование`

**Ignores:**

- Topics with "TSS" or "TSSR" in the title
- Administrative topics ("Общие", "Топливо", "План", etc.)

## Output

The bot automatically:

1. Downloads all photos and text messages
2. Extracts equipment lists from text via AI
3. Analyzes photos through Google Gemini (two-stage process)
4. Sorts files into folders:
   - `Demontaj/` — dismantled equipment
   - `Montaj/` — installed equipment
   - `Other/` — miscellaneous photos
5. Generates a `README.txt` with a detailed report
6. Packages everything into a ZIP archive named by site ID

### Output File Structure

```text
output/
├── UA4571_MAIN.zip
├── UA4571_VDO.zip          # if a VDO topic exists
downloads/
├── topic_12345/            # temporary folder (deleted after processing)
│   ├── messages_log.txt
│   ├── README.txt
│   ├── Demontaj/
│   │   ├── Anten_T1003M6R011_photo_123.jpg
│   │   └── DCDU12B_photo_124.jpg
│   └── Montaj/
│       └── RRU_5516_photo_125.jpg
```

## Project Structure

```text
├── main.py                     # entry point (autonomous mode)
├── requirements.txt            # production dependencies
├── requirements-dev.txt        # development dependencies
├── Dockerfile                  # Docker image (Python 3.11-slim)
├── docker-compose.yml          # Docker Compose configuration
├── pytest.ini                  # test configuration
├── work_auto.service           # systemd service
├── check_models.py             # utility to check available Gemini models
├── modules/
│   ├── telegram_client.py      # Telegram client with event-driven architecture
│   ├── ai_processor.py         # two-stage AI photo analysis
│   ├── task_queue.py           # task queue (Producer-Consumer)
│   ├── database.py             # SQLite state persistence
│   └── utils.py                # utilities (ZIP, README, site ID extraction)
├── tests/
│   ├── test_ai_processor.py
│   ├── test_database.py
│   ├── test_error_scenarios.py
│   ├── test_integration_pipeline.py
│   ├── test_telegram_logic.py
│   └── test_utils.py
├── .github/
│   └── workflows/
│       └── ci.yml              # CI/CD: ruff + bandit + pytest
├── logs/                       # rotating logs (5 MB x 5 files)
├── downloads/                  # temporary processing files
└── output/                     # final ZIP archives
```

## Architecture

```text
Telegram NewMessage Event
        ↓
   Topic Timer (resets on new messages)
        ↓
   Task Queue (FIFO)
        ↓
   Worker Pool (2–8 parallel workers)
        ↓
   Pipeline: Download → AI Analysis → Packaging → Delivery
        ↓
   SQLite (state persistence)
```

- **Event-driven** — reacts to new messages in real time
- **Producer-Consumer** — efficient processing via a task queue
- **Async/Thread hybrid** — async for Telegram, ThreadPool for AI and file I/O
- **Persistence** — SQLite (WAL mode) stores processing state and enables crash recovery
- **Caching** — LRU cache for topic titles (500 entries, 1-hour TTL)

## Development

### Install dev dependencies

```bash
pip install -r requirements-dev.txt
```

### Run tests

```bash
pytest
```

### CI/CD

GitHub Actions runs automatically on push to main and on pull requests:

1. **Lint** — `ruff check` (code style)
2. **Security** — `bandit` (vulnerability scanning)
3. **Tests** — `pytest` (unit and integration tests)

## Logging and Monitoring

- **Log file:** `logs/bot.log` (rotation: 5 MB x 5 files)
- **Healthcheck:** file updated every 30 seconds (Docker checks every 60 seconds)
- **Telegram reports:** periodic messages with metrics (processed, errors, timing)

## Important Notes

- Make sure the bot has read access to messages in the target group
- A valid `GEMINI_API_KEY` is required for AI analysis
- First-time authentication may require entering a Telegram verification code
- The session is persisted in `SESSION_FILE`
- The database (`bot_memory.db`) stores processing progress — do not delete it unnecessarily
