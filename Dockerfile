# Базовый образ Python на стабильной версии slim
FROM python:3.11-slim

# Установим рабочую директорию в контейнере
WORKDIR /app

# Настройка переменных окружения Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем системные зависимости для Pillow и Telethon, затем Python-зависимости
# и очищаем кэш apt для уменьшения размера образа
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    zlib1g-dev \
    libjpeg-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y build-essential \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Создаем папки для хранения данных
RUN mkdir -p /app/downloads /app/output /app/logs

# Копируем исходный код бота в контейнер
COPY . .

# Указываем команду для запуска
CMD ["python", "main.py"]
