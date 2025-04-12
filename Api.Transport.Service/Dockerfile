# Використовуємо офіційний Python образ як базовий
FROM python:3.11-slim

# Встановлюємо змінні середовища
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=on \
    PYTHONPATH=/app \
    # Встановлюємо порт за замовчуванням тут, щоб він був доступний для CMD
    APP_PORT=8000

# Встановлюємо робочу директорію всередині контейнера
WORKDIR /app

# Копіюємо файл залежностей
COPY requirements.txt .

# Оновлюємо pip та встановлюємо залежності
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копіюємо весь код додатку в робочу директорію /app
COPY . .

# Відкриваємо порт, який слухатиме Uvicorn всередині контейнера
EXPOSE ${APP_PORT}

# Команда для запуску додатку при старті контейнера
# ВИПРАВЛЕНО: Використовуємо shell форму CMD для підстановки змінних
CMD uvicorn main:app --host 0.0.0.0 --port ${APP_PORT}