# core/config.py
import os
import logging
from datetime import timezone
from typing import List, Any # Додали Any
from dotenv import load_dotenv # <--- Імпортуємо функцію

# ЗАВАНТАЖУЄМО ЗМІННІ З .env ФАЙЛУ НА ПОЧАТКУ
# Це завантажить змінні в os.environ, якщо файл .env існує
load_dotenv()

logger = logging.getLogger(__name__)

# --- Функція для отримання змінної середовища з логуванням ---
# (Залишається без змін, тепер вона читатиме і ті змінні, що завантажені з .env)
def get_env_variable(var_name: str, default_value: str | None = None) -> str | None:
    value = os.getenv(var_name, default_value)
    # ... (решта функції без змін) ...
    return value

# --- Конфігурація ---
# Тепер os.getenv буде бачити змінні, завантажені з .env
# Значення за замовчуванням використовуються, якщо змінної немає ні в середовищі, ні в .env
GTFS_URL: str = get_env_variable("GTFS_URL", "http://track.ua-gis.com/gtfs/lviv/static.zip") # type: ignore
MONGO_CONNECTION_STRING: str = get_env_variable("MONGO_CONNECTION_STRING", "mongodb://localhost:27017/") # type: ignore
MONGO_DB_NAME: str = get_env_variable("MONGO_DB_NAME", "gtfs_lviv") # type: ignore

# ... (Імена колекцій та файлів GTFS залишаються без змін) ...
ROUTES_COLLECTION: str = "routes"
SHAPES_COLLECTION: str = "shapes"
STOPS_COLLECTION: str = "stops"
TRIPS_COLLECTION: str = "trips"
STOP_TIMES_COLLECTION: str = "stop_times"
CALENDAR_COLLECTION: str = "calendar"
CALENDAR_DATES_COLLECTION: str = "calendar_dates"
META_COLLECTION: str = "metadata"

ROUTES_FILE: str = 'routes.txt'
TRIPS_FILE: str = 'trips.txt'
SHAPES_FILE: str = 'shapes.txt'
STOPS_FILE: str = 'stops.txt'
STOP_TIMES_FILE: str = 'stop_times.txt'
CALENDAR_FILE: str = 'calendar.txt'
CALENDAR_DATES_FILE: str = 'calendar_dates.txt'


# Scheduler Configuration
default_interval = 7 * 24 * 60 * 60 # 1 тиждень
try:
    update_interval_str = get_env_variable("UPDATE_INTERVAL_SECONDS", str(default_interval))
    UPDATE_INTERVAL_SECONDS: int = int(update_interval_str) # type: ignore
    if UPDATE_INTERVAL_SECONDS < 60:
        logger.warning(f"Заданий UPDATE_INTERVAL_SECONDS ({UPDATE_INTERVAL_SECONDS}) менше 60. Встановлено 60.")
        UPDATE_INTERVAL_SECONDS = 60
except (ValueError, TypeError):
    logger.warning(f"Некоректне значення для UPDATE_INTERVAL_SECONDS. Використовується значення за замовчуванням: {default_interval}")
    UPDATE_INTERVAL_SECONDS = default_interval

# Timezone
APP_TIMEZONE_STR: str = get_env_variable("APP_TIMEZONE", "Europe/Kiev") # type: ignore
APP_TIMEZONE: timezone | Any | None = None # Any для pytz

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        APP_TIMEZONE = ZoneInfo(APP_TIMEZONE_STR)
        logger.info(f"Використовується zoneinfo для часової зони: {APP_TIMEZONE_STR}")
    except ZoneInfoNotFoundError:
        logger.warning(f"Часова зона '{APP_TIMEZONE_STR}' не знайдена zoneinfo. Перевірте наявність 'tzdata'. Спробуємо pytz.")
        APP_TIMEZONE = None
except ImportError:
    logger.warning("Модуль zoneinfo недоступний (Python < 3.9?). Спробуємо pytz.")
    APP_TIMEZONE = None

if APP_TIMEZONE is None:
    try:
        import pytz # type: ignore
        APP_TIMEZONE = pytz.timezone(APP_TIMEZONE_STR)
        logger.info(f"Використовується pytz для часової зони: {APP_TIMEZONE_STR}")
    except ImportError:
        logger.error("Не вдалося імпортувати pytz. Будь ласка, встановіть його ('pip install pytz'). Використовується UTC.")
        APP_TIMEZONE = timezone.utc
    except Exception as e_pytz:
         logger.error(f"Помилка pytz ({e_pytz}). Використовується UTC.")
         APP_TIMEZONE = timezone.utc

if APP_TIMEZONE is None:
    logger.error("Не вдалося визначити часову зону. Використовується UTC.")
    APP_TIMEZONE = timezone.utc

# CORS Origins
default_cors = "http://localhost:3000,http://localhost:5173"
cors_origins_str = get_env_variable("CORS_ORIGINS", default_cors)
CORS_ORIGINS: List[str] = [origin.strip() for origin in cors_origins_str.split(',') if origin.strip()] # type: ignore

# Порт та хост для Uvicorn
APP_PORT: int = int(get_env_variable("APP_PORT", "8000")) # type: ignore
APP_HOST: str = get_env_variable("APP_HOST", "0.0.0.0") # type: ignore