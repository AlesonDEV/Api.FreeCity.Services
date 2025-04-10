# gtfs/processor.py
import logging
import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

# Імпортуємо необхідні функції та конфігурацію
from core.config import (
    GTFS_URL, MONGO_DB_NAME, META_COLLECTION,
    ROUTES_FILE, TRIPS_FILE, SHAPES_FILE, STOPS_FILE, STOP_TIMES_FILE,
    CALENDAR_FILE, CALENDAR_DATES_FILE,
    ROUTES_COLLECTION, SHAPES_COLLECTION, STOPS_COLLECTION, TRIPS_COLLECTION,
    STOP_TIMES_COLLECTION, CALENDAR_COLLECTION, CALENDAR_DATES_COLLECTION
)
from core.database import get_database # Функція для отримання об'єкту БД
from .downloader import download_gtfs_zip
from .parsers import (
    stream_parse_shapes, stream_parse_stops, stream_parse_trips_and_routes,
    stream_parse_stop_times, stream_parse_calendar, stream_parse_calendar_dates
)
from .updater import update_mongo_collection

logger = logging.getLogger(__name__)

async def update_gtfs_data_in_db() -> bool:
    """
    Основна функція для завантаження GTFS, обробки та оновлення даних в MongoDB.
    """
    logger.info("Початок повного оновлення даних GTFS в MongoDB (з розкладом)...")
    db = None
    zip_file = None
    success = False
    start_time = time.monotonic()

    try:
        db = get_database()
        # Виконуємо синхронне завантаження в окремому потоці
        zip_file = await asyncio.to_thread(download_gtfs_zip, GTFS_URL)
        if zip_file is None: raise Exception("Не вдалося завантажити GTFS архів.")

        required = [ROUTES_FILE, TRIPS_FILE, SHAPES_FILE, STOPS_FILE,
                    STOP_TIMES_FILE, CALENDAR_FILE, CALENDAR_DATES_FILE]
        available_files = set(zip_file.namelist())
        missing = [f for f in required if f not in available_files]
        if missing: raise Exception(f"В архіві відсутні необхідні файли: {', '.join(missing)}")

        # Виконуємо оновлення послідовно для кращого контролю та логування
        # Порядок може бути важливим, якщо є зовнішні ключі (у GTFS їх немає, але логічно)

        logger.info("--- Оновлення Calendar ---")
        await update_mongo_collection(db, CALENDAR_COLLECTION, stream_parse_calendar(zip_file, CALENDAR_FILE))

        logger.info("--- Оновлення Calendar Dates ---")
        await update_mongo_collection(db, CALENDAR_DATES_COLLECTION, stream_parse_calendar_dates(zip_file, CALENDAR_DATES_FILE))

        logger.info("--- Оновлення Stops ---")
        await update_mongo_collection(db, STOPS_COLLECTION, stream_parse_stops(zip_file, STOPS_FILE))

        logger.info("--- Оновлення Shapes ---")
        await update_mongo_collection(db, SHAPES_COLLECTION, stream_parse_shapes(zip_file, SHAPES_FILE))

        logger.info("--- Оновлення Routes & Trips ---")
        routes_gen, trips_gen = await stream_parse_trips_and_routes(zip_file, ROUTES_FILE, TRIPS_FILE)
        # Краще спочатку створити колекцію trips, потім routes, хоча тут порядок не надто критичний
        await update_mongo_collection(db, TRIPS_COLLECTION, trips_gen)
        await update_mongo_collection(db, ROUTES_COLLECTION, routes_gen)

        logger.info("--- Оновлення Stop Times ---")
        await update_mongo_collection(db, STOP_TIMES_COLLECTION, stream_parse_stop_times(zip_file, STOP_TIMES_FILE))


        # Оновлення метаданих
        update_time = datetime.now(timezone.utc)
        await db[META_COLLECTION].update_one(
            {"_id": "gtfs_update_status"},
            {"$set": {"last_successful_update_utc": update_time}},
            upsert=True
        )
        success = True
        elapsed_time = time.monotonic() - start_time
        logger.info(f"Оновлення всіх колекцій GTFS в MongoDB ЗАВЕРШЕНО УСПІШНО за {elapsed_time:.2f} сек.")

    except Exception as e:
        logger.error(f"Помилка під час оновлення GTFS даних в MongoDB: {e}", exc_info=True)
        success = False
        # Можна додати запис помилки в мета-колекцію
        try:
            if db:
                await db[META_COLLECTION].update_one(
                    {"_id": "gtfs_update_status"},
                    {"$set": {"last_error_utc": datetime.now(timezone.utc), "last_error_message": str(e)}},
                    upsert=True
                )
        except Exception as meta_e:
             logger.error(f"Не вдалося записати помилку оновлення в метадані: {meta_e}")

    finally:
        if zip_file: zip_file.close()

    return success