# gtfs/updater.py
import logging
import time # Додаємо імпорт time
from typing import Dict, List, Optional, Any, AsyncGenerator
import motor.motor_asyncio
# Імпортуємо константи та типи з pymongo
from pymongo import ReplaceOne, InsertOne, IndexModel, ASCENDING, DESCENDING, GEOSPHERE
from pymongo.errors import BulkWriteError

# Імпортуємо імена колекцій з конфігурації
from core.config import (
    STOPS_COLLECTION, TRIPS_COLLECTION, STOP_TIMES_COLLECTION, ROUTES_COLLECTION,
    CALENDAR_DATES_COLLECTION, SHAPES_COLLECTION, CALENDAR_COLLECTION
)

logger = logging.getLogger(__name__)

# Визначення індексів
# Додаємо індекс для calendar по датах
calendar_indexes = [
    IndexModel([("start_date", ASCENDING)], name="start_date_asc"),
    IndexModel([("end_date", ASCENDING)], name="end_date_asc")
]
stops_indexes = [
    IndexModel([("location", GEOSPHERE)], name="location_2dsphere"),
    IndexModel([("stop_id", ASCENDING)], name="stop_id_asc") # Додатковий індекс по ID
]
trips_indexes = [
    IndexModel([("trip_id", ASCENDING)], name="trip_id_asc", unique=True), # trip_id має бути унікальним
    IndexModel([("route_id", ASCENDING)], name="route_id_asc"),
    IndexModel([("service_id", ASCENDING)], name="service_id_asc")
]
stop_times_indexes = [
    # Основний індекс для пошуку відправлень
    IndexModel([("stop_id", ASCENDING), ("departure_time", ASCENDING)], name="stop_departure_asc"),
    # Додаткові індекси для інших можливих запитів
    IndexModel([("trip_id", ASCENDING)], name="trip_id_asc"),
    IndexModel([("departure_time", ASCENDING)], name="departure_time_asc")
]
calendar_dates_indexes = [
    # Основний індекс для пошуку винятків на дату
    IndexModel([("date", ASCENDING), ("service_id", ASCENDING)], name="date_service_asc"),
    # Можливо, окремий індекс по service_id
    IndexModel([("service_id", ASCENDING)], name="service_id_asc")
]

# Словник індексів для кожної колекції
collection_indexes: Dict[str, List[IndexModel]] = {
    STOPS_COLLECTION: stops_indexes,
    TRIPS_COLLECTION: trips_indexes,
    STOP_TIMES_COLLECTION: stop_times_indexes,
    CALENDAR_DATES_COLLECTION: calendar_dates_indexes,
    CALENDAR_COLLECTION: calendar_indexes,
    ROUTES_COLLECTION: [], # _id=route_id індексується автоматично
    SHAPES_COLLECTION: [], # _id=shape_id індексується автоматично
}

async def update_mongo_collection(db: motor.motor_asyncio.AsyncIOMotorDatabase,
                                  collection_name: str,
                                  data_stream: AsyncGenerator[Dict[str, Any], None],
                                  clear_collection: bool = True):
    """Очищає колекцію, записує дані та створює/оновлює індекси."""
    # Отримуємо список індексів для поточної колекції
    indexes_to_create: Optional[List[IndexModel]] = collection_indexes.get(collection_name)
    collection = db[collection_name]
    op_count = 0 # Лічильник успішних операцій запису/оновлення
    start_time = time.monotonic() # Початок вимірювання часу

    if clear_collection:
        try:
            logger.info(f"Очищення колекції '{collection_name}'...")
            delete_result = await collection.delete_many({})
            logger.info(f"Видалено {delete_result.deleted_count} документів з '{collection_name}'.")
        except Exception as e:
            logger.error(f"Помилка очищення колекції '{collection_name}': {e}", exc_info=True)
            raise # Перериваємо, якщо не вдалося очистити

    logger.info(f"Запис даних у колекцію '{collection_name}'...")
    batch_size = 1000 # Розмір пакету для bulk_write
    operations: List[InsertOne | ReplaceOne] = [] # Список операцій для пакетного запису

    try:
        processed_docs_in_stream = 0
        async for doc in data_stream:
            processed_docs_in_stream += 1
            # Використовуємо ReplaceOne з _id, якщо він є в документі
            # Або InsertOne, якщо _id генерується MongoDB
            if "_id" in doc and doc["_id"] is not None: # Перевіряємо наявність і що _id не None
                 operations.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            else:
                 # Якщо _id немає, або він None, дозволяємо MongoDB генерувати його
                 if "_id" in doc: del doc["_id"] # Видаляємо None _id перед InsertOne
                 operations.append(InsertOne(doc))

            if len(operations) >= batch_size:
                try:
                    result = await collection.bulk_write(operations, ordered=False)
                    # Рахуємо всі типи операцій
                    op_count += result.inserted_count + result.upserted_count + result.matched_count + result.modified_count
                    logger.debug(f"Записано пакет {len(operations)} у '{collection_name}'...")
                    operations = [] # Очищаємо пакет для наступної ітерації
                except BulkWriteError as bwe:
                    logger.error(f"Помилка bulk_write у '{collection_name}' (деталі в errors): {bwe.details.get('writeErrors')}", exc_info=False) # Не логуємо повний traceback для BulkWriteError
                    op_count += bwe.details.get('nInserted', 0) + bwe.details.get('nUpserted', 0) + bwe.details.get('nMatched', 0)
                    operations = []
                except Exception as e:
                     logger.error(f"Загальна помилка під час bulk_write у '{collection_name}': {e}", exc_info=True)
                     operations = []

        # Записуємо залишок операцій
        if operations:
             try:
                 result = await collection.bulk_write(operations, ordered=False)
                 op_count += result.inserted_count + result.upserted_count + result.matched_count + result.modified_count
                 logger.debug(f"Записано останні {len(operations)} у '{collection_name}'.")
             except BulkWriteError as bwe:
                 logger.error(f"Помилка під час запису останнього пакету у '{collection_name}': {bwe.details.get('writeErrors')}", exc_info=False)
                 op_count += bwe.details.get('nInserted', 0) + bwe.details.get('nUpserted', 0) + bwe.details.get('nMatched', 0)
             except Exception as e:
                 logger.error(f"Загальна помилка під час запису останнього пакету у '{collection_name}': {e}", exc_info=True)

        write_duration = time.monotonic() - start_time
        logger.info(f"Завершено запис у колекцію '{collection_name}'. Всього операцій: {op_count}. Отримано з парсера: {processed_docs_in_stream}. Час запису: {write_duration:.2f} сек.")

    except Exception as e:
        # Логуємо помилку, що сталася під час читання з data_stream
        logger.error(f"Помилка під час ітерації data_stream для '{collection_name}': {e}", exc_info=True)
        # Не перериваємо повністю, спробуємо створити індекси для вже записаних даних

    # Створення/Оновлення індексів
    # ВИПРАВЛЕНО: Використовуємо indexes_to_create замість indexes
    if indexes_to_create:
        logger.info(f"Створення/оновлення {len(indexes_to_create)} індекс(ів) для '{collection_name}'...")
        index_start_time = time.monotonic()
        try:
            # Запускаємо створення індексів
            # ВИПРАВЛЕНО: Використовуємо indexes_to_create
            await collection.create_indexes(indexes_to_create)
            index_duration = time.monotonic() - index_start_time
            logger.info(f"Індекси для '{collection_name}' створено/оновлено за {index_duration:.2f} сек.")
        except Exception as e:
            # Логуємо помилку, але не перериваємо процес оновлення повністю
            logger.warning(f"Не вдалося створити/оновити індекси для '{collection_name}': {e}", exc_info=True) # Додамо traceback для діагностики
    else:
         logger.info(f"Для колекції '{collection_name}' не визначено додаткових індексів (крім _id).")