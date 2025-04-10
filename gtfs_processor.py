# gtfs_processor.py
import csv
import datetime
import io
import logging
import sys
import zipfile
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any, AsyncGenerator
import asyncio

import requests
import motor.motor_asyncio # Імпортуємо motor

logger = logging.getLogger(__name__)

# --- Конфігурація ---
GTFS_URL = "http://track.ua-gis.com/gtfs/lviv/static.zip"
MONGO_CONNECTION_STRING = "mongodb+srv://yuradebelyak:Yura2005@cluster0.wrsxvar.mongodb.net/" # Ваш рядок підключення до MongoDB
MONGO_DB_NAME = "gtfs_lviv" # Назва бази даних
# Колекції
ROUTES_COLLECTION = "routes"
SHAPES_COLLECTION = "shapes"
STOPS_COLLECTION = "stops"
META_COLLECTION = "metadata" # Для зберігання часу останнього оновлення

# GTFS файли
ROUTES_FILE = 'routes.txt'
TRIPS_FILE = 'trips.txt'
SHAPES_FILE = 'shapes.txt'
STOPS_FILE = 'stops.txt'

# --- Допоміжні функції для роботи з MongoDB ---

def download_gtfs_zip(url):
    """Завантажує GTFS ZIP архів з URL і повертає об'єкт ZipFile."""
    logger.info(f"Завантаження GTFS архіву з: {url}")
    try:
        response = requests.get(url, stream=True, timeout=60) # Timeout 60 секунд
        response.raise_for_status() # Перевірка на HTTP помилки (4xx, 5xx)

        # Перевірка типу контенту (опціонально, але корисно)
        content_type = response.headers.get('content-type')
        if 'zip' not in content_type:
             logger.info(f"Попередження: Отримано неочікуваний тип контенту '{content_type}' замість zip.")

        # Читаємо контент в пам'ять
        zip_content = io.BytesIO(response.content)
        logger.info(f"Архів успішно завантажено ({len(response.content) / 1024 / 1024:.2f} MB).")

        # Відкриваємо ZIP архів з байтів
        zip_ref = zipfile.ZipFile(zip_content)
        logger.info("Архів успішно відкрито в пам'яті.")
        return zip_ref

    except requests.exceptions.RequestException as e:
        logger.error(f"Помилка завантаження архіву: {e}")
        return None
    except zipfile.BadZipFile:
        logger.error(f"Помилка: Завантажений файл не є коректним ZIP архівом.")
        return None
    except Exception as e:
        logger.error(f"Неочікувана помилка під час завантаження або відкриття архіву: {e}")
        return None

async def get_mongo_db():
    """Створює підключення до MongoDB."""
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_CONNECTION_STRING)
        # Перевірка підключення (опціонально, але корисно)
        await client.admin.command('ping')
        logger.info(f"Успішно підключено до MongoDB: {MONGO_CONNECTION_STRING}")
        return client[MONGO_DB_NAME]
    except Exception as e:
        logger.error(f"Не вдалося підключитися до MongoDB: {e}")
        raise # Передаємо помилку далі

# --- Функції парсингу (адаптовані для генерації документів MongoDB) ---

async def stream_parse_shapes(zip_ref: zipfile.ZipFile, filename: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Потоково парсить shapes.txt і групує точки за shape_id."""
    shapes_buffer = defaultdict(list)
    processed_shape_ids = set()
    logger.info(f"Потокова обробка '{filename}'...")

    try:
        # Важливо: Оскільки ми групуємо, нам потрібно прочитати весь файл перед тим, як генерувати документи
        # TODO: Для дуже великих файлів може знадобитися підхід з сортуванням на диску або більш складна логіка
        temp_shapes = defaultdict(list)
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    shape_id = row['shape_id']
                    lat = float(row['shape_pt_lat'])
                    lon = float(row['shape_pt_lon'])
                    sequence = int(row['shape_pt_sequence'])
                    temp_shapes[shape_id].append((sequence, [lat, lon]))
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning(f"[{filename} Рядок {line_num}]: Пропуск некоректного рядка шейпу: {row}. Помилка: {e}")
                    continue

        # Сортуємо та генеруємо документи
        for shape_id, points in temp_shapes.items():
            points.sort(key=lambda item: item[0])
            coordinates = [coord for seq, coord in points]
            # Документ для MongoDB
            yield {
                "_id": shape_id, # Використовуємо shape_id як унікальний _id
                "shape_id": shape_id,
                "coordinates": coordinates
            }
        logger.info(f"Завершено обробку {len(temp_shapes)} шейпів з '{filename}'.")

    except Exception as e:
        logger.error(f"Помилка під час обробки файлу '{filename}' з архіву: {e}", exc_info=True)
        raise # Передаємо помилку далі


async def stream_parse_routes_with_shapes(zip_ref: zipfile.ZipFile, routes_filename: str, trips_filename: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Парсить routes.txt, зв'язує shape_ids з trips.txt і генерує документи маршрутів."""
    routes = {}
    logger.info(f"Обробка '{routes_filename}' та '{trips_filename}'...")

    # 1. Парсинг routes.txt
    try:
        with io.TextIOWrapper(zip_ref.open(routes_filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                 try:
                    route_id = row['route_id']
                    if not route_id: continue
                    routes[route_id] = {
                        "_id": route_id, # Використовуємо route_id як _id
                        "route_id": route_id,
                        "agency_id": row.get('agency_id'),
                        "route_short_name": row.get('route_short_name'),
                        "route_long_name": row.get('route_long_name'),
                        "route_desc": row.get('route_desc'),
                        "route_type": row.get('route_type'),
                        "route_url": row.get('route_url'),
                        "route_color": row.get('route_color'),
                        "route_text_color": row.get('route_text_color'),
                        "shape_ids": set() # Тимчасово set
                    }
                 except KeyError as e:
                     logger.warning(f"[{routes_filename}]: Пропуск рядка маршруту через відсутній ключ: {row}. Помилка: {e}")
                     continue
    except Exception as e:
        logger.error(f"Помилка читання '{routes_filename}': {e}", exc_info=True)
        raise

    # 2. Зв'язування shape_ids з trips.txt
    try:
        with io.TextIOWrapper(zip_ref.open(trips_filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                try:
                    route_id = row['route_id']
                    shape_id = row['shape_id']
                    if route_id in routes and shape_id:
                        routes[route_id]['shape_ids'].add(shape_id)
                except KeyError:
                    continue # Пропускаємо рядки з відсутніми ключами
    except Exception as e:
        logger.error(f"Помилка читання '{trips_filename}': {e}", exc_info=True)
        # Не перериваємо, маршрути будуть без shape_ids
        pass

    # 3. Генерація документів
    for route_id, route_data in routes.items():
        route_data['shape_ids'] = sorted(list(route_data['shape_ids'])) # Конвертуємо set в list
        yield route_data

    logger.info(f"Завершено обробку {len(routes)} маршрутів.")


async def stream_parse_stops(zip_ref: zipfile.ZipFile, filename: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Потоково парсить stops.txt і генерує документи зупинок."""
    logger.info(f"Потокова обробка '{filename}'...")
    try:
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    stop_id = row.get('stop_id')
                    stop_lat_str = row.get('stop_lat')
                    stop_lon_str = row.get('stop_lon')
                    if not all([stop_id, stop_lat_str, stop_lon_str]): continue

                    stop_lat = float(stop_lat_str)
                    stop_lon = float(stop_lon_str)

                    # Документ для MongoDB
                    stop_doc = {
                        "_id": stop_id, # Використовуємо stop_id як _id
                        "stop_id": stop_id,
                        "stop_code": row.get('stop_code'),
                        "stop_name": row.get('stop_name', 'Без назви'),
                        "stop_desc": row.get('stop_desc'),
                        "stop_lat": stop_lat,
                        "stop_lon": stop_lon,
                        # Додаємо поле location для геопросторових запитів
                        "location": {
                            "type": "Point",
                            "coordinates": [stop_lon, stop_lat] # GeoJSON: [longitude, latitude]
                        },
                        "zone_id": row.get('zone_id'),
                        "stop_url": row.get('stop_url'),
                        "location_type": row.get('location_type'),
                        "parent_station": row.get('parent_station'),
                        "wheelchair_boarding": row.get('wheelchair_boarding')
                    }
                    yield stop_doc
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning(f"[{filename} Рядок {line_num}]: Пропуск некоректного рядка зупинки: {row}. Помилка: {e}")
                    continue
        logger.info(f"Завершено потокову обробку '{filename}'.")
    except Exception as e:
        logger.error(f"Помилка під час потокової обробки файлу '{filename}' з архіву: {e}", exc_info=True)
        raise


async def update_mongo_collection(db: motor.motor_asyncio.AsyncIOMotorDatabase,
                                  collection_name: str,
                                  data_stream: AsyncGenerator[Dict[str, Any], None]):
    """Очищає колекцію та записує нові дані з генератора."""
    collection = db[collection_name]
    logger.info(f"Очищення колекції '{collection_name}'...")
    await collection.delete_many({}) # Видаляємо всі старі дані

    logger.info(f"Запис нових даних у колекцію '{collection_name}'...")
    # Використовуємо bulk_write для ефективного запису
    # Можна налаштувати batch_size
    batch_size = 500
    operations = []
    count = 0
    from pymongo import ReplaceOne # Імпортуємо тут, щоб уникнути помилки, якщо pymongo не встановлено

    async for doc in data_stream:
        # Використовуємо ReplaceOne з upsert=True, що еквівалентно вставці або заміні за _id
        operations.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        if len(operations) >= batch_size:
            try:
                result = await collection.bulk_write(operations)
                count += result.bulk_api_result.get('nUpserted', 0) + result.bulk_api_result.get('nMatched', 0)
                logger.info(f"Записано {len(operations)} документів у '{collection_name}' (всього: {count})...")
                operations = []
            except Exception as e:
                 logger.error(f"Помилка під час bulk_write у '{collection_name}': {e}", exc_info=True)
                 # Можна додати логіку повторної спроби або пропуску пакету
                 operations = [] # Очищаємо пакет, щоб продовжити з наступним

    # Записуємо залишок операцій
    if operations:
         try:
            result = await collection.bulk_write(operations)
            count += result.bulk_api_result.get('nUpserted', 0) + result.bulk_api_result.get('nMatched', 0)
            logger.info(f"Записано останні {len(operations)} документів у '{collection_name}' (всього: {count}).")
         except Exception as e:
             logger.error(f"Помилка під час запису останнього пакету у '{collection_name}': {e}", exc_info=True)

    logger.info(f"Завершено запис у колекцію '{collection_name}'. Всього записано/оновлено: {count}.")

    # Створення індексів (опціонально, але рекомендується)
    if collection_name == STOPS_COLLECTION:
        logger.info(f"Створення геопросторового індексу для '{collection_name}'...")
        try:
            await collection.create_index([("location", "2dsphere")])
            logger.info(f"Геопросторовий індекс для '{collection_name}' створено/оновлено.")
        except Exception as e:
            logger.warning(f"Не вдалося створити геопросторовий індекс для '{collection_name}': {e}")


async def update_mongo_from_gtfs_url():
    """
    Основна функція для завантаження GTFS, обробки та оновлення даних в MongoDB.
    """
    logger.info("Початок повного оновлення даних GTFS в MongoDB...")
    db = None
    zip_file = None
    success = False
    try:
        db = await get_mongo_db()
        zip_file = await asyncio.to_thread(download_gtfs_zip, GTFS_URL) # Виконуємо синхронне завантаження в окремому потоці

        if zip_file is None:
            raise Exception("Не вдалося завантажити GTFS архів.")

        # Перевірка файлів
        required = [ROUTES_FILE, TRIPS_FILE, SHAPES_FILE, STOPS_FILE]
        available_files = zip_file.namelist()
        missing = [f for f in required if f not in available_files]
        if missing:
            raise Exception(f"В архіві відсутні необхідні файли: {', '.join(missing)}")

        # Запускаємо оновлення колекцій
        # Використовуємо asyncio.gather для паралельного виконання, якщо це безпечно
        # Але оскільки stream_parse_routes_with_shapes читає 2 файли, краще послідовно
        logger.info("--- Оновлення колекції зупинок ---")
        stops_stream = stream_parse_stops(zip_file, STOPS_FILE)
        await update_mongo_collection(db, STOPS_COLLECTION, stops_stream)

        logger.info("--- Оновлення колекції шейпів ---")
        shapes_stream = stream_parse_shapes(zip_file, SHAPES_FILE)
        await update_mongo_collection(db, SHAPES_COLLECTION, shapes_stream)

        logger.info("--- Оновлення колекції маршрутів (з прив'язкою шейпів) ---")
        routes_stream = stream_parse_routes_with_shapes(zip_file, ROUTES_FILE, TRIPS_FILE)
        await update_mongo_collection(db, ROUTES_COLLECTION, routes_stream)

        # Оновлюємо метадані про час останнього успішного оновлення
        await db[META_COLLECTION].update_one(
            {"_id": "gtfs_update_status"},
            {"$set": {"last_successful_update_utc": datetime.datetime.now(datetime.timezone.utc)}},
            upsert=True
        )

        logger.info("Оновлення всіх колекцій GTFS в MongoDB завершено успішно.")
        success = True

    except Exception as e:
        logger.error(f"Помилка під час оновлення GTFS даних в MongoDB: {e}", exc_info=True)
        success = False
    finally:
        if zip_file:
            zip_file.close()
            logger.info("ZIP архів закрито.")
        # Можна додати закриття клієнта MongoDB, якщо він не використовується постійно
        # Але в FastAPI краще тримати клієнт відкритим протягом життя додатку

    return success # Повертаємо статус успішності