# gtfs_processor.py

import csv
import io
import logging
import zipfile
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any

import requests # Не забуваємо про залежність

logger = logging.getLogger(__name__)

# --- Конфігурація ---
# Винесемо URL та імена файлів сюди, щоб їх можна було імпортувати
GTFS_URL = "http://track.ua-gis.com/gtfs/lviv/static.zip"
ROUTES_FILE = 'routes.txt'
TRIPS_FILE = 'trips.txt'
SHAPES_FILE = 'shapes.txt'

# --- Функції обробки GTFS (майже без змін, але без залежності від FastAPI) ---

def download_gtfs_zip(url: str) -> Optional[zipfile.ZipFile]:
    """Завантажує GTFS ZIP архів з URL і повертає об'єкт ZipFile."""
    logger.info(f"Завантаження GTFS архіву з: {url}")
    try:
        # Використовуємо синхронний requests
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        zip_content = io.BytesIO(response.content)
        logger.info(f"Архів успішно завантажено ({len(response.content) / 1024 / 1024:.2f} MB).")
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

def parse_shapes(zip_ref: zipfile.ZipFile, filename: str) -> Optional[Dict[str, List[List[float]]]]:
    """Розбирає shapes.txt з об'єкту ZipFile."""
    shapes = defaultdict(list)
    logger.info(f"Обробка файлу '{filename}' з архіву...")
    try:
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    lat = float(row['shape_pt_lat'])
                    lon = float(row['shape_pt_lon'])
                    sequence = int(row['shape_pt_sequence'])
                    shape_id = row['shape_id']
                    shapes[shape_id].append((sequence, [lat, lon]))
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning(f"[{filename} Рядок {line_num}]: Пропуск некоректного рядка шейпу: {row}. Помилка: {e}")
                    continue
        processed_shapes = {}
        for shape_id, points in shapes.items():
            points.sort(key=lambda item: item[0])
            processed_shapes[shape_id] = [coord for seq, coord in points]
        logger.info(f"Успішно оброблено {len(processed_shapes)} унікальних шейпів з '{filename}'.")
        return processed_shapes
    except Exception as e:
        logger.error(f"Помилка під час обробки файлу '{filename}' з архіву: {e}")
        return None

def parse_routes(zip_ref: zipfile.ZipFile, filename: str) -> Optional[Dict[str, Dict]]:
    """Розбирає routes.txt з об'єкту ZipFile."""
    routes = {}
    logger.info(f"Обробка файлу '{filename}' з архіву...")
    try:
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    route_id = row['route_id']
                    if not route_id:
                         logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка маршруту без route_id: {row}")
                         continue
                    routes[route_id] = {
                        "route_id": route_id,
                        "agency_id": row.get('agency_id', ''),
                        "route_short_name": row.get('route_short_name', ''),
                        "route_long_name": row.get('route_long_name', ''),
                        "route_desc": row.get('route_desc', ''),
                        "route_type": row.get('route_type', ''),
                        "route_url": row.get('route_url', ''),
                        "route_color": row.get('route_color', ''),
                        "route_text_color": row.get('route_text_color', ''),
                        "shape_ids": set()
                    }
                except KeyError as e:
                     logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка маршруту через відсутній ключ: {row}. Помилка: {e}")
                     continue
        logger.info(f"Успішно оброблено {len(routes)} маршрутів з '{filename}'.")
        return routes
    except Exception as e:
        logger.error(f"Помилка під час обробки файлу '{filename}' з архіву: {e}")
        return None

def link_shapes_to_routes(zip_ref: zipfile.ZipFile, filename: str, routes_data: Dict[str, Dict]) -> bool:
    """Читає trips.txt з архіву і додає shape_id до routes_data."""
    if not routes_data:
        logger.error("Дані маршрутів порожні, неможливо зв'язати шейпи.")
        return False
    logger.info(f"Зв'язування шейпів з маршрутами за допомогою файлу '{filename}' з архіву...")
    linked_shapes_count = 0
    routes_updated = set()
    try:
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    route_id = row['route_id']
                    shape_id = row['shape_id']
                    if route_id in routes_data and shape_id:
                        routes_data[route_id]['shape_ids'].add(shape_id)
                        routes_updated.add(route_id)
                        linked_shapes_count += 1
                except KeyError as e:
                    logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка поїздки через відсутній ключ: {row}. Помилка: {e}")
                    continue
        for route_id in routes_data:
            routes_data[route_id]['shape_ids'] = sorted(list(routes_data[route_id]['shape_ids']))
        logger.info(f"Успішно додано {linked_shapes_count} посилань на шейпи до {len(routes_updated)} маршрутів.")
        return True
    except Exception as e:
        logger.error(f"Помилка під час обробки файлу '{filename}' з архіву: {e}")
        return False


def process_gtfs_data_from_url() -> Optional[Tuple[List[Dict], Dict[str, List[List[float]]]]]:
    """
    Основна функція для завантаження та обробки GTFS даних.
    Повертає кортеж (список_маршрутів, словник_шейпів) або None у разі помилки.
    """
    logger.info("Початок процесу завантаження та обробки GTFS даних...")
    zip_file = None
    try:
        zip_file = download_gtfs_zip(GTFS_URL)
        if zip_file is None:
            raise Exception("Не вдалося завантажити або відкрити GTFS архів.")

        # Перевірка наявності файлів
        required = [ROUTES_FILE, TRIPS_FILE, SHAPES_FILE]
        available_files = zip_file.namelist()
        missing = [f for f in required if f not in available_files]
        if missing:
            raise Exception(f"В архіві відсутні необхідні файли: {', '.join(missing)}")

        # Обробляємо послідовно для простоти
        shapes_data = parse_shapes(zip_file, SHAPES_FILE)
        routes_dict = parse_routes(zip_file, ROUTES_FILE)

        if shapes_data is None or routes_dict is None:
             raise Exception("Помилка під час парсингу shapes або routes файлів.")

        link_success = link_shapes_to_routes(zip_file, TRIPS_FILE, routes_dict)
        if not link_success:
             raise Exception("Помилка під час зв'язування шейпів з маршрутами.")

        # Готуємо фінальні дані
        final_routes_list = list(routes_dict.values())
        final_shapes_dict = shapes_data

        logger.info("GTFS дані успішно оброблено.")
        return final_routes_list, final_shapes_dict

    except Exception as e:
        logger.error(f"Загальна помилка під час обробки GTFS: {e}")
        return None # Повертаємо None у разі будь-якої помилки
    finally:
        if zip_file:
            zip_file.close()
            logger.info("ZIP архів закрито.")

# Можна додати тестовий запуск, якщо потрібно
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     result = process_gtfs_data_from_url()
#     if result:
#         routes, shapes = result
#         print(f"Отримано {len(routes)} маршрутів і {len(shapes)} шейпів.")
#     else:
#         print("Не вдалося обробити дані.")