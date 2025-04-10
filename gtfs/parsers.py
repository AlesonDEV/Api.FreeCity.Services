# gtfs/parsers.py
import csv
import io
import logging
import zipfile
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any, AsyncGenerator
from datetime import date, datetime, time, timezone

logger = logging.getLogger(__name__)

# --- Допоміжні функції ---

def gtfs_time_to_seconds(time_str: Optional[str]) -> Optional[int]:
    """Перетворює час у форматі GTFS (HH:MM:SS, може бути > 24) на секунди від півночі."""
    if not time_str:
        return None
    try:
        # Видаляємо можливі пробіли на початку/кінці
        time_str = time_str.strip()
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s
    except (ValueError, AttributeError):
        # Не логуємо тут, щоб не спамити лог, логування буде вище
        return None

def parse_gtfs_date(date_str: Optional[str]) -> Optional[date]:
    """Перетворює дату YYYYMMDD на об'єкт date."""
    if not date_str or len(date_str.strip()) != 8:
        return None
    try:
        date_str = date_str.strip()
        return date(int(date_str[0:4]), int(date_str[4:6]), int(date_str[6:8]))
    except ValueError:
        return None

# --- Функції парсингу (stream_parse_*) ---

async def stream_parse_shapes(zip_ref: zipfile.ZipFile, filename: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Потоково парсить shapes.txt і групує точки за shape_id."""
    logger.info(f"Потокова обробка '{filename}'...")
    try:
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
                    logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка шейпу: {row}. Помилка: {e}")
                    continue

        # Сортуємо та генеруємо документи
        count = 0
        for shape_id, points in temp_shapes.items():
            points.sort(key=lambda item: item[0])
            coordinates = [[lat, lon] for seq, (lat, lon) in points] # Переконуємось у форматі [lat, lon]
            if coordinates: # Тільки якщо є координати
                yield {
                    "_id": shape_id,
                    "shape_id": shape_id,
                    "coordinates": coordinates
                }
                count += 1
        logger.info(f"Завершено обробку {count} шейпів з '{filename}'.")

    except KeyError as e:
        logger.error(f"Помилка: Не знайдено очікувану колонку в '{filename}'. Перевірте заголовки файлу. Помилка: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Помилка під час обробки файлу '{filename}': {e}", exc_info=True)
        raise


async def stream_parse_stops(zip_ref: zipfile.ZipFile, filename: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Потоково парсить stops.txt і генерує документи зупинок."""
    logger.info(f"Потокова обробка '{filename}'...")
    count = 0
    try:
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    stop_id = row.get('stop_id')
                    stop_name = row.get('stop_name', 'Без назви').strip()
                    stop_lat_str = row.get('stop_lat')
                    stop_lon_str = row.get('stop_lon')

                    if not all([stop_id, stop_lat_str, stop_lon_str]):
                        logger.warning(f"[{filename} Рядок {line_num}]: Пропуск зупинки через відсутність ID або координат: {row}")
                        continue

                    stop_lat = float(stop_lat_str)
                    stop_lon = float(stop_lon_str)

                    # Перевірка діапазону координат (опціонально)
                    if not (-90 <= stop_lat <= 90 and -180 <= stop_lon <= 180):
                         logger.warning(f"[{filename} Рядок {line_num}]: Некоректні координати для зупинки {stop_id}: lat={stop_lat}, lon={stop_lon}")
                         continue

                    stop_doc = {
                        "_id": stop_id,
                        "stop_id": stop_id,
                        "stop_code": row.get('stop_code') or None, # Пустий рядок -> None
                        "stop_name": stop_name,
                        "stop_desc": row.get('stop_desc') or None,
                        "stop_lat": stop_lat,
                        "stop_lon": stop_lon,
                        "location": {
                            "type": "Point",
                            "coordinates": [stop_lon, stop_lat]
                        },
                        "zone_id": row.get('zone_id') or None,
                        "stop_url": row.get('stop_url') or None,
                        "location_type": row.get('location_type') or '0', # За замовчуванням 0, якщо порожнє
                        "parent_station": row.get('parent_station') or None,
                        "wheelchair_boarding": row.get('wheelchair_boarding') or None # Пустий рядок -> None
                    }
                    yield stop_doc
                    count += 1
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning(f"[{filename} Рядок {line_num}]: Пропуск некоректного рядка зупинки: {row}. Помилка: {e}")
                    continue
        logger.info(f"Завершено потокову обробку {count} зупинок з '{filename}'.")
    except KeyError as e:
        logger.error(f"Помилка: Не знайдено очікувану колонку в '{filename}'. Перевірте заголовки файлу. Помилка: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Помилка під час потокової обробки '{filename}': {e}", exc_info=True)
        raise

async def stream_parse_trips_and_routes(zip_ref: zipfile.ZipFile, routes_filename: str, trips_filename: str) -> Tuple[AsyncGenerator[Dict[str, Any], None], AsyncGenerator[Dict[str, Any], None]]:
    """Парсить routes.txt та trips.txt, повертає два генератори."""
    routes: Dict[str, Dict[str, Any]] = {}
    trips_list: List[Dict[str, Any]] = []
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
                        "_id": route_id,
                        "route_id": route_id,
                        "agency_id": row.get('agency_id') or None,
                        "route_short_name": row.get('route_short_name') or None,
                        "route_long_name": row.get('route_long_name') or None,
                        "route_desc": row.get('route_desc') or None,
                        "route_type": row.get('route_type') or None,
                        "route_url": row.get('route_url') or None,
                        "route_color": row.get('route_color') or None,
                        "route_text_color": row.get('route_text_color') or None,
                        "shape_ids": set()
                    }
                except KeyError as e:
                    logger.warning(f"[{routes_filename}]: Пропуск рядка маршруту через відсутній ключ: {row}. Помилка: {e}")
                    continue
    except KeyError as e:
        logger.error(f"Помилка: Не знайдено очікувану колонку в '{routes_filename}'. Помилка: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Помилка читання '{routes_filename}': {e}", exc_info=True)
        raise

    # 2. Парсинг trips.txt та зв'язування shape_ids
    try:
        with io.TextIOWrapper(zip_ref.open(trips_filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    trip_id = row['trip_id']
                    route_id = row['route_id']
                    service_id = row['service_id']
                    shape_id = row.get('shape_id')

                    if not all([trip_id, route_id, service_id]):
                        logger.warning(f"[{trips_filename} Рядок {line_num}]: Пропуск рейсу через відсутність trip_id, route_id або service_id: {row}")
                        continue

                    # Зв'язуємо shape_id
                    if route_id in routes and shape_id:
                        routes[route_id]['shape_ids'].add(shape_id)

                    # Готуємо документ рейсу
                    trip_doc = {
                        "_id": trip_id,
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "service_id": service_id,
                        "trip_headsign": row.get('trip_headsign') or None,
                        "direction_id": int(d_id) if (d_id := row.get('direction_id')) in ('0', '1') else None,
                        "block_id": row.get('block_id') or None,
                        "shape_id": shape_id or None,
                        "wheelchair_accessible": int(wc) if (wc := row.get('wheelchair_accessible', '')) and wc.isdigit() else None,
                        "bikes_allowed": int(ba) if (ba := row.get('bikes_allowed', '')) and ba.isdigit() else None,
                    }
                    trips_list.append(trip_doc)

                except (KeyError, ValueError) as e:
                    logger.warning(f"[{trips_filename} Рядок {line_num}]: Пропуск рядка рейсу: {row}. Помилка: {e}")
                    continue
    except KeyError as e:
        logger.error(f"Помилка: Не знайдено очікувану колонку в '{trips_filename}'. Помилка: {e}", exc_info=True)
        # Не перериваємо, маршрути будуть без shape_ids, а рейси не будуть завантажені
        trips_list = [] # Очищаємо список рейсів у разі помилки читання файлу
    except Exception as e:
        logger.error(f"Помилка читання '{trips_filename}': {e}", exc_info=True)
        trips_list = []

    # 3. Створюємо асинхронні генератори
    async def routes_generator():
        logger.info(f"Генерація {len(routes)} документів маршрутів...")
        processed_count = 0
        for route_id, route_data in routes.items():
            route_data['shape_ids'] = sorted(list(route_data['shape_ids']))
            yield route_data
            processed_count += 1
        logger.info(f"Завершено генерацію {processed_count} документів маршрутів.")

    async def trips_generator():
        logger.info(f"Генерація {len(trips_list)} документів рейсів...")
        processed_count = 0
        for trip_doc in trips_list:
            yield trip_doc
            processed_count += 1
        logger.info(f"Завершено генерацію {processed_count} документів рейсів.")

    return routes_generator(), trips_generator()


async def stream_parse_stop_times(zip_ref: zipfile.ZipFile, filename: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Потоково парсить stop_times.txt і генерує документи."""
    logger.info(f"Потокова обробка '{filename}'...")
    count = 0
    skipped = 0
    try:
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    trip_id = row.get('trip_id')
                    stop_id = row.get('stop_id')
                    stop_sequence_str = row.get('stop_sequence')
                    departure_str = row.get('departure_time')
                    arrival_str = row.get('arrival_time')

                    if not all([trip_id, stop_id, stop_sequence_str, departure_str, arrival_str]):
                        skipped += 1
                        continue # Пропускаємо неповні рядки

                    stop_sequence = int(stop_sequence_str)
                    departure_seconds = gtfs_time_to_seconds(departure_str)
                    arrival_seconds = gtfs_time_to_seconds(arrival_str)

                    if departure_seconds is None or arrival_seconds is None:
                        logger.warning(f"[{filename} Рядок {line_num}]: Пропуск через некоректний час: {row}")
                        skipped += 1
                        continue

                    stop_time_doc = {
                        "trip_id": trip_id,
                        "arrival_time": arrival_seconds,
                        "departure_time": departure_seconds,
                        "stop_id": stop_id,
                        "stop_sequence": stop_sequence,
                        "stop_headsign": row.get('stop_headsign') or None,
                        "pickup_type": int(pt) if (pt := row.get('pickup_type', '')) and pt.isdigit() else 0,
                        "drop_off_type": int(dot) if (dot := row.get('drop_off_type', '')) and dot.isdigit() else 0,
                        "shape_dist_traveled": float(sdt) if (sdt := row.get('shape_dist_traveled', '')) else None,
                        "timepoint": int(tp) if (tp := row.get('timepoint', '')) and tp.isdigit() else None, # None замість 0, якщо не вказано
                    }
                    yield stop_time_doc
                    count += 1
                except (ValueError, KeyError, TypeError) as e:
                     logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка stop_time: {row}. Помилка: {e}")
                     skipped += 1
                     continue
        logger.info(f"Завершено потокову обробку {count} записів з '{filename}'. Пропущено: {skipped}.")
    except KeyError as e:
        logger.error(f"Помилка: Не знайдено очікувану колонку в '{filename}'. Помилка: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Помилка під час потокової обробки '{filename}': {e}", exc_info=True)
        raise

async def stream_parse_calendar(zip_ref: zipfile.ZipFile, filename: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Парсить calendar.txt."""
    logger.info(f"Обробка '{filename}'...")
    count = 0
    try:
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    service_id = row['service_id']
                    start_date = parse_gtfs_date(row.get('start_date'))
                    end_date = parse_gtfs_date(row.get('end_date'))

                    if not service_id or not start_date or not end_date:
                        logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка calendar через відсутність service_id або некоректні дати: {row}")
                        continue

                    calendar_doc = {
                        "_id": service_id,
                        "service_id": service_id,
                        "monday": bool(int(row['monday'])),
                        "tuesday": bool(int(row['tuesday'])),
                        "wednesday": bool(int(row['wednesday'])),
                        "thursday": bool(int(row['thursday'])),
                        "friday": bool(int(row['friday'])),
                        "saturday": bool(int(row['saturday'])),
                        "sunday": bool(int(row['sunday'])),
                        # Зберігаємо як datetime об'єкти (час 00:00 та 23:59:59 UTC для коректних запитів $lte/$gte)
                        "start_date": datetime.combine(start_date, time.min).replace(tzinfo=timezone.utc),
                        "end_date": datetime.combine(end_date, time.max).replace(tzinfo=timezone.utc) # time.max небезпечно, краще просто дату
                        # "end_date": datetime.combine(end_date, time.min).replace(tzinfo=timezone.utc) # Зберігаємо початок дня
                    }
                    yield calendar_doc
                    count += 1
                except (ValueError, KeyError, TypeError) as e:
                     logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка calendar: {row}. Помилка: {e}")
                     continue
        logger.info(f"Завершено обробку {count} записів з '{filename}'.")
    except KeyError as e:
        logger.error(f"Помилка: Не знайдено очікувану колонку в '{filename}'. Помилка: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Помилка під час обробки '{filename}': {e}", exc_info=True)
        raise

async def stream_parse_calendar_dates(zip_ref: zipfile.ZipFile, filename: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Парсить calendar_dates.txt."""
    logger.info(f"Обробка '{filename}'...")
    count = 0
    try:
        with io.TextIOWrapper(zip_ref.open(filename, mode='r'), encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            line_num = 1
            for row in reader:
                line_num += 1
                try:
                    service_id = row['service_id']
                    exception_date = parse_gtfs_date(row.get('date'))
                    exception_type_str = row.get('exception_type')

                    if not service_id or not exception_date or exception_type_str not in ('1', '2'):
                         logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка calendar_dates через некоректні дані: {row}")
                         continue

                    exception_type = int(exception_type_str)

                    calendar_date_doc = {
                        # _id генерується MongoDB, щоб уникнути конфліктів при upsert
                        "service_id": service_id,
                        "date": datetime.combine(exception_date, time.min).replace(tzinfo=timezone.utc), # Зберігаємо початок дня UTC
                        "exception_type": exception_type
                    }
                    yield calendar_date_doc
                    count += 1
                except (ValueError, KeyError, TypeError) as e:
                     logger.warning(f"[{filename} Рядок {line_num}]: Пропуск рядка calendar_dates: {row}. Помилка: {e}")
                     continue
        logger.info(f"Завершено обробку {count} записів з '{filename}'.")
    except KeyError as e:
        logger.error(f"Помилка: Не знайдено очікувану колонку в '{filename}'. Помилка: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Помилка під час обробки '{filename}': {e}", exc_info=True)
        raise