# api/endpoints/schedule.py
import logging
from typing import List, Optional, Set
from fastapi import APIRouter, Depends, HTTPException, Query, Path, status
import motor.motor_asyncio
from datetime import date, datetime, time, timedelta, timezone
from pymongo import ASCENDING # Імпортуємо ASCENDING

from api.models import DepartureInfo # Модель відповіді
from api.deps import get_db_dependency # Залежність для отримання БД
from core.config import (
    APP_TIMEZONE, # Імпортуємо часову зону з конфігурації
    ROUTES_COLLECTION, TRIPS_COLLECTION, STOP_TIMES_COLLECTION,
    CALENDAR_COLLECTION, CALENDAR_DATES_COLLECTION
)

router = APIRouter()
logger = logging.getLogger(__name__)

# --- Допоміжні функції для розкладу ---

async def get_active_service_ids(db: motor.motor_asyncio.AsyncIOMotorDatabase, target_date: date) -> set[str]:
    """Визначає активні service_id для вказаної дати."""
    active_services: Set[str] = set()
    # Перевіряємо день тижня (0 = понеділок, 6 = неділя)
    day_of_week_index = target_date.weekday()
    day_name = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][day_of_week_index]

    # Для запитів до MongoDB використовуємо datetime об'єкти UTC (початок дня)
    # Це важливо, оскільки дати в calendar і calendar_dates зберігаються як datetime UTC
    target_dt_utc_start = datetime.combine(target_date, time.min).replace(tzinfo=timezone.utc)
    # Кінець дня також в UTC для перевірки діапазону
    target_dt_utc_end = datetime.combine(target_date, time.max).replace(tzinfo=timezone.utc)

    logger.debug(f"Визначення активних сервісів для дати {target_date} ({day_name}), UTC: {target_dt_utc_start}")

    # 1. Перевірка в calendar
    # Використовуємо $lte та $gte для коректного порівняння дат, збережених як datetime
    calendar_filter = {
        day_name: True, # Активний у цей день тижня
        "start_date": {"$lte": target_dt_utc_start}, # Почався до або в цей день
        "end_date": {"$gte": target_dt_utc_start}    # Закінчується в або після цього дня (перевірка по початку дня достатня)
    }
    try:
        async for doc in db[CALENDAR_COLLECTION].find(calendar_filter, {"_id": 1}):
            active_services.add(doc["_id"])
        logger.debug(f"Знайдено {len(active_services)} сервісів у calendar, активних на {target_date}.")
    except Exception as e:
        logger.error(f"Помилка запиту до {CALENDAR_COLLECTION}: {e}", exc_info=True)
        # Можна повернути помилку або порожній набір
        raise HTTPException(status_code=500, detail="Помилка отримання даних календаря")

    # 2. Перевірка винятків в calendar_dates
    exceptions_found = 0
    try:
        # Шукаємо точну дату (збережену як початок дня UTC)
        async for doc in db[CALENDAR_DATES_COLLECTION].find({"date": target_dt_utc_start}, {"service_id": 1, "exception_type": 1}):
            exceptions_found += 1
            service_id = doc["service_id"]
            exception_type = doc["exception_type"]

            if exception_type == 1: # Service added
                active_services.add(service_id)
                logger.debug(f"Сервіс {service_id} додано через виняток на {target_date}")
            elif exception_type == 2: # Service removed
                if service_id in active_services:
                    active_services.discard(service_id)
                    logger.debug(f"Сервіс {service_id} видалено через виняток на {target_date}")
                else:
                    logger.debug(f"Спроба видалення неактивного сервісу {service_id} через виняток на {target_date}")

        logger.debug(f"Застосовано {exceptions_found} винятків з calendar_dates для {target_date}.")
    except Exception as e:
         logger.error(f"Помилка запиту до {CALENDAR_DATES_COLLECTION}: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail="Помилка отримання даних винятків календаря")

    logger.info(f"Кількість активних service_id на {target_date}: {len(active_services)}")
    if not active_services:
        logger.warning(f"На дату {target_date} не знайдено ЖОДНОГО активного сервісу.")

    return active_services


def format_seconds_to_gtfs_time(total_seconds: Optional[int]) -> str:
    """Перетворює секунди від півночі на рядок HH:MM:SS (може бути > 24)."""
    if total_seconds is None or total_seconds < 0:
        logger.warning(f"Отримано некоректне значення секунд для форматування: {total_seconds}")
        return "00:00:00" # Повертаємо значення за замовчуванням або можна кидати помилку
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    # Використовуємо f-string для форматування з ведучими нулями
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# --- Ендпоінт ---

@router.get("/stops/{stop_id}/next-departures",
            response_model=List[DepartureInfo],
            tags=["Розклад"],
            summary="Отримати наступні відправлення для зупинки")
async def get_next_departures_endpoint(
    stop_id: str = Path(..., description="ID зупинки (напр., '4714' для Автовокзалу)", example="4714"),
    limit: int = Query(10, ge=1, le=50, description="Максимальна кількість відправлень для повернення"),
    start_time_str: Optional[str] = Query(
        None,
        alias="startTime",
        pattern=r"^\d{1,2}:\d{2}:\d{2}$", # Дозволяє години > 23, напр. 25:01:00
        description="Час початку пошуку (HH:MM:SS). Якщо не вказано, використовується поточний час сервера.",
        example=datetime.now().strftime("%H:%M:%S") # Поточний час як приклад
    ),
    date_str: Optional[str] = Query(
        None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Дата пошуку (YYYY-MM-DD). Якщо не вказано, використовується поточна дата сервера.",
        example=datetime.now().strftime("%Y-%m-%d") # Приклад - сьогоднішня дата
    ),
    db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)
):
    """
    Повертає список найближчих відправлень (`limit` штук) для вказаної
    зупинки (`stop_id`), починаючи з вказаного часу (`startTime`)
    на вказану дату (`date`). Враховує активні сервіси (робочі/вихідні дні та винятки) на цю дату.
    """
    # 1. Визначаємо дату та час пошуку
    try:
        if APP_TIMEZONE is None:
            # Це має оброблятися при старті, але для безпеки
             raise RuntimeError("Часова зона програми не налаштована.")
        # Використовуємо часову зону з конфігурації
        target_date = date.fromisoformat(date_str) if date_str else datetime.now(APP_TIMEZONE).date()
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Некоректний формат дати. Використовуйте YYYY-MM-DD.")
    except RuntimeError as e:
         raise HTTPException(status_code=500, detail=str(e))


    current_time_seconds: int
    if start_time_str:
        try:
            # Розбираємо час безпосередньо
            h, m, s = map(int, start_time_str.split(':'))
            # Перевіряємо коректність (дозволяємо години >= 24)
            if not (0 <= m < 60 and 0 <= s < 60 and h >= 0):
                 raise ValueError("Некоректні значення хвилин або секунд")
            current_time_seconds = h * 3600 + m * 60 + s
        except ValueError:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Некоректний формат часу. Використовуйте HH:MM:SS.")
    else:
        # Поточний час у потрібній часовій зоні
        now_local = datetime.now(APP_TIMEZONE)
        current_time_seconds = now_local.hour * 3600 + now_local.minute * 60 + now_local.second

    logger.info(f"Пошук відправлень для stop_id={stop_id}, date={target_date}, time_seconds>={current_time_seconds}, limit={limit}")

    # 2. Визначаємо активні service_id
    try:
        active_services = await get_active_service_ids(db, target_date)
        if not active_services:
            # Це нормальна ситуація, якщо транспорт не ходить у цей день
            logger.info(f"На дату {target_date} не знайдено активних сервісів.")
            return [] # Повертаємо порожній список
    except HTTPException as http_exc:
         # Перевикидаємо HTTP помилки з get_active_service_ids
         raise http_exc
    except Exception as e:
        logger.error(f"Невизначена помилка при отриманні активних сервісів: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Помилка сервера при визначенні активних сервісів.")


    # 3. Пошук відправлень за допомогою Aggregation Pipeline
    departures: List[DepartureInfo] = []
    try:
        # Беремо трохи більше записів stop_times про запас,
        # оскільки фільтрація за service_id відбувається пізніше
        initial_limit = limit * 5 # Можна налаштувати цей множник

        pipeline = [
            # Етап 1: Знайти відповідні stop_times для зупинки та часу
            {
                "$match": {
                    "stop_id": stop_id,
                    "departure_time": {"$gte": current_time_seconds}
                }
            },
            # Етап 2: Відсортувати за часом відправлення
            {
                "$sort": {"departure_time": ASCENDING} # Використовуємо імпортовану константу
            },
            # Етап 3: Обмежити кількість документів для наступних етапів
            {
                "$limit": initial_limit
            },
            # Етап 4: "Приєднати" інформацію про рейс (trip)
            {
                "$lookup": {
                    "from": TRIPS_COLLECTION,
                    "localField": "trip_id",
                    "foreignField": "_id", # Зв'язок по trip_id, який є _id в trips
                    "as": "tripInfoList"
                }
            },
            # Залишаємо тільки ті stop_times, для яких знайшовся trip
            { "$match": { "tripInfoList": { "$ne": [] } } },
            # Розгорнути масив tripInfoList
            { "$unwind": "$tripInfoList" },
            # Етап 5: Перевірити, чи trip активний сьогодні (належить активному сервісу)
            {
                "$match": {
                    "tripInfoList.service_id": {"$in": list(active_services)}
                }
            },
            # Етап 6: "Приєднати" інформацію про маршрут (route)
            {
                "$lookup": {
                    "from": ROUTES_COLLECTION,
                    "localField": "tripInfoList.route_id",
                    "foreignField": "_id", # Зв'язок по route_id
                    "as": "routeInfoList"
                }
            },
             # Розгорнути масив routeInfo, зберігаючи запис, навіть якщо маршрут не знайдено
            { "$unwind": { "path": "$routeInfoList", "preserveNullAndEmptyArrays": True } },
            # Етап 7: Обмежити кількість РЕЗУЛЬТУЮЧИХ записів
            {
                "$limit": limit
            },
            # Етап 8: Сформувати фінальний документ відповіді
            {
                "$project": {
                    "_id": 0, # Виключити _id з stop_times
                    "departure_time_seconds": "$departure_time",
                    "route_short_name": "$routeInfoList.route_short_name",
                    "route_long_name": "$routeInfoList.route_long_name",
                    # Пріоритет stop_headsign з stop_times, якщо є, інакше з trip
                    "trip_headsign": {"$ifNull": ["$stop_headsign", "$tripInfoList.trip_headsign"]},
                    "route_color": "$routeInfoList.route_color",
                    "wheelchair_accessible": "$tripInfoList.wheelchair_accessible",
                }
            }
        ]

        # Виконуємо aggregation pipeline
        departure_docs_cursor = db[STOP_TIMES_COLLECTION].aggregate(pipeline)
        # Обмеження вже застосовано в pipeline, але to_list все одно потрібен
        departure_docs = await departure_docs_cursor.to_list(length=limit)

        # Форматуємо результат у відповідь API
        for doc in departure_docs:
            dep_seconds = doc.get("departure_time_seconds")
            if isinstance(dep_seconds, (int, float)):
                 dep_info = DepartureInfo(
                     departure_time=format_seconds_to_gtfs_time(int(dep_seconds)),
                     route_short_name=doc.get("route_short_name"),
                     route_long_name=doc.get("route_long_name"),
                     trip_headsign=doc.get("trip_headsign"),
                     route_color=doc.get("route_color"),
                     wheelchair_accessible=doc.get("wheelchair_accessible")
                 )
                 departures.append(dep_info)
            else:
                 logger.warning(f"Пропущено запис відправлення через некоректний departure_time_seconds: {doc}")

        logger.info(f"Знайдено і відформатовано {len(departures)} відправлень для stop_id={stop_id}.")

    except Exception as e:
        logger.error(f"Помилка aggregation pipeline для stop_id={stop_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Помилка сервера при пошуку відправлень.")

    return departures


@router.get(
    "/stops/{stop_id}/next-departure", # Зверніть увагу: singular 'departure'
    response_model=Optional[DepartureInfo], # Може повернути null/None, якщо немає
    tags=["Розклад"],
    summary="Отримати наступне відправлення конкретного маршруту з зупинки"
)
async def get_next_departure_for_route(
    stop_id: str = Path(..., description="ID зупинки", example="4714"),
    # Додаємо обов'язковий query параметр route_id
    route_id: str = Query(..., description="ID маршруту, для якого шукати відправлення", example="992"),
    start_time_str: Optional[str] = Query(None, alias="startTime", pattern=r"^\d{1,2}:\d{2}:\d{2}$", description="Час початку HH:MM:SS (за замовч. - поточний)"),
    date_str: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$", description="Дата YYYY-MM-DD (за замовч. - сьогодні)"),
    db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)
):
    """
    Повертає інформацію про **одне** найближче відправлення вказаного
    маршруту (`route_id`) із зазначеної зупинки (`stop_id`), починаючи
    з вказаного часу (`startTime`) на вказану дату (`date`).
    Якщо відправлень немає, повертає `null`.
    """
    # 1. Визначаємо дату та час пошуку (аналогічно попередньому ендпоінту)
    try:
        if APP_TIMEZONE is None: raise RuntimeError("Часова зона програми не налаштована.")
        target_date = date.fromisoformat(date_str) if date_str else datetime.now(APP_TIMEZONE).date()
    except (ValueError, TypeError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Некоректний формат дати. Використовуйте YYYY-MM-DD.")
    except RuntimeError as e:
         raise HTTPException(status_code=500, detail=str(e))

    current_time_seconds: int
    if start_time_str:
        try:
            h, m, s = map(int, start_time_str.split(':'))
            if not (0 <= m < 60 and 0 <= s < 60 and h >= 0): raise ValueError
            current_time_seconds = h * 3600 + m * 60 + s
        except ValueError:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Некоректний формат часу. Використовуйте HH:MM:SS.")
    else:
        now_local = datetime.now(APP_TIMEZONE)
        current_time_seconds = now_local.hour * 3600 + now_local.minute * 60 + now_local.second

    logger.info(f"Пошук НАСТУПНОГО відправлення для route_id={route_id}, stop_id={stop_id}, date={target_date}, time_seconds>={current_time_seconds}")

    # 2. Визначаємо активні service_id
    try:
        active_services = await get_active_service_ids(db, target_date)
        if not active_services:
            logger.info(f"На дату {target_date} не знайдено активних сервісів.")
            # Якщо немає активних сервісів, то й відправлень не буде
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="На вказану дату немає активних рейсів.")
    except HTTPException as http_exc:
         raise http_exc
    except Exception as e:
        logger.error(f"Помилка визначення активних сервісів: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Помилка сервера при визначенні активних сервісів.")

    # 3. Пошук за допомогою Aggregation Pipeline (змінений)
    try:
        pipeline = [
            # Етап 1: Знайти відповідні stop_times
            { "$match": { "stop_id": stop_id, "departure_time": {"$gte": current_time_seconds} } },
            # Етап 2: Відсортувати за часом відправлення
            { "$sort": {"departure_time": ASCENDING} },
            # Етап 3: Приєднати інформацію про рейс (trip)
            { "$lookup": { "from": TRIPS_COLLECTION, "localField": "trip_id", "foreignField": "_id", "as": "tripInfoList" } },
            { "$match": { "tripInfoList": { "$ne": [] } } }, # Відкинути, якщо рейс не знайдено
            { "$unwind": "$tripInfoList" },
            # Етап 4: Фільтрувати за АКТИВНИМ service_id ТА ЗАДАНИМ route_id
            {
                "$match": {
                    "tripInfoList.service_id": {"$in": list(active_services)},
                    "tripInfoList.route_id": route_id # <-- ДОДАТКОВИЙ ФІЛЬТР
                }
            },
            # Етап 5: Обмежити результат ОДНИМ записом (найближче відправлення)
            { "$limit": 1 },
             # Етап 6: Приєднати інформацію про маршрут (route) - опціонально для кольору/назви
            { "$lookup": { "from": ROUTES_COLLECTION, "localField": "tripInfoList.route_id", "foreignField": "_id", "as": "routeInfoList" } },
            { "$unwind": { "path": "$routeInfoList", "preserveNullAndEmptyArrays": True } },
            # Етап 7: Сформувати фінальний документ
            {
                "$project": {
                    "_id": 0,
                    "departure_time_seconds": "$departure_time",
                    "route_short_name": "$routeInfoList.route_short_name",
                    "route_long_name": "$routeInfoList.route_long_name",
                    "trip_headsign": {"$ifNull": ["$stop_headsign", "$tripInfoList.trip_headsign"]},
                    "route_color": "$routeInfoList.route_color",
                    "wheelchair_accessible": "$tripInfoList.wheelchair_accessible",
                }
            }
        ]

        # Виконуємо aggregation pipeline
        departure_docs_cursor = db[STOP_TIMES_COLLECTION].aggregate(pipeline)
        # Отримуємо перший (і єдиний) результат або None
        result_doc = await departure_docs_cursor.to_list(length=1)

        if not result_doc:
            # Якщо агрегація нічого не повернула
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Наступне відправлення для маршруту {route_id} з зупинки {stop_id} не знайдено на вказаний час/дату.")

        doc = result_doc[0] # Беремо перший документ

        # Форматуємо результат
        dep_seconds = doc.get("departure_time_seconds")
        if isinstance(dep_seconds, (int, float)):
             dep_info = DepartureInfo(
                 departure_time=format_seconds_to_gtfs_time(int(dep_seconds)),
                 route_short_name=doc.get("route_short_name"),
                 route_long_name=doc.get("route_long_name"),
                 trip_headsign=doc.get("trip_headsign"),
                 route_color=doc.get("route_color"),
                 wheelchair_accessible=doc.get("wheelchair_accessible")
             )
             return dep_info
        else:
             logger.error(f"Отримано некоректний departure_time_seconds після агрегації: {doc}")
             raise HTTPException(status_code=500, detail="Помилка форматування часу відправлення.")


    except HTTPException as http_exc:
        # Перевикидаємо 404 помилку, якщо вона виникла вище
        raise http_exc
    except Exception as e:
        logger.error(f"Помилка aggregation pipeline для наступного відправлення route={route_id}, stop={stop_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Помилка сервера при пошуку наступного відправлення.")
