# api/endpoints/stops.py
import logging
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, Path # Додаємо Path
import motor.motor_asyncio

# Імпортуємо необхідні моделі та залежності
from api.models import StopInfo, RouteBasicInfo # Додаємо RouteBasicInfo
from api.deps import get_db_dependency
from core.config import STOPS_COLLECTION, STOP_TIMES_COLLECTION, TRIPS_COLLECTION, ROUTES_COLLECTION

router = APIRouter()
logger = logging.getLogger(__name__)

# ... (існуючі ендпоінти get_stops_endpoint та get_stop_by_id_endpoint залишаються) ...
@router.get("/stops", response_model=List[StopInfo], tags=["GTFS Дані"])
async def get_stops_endpoint(db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)):
    """Отримує список всіх зупинок з бази даних."""
    try:
        stops_cursor = db[STOPS_COLLECTION].find({}, {"location": 0})
        stops_list = await stops_cursor.to_list(length=20000)
        if not stops_list and await db[STOPS_COLLECTION].estimated_document_count() == 0:
             return []
        elif not stops_list:
             logger.warning("Зупинки є в БД, але запит find повернув порожній список.")
             return []
        return stops_list
    except Exception as e:
        logger.error(f"Помилка отримання зупинок з БД: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Не вдалося отримати зупинки з бази даних.")

@router.get("/stops/{stop_id}", response_model=StopInfo, tags=["GTFS Дані"])
async def get_stop_by_id_endpoint(
    stop_id: str = Path(..., description="ID зупинки", example="4714"),
    db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)
):
    """Отримує інформацію про конкретну зупинку за її ID."""
    try:
        stop_doc = await db[STOPS_COLLECTION].find_one({"_id": stop_id}, {"location": 0})
        if not stop_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Зупинку з ID '{stop_id}' не знайдено.")
        return stop_doc
    except Exception as e:
        logger.error(f"Помилка отримання зупинки {stop_id} з БД: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Не вдалося отримати зупинку з бази даних.")


# --- НОВИЙ ЕНДПОІНТ: Маршрути, що проходять через зупинку ---
@router.get(
    "/stops/{stop_id}/routes",
    response_model=List[RouteBasicInfo], # Використовуємо нову модель
    tags=["Розклад"], # Додаємо до тегу "Розклад" або "GTFS Дані"
    summary="Отримати список маршрутів, що обслуговують зупинку"
)
async def get_routes_for_stop(
    stop_id: str = Path(..., description="ID зупинки, для якої шукати маршрути", example="4714"),
    db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)
):
    """
    Повертає список унікальних маршрутів (з основною інформацією),
    які мають хоча б один рейс, що проходить через вказану зупинку (`stop_id`).
    """
    logger.info(f"Запит маршрутів для зупинки stop_id={stop_id}")

    # Перевіримо, чи існує зупинка (опціонально, але добре для валідації)
    stop_exists = await db[STOPS_COLLECTION].find_one({"_id": stop_id}, {"_id": 1})
    if not stop_exists:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Зупинку з ID '{stop_id}' не знайдено.")

    pipeline = [
        # 1. Знайти всі stop_times для цієї зупинки
        { "$match": { "stop_id": stop_id } },
        # 2. Згрупувати за trip_id, щоб отримати унікальні рейси
        { "$group": { "_id": "$trip_id" } },
        # 3. Приєднати дані про рейси (trips)
        {
            "$lookup": {
                "from": TRIPS_COLLECTION,
                "localField": "_id", # _id тут це trip_id з попереднього етапу
                "foreignField": "_id", # trip_id є _id в колекції trips
                "as": "tripInfo"
            }
        },
        # 4. Розгорнути масив (має бути один елемент) і відкинути ті, де рейс не знайдено
        { "$unwind": "$tripInfo" },
        # 5. Згрупувати за route_id, щоб отримати унікальні маршрути
        {
            "$group": {
                "_id": "$tripInfo.route_id" # Групуємо за route_id
            }
        },
         # 6. Отримати повну інформацію про ці маршрути
        {
            "$lookup": {
                "from": ROUTES_COLLECTION,
                "localField": "_id", # _id тут це route_id
                "foreignField": "_id", # route_id є _id в колекції routes
                "as": "routeInfo"
            }
        },
        { "$unwind": "$routeInfo" },
        # 7. Замінити корінь документа на інформацію про маршрут
        { "$replaceRoot": { "newRoot": "$routeInfo" } },
        # 8. Проекція для моделі RouteBasicInfo (опціонально, якщо модель проста)
        {
             "$project": {
                  "_id": 1,
                  "route_id": 1,
                  "route_short_name": 1,
                  "route_long_name": 1,
                  "route_color": 1,
                  "route_type": 1
             }
        },
        # 9. Сортування (опціонально)
        { "$sort": { "route_short_name": 1, "route_id": 1 } }
    ]

    try:
        routes_cursor = db[STOP_TIMES_COLLECTION].aggregate(pipeline)
        routes_list = await routes_cursor.to_list(length=1000) # Обмеження на кількість маршрутів
        logger.info(f"Знайдено {len(routes_list)} маршрутів для зупинки stop_id={stop_id}")
        return routes_list
    except Exception as e:
        logger.error(f"Помилка aggregation pipeline для отримання маршрутів зупинки {stop_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Помилка сервера при пошуку маршрутів для зупинки.")