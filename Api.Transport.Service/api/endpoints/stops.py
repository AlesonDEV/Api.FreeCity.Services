# api/endpoints/stops.py
import logging
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
import motor.motor_asyncio

from api.models import StopInfo
from api.deps import get_db_dependency
from core.config import STOPS_COLLECTION

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/stops", response_model=List[StopInfo], tags=["GTFS Дані"])
async def get_stops_endpoint(db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)):
    """Отримує список всіх зупинок з бази даних."""
    try:
        # Виключаємо поле 'location' GeoJSON, якщо воно не потрібне у відповіді
        stops_cursor = db[STOPS_COLLECTION].find({}, {"location": 0})
        stops_list = await stops_cursor.to_list(length=20000) # Збільшимо ліміт для зупинок
        if not stops_list:
            count = await db[STOPS_COLLECTION].estimated_document_count()
            if count == 0:
                 return []
            else:
                 logger.warning("Зупинки є в БД, але запит find повернув порожній список.")
                 return []
        return stops_list
    except Exception as e:
        logger.error(f"Помилка отримання зупинок з БД: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Не вдалося отримати зупинки з бази даних."
        )

# Можна додати ендпоінт /stops/{stop_id} за аналогією з /routes/{route_id}
@router.get("/stops/{stop_id}", response_model=StopInfo, tags=["GTFS Дані"])
async def get_stop_by_id_endpoint(stop_id: str, db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)):
    """Отримує інформацію про конкретну зупинку за її ID."""
    try:
        stop_doc = await db[STOPS_COLLECTION].find_one({"_id": stop_id}, {"location": 0})
        if not stop_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Зупинку з ID '{stop_id}' не знайдено.")
        return stop_doc
    except Exception as e:
        logger.error(f"Помилка отримання зупинки {stop_id} з БД: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Не вдалося отримати зупинку з бази даних."
        )