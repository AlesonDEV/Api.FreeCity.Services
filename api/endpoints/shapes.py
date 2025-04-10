# api/endpoints/shapes.py
import logging
from typing import Dict, List
from fastapi import APIRouter, Depends, HTTPException, status
import motor.motor_asyncio

from api.deps import get_db_dependency
from core.config import SHAPES_COLLECTION

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/shapes", response_model=Dict[str, List[List[float]]], tags=["GTFS Дані"])
async def get_shapes_endpoint(db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)):
    """
    Отримує словник всіх шейпів { shape_id: [[lat, lon], ...] }.
    УВАГА: Може бути ресурсоємним для великих GTFS фідів.
    """
    try:
        shapes_cursor = db[SHAPES_COLLECTION].find({}, {"_id": 0, "shape_id": 1, "coordinates": 1})
        shapes_dict = {}
        # Обмеження для запобігання переповненню пам'яті
        limit = 10000
        count = 0
        async for shape_doc in shapes_cursor:
            if count >= limit:
                logger.warning(f"Досягнуто ліміт ({limit}) при отриманні шейпів. Можливо, не всі шейпи повернуто.")
                break
            if "shape_id" in shape_doc and "coordinates" in shape_doc:
                shapes_dict[shape_doc["shape_id"]] = shape_doc["coordinates"]
                count += 1

        if not shapes_dict:
             count_in_db = await db[SHAPES_COLLECTION].estimated_document_count()
             if count_in_db == 0:
                 # Повертаємо порожній об'єкт, якщо даних немає
                 return {}
             else:
                 logger.warning("Шейпи є в БД, але запит find повернув порожній результат.")
                 return {} # Повертаємо порожній об'єкт

        return shapes_dict
    except Exception as e:
        logger.error(f"Помилка отримання шейпів з БД: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Не вдалося отримати шейпи з бази даних."
        )