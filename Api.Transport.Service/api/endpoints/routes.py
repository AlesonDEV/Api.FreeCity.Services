# api/endpoints/routes.py
import logging
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
import motor.motor_asyncio

from api.models import RouteInfo
from api.deps import get_db_dependency
from core.config import ROUTES_COLLECTION

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/routes", response_model=List[RouteInfo], tags=["GTFS Дані"])
async def get_routes_endpoint(db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)):
    """Отримує список всіх маршрутів з бази даних."""
    try:
        routes_cursor = db[ROUTES_COLLECTION].find({})
        # length=None може бути неефективним для дуже великих колекцій
        routes_list = await routes_cursor.to_list(length=2000) # Обмежимо довжину для безпеки
        if not routes_list:
             # Перевіряємо, чи колекція дійсно порожня
            count = await db[ROUTES_COLLECTION].estimated_document_count()
            if count == 0:
                 # Це не помилка, просто немає даних
                 return [] # Повертаємо порожній список
            else:
                 # Якщо count > 0, але список порожній, можливо проблема з запитом/даними
                 logger.warning("Маршрути є в БД, але запит find повернув порожній список.")
                 # Повертаємо порожній список, клієнт має це обробити
                 return []
        return routes_list
    except Exception as e:
        logger.error(f"Помилка отримання маршрутів з БД: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Не вдалося отримати маршрути з бази даних."
        )

@router.get("/routes/{route_id}", response_model=RouteInfo, tags=["GTFS Дані"])
async def get_route_by_id_endpoint(route_id: str, db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)):
    """Отримує інформацію про конкретний маршрут за його ID."""
    try:
        # Шукаємо за _id, яке дорівнює route_id
        route_doc = await db[ROUTES_COLLECTION].find_one({"_id": route_id})
        if not route_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Маршрут з ID '{route_id}' не знайдено.")
        return route_doc
    except Exception as e:
        logger.error(f"Помилка отримання маршруту {route_id} з БД: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Не вдалося отримати маршрут з бази даних."
        )