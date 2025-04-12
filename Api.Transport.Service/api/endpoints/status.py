# api/endpoints/status.py
import asyncio
import logging
from fastapi import APIRouter, Depends
import motor.motor_asyncio
from datetime import datetime, timezone

from api.models import StatusResponse
from api.deps import get_db_dependency
from core.config import (
    ROUTES_COLLECTION, SHAPES_COLLECTION, STOPS_COLLECTION,
    TRIPS_COLLECTION, STOP_TIMES_COLLECTION, META_COLLECTION
)
# ЗМІНЕНО: Імпортуємо стан з core.state
from core.state import app_state

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/status", response_model=StatusResponse, tags=["Статус"])
async def get_status_endpoint(db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_db_dependency)):
    # ... (решта коду функції get_status_endpoint без змін,
    #      вона використовує app_state, імпортований вище)
    status = "OK"
    message = "Сервіс працює. Дані читаються з MongoDB."
    last_update = None
    next_update_dt = None
    counts = {
        "db_routes_count": None, "db_shapes_count": None, "db_stops_count": None,
        "db_trips_count": None, "db_stop_times_count": None
    }
    try:
        update_info = await db[META_COLLECTION].find_one({"_id": "gtfs_update_status"})
        if update_info: last_update = update_info.get("last_successful_update_utc")
        count_tasks = {
            "db_routes_count": db[ROUTES_COLLECTION].estimated_document_count(),
            "db_shapes_count": db[SHAPES_COLLECTION].estimated_document_count(),
            "db_stops_count": db[STOPS_COLLECTION].estimated_document_count(),
            "db_trips_count": db[TRIPS_COLLECTION].estimated_document_count(),
            "db_stop_times_count": db[STOP_TIMES_COLLECTION].estimated_document_count(),
        }
        results = await asyncio.gather(*count_tasks.values(), return_exceptions=True)
        counts = dict(zip(count_tasks.keys(), results))
        for key, result in counts.items():
            if isinstance(result, Exception):
                logger.warning(f"Не вдалося отримати кількість документів для {key}: {result}")
                counts[key] = None
    except Exception as e:
        logger.warning(f"Не вдалося отримати статус оновлення або кількість документів: {e}", exc_info=True)
        status = "Warning"
        message = "Не вдалося отримати повний статус бази даних."

    last_error = app_state.get("last_update_error")
    if last_error:
        if status != "Warning": status = "Error"
        message = f"{message} Помилка останнього фонового оновлення: {last_error}"

    next_update_ts = app_state.get("next_update_time")
    if next_update_ts:
        try:
            next_update_dt = datetime.fromtimestamp(next_update_ts, tz=timezone.utc)
        except (OSError, OverflowError, TypeError):
            logger.warning(f"Некоректний timestamp для наступного оновлення: {next_update_ts}")
            next_update_dt = None

    # Використовуємо counts напряму
    return StatusResponse(
        status=status, message=message, last_successful_update_utc=last_update,
        next_update_approx_utc=next_update_dt,
        update_in_progress=app_state.get("update_task_running", False),
        **counts
    )