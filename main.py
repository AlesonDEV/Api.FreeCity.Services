# main.py

import logging
import asyncio
import time # Для відстеження часу останнього оновлення
from contextlib import asynccontextmanager
from datetime import datetime, timezone # Для мітки часу
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field # Для моделей даних

# Імпортуємо функцію обробки з нашого нового файлу
from gtfs_processor import process_gtfs_data_from_url

# --- Конфігурація ---
# Інтервал оновлення даних (1 тиждень в секундах)
UPDATE_INTERVAL_SECONDS = 7 * 24 * 60 * 60
# UPDATE_INTERVAL_SECONDS = 60 # Для тестування можна встановити менший інтервал (наприклад, 60 секунд)

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Моделі даних Pydantic (ті самі, що й раніше) ---
class RouteShape(BaseModel):
    lat: float = Field(..., description="Широта точки")
    lon: float = Field(..., description="Довгота точки")

class RouteInfo(BaseModel):
    route_id: str
    agency_id: Optional[str] = None
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    route_desc: Optional[str] = None
    route_type: Optional[str] = None
    route_url: Optional[str] = None
    route_color: Optional[str] = None
    route_text_color: Optional[str] = None
    shape_ids: List[str] = Field(default_factory=list, description="Список ID шейпів, пов'язаних з цим маршрутом")

class StatusResponse(BaseModel):
    status: str
    message: str
    last_update_utc: Optional[datetime] = None
    next_update_approx_utc: Optional[datetime] = None
    routes_count: Optional[int] = None
    shapes_count: Optional[int] = None


class StopInfo(BaseModel):
    """Модель для інформації про зупинку."""
    stop_id: str
    stop_code: Optional[str] = None
    stop_name: str
    stop_desc: Optional[str] = None
    stop_lat: float
    stop_lon: float
    zone_id: Optional[str] = None
    stop_url: Optional[str] = None
    location_type: Optional[str] = None
    parent_station: Optional[str] = None
    wheelchair_boarding: Optional[str] = None

class StatusResponse(BaseModel): # Оновлюємо модель статусу
    status: str
    message: str
    last_update_utc: Optional[datetime] = None
    next_update_approx_utc: Optional[datetime] = None
    routes_count: Optional[int] = None
    shapes_count: Optional[int] = None
    stops_count: Optional[int] = None # Додаємо кількість зупинок

# --- Глобальний стан ---
app_state: Dict[str, Any] = {
    "routes": [],
    "shapes": {},
    "stops": [], # Додаємо сховище для зупинок
    "data_loaded": False,
    "loading_in_progress": False,
    "loading_error": None,
    "last_successful_update": None,
    "next_update_time": None
}

# --- Фонове завдання ---
async def background_update_task():
    logger.info("Запуск фонового завдання для періодичного оновлення GTFS.")
    while True:
        try:
            logger.info("Початок планового оновлення GTFS даних...")
            app_state["loading_in_progress"] = True
            app_state["loading_error"] = None

            processed_data = await asyncio.to_thread(process_gtfs_data_from_url)

            if processed_data:
                # Розпаковуємо три значення
                routes, shapes, stops = processed_data
                app_state["routes"] = routes
                app_state["shapes"] = shapes
                app_state["stops"] = stops # Зберігаємо зупинки
                app_state["data_loaded"] = True
                app_state["last_successful_update"] = datetime.now(timezone.utc)
                logger.info(f"GTFS дані успішно оновлено. Маршрутів: {len(routes)}, Шейпів: {len(shapes)}, Зупинок: {len(stops)}. Наступне оновлення приблизно через {UPDATE_INTERVAL_SECONDS / 3600:.1f} годин.")
            else:
                app_state["loading_error"] = "Помилка під час обробки GTFS даних (див. лог сервера)."
                logger.error("Не вдалося оновити GTFS дані.")

            app_state["loading_in_progress"] = False
        except Exception as e:
            logger.exception("Критична помилка у фоновому завданні оновлення GTFS.")
            app_state["loading_error"] = f"Внутрішня помилка сервера: {e}"
            app_state["loading_in_progress"] = False
        finally:
            app_state["next_update_time"] = time.time() + UPDATE_INTERVAL_SECONDS
            next_update_dt = datetime.fromtimestamp(app_state["next_update_time"], tz=timezone.utc)
            logger.info(f"Наступна спроба оновлення запланована на {next_update_dt.isoformat()}")
            await asyncio.sleep(UPDATE_INTERVAL_SECONDS)

# --- Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск події startup...")
    logger.info("Виконується первинне завантаження GTFS даних...")
    app_state["loading_in_progress"] = True
    initial_data = await asyncio.to_thread(process_gtfs_data_from_url)
    if initial_data:
        routes, shapes, stops = initial_data # Розпаковуємо три значення
        app_state["routes"] = routes
        app_state["shapes"] = shapes
        app_state["stops"] = stops # Зберігаємо зупинки
        app_state["data_loaded"] = True
        app_state["last_successful_update"] = datetime.now(timezone.utc)
        logger.info("Первинне завантаження GTFS даних успішне.")
    else:
        app_state["loading_error"] = "Помилка під час первинного завантаження GTFS даних."
        app_state["data_loaded"] = False
        logger.error("Не вдалося виконати первинне завантаження GTFS даних!")
    app_state["loading_in_progress"] = False

    update_task = asyncio.create_task(background_update_task())
    logger.info("Фонове завдання для періодичного оновлення GTFS запущено.")
    yield
    logger.info("Запуск події shutdown...")
    update_task.cancel()
    try:
        await update_task
    except asyncio.CancelledError:
        logger.info("Фонове завдання оновлення GTFS скасовано.")

# --- FastAPI App та CORS ---
app = FastAPI(
    title="Lviv GTFS API (оновлюваний)",
    description="API для отримання оброблених даних GTFS статичного фідe Львова з періодичним оновленням.",
    version="1.2.0", # Оновили версію
    lifespan=lifespan
)
# ... (Налаштування CORS залишається таким же) ...
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# --- Ендпоінти ---

# Оновлюємо /api/status
@app.get("/api/status",
         response_model=StatusResponse,
         tags=["Статус"],
         summary="Перевірка стану завантаження та оновлення даних")
async def get_status():
    status = "OK"
    message = "Дані GTFS завантажені."
    next_update_dt = None
    # ... (Логіка визначення status/message залишається) ...
    if app_state["loading_in_progress"]:
        status = "Loading"
        message = "Виконується завантаження/оновлення GTFS даних..."
    elif not app_state["data_loaded"] and app_state["loading_error"]:
        status = "Error"
        message = f"Помилка первинного завантаження: {app_state['loading_error']}"
    elif app_state["loading_error"]:
        status = "Warning"
        message = f"Дані можуть бути застарілими. Помилка останнього оновлення: {app_state['loading_error']}"
    elif not app_state["data_loaded"]:
        status = "Error"
        message = "Дані не завантажені, причина невідома."

    if app_state.get("next_update_time"):
        next_update_dt = datetime.fromtimestamp(app_state["next_update_time"], tz=timezone.utc)

    return StatusResponse(
        status=status,
        message=message,
        last_update_utc=app_state.get("last_successful_update"),
        next_update_approx_utc=next_update_dt,
        routes_count=len(app_state["routes"]) if app_state["data_loaded"] else None,
        shapes_count=len(app_state["shapes"]) if app_state["data_loaded"] else None,
        stops_count=len(app_state["stops"]) if app_state["data_loaded"] else None, # Додаємо кількість зупинок
    )

def check_data_availability():
    # ... (Функція залишається такою ж) ...
    if app_state["loading_in_progress"] and not app_state["data_loaded"]:
        raise HTTPException(status_code=503, detail="Сервіс тимчасово недоступний, іде первинне завантаження даних. Спробуйте пізніше.")
    if not app_state["data_loaded"]:
         raise HTTPException(status_code=503, detail=f"Сервіс недоступний через помилку завантаження даних: {app_state.get('loading_error', 'Невідома помилка')}")


# ... (Ендпоінти /api/routes та /api/shapes залишаються такими ж) ...
@app.get("/api/routes", response_model=List[RouteInfo], tags=["GTFS Дані"], summary="Отримати список всіх маршрутів")
async def get_routes():
    check_data_availability()
    if not app_state["routes"]:
         raise HTTPException(status_code=404, detail="Дані маршрутів не знайдено (список порожній).")
    return app_state["routes"]

@app.get("/api/shapes", response_model=Dict[str, List[List[float]]], tags=["GTFS Дані"], summary="Отримати всі шейпи (геометрії) маршрутів")
async def get_shapes():
    check_data_availability()
    if not app_state["shapes"]:
        raise HTTPException(status_code=404, detail="Дані шейпів не знайдено (словник порожній).")
    return app_state["shapes"]

@app.get("/api/routes/{route_id}", response_model=RouteInfo, tags=["GTFS Дані"], summary="Отримати інформацію про конкретний маршрут за ID")
async def get_route_by_id(route_id: str):
    check_data_availability()
    found_route = next((route for route in app_state["routes"] if route["route_id"] == route_id), None)
    if not found_route:
        raise HTTPException(status_code=404, detail=f"Маршрут з ID '{route_id}' не знайдено.")
    return found_route


# ДОДАЄМО НОВИЙ ЕНДПОІНТ ДЛЯ ЗУПИНОК
@app.get("/api/stops",
         response_model=List[StopInfo],
         tags=["GTFS Дані"],
         summary="Отримати список всіх зупинок")
async def get_stops():
    """
    Повертає список всіх оброблених зупинок з їх атрибутами, включаючи координати.
    """
    check_data_availability() # Перевіряємо, чи дані доступні
    if not app_state["stops"]:
         # Якщо дані завантажені, але список порожній
         raise HTTPException(status_code=404, detail="Дані зупинок не знайдено.")
    return app_state["stops"]

# --- Запуск сервера ---
# ... (Блок if __name__ == "__main__": залишається таким же) ...
if __name__ == "__main__":
    import uvicorn
    print("Запуск FastAPI сервера на http://127.0.0.1:8000")
    print("API документація доступна за адресою http://127.0.0.1:8000/docs")
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)