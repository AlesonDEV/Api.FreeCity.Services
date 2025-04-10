# main.py
import logging
import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
import motor.motor_asyncio # Імпортуємо motor

# Імпортуємо функцію оновлення та константи
from gtfs_processor import (
    update_mongo_from_gtfs_url,
    MONGO_CONNECTION_STRING,
    MONGO_DB_NAME,
    ROUTES_COLLECTION,
    SHAPES_COLLECTION,
    STOPS_COLLECTION,
    META_COLLECTION
)

# --- Конфігурація ---
UPDATE_INTERVAL_SECONDS = 7 * 24 * 60 * 60 # 1 тиждень
# UPDATE_INTERVAL_SECONDS = 120 # Для тестування

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Pydantic Моделі ---
# Використовуємо конфігурацію для обробки _id з MongoDB
# та дозволу довільних типів (якщо потрібно)
common_config = ConfigDict(
    populate_by_name=True, # Дозволяє використовувати alias
    arbitrary_types_allowed=True, # Може знадобитися для datetime
    json_encoders={datetime: lambda dt: dt.isoformat()} # Для серіалізації datetime
)

class RouteInfo(BaseModel):
    model_config = common_config
    id: str = Field(..., alias="_id") # Мапимо _id на id
    route_id: str
    agency_id: Optional[str] = None
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    # ... (інші поля як раніше)
    shape_ids: List[str] = Field(default_factory=list)

class StopInfo(BaseModel):
    model_config = common_config
    id: str = Field(..., alias="_id")
    stop_id: str
    stop_code: Optional[str] = None
    stop_name: str
    # ... (інші поля як раніше)
    stop_lat: float
    stop_lon: float
    # Не включаємо 'location', якщо він не потрібен у відповіді API
    wheelchair_boarding: Optional[str] = None

class ShapeInfo(BaseModel):
     model_config = common_config
     id: str = Field(..., alias="_id")
     shape_id: str
     coordinates: List[List[float]] # Масив [lat, lon]

class StatusResponse(BaseModel):
    model_config = common_config
    status: str
    message: str
    last_successful_update_utc: Optional[datetime] = None
    next_update_approx_utc: Optional[datetime] = None
    db_routes_count: Optional[int] = None
    db_shapes_count: Optional[int] = None
    db_stops_count: Optional[int] = None
    update_in_progress: bool = False


# --- Глобальний стан (тільки для керування фоновим завданням) ---
app_state: Dict[str, Any] = {
    "db": None, # Зберігатимемо об'єкт бази даних motor
    "update_task_running": False,
    "next_update_time": None,
    "last_update_error": None # Помилка останнього оновлення
}

# --- Фонове завдання ---
async def background_update_task():
    """Періодично запускає оновлення GTFS даних в MongoDB."""
    logger.info("Запуск фонового завдання для періодичного оновлення GTFS в MongoDB.")
    app_state["update_task_running"] = True
    while True:
        try:
            logger.info("Початок планового оновлення GTFS даних в MongoDB...")
            app_state["last_update_error"] = None # Скидаємо помилку

            # Викликаємо функцію оновлення (вже асинхронна, якщо використовує motor)
            success = await update_mongo_from_gtfs_url() # Ця функція тепер все робить сама

            if success:
                 logger.info("Оновлення GTFS в MongoDB завершено успішно.")
            else:
                app_state["last_update_error"] = "Помилка під час оновлення даних в MongoDB (див. лог)."
                logger.error("Не вдалося оновити GTFS дані в MongoDB.")

        except Exception as e:
            logger.exception("Критична помилка у фоновому завданні оновлення MongoDB.")
            app_state["last_update_error"] = f"Внутрішня помилка сервера в фоновому завданні: {e}"
        finally:
             # Плануємо наступний запуск
            app_state["next_update_time"] = time.time() + UPDATE_INTERVAL_SECONDS
            next_update_dt = datetime.fromtimestamp(app_state["next_update_time"], tz=timezone.utc)
            logger.info(f"Наступна спроба оновлення MongoDB запланована на {next_update_dt.isoformat()}")
            await asyncio.sleep(UPDATE_INTERVAL_SECONDS)


# --- Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск події startup (MongoDB)...")
    db_client = None
    try:
        # Встановлюємо з'єднання з MongoDB
        db_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_CONNECTION_STRING)
        await db_client.admin.command('ping') # Перевірка
        app_state["db"] = db_client[MONGO_DB_NAME]
        logger.info(f"Успішно підключено до MongoDB і встановлено об'єкт бази даних.")

        # Опціонально: Виконати первинне оновлення БД при старті, якщо потрібно
        # await update_mongo_from_gtfs_url()
        # logger.info("Первинне оновлення MongoDB завершено.")

        # Запускаємо фонове завдання для періодичного оновлення
        app_state["update_task_running"] = False # Позначимо, що воно ще не запущено
        asyncio.create_task(background_update_task())

        yield # Додаток працює

    finally:
        logger.info("Запуск події shutdown (MongoDB)...")
        if app_state["update_task_running"] and "update_task" in app_state:
             app_state["update_task"].cancel() # Скасовуємо завдання, якщо воно було збережено
             try:
                 await app_state["update_task"]
             except asyncio.CancelledError:
                 logger.info("Фонове завдання оновлення MongoDB скасовано.")
        if db_client:
            db_client.close()
            logger.info("З'єднання з MongoDB закрито.")

# --- FastAPI App та CORS ---
app = FastAPI(
    title="Lviv GTFS API (MongoDB)",
    description="API для отримання оброблених даних GTFS Львова з MongoDB з періодичним оновленням.",
    version="2.0.0",
    lifespan=lifespan
)
# ... (Налаштування CORS залишається) ...
origins = [ "http://localhost", "http://localhost:3000", "http://localhost:3001", "http://localhost:5173" ]
app.add_middleware( CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["GET", "OPTIONS"], allow_headers=["*"], )

# --- Ендпоінти API (читання з MongoDB) ---

# Допоміжна функція для отримання об'єкту БД
def get_db() -> motor.motor_asyncio.AsyncIOMotorDatabase:
    db = app_state.get("db")
    if not db:
        # Ця ситуація не повинна виникати, якщо lifespan відпрацював коректно
        logger.error("Об'єкт бази даних недоступний у app_state!")
        raise HTTPException(status_code=500, detail="Внутрішня помилка сервера: з'єднання з БД втрачено.")
    return db

@app.get("/api/status", response_model=StatusResponse, tags=["Статус"])
async def get_status():
    db = get_db()
    status = "OK"
    message = "Сервіс працює. Дані читаються з MongoDB."
    last_update = None
    next_update_dt = None
    routes_count = None
    shapes_count = None
    stops_count = None

    try:
        # Отримуємо час останнього оновлення з мета-колекції
        update_info = await db[META_COLLECTION].find_one({"_id": "gtfs_update_status"})
        if update_info:
            last_update = update_info.get("last_successful_update_utc")

        # Отримуємо кількість документів (приблизно)
        routes_count = await db[ROUTES_COLLECTION].estimated_document_count()
        shapes_count = await db[SHAPES_COLLECTION].estimated_document_count()
        stops_count = await db[STOPS_COLLECTION].estimated_document_count()

    except Exception as e:
        logger.warning(f"Не вдалося отримати статус оновлення або кількість документів: {e}")
        status = "Warning"
        message = "Не вдалося отримати повний статус бази даних."

    if app_state.get("last_update_error"):
        status = "Error" if status != "Warning" else "Warning" # Не перезаписуємо Warning на Error
        message = f"{message} Помилка останнього фонового оновлення: {app_state['last_update_error']}"

    if app_state.get("next_update_time"):
        next_update_dt = datetime.fromtimestamp(app_state["next_update_time"], tz=timezone.utc)

    return StatusResponse(
        status=status,
        message=message,
        last_successful_update_utc=last_update,
        next_update_approx_utc=next_update_dt,
        db_routes_count=routes_count,
        db_shapes_count=shapes_count,
        db_stops_count=stops_count,
        update_in_progress=app_state.get("update_task_running", False) # Показуємо, чи активне завдання
    )


@app.get("/api/routes", response_model=List[RouteInfo], tags=["GTFS Дані"])
async def get_routes():
    db = get_db()
    routes_cursor = db[ROUTES_COLLECTION].find({})
    # Виключаємо поле 'location', якщо воно випадково потрапило
    routes_list = await routes_cursor.to_list(length=None) # length=None для отримання всіх
    if not routes_list:
        # Перевіряємо, чи дійсно база даних порожня або сталася помилка
        count = await db[ROUTES_COLLECTION].estimated_document_count()
        if count == 0:
             raise HTTPException(status_code=404, detail="Маршрути не знайдено в базі даних.")
        else:
             # Можливо, інша проблема
             raise HTTPException(status_code=500, detail="Не вдалося отримати маршрути з бази даних.")
    return routes_list


@app.get("/api/shapes", response_model=Dict[str, List[List[float]]], tags=["GTFS Дані"])
async def get_shapes():
    """
    Повертає словник { shape_id: [[lat, lon], ...] }.
    УВАГА: Може бути неефективним для великої кількості шейпів.
    """
    db = get_db()
    shapes_cursor = db[SHAPES_COLLECTION].find({}, {"_id": 0, "shape_id": 1, "coordinates": 1}) # Проекція полів
    shapes_dict = {}
    async for shape_doc in shapes_cursor:
        if "shape_id" in shape_doc and "coordinates" in shape_doc:
            shapes_dict[shape_doc["shape_id"]] = shape_doc["coordinates"]

    if not shapes_dict:
        count = await db[SHAPES_COLLECTION].estimated_document_count()
        if count == 0:
             raise HTTPException(status_code=404, detail="Шейпи не знайдено в базі даних.")
        else:
             raise HTTPException(status_code=500, detail="Не вдалося отримати шейпи з бази даних.")
    return shapes_dict


@app.get("/api/stops", response_model=List[StopInfo], tags=["GTFS Дані"])
async def get_stops():
    db = get_db()
    # Виключаємо поле 'location' з відповіді, якщо воно не потрібне моделі StopInfo
    stops_cursor = db[STOPS_COLLECTION].find({}, {"location": 0})
    stops_list = await stops_cursor.to_list(length=None)
    if not stops_list:
        count = await db[STOPS_COLLECTION].estimated_document_count()
        if count == 0:
             raise HTTPException(status_code=404, detail="Зупинки не знайдено в базі даних.")
        else:
             raise HTTPException(status_code=500, detail="Не вдалося отримати зупинки з бази даних.")
    return stops_list


@app.get("/api/routes/{route_id}", response_model=RouteInfo, tags=["GTFS Дані"])
async def get_route_by_id(route_id: str):
    db = get_db()
    # Шукаємо за _id, оскільки ми його так встановили
    route_doc = await db[ROUTES_COLLECTION].find_one({"_id": route_id})
    if not route_doc:
        raise HTTPException(status_code=404, detail=f"Маршрут з ID '{route_id}' не знайдено.")
    return route_doc

# --- Запуск сервера ---
if __name__ == "__main__":
    import uvicorn
    # Переконуємось, що константа інтервалу існує
    UPDATE_INTERVAL_SECONDS = 7 * 24 * 60 * 60
    print("Запуск FastAPI сервера (MongoDB) на http://127.0.0.1:8000")
    print("API документація доступна за адресою http://127.0.0.1:8000/docs")
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)