import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Імпортуємо конфігурацію, lifespan, роутери
from core.config import CORS_ORIGINS
from core.lifespan import lifespan
from api.endpoints import status, routes, shapes, stops, schedule

# Імпортуємо стан (якщо потрібно для логіки main, хоча зараз ні)
# from core.state import app_state

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Створюємо додаток FastAPI з lifespan
app = FastAPI(
    title="Lviv GTFS API (MongoDB, Refactored)",
    description="API для отримання даних GTFS Львова з MongoDB з періодичним оновленням та розкладом.",
    version="2.5.0", # Оновили версію
    lifespan=lifespan # Підключаємо lifespan
)

# Налаштування CORS
# Переконайтесь, що CORS_ORIGINS в config.py налаштовано правильно
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"], # Дозволяємо тільки безпечні методи + OPTIONS
    allow_headers=["*"], # Дозволяємо всі заголовки (можна обмежити)
)

# Префікс для всіх API ендпоінтів
api_prefix = "/api"

# Підключення роутерів
app.include_router(status.router, prefix=api_prefix) # Теги вже визначені в роутері
app.include_router(routes.router, prefix=api_prefix)
app.include_router(shapes.router, prefix=api_prefix)
app.include_router(stops.router, prefix=api_prefix)
app.include_router(schedule.router, prefix=api_prefix)


# Кореневий ендпоінт для перевірки
@app.get("/", include_in_schema=False)
async def root():
    return {"message": "Welcome to Lviv GTFS API. Go to /docs for API documentation."}

# Запуск сервера
if __name__ == "__main__":
    import uvicorn
    from core.config import UPDATE_INTERVAL_SECONDS # Імпорт для логування

    # Використовуйте logging.INFO або logging.DEBUG для більшої деталізації
    log_level = "info"
    print(f"Запуск FastAPI сервера (MongoDB, Refactored) на http://127.0.0.1:8000.")
    print(f"Оновлення GTFS даних кожні {UPDATE_INTERVAL_SECONDS} секунд.")
    print(f"API документація: http://127.0.0.1:8000/docs")
    print(f"Рівень логування: {log_level.upper()}")

    # Важливо: reload=False для стабільної роботи lifespan та scheduler
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=False,
        log_level=log_level
    )