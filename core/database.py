# core/database.py
import logging
import motor.motor_asyncio
from .config import MONGO_CONNECTION_STRING, MONGO_DB_NAME

logger = logging.getLogger(__name__)

class DataBase:
    """Клас для зберігання клієнта та об'єкта бази даних MongoDB."""
    client: motor.motor_asyncio.AsyncIOMotorClient | None = None
    db: motor.motor_asyncio.AsyncIOMotorDatabase | None = None

# Глобальний екземпляр для зберігання підключення
db_instance = DataBase()

async def connect_to_mongo():
    """Встановлює з'єднання з MongoDB."""
    if db_instance.client:
        logger.info("З'єднання з MongoDB вже встановлено.")
        return
    logger.info(f"Підключення до MongoDB: {MONGO_CONNECTION_STRING}...")
    try:
        db_instance.client = motor.motor_asyncio.AsyncIOMotorClient(
            MONGO_CONNECTION_STRING,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=15000,
            uuidRepresentation='standard' # Рекомендовано для сучасних версій
        )
        # Перевірка підключення
        await db_instance.client.admin.command('ping')
        db_instance.db = db_instance.client[MONGO_DB_NAME]
        logger.info(f"Успішно підключено до MongoDB, база даних: '{MONGO_DB_NAME}'.")
    except Exception as e:
        logger.error(f"Не вдалося підключитися до MongoDB: {e}", exc_info=True)
        db_instance.client = None
        db_instance.db = None
        raise RuntimeError(f"Не вдалося підключитися до MongoDB: {e}") # Перевикидаємо помилку

async def close_mongo_connection():
    """Закриває з'єднання з MongoDB."""
    logger.info("Закриття з'єднання з MongoDB...")
    if db_instance.client:
        db_instance.client.close()
        db_instance.client = None
        db_instance.db = None
        logger.info("З'єднання з MongoDB закрито.")

def get_database() -> motor.motor_asyncio.AsyncIOMotorDatabase:
    """Повертає активний об'єкт бази даних motor."""
    if db_instance.db is None:
        logger.critical("Спроба отримати об'єкт БД до встановлення з'єднання!")
        # Ця помилка не повинна виникати, якщо lifespan працює правильно
        raise RuntimeError("З'єднання з базою даних не встановлено.")
    return db_instance.db