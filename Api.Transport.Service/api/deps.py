# api/deps.py
import motor.motor_asyncio
from fastapi import HTTPException, status

# Імпортуємо функцію отримання БД з модуля core.database
from core.database import get_database as get_core_database

async def get_db_dependency() -> motor.motor_asyncio.AsyncIOMotorDatabase:
    """
    FastAPI dependency to get database instance.
    Raises HTTPException 500 if database is not available.
    """
    try:
        # Отримуємо об'єкт БД, який був ініціалізований при старті
        db = get_core_database()
        return db
    except RuntimeError as e:
        # Якщо get_database() викликає помилку (не повинен при правильному lifespan)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Помилка доступу до бази даних: {e}"
        )
    except Exception as e:
         # Інші можливі помилки
         raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Внутрішня помилка сервера при отриманні з'єднання з БД: {e}"
        )