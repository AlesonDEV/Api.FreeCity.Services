# core/lifespan.py
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from .database import connect_to_mongo, close_mongo_connection
from .scheduler import background_update_task
# ЗМІНЕНО: Імпортуємо стан з core.state
from .state import app_state
# Опціонально, якщо потрібне первинне оновлення
# from gtfs.processor import update_gtfs_data_in_db

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Lifespan: Запуск події startup...")
    await connect_to_mongo()

    # Опціональне первинне оновлення
    # logger.info("Lifespan: Виконується первинне оновлення GTFS в MongoDB...")
    # await update_gtfs_data_in_db()
    # logger.info("Lifespan: Первинне оновлення MongoDB завершено.")

    logger.info("Lifespan: Запуск фонового завдання оновлення...")
    update_task = asyncio.create_task(background_update_task())
    # Зберігаємо посилання на завдання в стані, імпортованому з core.state
    app_state["update_task"] = update_task

    yield

    logger.info("Lifespan: Запуск події shutdown...")
    task_to_cancel = app_state.get("update_task") # Отримуємо посилання на завдання
    if task_to_cancel and not task_to_cancel.done():
        logger.info("Lifespan: Скасування фонового завдання...")
        task_to_cancel.cancel()
        try:
            await asyncio.wait_for(task_to_cancel, timeout=5.0)
        except asyncio.CancelledError:
            logger.info("Lifespan: Фонове завдання оновлення скасовано успішно.")
        except asyncio.TimeoutError:
            logger.warning("Lifespan: Не вдалося коректно скасувати фонове завдання за відведений час.")
        except Exception as e:
             logger.error(f"Lifespan: Помилка при очікуванні скасованого завдання: {e}", exc_info=True)

    await close_mongo_connection()
    logger.info("Lifespan: Завершено.")