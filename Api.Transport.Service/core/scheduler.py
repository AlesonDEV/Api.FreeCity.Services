# core/scheduler.py
import logging
import asyncio
import time
from datetime import datetime, timezone

from gtfs.processor import update_gtfs_data_in_db
from .config import UPDATE_INTERVAL_SECONDS
# ЗМІНЕНО: Імпортуємо стан з core.state
from .state import app_state

logger = logging.getLogger(__name__)

async def background_update_task():
    logger.info("Запуск фонового завдання для періодичного оновлення GTFS в MongoDB.")
    # Використовуємо app_state, імпортований з core.state
    app_state["update_task_running"] = True
    await asyncio.sleep(10) # Даємо серверу запуститися

    while True:
        start_run_time = time.monotonic()
        try:
            logger.info("Початок планового оновлення GTFS даних в MongoDB...")
            app_state["last_update_error"] = None # Скидаємо помилку

            success = await update_gtfs_data_in_db() # Викликаємо оновлення

            if success:
                 logger.info("Фонове оновлення GTFS в MongoDB завершено успішно.")
                 # Тут більше не оновлюємо last_successful_update, це робить процесор
            else:
                # Процесор вже залогував помилку
                app_state["last_update_error"] = "Помилка під час фонового оновлення даних в MongoDB (див. лог)."

        except asyncio.CancelledError:
             logger.info("Фонове завдання оновлення було скасовано.")
             app_state["update_task_running"] = False
             break # Виходимо з циклу
        except Exception as e:
            logger.exception("Критична помилка у фоновому завданні оновлення MongoDB.")
            app_state["last_update_error"] = f"Внутрішня помилка сервера в фоновому завданні: {e}"
            # Продовжуємо цикл

        finally:
             run_duration = time.monotonic() - start_run_time
             wait_time = max(10.0, UPDATE_INTERVAL_SECONDS - run_duration)

             app_state["next_update_time"] = time.time() + wait_time
             next_update_dt = datetime.fromtimestamp(app_state["next_update_time"], tz=timezone.utc)
             logger.info(f"Наступна спроба оновлення MongoDB запланована приблизно на {next_update_dt.isoformat()} (через {wait_time:.0f} сек).")

             try:
                 await asyncio.sleep(wait_time)
             except asyncio.CancelledError:
                 logger.info("Очікування скасовано під час sleep.")
                 app_state["update_task_running"] = False
                 break