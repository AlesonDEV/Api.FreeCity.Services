# core/scheduler.py
import logging
import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

# Імпортуємо функцію оновлення та конфігурацію
from gtfs.processor import update_gtfs_data_in_db
from .config import UPDATE_INTERVAL_SECONDS, META_COLLECTION
# Імпортуємо стан та функцію отримання БД
from .state import app_state
from .database import get_database # Потрібна для читання часу останнього оновлення

logger = logging.getLogger(__name__)

async def background_update_task():
    """
    Періодично запускає оновлення GTFS даних в MongoDB,
    очікуючи до наступного запланованого часу перед кожним запуском.
    """
    logger.info("Запуск фонового завдання для періодичного оновлення GTFS в MongoDB.")
    app_state["update_task_running"] = True

    # Даємо трохи часу на стабілізацію після запуску сервера
    await asyncio.sleep(5)

    while True:
        start_wait_time = time.monotonic()
        wait_time: float = 0.0
        last_update_ts: Optional[datetime] = None

        try:
            # 1. Визначаємо час наступного запуску
            try:
                db = get_database()
                update_info = await db[META_COLLECTION].find_one({"_id": "gtfs_update_status"})
                if update_info:
                    last_update_ts = update_info.get("last_successful_update_utc")
                    # Переконуємося, що це datetime об'єкт з часовою зоною UTC
                    if last_update_ts and isinstance(last_update_ts, datetime) and last_update_ts.tzinfo is None:
                        last_update_ts = last_update_ts.replace(tzinfo=timezone.utc)

            except Exception as e:
                 logger.warning(f"Не вдалося отримати час останнього оновлення з БД: {e}. Буде використано поточний час для розрахунку.")
                 last_update_ts = None # Скидаємо, якщо сталася помилка

            now_utc = datetime.now(timezone.utc)

            # Якщо є час останнього оновлення, розраховуємо наступний від нього
            if last_update_ts:
                next_scheduled_update_time = last_update_ts + timedelta(seconds=UPDATE_INTERVAL_SECONDS)
                logger.info(f"Останнє успішне оновлення: {last_update_ts.isoformat()}. Запланований інтервал: {UPDATE_INTERVAL_SECONDS} сек.")
            else:
                # Якщо це перший запуск або помилка отримання часу,
                # плануємо перший запуск через невеликий проміжок часу (напр. 1 хв)
                # АБО можна виконати одразу (wait_time = 0)
                logger.warning(f"Немає даних про останнє оновлення. Плануємо перше оновлення через {UPDATE_INTERVAL_SECONDS} секунд.")
                next_scheduled_update_time = now_utc + timedelta(seconds=UPDATE_INTERVAL_SECONDS)

            # Розраховуємо час очікування до наступного запланованого моменту
            wait_time = (next_scheduled_update_time - now_utc).total_seconds()
            wait_time = max(10.0, wait_time) # Чекаємо щонайменше 10 секунд

            app_state["next_update_time"] = time.time() + wait_time # Зберігаємо приблизний timestamp для /status
            next_update_dt_log = datetime.fromtimestamp(app_state["next_update_time"], tz=timezone.utc)
            logger.info(f"Наступне оновлення MongoDB заплановано приблизно на {next_update_dt_log.isoformat()} (очікування {wait_time:.0f} сек).")

            # 2. Очікуємо до запланованого часу
            await asyncio.sleep(wait_time)

            # 3. Виконуємо оновлення
            logger.info("Початок планового оновлення GTFS даних в MongoDB...")
            app_state["last_update_error"] = None # Скидаємо помилку перед запуском

            # Викликаємо функцію оновлення
            success = await update_gtfs_data_in_db()

            if success:
                 logger.info("Планове оновлення GTFS в MongoDB завершено успішно.")
                 # Час останнього оновлення запише сама функція update_gtfs_data_in_db
            else:
                app_state["last_update_error"] = "Помилка під час планового оновлення даних в MongoDB (див. лог)."
                logger.error("Не вдалося виконати планове оновлення GTFS даних в MongoDB.")

        except asyncio.CancelledError:
             logger.info("Фонове завдання оновлення було скасовано під час очікування або виконання.")
             app_state["update_task_running"] = False
             break # Виходимо з циклу
        except Exception as e:
            # Логуємо критичні помилки, що сталися поза межами update_gtfs_data_in_db (напр., при отриманні БД)
            logger.exception("Критична помилка у фоновому завданні оновлення MongoDB (цикл while).")
            app_state["last_update_error"] = f"Внутрішня помилка сервера в фоновому завданні: {e}"
            # Не перериваємо цикл, спробуємо ще раз після стандартного інтервалу (щоб уникнути частого перезапуску при помилках)
            fail_wait_time = max(60.0, UPDATE_INTERVAL_SECONDS / 10) # Чекаємо 1/10 інтервалу або 1 хв
            logger.info(f"Спробуємо перезапустити цикл оновлення через {fail_wait_time:.0f} секунд після помилки.")
            try:
                await asyncio.sleep(fail_wait_time)
            except asyncio.CancelledError:
                 logger.info("Очікування після помилки скасовано.")
                 app_state["update_task_running"] = False
                 break
        # Кінець циклу while, переходимо до наступної ітерації (розрахунку часу очікування)