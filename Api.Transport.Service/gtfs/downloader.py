# gtfs/downloader.py
import io
import logging
import zipfile
from typing import Optional
import requests # Переконайтесь, що requests встановлено

logger = logging.getLogger(__name__)

def download_gtfs_zip(url: str) -> Optional[zipfile.ZipFile]:
    """Завантажує GTFS ZIP архів з URL і повертає об'єкт ZipFile."""
    logger.info(f"Завантаження GTFS архіву з: {url}")
    try:
        response = requests.get(url, stream=True, timeout=60) # Таймаут 60 секунд
        response.raise_for_status() # Перевірка на HTTP помилки (4xx, 5xx)

        # Перевірка типу контенту
        content_type = response.headers.get('content-type', '').lower()
        if 'zip' not in content_type and 'octet-stream' not in content_type:
             logger.warning(f"Отримано неочікуваний тип контенту '{content_type}' замість zip/octet-stream.")

        # Читаємо контент в пам'ять
        zip_content = io.BytesIO(response.content)
        file_size_mb = len(response.content) / 1024 / 1024
        logger.info(f"Архів успішно завантажено ({file_size_mb:.2f} MB).")

        if file_size_mb == 0:
            logger.error("Завантажено порожній файл.")
            return None

        # Відкриваємо ZIP архів з байтів
        zip_ref = zipfile.ZipFile(zip_content)
        logger.info("Архів успішно відкрито в пам'яті.")
        return zip_ref

    except requests.exceptions.RequestException as e:
        logger.error(f"Помилка завантаження архіву: {e}", exc_info=True)
        return None
    except zipfile.BadZipFile:
        logger.error("Помилка: Завантажений файл не є коректним ZIP архівом.", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Неочікувана помилка під час завантаження або відкриття архіву: {e}", exc_info=True)
        return None