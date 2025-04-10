# core/state.py
from typing import Dict, Any, Optional, Coroutine
import asyncio

# Глобальний стан, доступний для різних модулів
app_state: Dict[str, Any] = {
    "db": None, # Об'єкт бази даних motor
    "update_task": None, # Посилання на фонове завдання asyncio.Task
    "update_task_running": False, # Чи запущено зараз фонове завдання
    "next_update_time": None, # Час наступного оновлення (timestamp)
    "last_update_error": None # Повідомлення про помилку останнього оновлення
}