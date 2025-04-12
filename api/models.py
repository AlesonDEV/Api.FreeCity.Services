# api/models.py
from pydantic import BaseModel, Field, ConfigDict, field_validator, field_serializer
from typing import List, Dict, Optional, Any
from datetime import datetime, date

# Загальна конфігурація для моделей
common_config = ConfigDict(
    populate_by_name=True,       # Дозволяє використовувати alias для _id
    arbitrary_types_allowed=True, # Для datetime та інших типів
    json_encoders={datetime: lambda dt: dt.isoformat(timespec='seconds') + 'Z'} # ISO формат UTC
)

# --- Моделі для відповіді API ---

class RouteInfo(BaseModel):
    """Інформація про маршрут."""
    model_config = common_config
    # Використовуємо alias для мапінгу _id з MongoDB на поле id
    id: str = Field(..., alias="_id", description="Унікальний ідентифікатор маршруту (збігається з route_id)")
    route_id: str = Field(..., description="GTFS ID маршруту")
    agency_id: Optional[str] = Field(None, description="ID перевізника")
    route_short_name: Optional[str] = Field(None, description="Короткий номер/назва маршруту")
    route_long_name: Optional[str] = Field(None, description="Повна назва маршруту")
    route_desc: Optional[str] = Field(None, description="Опис маршруту")
    route_type: Optional[str] = Field(None, description="Тип маршруту (GTFS код)")
    route_url: Optional[str] = Field(None, description="URL маршруту")
    route_color: Optional[str] = Field(None, description="Колір маршруту (HEX без #)")
    route_text_color: Optional[str] = Field(None, description="Колір тексту для маршруту (HEX без #)")
    shape_ids: List[str] = Field(default_factory=list, description="Список ID геометрій (шейпів), пов'язаних з маршрутом")

class StopLocation(BaseModel):
    """Гео-координати зупинки (для вкладеної структури)."""
    type: str = "Point"
    coordinates: List[float] # [longitude, latitude]

class StopInfo(BaseModel):
    """Інформація про зупинку."""
    model_config = common_config
    id: str = Field(..., alias="_id", description="Унікальний ID зупинки (збігається з stop_id)")
    stop_id: str = Field(..., description="GTFS ID зупинки")
    stop_code: Optional[str] = Field(None, description="Короткий код зупинки (якщо є)")
    stop_name: str = Field(..., description="Назва зупинки")
    stop_desc: Optional[str] = Field(None, description="Опис зупинки")
    stop_lat: float = Field(..., description="Широта")
    stop_lon: float = Field(..., description="Довгота")
    # location: Optional[StopLocation] = Field(None, description="Гео-координати у форматі GeoJSON") # Можна додати, якщо потрібно
    zone_id: Optional[str] = Field(None, description="ID тарифної зони")
    stop_url: Optional[str] = Field(None, description="URL зупинки")
    location_type: Optional[str] = Field(None, description="Тип локації (0: зупинка, 1: станція)")
    parent_station: Optional[str] = Field(None, description="ID батьківської станції (для платформ)")
    wheelchair_boarding: Optional[str] = Field(None, description="Доступність для візків (0: немає інфо, 1: доступно, 2: недоступно)")

class ShapeInfo(BaseModel):
    """Геометрія маршруту (шейп)."""
    model_config = common_config
    id: str = Field(..., alias="_id", description="Унікальний ID шейпу (збігається з shape_id)")
    shape_id: str = Field(..., description="GTFS ID шейпу")
    coordinates: List[List[float]] = Field(..., description="Масив координат [latitude, longitude]")

class DepartureInfo(BaseModel):
    """Інформація про наступне відправлення."""
    model_config = common_config
    departure_time: str = Field(..., description="Час відправлення (HH:MM:SS, може бути > 24)")
    route_short_name: Optional[str] = Field(None, description="Короткий номер/назва маршруту")
    route_long_name: Optional[str] = Field(None, description="Повна назва маршруту")
    trip_headsign: Optional[str] = Field(None, description="Напрямок руху рейсу (headsign)")
    route_color: Optional[str] = Field(None, description="Колір маршруту")
    wheelchair_accessible: Optional[int] = Field(None, description="Доступність рейсу для візків (0: немає інфо, 1: доступно, 2: недоступно)")

    # Валідатор для перевірки формату часу (можна додати, якщо потрібно)
    # @field_validator('departure_time')
    # @classmethod
    # def check_time_format(cls, v: str) -> str:
    #     try:
    #         parts = v.split(':')
    #         assert len(parts) == 3
    #         h, m, s = map(int, parts)
    #         assert 0 <= m < 60 and 0 <= s < 60 and h >= 0
    #     except (AssertionError, ValueError, TypeError):
    #         raise ValueError(f"Некоректний формат часу відправлення: '{v}'. Очікується HH:MM:SS.")
    #     return v

class StatusResponse(BaseModel):
    """Відповідь ендпоінта статусу."""
    model_config = common_config
    status: str = Field(..., description="Поточний стан сервісу (OK, Loading, Warning, Error)")
    message: str = Field(..., description="Повідомлення про стан")
    last_successful_update_utc: Optional[datetime] = Field(None, description="Час останнього успішного оновлення даних в БД (UTC)")
    next_update_approx_utc: Optional[datetime] = Field(None, description="Приблизний час наступного запланованого оновлення (UTC)")
    update_in_progress: bool = Field(False, description="Чи виконується оновлення даних зараз")
    db_routes_count: Optional[int] = Field(None, description="Приблизна кількість маршрутів у БД")
    db_shapes_count: Optional[int] = Field(None, description="Приблизна кількість шейпів у БД")
    db_stops_count: Optional[int] = Field(None, description="Приблизна кількість зупинок у БД")
    db_trips_count: Optional[int] = Field(None, description="Приблизна кількість рейсів у БД")
    db_stop_times_count: Optional[int] = Field(None, description="Приблизна кількість записів часу зупинок у БД")

class RouteBasicInfo(BaseModel):
    """Скорочена інформація про маршрут для списків."""
    model_config = common_config # Використовуємо спільну конфігурацію
    id: str = Field(..., alias="_id")
    route_id: str = Field(..., description="GTFS ID маршруту", example="992")
    route_short_name: Optional[str] = Field(None, description="Короткий номер/назва", example="А39")
    route_long_name: Optional[str] = Field(None, description="Повна назва", example="вул. Грінченка - Кривчиці")
    route_color: Optional[str] = Field(None, description="Колір маршруту (HEX без #)", example="556b2f")
    route_type: Optional[str] = Field(None, description="Тип маршруту (GTFS код)", example="3")