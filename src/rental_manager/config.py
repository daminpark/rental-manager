"""Configuration for the rental manager system."""

from datetime import time
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LockType(str, Enum):
    """Type of lock determining default timing."""

    ROOM = "room"
    BATHROOM = "bathroom"
    KITCHEN = "kitchen"
    FRONT = "front"
    BACK = "back"
    STORAGE = "storage"


class CalendarType(str, Enum):
    """Type of calendar/listing."""

    ROOM = "room"
    SUITE_A = "suite_a"
    SUITE_B = "suite_b"
    WHOLE_HOUSE = "whole_house"
    BOTH_HOUSES = "both_houses"


# Slot allocation - uniform across all locks
# Slot 1: Master code
# Slots 2-3: Room 1
# Slots 4-5: Room 2
# Slots 6-7: Room 3
# Slots 8-9: Room 4
# Slots 10-11: Room 5
# Slots 12-13: Room 6
# Slots 14-15: Suite A
# Slots 16-17: Suite B
# Slots 18-19: Whole house / Both houses (shared, mutually exclusive)
# Slot 20: Emergency code

SLOT_ASSIGNMENTS: dict[str, tuple[int, int]] = {
    "room_1": (2, 3),
    "room_2": (4, 5),
    "room_3": (6, 7),
    "room_4": (8, 9),
    "room_5": (10, 11),
    "room_6": (12, 13),
    "suite_a": (14, 15),
    "suite_b": (16, 17),
    "whole_home": (18, 19),  # Shared between {house}vbr and 193195vbr
}

MASTER_CODE_SLOT = 1
EMERGENCY_CODE_SLOT = 20


class DefaultTiming(BaseModel):
    """Default code activation/deactivation times for a lock type."""

    activate: time
    deactivate: time


# Default timings per lock type
DEFAULT_TIMINGS: dict[LockType, DefaultTiming] = {
    LockType.ROOM: DefaultTiming(activate=time(12, 0), deactivate=time(11, 0)),
    LockType.BATHROOM: DefaultTiming(activate=time(15, 0), deactivate=time(11, 0)),
    LockType.KITCHEN: DefaultTiming(activate=time(15, 0), deactivate=time(11, 0)),
    LockType.FRONT: DefaultTiming(activate=time(11, 0), deactivate=time(14, 0)),
    LockType.BACK: DefaultTiming(activate=time(0, 0), deactivate=time(0, 0)),  # Master code only, no guest timing
    LockType.STORAGE: DefaultTiming(activate=time(1, 0), deactivate=time(23, 59)),
}


class LockConfig(BaseModel):
    """Configuration for a single lock."""

    entity_id: str
    lock_type: LockType
    calendars: list[str]  # Calendar IDs that grant access to this lock
    stagger_minutes: int = 0  # Stagger offset for this lock


class CalendarConfig(BaseModel):
    """Configuration for a single calendar."""

    calendar_id: str
    name: str
    calendar_type: CalendarType
    ical_url: str
    rooms: list[int] = Field(default_factory=list)  # Room numbers included (for suites)


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="RENTAL_",
    )

    # Database
    database_url: str = "sqlite+aiosqlite:///./rental_manager.db"

    # Polling interval for calendar fetches (seconds)
    calendar_poll_interval: int = 120

    # Retry settings for code sync
    code_sync_timeout_seconds: int = 120  # Time before considering a code "stuck"
    code_sync_max_retries: int = 3

    # Server settings
    host: str = "0.0.0.0"
    port: int = 8099
    debug: bool = False

    # Home Assistant connection (single instance via Supervisor API)
    ha_url: str = ""
    ha_token: str = ""

    # House code for this instance
    house_code: str = "195"


def build_locks(house_code: str) -> list[LockConfig]:
    """Build lock configuration for the given house."""
    # All calendars for this house
    all_calendars = [
        f"{house_code}_room_1", f"{house_code}_room_2", f"{house_code}_room_3",
        f"{house_code}_room_4", f"{house_code}_room_5", f"{house_code}_room_6",
        f"{house_code}_suite_a", f"{house_code}_suite_b", f"{house_code}vbr", "193195vbr"
    ]
    # All except room 3 (has ensuite)
    bathroom_calendars = [
        f"{house_code}_room_1", f"{house_code}_room_2",
        f"{house_code}_room_4", f"{house_code}_room_5", f"{house_code}_room_6",
        f"{house_code}_suite_a", f"{house_code}_suite_b", f"{house_code}vbr", "193195vbr"
    ]

    return [
        LockConfig(
            entity_id=f"lock.{house_code}_back_lock",
            lock_type=LockType.BACK,
            calendars=[],  # Master code only — no guest codes
            stagger_minutes=0,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_front_lock",
            lock_type=LockType.FRONT,
            calendars=all_calendars,
            stagger_minutes=0,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_k_lock",
            lock_type=LockType.KITCHEN,
            calendars=all_calendars,
            stagger_minutes=0,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_v_lock",
            lock_type=LockType.STORAGE,
            calendars=all_calendars,
            stagger_minutes=0,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_a_lock",
            lock_type=LockType.BATHROOM,
            calendars=bathroom_calendars,
            stagger_minutes=0,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_b_lock",
            lock_type=LockType.BATHROOM,
            calendars=bathroom_calendars,
            stagger_minutes=0,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_1_lock",
            lock_type=LockType.ROOM,
            calendars=[f"{house_code}_room_1", f"{house_code}_suite_a", f"{house_code}vbr", "193195vbr"],
            stagger_minutes=0,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_2_lock",
            lock_type=LockType.ROOM,
            calendars=[f"{house_code}_room_2", f"{house_code}_suite_a", f"{house_code}vbr", "193195vbr"],
            stagger_minutes=2,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_3_lock",
            lock_type=LockType.ROOM,
            calendars=[f"{house_code}_room_3", f"{house_code}vbr", "193195vbr"],
            stagger_minutes=4,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_4_lock",
            lock_type=LockType.ROOM,
            calendars=[f"{house_code}_room_4", f"{house_code}_suite_b", f"{house_code}vbr", "193195vbr"],
            stagger_minutes=6,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_5_lock",
            lock_type=LockType.ROOM,
            calendars=[f"{house_code}_room_5", f"{house_code}_suite_b", f"{house_code}vbr", "193195vbr"],
            stagger_minutes=8,
        ),
        LockConfig(
            entity_id=f"lock.{house_code}_6_lock",
            lock_type=LockType.ROOM,
            calendars=[f"{house_code}_room_6", f"{house_code}_suite_b", f"{house_code}vbr", "193195vbr"],
            stagger_minutes=10,
        ),
    ]


# Calendar iCal URLs are configured via the web UI after installation.
# On first run, calendars are created with empty URLs.
# Go to the Calendars view to set each calendar's iCal URL.

# Calendar metadata keyed by calendar_id
_CALENDAR_META: dict[str, tuple[str, CalendarType, list[int]]] = {
    "195_room_1": ("195 Room 1", CalendarType.ROOM, [1]),
    "195_room_2": ("195 Room 2", CalendarType.ROOM, [2]),
    "195_room_3": ("195 Room 3", CalendarType.ROOM, [3]),
    "195_room_4": ("195 Room 4", CalendarType.ROOM, [4]),
    "195_room_5": ("195 Room 5", CalendarType.ROOM, [5]),
    "195_room_6": ("195 Room 6", CalendarType.ROOM, [6]),
    "195_suite_a": ("195 Suite A (Rooms 1+2)", CalendarType.SUITE_A, [1, 2]),
    "195_suite_b": ("195 Suite B (Rooms 4+5+6)", CalendarType.SUITE_B, [4, 5, 6]),
    "195vbr": ("195 Vauxhall Bridge Road (Whole House)", CalendarType.WHOLE_HOUSE, [1, 2, 3, 4, 5, 6]),
    "193_room_1": ("193 Room 1", CalendarType.ROOM, [1]),
    "193_room_2": ("193 Room 2", CalendarType.ROOM, [2]),
    "193_room_3": ("193 Room 3", CalendarType.ROOM, [3]),
    "193_room_4": ("193 Room 4", CalendarType.ROOM, [4]),
    "193_room_5": ("193 Room 5", CalendarType.ROOM, [5]),
    "193_room_6": ("193 Room 6", CalendarType.ROOM, [6]),
    "193_suite_a": ("193 Suite A (Rooms 1+2)", CalendarType.SUITE_A, [1, 2]),
    "193_suite_b": ("193 Suite B (Rooms 4+5+6)", CalendarType.SUITE_B, [4, 5, 6]),
    "193vbr": ("193 Vauxhall Bridge Road (Whole House)", CalendarType.WHOLE_HOUSE, [1, 2, 3, 4, 5, 6]),
    "193195vbr": ("193 & 195 Vauxhall Bridge Road (Both Houses)", CalendarType.BOTH_HOUSES, [1, 2, 3, 4, 5, 6]),
}


def build_calendars(house_code: str) -> list[CalendarConfig]:
    """Build calendar configuration for the given house.

    Includes this house's individual room/suite/whole-house calendars
    plus the shared 193195vbr calendar.
    """
    prefix = f"{house_code}_"
    whole_house_id = f"{house_code}vbr"
    both_houses_id = "193195vbr"

    calendars = []
    for cal_id, (name, cal_type, rooms) in _CALENDAR_META.items():
        # Include calendars that belong to this house or the both-houses calendar
        if cal_id.startswith(prefix) or cal_id == whole_house_id or cal_id == both_houses_id:
            calendars.append(CalendarConfig(
                calendar_id=cal_id,
                name=name,
                calendar_type=cal_type,
                ical_url="",
                rooms=rooms,
            ))

    return calendars


def get_slot_for_calendar(calendar_id: str) -> tuple[int, int]:
    """Get the slot numbers for a calendar.

    Returns tuple of (slot_a, slot_b) for back-to-back booking support.
    """
    # Extract the calendar type from the ID
    if calendar_id.endswith("_room_1") or calendar_id == "195_room_1" or calendar_id == "193_room_1":
        return SLOT_ASSIGNMENTS["room_1"]
    elif calendar_id.endswith("_room_2"):
        return SLOT_ASSIGNMENTS["room_2"]
    elif calendar_id.endswith("_room_3"):
        return SLOT_ASSIGNMENTS["room_3"]
    elif calendar_id.endswith("_room_4"):
        return SLOT_ASSIGNMENTS["room_4"]
    elif calendar_id.endswith("_room_5"):
        return SLOT_ASSIGNMENTS["room_5"]
    elif calendar_id.endswith("_room_6"):
        return SLOT_ASSIGNMENTS["room_6"]
    elif calendar_id.endswith("_suite_a"):
        return SLOT_ASSIGNMENTS["suite_a"]
    elif calendar_id.endswith("_suite_b"):
        return SLOT_ASSIGNMENTS["suite_b"]
    elif calendar_id in ("195vbr", "193vbr", "193195vbr"):
        return SLOT_ASSIGNMENTS["whole_home"]
    else:
        raise ValueError(f"Unknown calendar ID: {calendar_id}")


# Global settings instance
settings = Settings()
