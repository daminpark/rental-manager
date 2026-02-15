"""Code generation and slot allocation."""

import re
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import Optional

from rental_manager.config import (
    DEFAULT_TIMINGS,
    EMERGENCY_CODE_SLOT,
    MASTER_CODE_SLOT,
    SLOT_ASSIGNMENTS,
    CalendarType,
    LockType,
    get_slot_for_calendar,
)


def generate_code_from_phone(phone: Optional[str]) -> Optional[str]:
    """Generate a 4-digit code from the last 4 digits of a phone number.

    Args:
        phone: Phone number string (may contain non-digit characters)

    Returns:
        4-digit code string, or None if phone is invalid
    """
    if not phone:
        return None

    # Extract only digits
    digits = re.sub(r"[^\d]", "", phone)

    if len(digits) < 4:
        return None

    # Return last 4 digits
    return digits[-4:]


@dataclass
class SlotAllocation:
    """Represents the allocation of a code to a slot."""

    lock_entity_id: str
    slot_number: int
    code: str
    activate_at: datetime
    deactivate_at: datetime
    calendar_id: str
    booking_uid: str
    guest_name: str


@dataclass
class BookingCodeInfo:
    """Information needed to generate codes for a booking."""

    calendar_id: str
    booking_uid: str
    guest_name: str
    phone: Optional[str]
    check_in_date: date
    check_out_date: date
    is_blocked: bool


class SlotAllocator:
    """Manages slot allocation for locks."""

    def __init__(self):
        # Track which slots are in use: {(lock_entity_id, slot_number): booking_uid}
        self._slot_usage: dict[tuple[str, int], str] = {}

    def get_calendar_slot_range(self, calendar_id: str) -> tuple[int, int]:
        """Get the slot range for a calendar.

        Returns tuple of (slot_a, slot_b) for back-to-back booking support.
        """
        return get_slot_for_calendar(calendar_id)

    def allocate_slot_for_booking(
        self,
        lock_entity_id: str,
        calendar_id: str,
        booking_uid: str,
        existing_booking_uids: set[str],
    ) -> int:
        """Allocate a slot for a booking on a lock.

        Args:
            lock_entity_id: The lock entity ID
            calendar_id: The calendar ID
            booking_uid: The booking UID
            existing_booking_uids: UIDs of bookings currently active on this calendar

        Returns:
            The slot number to use

        Raises:
            ValueError: If no slots are available
        """
        slot_a, slot_b = self.get_calendar_slot_range(calendar_id)

        # Check if this booking already has a slot
        for slot in (slot_a, slot_b):
            key = (lock_entity_id, slot)
            if self._slot_usage.get(key) == booking_uid:
                return slot

        # Check if slot_a is free or has an expired booking
        key_a = (lock_entity_id, slot_a)
        uid_a = self._slot_usage.get(key_a)
        if uid_a is None or uid_a not in existing_booking_uids:
            self._slot_usage[key_a] = booking_uid
            return slot_a

        # Check if slot_b is free or has an expired booking
        key_b = (lock_entity_id, slot_b)
        uid_b = self._slot_usage.get(key_b)
        if uid_b is None or uid_b not in existing_booking_uids:
            self._slot_usage[key_b] = booking_uid
            return slot_b

        # Both slots are in use by active bookings
        raise ValueError(
            f"No slots available for calendar {calendar_id} on lock {lock_entity_id}. "
            f"Slot {slot_a} used by {uid_a}, slot {slot_b} used by {uid_b}."
        )

    def release_slot(self, lock_entity_id: str, slot_number: int) -> None:
        """Release a slot, making it available for reuse."""
        key = (lock_entity_id, slot_number)
        if key in self._slot_usage:
            del self._slot_usage[key]

    def clear_all(self) -> None:
        """Clear all slot allocations."""
        self._slot_usage.clear()


def calculate_code_times(
    lock_type: LockType,
    check_in_date: date,
    check_out_date: date,
    stagger_minutes: int = 0,
    override_activate: Optional[datetime] = None,
    override_deactivate: Optional[datetime] = None,
) -> tuple[datetime, datetime]:
    """Calculate activation and deactivation times for a code.

    Args:
        lock_type: Type of lock (determines default times)
        check_in_date: Guest check-in date
        check_out_date: Guest check-out date
        stagger_minutes: Minutes to stagger the times
        override_activate: Manual override for activation time
        override_deactivate: Manual override for deactivation time

    Returns:
        Tuple of (activate_at, deactivate_at) datetimes
    """
    timing = DEFAULT_TIMINGS[lock_type]

    # Calculate base activation time
    if override_activate:
        activate_at = override_activate
    else:
        activate_at = datetime.combine(check_in_date, timing.activate)
        activate_at += timedelta(minutes=stagger_minutes)

    # Calculate base deactivation time
    if override_deactivate:
        deactivate_at = override_deactivate
    else:
        # Deactivation is on checkout date
        deactivate_at = datetime.combine(check_out_date, timing.deactivate)
        deactivate_at += timedelta(minutes=stagger_minutes)

    return activate_at, deactivate_at


def is_whole_home_calendar(calendar_id: str) -> bool:
    """Check if a calendar is a whole-home or both-houses calendar."""
    return calendar_id in ("195vbr", "193vbr", "193195vbr")


def calendars_share_slots(calendar_id_a: str, calendar_id_b: str) -> bool:
    """Check if two calendars share the same slot range.

    This is true for whole-home calendars within the same house and 193195vbr.
    """
    # Whole home calendars share slots 18-19
    whole_home_calendars = {"195vbr", "193vbr", "193195vbr"}

    # Check if both are whole-home type
    if calendar_id_a in whole_home_calendars and calendar_id_b in whole_home_calendars:
        # 195vbr and 193195vbr share slots (on 195 locks)
        # 193vbr and 193195vbr share slots (on 193 locks)
        # But 195vbr and 193vbr don't affect each other (different houses)
        if calendar_id_a == "193195vbr" or calendar_id_b == "193195vbr":
            return True
        # Same house whole-home calendars would share, but there's only one per house
        return False

    return False
