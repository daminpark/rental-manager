"""Database models for rental manager."""

from datetime import datetime, date, time
from enum import Enum
from typing import Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Enum as SQLEnum,
    ForeignKey,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class CodeSyncState(str, Enum):
    """State of code synchronization for a slot."""

    IDLE = "idle"
    SETTING = "setting"
    CONFIRMING = "confirming"
    ACTIVE = "active"
    CLEARING = "clearing"
    RETRYING = "retrying"
    FAILED = "failed"


class House(Base):
    """A house (193 or 195)."""

    __tablename__ = "houses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(3), unique=True)  # "193" or "195"
    name: Mapped[str] = mapped_column(String(100))

    # Relationships
    locks: Mapped[list["Lock"]] = relationship("Lock", back_populates="house")

    def __repr__(self) -> str:
        return f"<House {self.code}>"


class Lock(Base):
    """A physical lock in a house."""

    __tablename__ = "locks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    house_id: Mapped[int] = mapped_column(ForeignKey("houses.id"))
    entity_id: Mapped[str] = mapped_column(String(100))  # e.g., "lock.195_front_lock"
    name: Mapped[str] = mapped_column(String(100))  # e.g., "195 Front Door"
    lock_type: Mapped[str] = mapped_column(String(20))  # room, bathroom, kitchen, front, storage
    stagger_minutes: Mapped[int] = mapped_column(Integer, default=0)

    # Master and emergency codes (stored here for quick access)
    master_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    emergency_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    # Relationships
    house: Mapped["House"] = relationship("House", back_populates="locks")
    code_slots: Mapped[list["CodeSlot"]] = relationship("CodeSlot", back_populates="lock")
    lock_calendars: Mapped[list["LockCalendar"]] = relationship(
        "LockCalendar", back_populates="lock"
    )

    __table_args__ = (UniqueConstraint("house_id", "entity_id", name="uq_lock_entity"),)

    def __repr__(self) -> str:
        return f"<Lock {self.entity_id}>"


class Calendar(Base):
    """A calendar (iCal feed) for a listing."""

    __tablename__ = "calendars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    calendar_id: Mapped[str] = mapped_column(String(50), unique=True)  # e.g., "195_room_1"
    name: Mapped[str] = mapped_column(String(100))
    calendar_type: Mapped[str] = mapped_column(String(20))  # room, suite_a, suite_b, whole_house, both_houses
    ical_url: Mapped[str] = mapped_column(String(500))
    last_fetched: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_fetch_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    bookings: Mapped[list["Booking"]] = relationship("Booking", back_populates="calendar")
    lock_calendars: Mapped[list["LockCalendar"]] = relationship(
        "LockCalendar", back_populates="calendar"
    )

    def __repr__(self) -> str:
        return f"<Calendar {self.calendar_id}>"


class LockCalendar(Base):
    """Association between locks and calendars (which calendars grant access to which locks)."""

    __tablename__ = "lock_calendars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lock_id: Mapped[int] = mapped_column(ForeignKey("locks.id"))
    calendar_id: Mapped[int] = mapped_column(ForeignKey("calendars.id"))

    # Relationships
    lock: Mapped["Lock"] = relationship("Lock", back_populates="lock_calendars")
    calendar: Mapped["Calendar"] = relationship("Calendar", back_populates="lock_calendars")

    __table_args__ = (UniqueConstraint("lock_id", "calendar_id", name="uq_lock_calendar"),)


class Booking(Base):
    """A booking from a calendar."""

    __tablename__ = "bookings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    calendar_id: Mapped[int] = mapped_column(ForeignKey("calendars.id"))
    uid: Mapped[str] = mapped_column(String(255))  # iCal UID
    guest_name: Mapped[str] = mapped_column(String(255))
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    channel: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # Airbnb, etc.
    reservation_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    check_in_date: Mapped[date] = mapped_column(Date)
    check_out_date: Mapped[date] = mapped_column(Date)
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False)  # "Blocked" events
    locked_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    code_locked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    calendar: Mapped["Calendar"] = relationship("Calendar", back_populates="bookings")
    code_assignments: Mapped[list["CodeAssignment"]] = relationship(
        "CodeAssignment", back_populates="booking"
    )
    time_overrides: Mapped[list["TimeOverride"]] = relationship(
        "TimeOverride", back_populates="booking"
    )

    __table_args__ = (
        UniqueConstraint(
            "calendar_id", "guest_name", "check_in_date", "check_out_date",
            name="uq_booking_content",
        ),
    )

    def __repr__(self) -> str:
        return f"<Booking {self.guest_name} {self.check_in_date}-{self.check_out_date}>"


class CodeSlot(Base):
    """A code slot on a lock."""

    __tablename__ = "code_slots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lock_id: Mapped[int] = mapped_column(ForeignKey("locks.id"))
    slot_number: Mapped[int] = mapped_column(Integer)  # 1-20
    current_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    sync_state: Mapped[str] = mapped_column(
        String(20), default=CodeSyncState.IDLE.value
    )
    sync_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    lock: Mapped["Lock"] = relationship("Lock", back_populates="code_slots")
    assignments: Mapped[list["CodeAssignment"]] = relationship(
        "CodeAssignment", back_populates="code_slot"
    )

    __table_args__ = (UniqueConstraint("lock_id", "slot_number", name="uq_lock_slot"),)

    def __repr__(self) -> str:
        return f"<CodeSlot {self.lock_id}:{self.slot_number}>"


class CodeAssignment(Base):
    """Assignment of a code to a slot for a booking."""

    __tablename__ = "code_assignments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code_slot_id: Mapped[int] = mapped_column(ForeignKey("code_slots.id"))
    booking_id: Mapped[int] = mapped_column(ForeignKey("bookings.id"))
    code: Mapped[str] = mapped_column(String(10))
    activate_at: Mapped[datetime] = mapped_column(DateTime)
    deactivate_at: Mapped[datetime] = mapped_column(DateTime)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    code_slot: Mapped["CodeSlot"] = relationship("CodeSlot", back_populates="assignments")
    booking: Mapped["Booking"] = relationship("Booking", back_populates="code_assignments")

    def __repr__(self) -> str:
        return f"<CodeAssignment slot={self.code_slot_id} booking={self.booking_id}>"


class TimeOverride(Base):
    """Manual override of activation/deactivation times for a booking on a specific lock."""

    __tablename__ = "time_overrides"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    booking_id: Mapped[int] = mapped_column(ForeignKey("bookings.id"))
    lock_id: Mapped[int] = mapped_column(ForeignKey("locks.id"))
    activate_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    deactivate_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    booking: Mapped["Booking"] = relationship("Booking", back_populates="time_overrides")

    __table_args__ = (UniqueConstraint("booking_id", "lock_id", name="uq_override_booking_lock"),)

    def __repr__(self) -> str:
        return f"<TimeOverride booking={self.booking_id} lock={self.lock_id}>"


class AuditLog(Base):
    """Audit log for tracking all code changes and actions."""

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    action: Mapped[str] = mapped_column(String(50))  # code_set, code_cleared, lock_unlocked, etc.
    lock_id: Mapped[Optional[int]] = mapped_column(ForeignKey("locks.id"), nullable=True)
    booking_id: Mapped[Optional[int]] = mapped_column(ForeignKey("bookings.id"), nullable=True)
    slot_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<AuditLog {self.timestamp} {self.action}>"


class EmergencyCodeShare(Base):
    """Track when emergency codes are shared with guests."""

    __tablename__ = "emergency_code_shares"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lock_id: Mapped[int] = mapped_column(ForeignKey("locks.id"))
    booking_id: Mapped[Optional[int]] = mapped_column(ForeignKey("bookings.id"), nullable=True)
    shared_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    shared_to: Mapped[str] = mapped_column(String(255))  # Guest name or description
    code_rotated: Mapped[bool] = mapped_column(Boolean, default=False)
    rotated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<EmergencyCodeShare lock={self.lock_id} shared_to={self.shared_to}>"
