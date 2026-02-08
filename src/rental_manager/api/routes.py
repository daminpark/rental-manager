"""API routes for the rental manager."""

from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from rental_manager.config import settings
from rental_manager.core.manager import RentalManager

router = APIRouter()

# Dependency to get the manager instance
_manager: Optional[RentalManager] = None


def get_manager() -> RentalManager:
    if _manager is None:
        raise HTTPException(status_code=500, detail="Manager not initialized")
    return _manager


def set_manager(manager: RentalManager) -> None:
    global _manager
    _manager = manager


# Request/Response models


class MasterCodeRequest(BaseModel):
    code: str


class EmergencyCodeRequest(BaseModel):
    lock_id: int
    code: str


class TimeOverrideRequest(BaseModel):
    booking_id: int
    lock_id: int
    activate_at: Optional[datetime] = None
    deactivate_at: Optional[datetime] = None
    notes: Optional[str] = None


class LockActionRequest(BaseModel):
    action: str  # "lock" or "unlock"


class AutoLockRequest(BaseModel):
    enabled: bool


class VolumeRequest(BaseModel):
    level: str  # "low", "high", or "off"


class ManualCodeRequest(BaseModel):
    slot_number: int
    code: str
    activate_at: Optional[datetime] = None
    deactivate_at: Optional[datetime] = None
    guest_name: Optional[str] = None


class CalendarUrlRequest(BaseModel):
    calendar_id: str
    ical_url: str


# Health and status endpoints


@router.get("/health")
async def health_check(manager: RentalManager = Depends(get_manager)):
    """Check the health of all components."""
    return await manager.health_check()


@router.get("/sync-status")
async def sync_status(manager: RentalManager = Depends(get_manager)):
    """Get the current sync status of all code slots."""
    return await manager.get_sync_status()


@router.get("/info")
async def get_info():
    """Get instance info (house code, version)."""
    return {
        "house_code": settings.house_code,
        "version": "0.1.0",
    }


# Lock endpoints


@router.get("/locks")
async def get_locks(
    manager: RentalManager = Depends(get_manager),
):
    """Get all locks for this house."""
    return await manager.get_locks()


@router.get("/locks/{lock_entity_id}")
async def get_lock(
    lock_entity_id: str,
    manager: RentalManager = Depends(get_manager),
):
    """Get a specific lock by entity ID."""
    locks = await manager.get_locks()
    lock = next((l for l in locks if l["entity_id"] == lock_entity_id), None)
    if not lock:
        raise HTTPException(status_code=404, detail="Lock not found")
    return lock


@router.post("/locks/{lock_entity_id}/action")
async def lock_action(
    lock_entity_id: str,
    request: LockActionRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Lock or unlock a specific lock."""
    if request.action not in ("lock", "unlock"):
        raise HTTPException(status_code=400, detail="Action must be 'lock' or 'unlock'")
    return await manager.lock_action(lock_entity_id, request.action)


@router.post("/locks/{lock_entity_id}/auto-lock")
async def set_auto_lock(
    lock_entity_id: str,
    request: AutoLockRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Enable or disable auto-lock on a lock."""
    return await manager.set_auto_lock(lock_entity_id, request.enabled)


@router.post("/locks/{lock_entity_id}/volume")
async def set_volume(
    lock_entity_id: str,
    request: VolumeRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Set the volume level on a lock."""
    if request.level not in ("low", "high", "off"):
        raise HTTPException(
            status_code=400, detail="Level must be 'low', 'high', or 'off'"
        )
    return await manager.set_volume(lock_entity_id, request.level)


# Master and emergency code endpoints


@router.post("/codes/master")
async def set_master_code(
    request: MasterCodeRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Set the master code on all locks."""
    if len(request.code) != 4 or not request.code.isdigit():
        raise HTTPException(status_code=400, detail="Code must be 4 digits")
    return await manager.set_master_code(request.code)


@router.get("/codes/emergency")
async def get_emergency_codes(
    manager: RentalManager = Depends(get_manager),
):
    """Get all emergency codes for all locks."""
    return await manager.get_emergency_codes()


@router.post("/codes/emergency/randomize")
async def randomize_emergency_codes(
    manager: RentalManager = Depends(get_manager),
):
    """Randomize emergency codes — each lock gets a unique random code."""
    return await manager.randomize_emergency_codes()


@router.post("/codes/emergency")
async def set_emergency_code(
    request: EmergencyCodeRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Set emergency code on a specific lock."""
    if len(request.code) != 4 or not request.code.isdigit():
        raise HTTPException(status_code=400, detail="Code must be 4 digits")
    return await manager.set_emergency_code(request.lock_id, request.code)


# Booking endpoints


@router.get("/bookings")
async def get_bookings(
    calendar_id: Optional[str] = Query(None, description="Filter by calendar ID"),
    from_date: Optional[date] = Query(None, description="Filter from date"),
    to_date: Optional[date] = Query(None, description="Filter to date"),
    manager: RentalManager = Depends(get_manager),
):
    """Get bookings, optionally filtered."""
    return await manager.get_bookings(calendar_id, from_date, to_date)


@router.get("/bookings/{booking_id}/lock-times")
async def get_booking_lock_times(
    booking_id: int,
    manager: RentalManager = Depends(get_manager),
):
    """Get the computed activation/deactivation times for a booking on each lock.

    Returns the default times (based on lock type + stagger) and any existing overrides.
    """
    return await manager.get_booking_lock_times(booking_id)


@router.post("/bookings/{booking_id}/time-override")
async def set_booking_time_override(
    booking_id: int,
    request: TimeOverrideRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Set a time override for a booking on a specific lock.

    This allows manual adjustment of when codes activate/deactivate,
    useful for early check-in or late checkout requests.
    """
    if request.booking_id != booking_id:
        raise HTTPException(
            status_code=400, detail="Booking ID in path and body must match"
        )
    return await manager.set_time_override(
        booking_id=request.booking_id,
        lock_id=request.lock_id,
        activate_at=request.activate_at,
        deactivate_at=request.deactivate_at,
        notes=request.notes,
    )


# Calendar endpoints


@router.get("/calendars")
async def get_calendars(manager: RentalManager = Depends(get_manager)):
    """Get all calendars."""
    from rental_manager.db.database import get_session_context
    from rental_manager.db.models import Calendar
    from sqlalchemy import select

    async with get_session_context() as session:
        result = await session.execute(select(Calendar))
        calendars = result.scalars().all()
        return [
            {
                "id": c.id,
                "calendar_id": c.calendar_id,
                "name": c.name,
                "calendar_type": c.calendar_type,
                "ical_url": c.ical_url,
                "last_fetched": c.last_fetched.isoformat() if c.last_fetched else None,
                "last_fetch_error": c.last_fetch_error,
            }
            for c in calendars
        ]


@router.put("/calendars/{calendar_id}/url")
async def update_calendar_url(
    calendar_id: str,
    request: CalendarUrlRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Update the iCal URL for a calendar."""
    from rental_manager.db.database import get_session_context
    from rental_manager.db.models import Calendar
    from sqlalchemy import select

    async with get_session_context() as session:
        result = await session.execute(
            select(Calendar).where(Calendar.calendar_id == calendar_id)
        )
        calendar = result.scalar_one_or_none()
        if not calendar:
            raise HTTPException(status_code=404, detail="Calendar not found")

        calendar.ical_url = request.ical_url
        await session.commit()

        return {
            "calendar_id": calendar.calendar_id,
            "ical_url": calendar.ical_url,
        }


@router.post("/calendars/refresh")
async def refresh_calendars(manager: RentalManager = Depends(get_manager)):
    """Manually trigger a calendar refresh."""
    await manager._poll_calendars()
    return {"status": "refreshed"}


# Audit log endpoint


@router.get("/audit-log")
async def get_audit_log(
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    lock_id: Optional[int] = None,
    action: Optional[str] = None,
    manager: RentalManager = Depends(get_manager),
):
    """Get the audit log."""
    from rental_manager.db.database import get_session_context
    from rental_manager.db.models import AuditLog
    from sqlalchemy import select

    async with get_session_context() as session:
        query = select(AuditLog).order_by(AuditLog.timestamp.desc())

        if lock_id:
            query = query.where(AuditLog.lock_id == lock_id)
        if action:
            query = query.where(AuditLog.action == action)

        query = query.offset(offset).limit(limit)
        result = await session.execute(query)
        logs = result.scalars().all()

        return [
            {
                "id": log.id,
                "timestamp": log.timestamp.isoformat(),
                "action": log.action,
                "lock_id": log.lock_id,
                "booking_id": log.booking_id,
                "slot_number": log.slot_number,
                "code": log.code,
                "details": log.details,
                "success": log.success,
                "error_message": log.error_message,
            }
            for log in logs
        ]
