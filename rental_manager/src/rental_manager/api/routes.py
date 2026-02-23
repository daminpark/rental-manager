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
    level: str  # "silent", "low", or "high"


class SlotCodeRequest(BaseModel):
    code: str


class BookingCodeRequest(BaseModel):
    code: str


# Health and status endpoints


@router.get("/health")
async def health_check(manager: RentalManager = Depends(get_manager)):
    """Check the health of all components."""
    return await manager.health_check()


@router.get("/sync-status")
async def sync_status(manager: RentalManager = Depends(get_manager)):
    """Get the current sync status of all code slots."""
    return await manager.get_sync_status()


@router.post("/sync-status/retry/{lock_entity_id}/{slot_number}")
async def retry_failed_slot(
    lock_entity_id: str,
    slot_number: int,
    manager: RentalManager = Depends(get_manager),
):
    """Retry a failed sync on a specific slot."""
    try:
        return await manager.retry_failed_slot(lock_entity_id, slot_number)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/sync-status/retry-all")
async def retry_all_failed(manager: RentalManager = Depends(get_manager)):
    """Retry all failed sync slots and failed ops."""
    try:
        slot_results = await manager.retry_all_failed()
        op_results = await manager.retry_all_failed_ops()
        return {
            "retried": slot_results["retried"] + op_results["retried"],
            "results": slot_results["results"] + op_results["results"],
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/sync-status/resync")
async def resync_all_codes(manager: RentalManager = Depends(get_manager)):
    """Re-sync all lock codes: set active codes, clear inactive slots."""
    return await manager.resync_all_codes()


@router.post("/sync-status/retry-op/{op_id}")
async def retry_failed_op(
    op_id: int,
    manager: RentalManager = Depends(get_manager),
):
    """Retry a failed non-code operation (auto-lock, lock, unlock)."""
    try:
        return await manager.retry_failed_op(op_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/sync-status/dismiss-op/{op_id}")
async def dismiss_failed_op(
    op_id: int,
    manager: RentalManager = Depends(get_manager),
):
    """Dismiss a failed operation without retrying."""
    try:
        return manager.dismiss_failed_op(op_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


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
    if request.level not in ("silent", "low", "high"):
        raise HTTPException(
            status_code=400, detail="Level must be 'silent', 'low', or 'high'"
        )
    return await manager.set_volume(lock_entity_id, request.level)


@router.post("/locks/volume-all")
async def set_volume_all(
    request: VolumeRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Set the volume level on ALL locks."""
    if request.level not in ("silent", "low", "high"):
        raise HTTPException(
            status_code=400, detail="Level must be 'silent', 'low', or 'high'"
        )
    return await manager.set_volume_all(request.level)


@router.post("/locks/auto-lock-all")
async def set_auto_lock_all(
    request: AutoLockRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Set auto-lock on all internal locks (excludes front/back doors)."""
    return await manager.set_auto_lock_all(request.enabled)


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


# Slot management endpoints


@router.post("/locks/{lock_entity_id}/clear-all-codes")
async def clear_all_codes(
    lock_entity_id: str,
    manager: RentalManager = Depends(get_manager),
):
    """Clear ALL code slots (1-20) on a lock. For setup use."""
    return await manager.clear_all_codes(lock_entity_id)


@router.post("/locks/{lock_entity_id}/slots/{slot_number}/set")
async def set_slot_code(
    lock_entity_id: str,
    slot_number: int,
    request: SlotCodeRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Set a code on a specific slot."""
    if slot_number < 1 or slot_number > 20:
        raise HTTPException(status_code=400, detail="Slot must be 1-20")
    if not request.code or not request.code.isdigit() or len(request.code) < 4:
        raise HTTPException(status_code=400, detail="Code must be at least 4 digits")
    return await manager.set_slot_code(lock_entity_id, slot_number, request.code)


@router.post("/locks/{lock_entity_id}/slots/{slot_number}/clear")
async def clear_slot_code(
    lock_entity_id: str,
    slot_number: int,
    manager: RentalManager = Depends(get_manager),
):
    """Clear a specific code slot."""
    if slot_number < 1 or slot_number > 20:
        raise HTTPException(status_code=400, detail="Slot must be 1-20")
    return await manager.clear_slot_code(lock_entity_id, slot_number)


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


@router.post("/bookings/{booking_id}/disable-code")
async def disable_booking_code(
    booking_id: int,
    manager: RentalManager = Depends(get_manager),
):
    """Disable (clear) the guest code for a booking across all assigned locks."""
    try:
        return await manager.disable_booking_code(booking_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/bookings/{booking_id}/enable-code")
async def enable_booking_code(
    booking_id: int,
    manager: RentalManager = Depends(get_manager),
):
    """Re-enable the guest code for a previously disabled booking."""
    try:
        return await manager.enable_booking_code(booking_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


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
                "ha_entity_id": c.ha_entity_id,
                "last_fetched": c.last_fetched.isoformat() if c.last_fetched else None,
                "last_fetch_error": c.last_fetch_error,
            }
            for c in calendars
        ]


@router.post("/calendars/refresh")
async def refresh_calendars(manager: RentalManager = Depends(get_manager)):
    """Manually trigger a calendar refresh."""
    await manager._poll_calendars()
    return {"status": "refreshed"}


@router.post("/bookings/{booking_id}/set-code")
async def set_booking_code(
    booking_id: int,
    request: BookingCodeRequest,
    manager: RentalManager = Depends(get_manager),
):
    """Manually set (override) the PIN code for a booking."""
    if not request.code or not request.code.isdigit() or len(request.code) < 4:
        raise HTTPException(status_code=400, detail="Code must be at least 4 digits")
    try:
        return await manager.set_booking_code(booking_id, request.code)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/bookings/{booking_id}/recode")
async def recode_booking(
    booking_id: int,
    manager: RentalManager = Depends(get_manager),
):
    """Re-send codes to all locks for this booking (if within active window)."""
    try:
        return await manager.recode_booking(booking_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# Unlock history endpoints


@router.get("/unlock-history")
async def get_unlock_history(
    lock_entity_id: Optional[str] = Query(None, description="Filter by lock entity ID"),
    booking_id: Optional[int] = Query(None, description="Filter by booking ID"),
    from_date: Optional[date] = Query(None, description="Filter from date"),
    to_date: Optional[date] = Query(None, description="Filter to date"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    manager: RentalManager = Depends(get_manager),
):
    """Get unlock event history across all locks."""
    return await manager.get_unlock_history(
        lock_entity_id=lock_entity_id,
        booking_id=booking_id,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
        offset=offset,
    )


@router.get("/locks/{lock_entity_id}/unlock-history")
async def get_lock_unlock_history(
    lock_entity_id: str,
    from_date: Optional[date] = Query(None, description="Filter from date"),
    to_date: Optional[date] = Query(None, description="Filter to date"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    manager: RentalManager = Depends(get_manager),
):
    """Get unlock event history for a specific lock."""
    return await manager.get_unlock_history(
        lock_entity_id=lock_entity_id,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
        offset=offset,
    )


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
    from rental_manager.db.models import AuditLog, Booking, Calendar, Lock
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    async with get_session_context() as session:
        query = select(AuditLog).order_by(AuditLog.timestamp.desc())

        if lock_id:
            query = query.where(AuditLog.lock_id == lock_id)
        if action:
            query = query.where(AuditLog.action == action)

        query = query.offset(offset).limit(limit)
        result = await session.execute(query)
        logs = result.scalars().all()

        # Build lock name lookup
        lock_ids = {log.lock_id for log in logs if log.lock_id}
        lock_names = {}
        if lock_ids:
            lock_result = await session.execute(
                select(Lock.id, Lock.name).where(Lock.id.in_(lock_ids))
            )
            lock_names = {row.id: row.name for row in lock_result}

        # Build booking info lookup
        booking_ids = {log.booking_id for log in logs if log.booking_id}
        booking_info: dict = {}
        if booking_ids:
            booking_result = await session.execute(
                select(Booking)
                .options(selectinload(Booking.calendar))
                .where(Booking.id.in_(booking_ids))
            )
            for b in booking_result.scalars():
                booking_info[b.id] = {
                    "guest_name": b.guest_name,
                    "calendar_name": b.calendar.name if b.calendar else None,
                    "check_in": b.check_in_date.isoformat(),
                    "check_out": b.check_out_date.isoformat(),
                }

        return [
            {
                "id": log.id,
                "timestamp": log.timestamp.isoformat(),
                "action": log.action,
                "lock_id": log.lock_id,
                "lock_name": lock_names.get(log.lock_id),
                "booking_id": log.booking_id,
                "booking": booking_info.get(log.booking_id),
                "slot_number": log.slot_number,
                "code": log.code,
                "details": log.details,
                "success": log.success,
                "error_message": log.error_message,
                "batch_id": log.batch_id,
            }
            for log in logs
        ]


@router.get("/logs")
async def get_logs(
    lines: int = Query(200, le=2000),
    search: Optional[str] = None,
):
    """Read the persistent log file. Returns the last N lines."""
    from pathlib import Path

    log_file = Path("/data/logs/rental_manager.log")
    if not log_file.exists():
        return {"lines": [], "total": 0}

    all_lines = log_file.read_text().splitlines()
    if search:
        all_lines = [l for l in all_lines if search.lower() in l.lower()]

    return {"lines": all_lines[-lines:], "total": len(all_lines)}
