"""Main rental manager that orchestrates all components."""

import asyncio
import random
import uuid
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Optional
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, joinedload

from rental_manager.config import (
    LockType,
    Settings,
    build_locks,
    build_calendars,
    get_slot_for_calendar,
    DEFAULT_TIMINGS,
    MASTER_CODE_SLOT,
    EMERGENCY_CODE_SLOT,
)
from rental_manager.core.code_manager import (
    SlotAllocator,
    calculate_code_times,
    generate_code_from_phone,
)
from rental_manager.core.ical_parser import ParsedBooking
from rental_manager.core.sync_manager import SyncManager, SyncState
from rental_manager.hosttools.client import HostToolsClient, parse_hosttools_reservations
from rental_manager.db.database import get_session_context
from rental_manager.db.models import (
    AuditLog,
    Booking,
    Calendar,
    CodeAssignment,
    CodeSlot,
    CodeSyncState,
    House as HouseModel,
    Lock,
    LockCalendar,
    TimeOverride,
    UnlockEvent,
)
from rental_manager.ha.client import HomeAssistantClient
from rental_manager.ha.event_listener import HAEventListener
from rental_manager.scheduler.scheduler import CodeScheduler, CodeScheduleEntry

logger = logging.getLogger(__name__)


class RentalManager:
    """Main rental manager coordinating all operations."""

    # Minimum gap between consecutive Z-Wave commands (seconds)
    ZWAVE_CMD_DELAY = 2

    def __init__(self, settings: Settings):
        self.settings = settings
        self._ha_client = HomeAssistantClient(settings.ha_url, settings.ha_token)
        self._hosttools_client: Optional[HostToolsClient] = None
        if settings.hosttools_auth_token:
            self._hosttools_client = HostToolsClient(settings.hosttools_auth_token)
            logger.info("HostTools API client initialized")
        self._slot_allocator = SlotAllocator()
        self._scheduler: Optional[CodeScheduler] = None
        self._sync_manager: Optional[SyncManager] = None
        self._running = False
        self._polling = False
        # Serializes all Z-Wave commands so only one is in flight at a time
        self._zwave_lock = asyncio.Lock()
        # Serializes code activations to prevent duplicate codes on shared locks
        self._activation_lock = asyncio.Lock()
        # Track failed non-code operations (auto-lock, lock/unlock)
        # List of dicts: {id, lock_entity_id, lock_name, action, error, retry_count, reason, failed_at}
        self._failed_ops: list[dict] = []
        self._failed_ops_counter = 0
        self._event_listener = HAEventListener(
            ha_url=settings.ha_url,
            ha_token=settings.ha_token,
            on_lock_event=self._on_ws_lock_event,
        )

    async def initialize(self) -> None:
        """Initialize the manager and all components."""
        logger.info("Initializing rental manager for house %s...", self.settings.house_code)

        # Initialize sync manager
        self._sync_manager = SyncManager(
            set_code=self._ha_set_code,
            clear_code=self._ha_clear_code,
            ping_lock=self._ha_ping_lock,
            on_sync_failed=self._on_sync_failed,
            timeout_seconds=self.settings.code_sync_timeout_seconds,
            max_retries=self.settings.code_sync_max_retries,
        )

        # Initialize scheduler
        self._scheduler = CodeScheduler(
            on_activate=self._on_code_activate,
            on_deactivate=self._on_code_deactivate,
            on_calendar_poll=self._poll_calendars,
            on_code_finalize=self._on_code_finalize,
            on_emergency_rotate=self._on_emergency_rotate,
            on_whole_house_checkin=self._whole_house_checkin,
            on_whole_house_checkout=self._whole_house_checkout,
            poll_interval_seconds=self.settings.calendar_poll_interval,
        )

        # Initialize database with default configuration if needed
        await self._ensure_default_config()

        logger.info("Rental manager initialized")

    async def start(self) -> None:
        """Start the manager."""
        if self._running:
            return

        self._running = True

        if self._sync_manager:
            self._sync_manager.start()

        if self._scheduler:
            self._scheduler.start()

        # Re-hydrate scheduler from existing DB assignments (survives restart)
        await self._rehydrate_scheduler()

        # Start event listener for Z-Wave lock events
        await self._event_listener.start()

        # Initial calendar poll
        await self._poll_calendars()

        logger.info("Rental manager started")

    async def _rehydrate_scheduler(self) -> None:
        """Re-load pending code assignments from DB into the scheduler.

        APScheduler DateTrigger jobs are in-memory only and lost on restart.
        This reads all code assignments whose deactivate_at is still in the
        future and re-schedules them so activations/deactivations fire on time.

        CodeSlot.current_code is NOT modified here — it persists across
        restarts and reflects what is physically on the Z-Wave lock.
        """
        if not self._scheduler:
            return

        now = datetime.now()
        scheduled_count = 0
        deactivate_only_count = 0

        async with get_session_context() as session:
            # Get all assignments that haven't fully expired yet
            result = await session.execute(
                select(CodeAssignment)
                .options(
                    joinedload(CodeAssignment.code_slot).joinedload(CodeSlot.lock),
                    joinedload(CodeAssignment.booking).joinedload(Booking.calendar),
                )
                .where(CodeAssignment.deactivate_at > now)
            )
            assignments = result.unique().scalars().all()

            missed_count = 0

            for assignment in assignments:
                booking = assignment.booking
                if not booking or not booking.calendar:
                    continue
                if booking.is_blocked or booking.code_disabled:
                    continue

                code = assignment.code
                if not code:
                    logger.debug(
                        "Skipping assignment with no code: booking=%s lock=%s slot=%d",
                        booking.guest_name, assignment.code_slot.lock.entity_id,
                        assignment.code_slot.slot_number,
                    )
                    continue

                lock = assignment.code_slot.lock
                slot = assignment.code_slot
                slot_number = slot.slot_number

                if assignment.activate_at <= now:
                    # Activation time has passed. Check if the code was actually
                    # pushed to the lock — if not, it was missed (e.g. addon
                    # restarted after activation time).
                    code_on_lock = (
                        slot.current_code == code
                        and slot.sync_state == CodeSyncState.ACTIVE.value
                    )
                    if code_on_lock:
                        # Code is on the lock — only schedule deactivation.
                        self._scheduler.schedule_deactivation_only(
                            lock_entity_id=lock.entity_id,
                            slot_number=slot_number,
                            booking_uid=booking.uid,
                            deactivate_at=assignment.deactivate_at,
                        )
                        deactivate_only_count += 1
                    else:
                        # Missed activation — schedule immediate catch-up.
                        logger.warning(
                            "Missed activation: %s slot %d for %s — scheduling catch-up",
                            lock.entity_id, slot_number, booking.guest_name,
                        )
                        entry = CodeScheduleEntry(
                            lock_entity_id=lock.entity_id,
                            slot_number=slot_number,
                            code=code,
                            activate_at=now,  # Activate ASAP
                            deactivate_at=assignment.deactivate_at,
                            booking_uid=booking.uid,
                            calendar_id=booking.calendar.calendar_id,
                            guest_name=booking.guest_name,
                        )
                        self._scheduler.schedule_code(entry)
                        missed_count += 1
                else:
                    # Activation is still in the future — schedule both.
                    entry = CodeScheduleEntry(
                        lock_entity_id=lock.entity_id,
                        slot_number=slot_number,
                        code=code,
                        activate_at=assignment.activate_at,
                        deactivate_at=assignment.deactivate_at,
                        booking_uid=booking.uid,
                        calendar_id=booking.calendar.calendar_id,
                        guest_name=booking.guest_name,
                    )
                    self._scheduler.schedule_code(entry)
                    scheduled_count += 1

        logger.info(
            f"Re-hydrated scheduler: {scheduled_count} future activations, "
            f"{deactivate_only_count} deactivations-only (already active), "
            f"{missed_count} missed activations (catch-up)"
        )

    async def stop(self) -> None:
        """Stop the manager."""
        self._running = False

        if self._scheduler:
            self._scheduler.stop()

        if self._sync_manager:
            self._sync_manager.stop()

        await self._event_listener.stop()
        await self._ha_client.close()
        if self._hosttools_client:
            await self._hosttools_client.close()

        logger.info("Rental manager stopped")

    async def _ensure_default_config(self) -> None:
        """Ensure default configuration exists in database."""
        async with get_session_context() as session:
            # Check if houses exist
            result = await session.execute(select(HouseModel))
            houses = result.scalars().all()

            if not houses:
                await self._create_default_config(session)

    def _urls_file(self) -> Path:
        """Path to the persistent calendar URLs file."""
        db_url = self.settings.database_url
        # Extract directory from DB path (e.g. "sqlite+aiosqlite:///data/x.db" -> "/data")
        if ":///" in db_url:
            db_path = db_url.split(":///", 1)[1]
            return Path(db_path).parent / "calendar_urls.json"
        return Path("calendar_urls.json")

    def _save_calendar_urls(self, urls: dict[str, str]) -> None:
        """Save calendar URLs to persistent file."""
        import json
        path = self._urls_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(urls, indent=2))
        logger.info("Saved %d calendar URLs to %s", len(urls), path)

    def _load_calendar_urls(self) -> dict[str, str]:
        """Load calendar URLs from persistent file."""
        import json
        path = self._urls_file()
        if path.exists():
            urls = json.loads(path.read_text())
            logger.info("Loaded %d calendar URLs from %s", len(urls), path)
            return urls
        return {}

    async def _create_default_config(self, session: AsyncSession) -> None:
        """Create default configuration in database."""
        house_code = self.settings.house_code
        logger.info("Creating default configuration for house %s...", house_code)

        # Load saved URLs from previous install (survives DB wipe)
        saved_urls = self._load_calendar_urls()

        # Create house
        house = HouseModel(
            code=house_code,
            name=f"{house_code} Vauxhall Bridge Road",
        )
        session.add(house)
        await session.flush()

        # Create calendars
        calendar_configs = build_calendars(house_code)
        calendar_map: dict[str, Calendar] = {}

        for cal_config in calendar_configs:
            cal = Calendar(
                calendar_id=cal_config.calendar_id,
                name=cal_config.name,
                calendar_type=cal_config.calendar_type.value,
                ical_url=saved_urls.get(cal_config.calendar_id, cal_config.ical_url),
                ha_entity_id=cal_config.ha_entity_id or None,
                hosttools_listing_id=cal_config.hosttools_listing_id or None,
            )
            session.add(cal)
            calendar_map[cal_config.calendar_id] = cal

        await session.flush()

        # Create locks
        lock_configs = build_locks(house_code)
        for lock_config in lock_configs:
            lock = Lock(
                house_id=house.id,
                entity_id=lock_config.entity_id,
                name=lock_config.entity_id.replace("lock.", "").replace("_", " ").title(),
                lock_type=lock_config.lock_type.value,
                stagger_minutes=lock_config.stagger_minutes,
            )
            session.add(lock)
            await session.flush()

            # Create code slots for this lock
            for slot_num in range(1, 21):
                code_slot = CodeSlot(
                    lock_id=lock.id,
                    slot_number=slot_num,
                    sync_state=CodeSyncState.IDLE.value,
                )
                session.add(code_slot)

            # Create lock-calendar associations
            for cal_id in lock_config.calendars:
                if cal_id in calendar_map:
                    assoc = LockCalendar(
                        lock_id=lock.id,
                        calendar_id=calendar_map[cal_id].id,
                    )
                    session.add(assoc)

        await session.commit()
        logger.info("Default configuration created")

    # Home Assistant callbacks

    async def _ha_set_code(
        self, lock_entity_id: str, slot_number: int, code: str
    ) -> None:
        """Set a code on a lock via Home Assistant.

        Serialized through _zwave_lock so only one Z-Wave command runs at a
        time, with a small delay between commands to avoid network congestion.
        """
        async with self._zwave_lock:
            await self._ha_client.set_lock_usercode(lock_entity_id, slot_number, code)
            await asyncio.sleep(self.ZWAVE_CMD_DELAY)

    async def _ha_clear_code(self, lock_entity_id: str, slot_number: int) -> None:
        """Clear a code from a lock via Home Assistant.

        Serialized through _zwave_lock so only one Z-Wave command runs at a
        time, with a small delay between commands to avoid network congestion.
        """
        async with self._zwave_lock:
            await self._ha_client.clear_lock_usercode(lock_entity_id, slot_number)
            await asyncio.sleep(self.ZWAVE_CMD_DELAY)

    async def _ha_ping_lock(self, lock_entity_id: str) -> bool:
        """Ping a lock via Home Assistant."""
        return await self._ha_client.ping_lock(lock_entity_id)

    async def _on_sync_failed(
        self, lock_entity_id: str, slot_number: int, code: str, error: str
    ) -> None:
        """Handle sync failure after all retries."""
        logger.error(
            f"Code sync failed on {lock_entity_id} slot {slot_number}: {error}"
        )

        # Try to find the booking via the sync manager's slot state
        booking_uid = None
        if self._sync_manager:
            slot_sync = self._sync_manager.get_slot_state(lock_entity_id, slot_number)
            booking_uid = slot_sync.booking_uid

        # Log to audit
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()

            booking = None
            details = ""
            if booking_uid:
                booking_result = await session.execute(
                    select(Booking)
                    .options(selectinload(Booking.calendar))
                    .where(Booking.uid == booking_uid)
                )
                booking = booking_result.scalar_one_or_none()
                details = self._booking_details(booking)

            audit = AuditLog(
                action="code_sync_failed",
                lock_id=lock.id if lock else None,
                booking_id=booking.id if booking else None,
                slot_number=slot_number,
                code=code,
                details=details,
                success=False,
                error_message=error,
            )
            session.add(audit)

        # Build a human-readable notification
        notify_details = details or f"code {code}"
        await self._notify_failure(
            f"Code sync FAILED on {lock_entity_id} slot {slot_number} "
            f"after all retries. {notify_details}. Error: {error}"
        )

    # Scheduler callbacks

    async def _whole_house_checkin(self, booking_uid: str) -> None:
        """Whole-house check-in: disable auto-lock and unlock all internal locks."""
        logger.info(f"Whole-house check-in routine for booking {booking_uid}")
        await self._set_internal_locks(
            auto_lock=False, lock_action="unlock",
            reason=f"Whole-house check-in: {booking_uid}",
        )

    async def _whole_house_checkout(self, booking_uid: str) -> None:
        """Whole-house check-out: enable auto-lock and lock all internal locks."""
        logger.info(f"Whole-house check-out routine for booking {booking_uid}")
        await self._set_internal_locks(
            auto_lock=True, lock_action="lock",
            reason=f"Whole-house check-out: {booking_uid}",
        )

    async def _set_internal_locks(
        self, auto_lock: bool, lock_action: str, reason: str
    ) -> None:
        """Set auto-lock and lock/unlock on all internal locks (not front/back).

        Processes locks sequentially with a stagger delay between each to
        avoid overwhelming the Z-Wave network. Retries each operation up to
        3 times. Logs each operation to the audit log and notifies on failure.
        """
        STAGGER_DELAY = 8  # seconds between each lock
        action_desc = f"auto-lock {'on' if auto_lock else 'off'} + {lock_action}"
        logger.info(f"Internal locks: {action_desc} — {reason}")
        bid = uuid.uuid4().hex[:12]

        INTERNAL_TYPES = ("room", "bathroom", "kitchen", "storage")

        async with get_session_context() as session:
            result = await session.execute(select(Lock))
            locks = [l for l in result.scalars().all() if l.lock_type in INTERNAL_TYPES]

            failures = []
            for i, lock in enumerate(locks):
                # Stagger: wait between locks (not before the first one)
                if i > 0:
                    logger.debug(
                        f"Stagger: waiting {STAGGER_DELAY}s before {lock.entity_id}"
                    )
                    await asyncio.sleep(STAGGER_DELAY)

                # Set auto-lock
                al_success = False
                al_error = None
                for attempt in range(4):
                    try:
                        await self._ha_client.set_auto_lock(lock.entity_id, auto_lock)
                        al_success = True
                        logger.info(
                            f"Auto-lock {'enabled' if auto_lock else 'disabled'} "
                            f"on {lock.entity_id}"
                        )
                        break
                    except Exception as e:
                        al_error = str(e)
                        logger.warning(
                            f"Auto-lock failed on {lock.entity_id} "
                            f"(attempt {attempt + 1}/4): {e}"
                        )
                        if attempt < 3:
                            await asyncio.sleep(5 * (attempt + 1))

                # Log auto-lock result and persist to DB
                if al_success:
                    lock.auto_lock_enabled = auto_lock
                session.add(AuditLog(
                    action=f"auto_lock_{'enable' if auto_lock else 'disable'}",
                    lock_id=lock.id,
                    details=reason,
                    success=al_success,
                    error_message=al_error,
                    batch_id=bid,
                ))

                # Wait before lock/unlock to let Z-Wave settle
                if al_success:
                    await asyncio.sleep(3)

                # Lock or unlock
                la_success = False
                la_error = None
                for attempt in range(4):
                    try:
                        if lock_action == "unlock":
                            await self._ha_client.unlock(lock.entity_id)
                        else:
                            await self._ha_client.lock(lock.entity_id)
                        la_success = True
                        logger.info(f"{lock_action}ed {lock.entity_id}")
                        break
                    except Exception as e:
                        la_error = str(e)
                        logger.warning(
                            f"{lock_action} failed on {lock.entity_id} "
                            f"(attempt {attempt + 1}/4): {e}"
                        )
                        if attempt < 3:
                            await asyncio.sleep(5 * (attempt + 1))

                # Log lock/unlock result
                session.add(AuditLog(
                    action=f"whole_house_{lock_action}",
                    lock_id=lock.id,
                    details=reason,
                    success=la_success,
                    error_message=la_error,
                    batch_id=bid,
                ))

                if not al_success:
                    failures.append((lock.entity_id, f"auto-lock: {al_error}"))
                    self._record_failed_op(
                        lock.entity_id, lock.name,
                        f"auto-lock {'on' if auto_lock else 'off'}",
                        al_error or "Unknown error", reason,
                    )
                if not la_success:
                    failures.append((lock.entity_id, f"{lock_action}: {la_error}"))
                    self._record_failed_op(
                        lock.entity_id, lock.name,
                        lock_action,
                        la_error or "Unknown error", reason,
                    )

            await session.commit()

        if failures:
            failed_desc = "; ".join(f"{f[0]}: {f[1]}" for f in failures)
            await self._notify_failure(
                f"Whole-house lock routine failed — {failed_desc}. Reason: {reason}"
            )

    @staticmethod
    def _booking_details(booking: Optional[Booking]) -> str:
        """Build a human-readable details string for audit log entries."""
        if not booking:
            return ""
        cal_name = ""
        if booking.calendar:
            # e.g. "195_room_1" → "Room 1", "195_suite_a" → "Suite A"
            cal_name = booking.calendar.name
        parts = [booking.guest_name]
        if cal_name:
            parts.append(cal_name)
        ci = booking.check_in_date.strftime("%b %-d")
        co = booking.check_out_date.strftime("%b %-d")
        parts.append(f"{ci}–{co}")
        return " · ".join(parts)

    async def _on_code_activate(
        self, lock_entity_id: str, slot_number: int, code: str, booking_uid: str
    ) -> None:
        """Activate a code on a lock.

        Serialized via _activation_lock to prevent race conditions when
        multiple bookings with the same code activate simultaneously.
        """
        async with self._activation_lock:
            await self._do_code_activate(lock_entity_id, slot_number, code, booking_uid)

    async def _do_code_activate(
        self, lock_entity_id: str, slot_number: int, code: str, booking_uid: str
    ) -> None:
        """Internal activation logic, must be called under _activation_lock."""
        # Guard: skip activation if booking is disabled
        async with get_session_context() as session:
            booking_result = await session.execute(
                select(Booking)
                .options(selectinload(Booking.calendar))
                .where(Booking.uid == booking_uid)
            )
            booking = booking_result.scalar_one_or_none()
            if booking and booking.code_disabled:
                logger.info(f"Skipping activation for disabled booking {booking_uid}")
                return

            # Guard: skip entirely if same code already on another slot of this lock.
            # Z-Wave locks cannot hold the same code on two different slots.
            lock_result = await session.execute(
                select(Lock).options(selectinload(Lock.code_slots))
                .where(Lock.entity_id == lock_entity_id)
            )
            lock = lock_result.scalar_one_or_none()
            if lock:
                for slot in lock.code_slots:
                    if (slot.slot_number != slot_number
                            and slot.current_code == code
                            and slot.sync_state == CodeSyncState.ACTIVE.value):
                        logger.info(
                            f"Skipping duplicate code {code} on {lock_entity_id} slot {slot_number} "
                            f"— already on slot {slot.slot_number}"
                        )
                        return

        details = self._booking_details(booking)

        logger.info(
            f"Activating code on {lock_entity_id} slot {slot_number} "
            f"for booking {booking_uid}"
        )

        if self._sync_manager:
            await self._sync_manager.set_code(
                lock_entity_id, slot_number, code, booking_uid
            )

        # Update DB CodeSlot to reflect what's on the physical lock
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).options(selectinload(Lock.code_slots))
                .where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()

            if lock:
                for slot in lock.code_slots:
                    if slot.slot_number == slot_number:
                        slot.current_code = code
                        slot.sync_state = CodeSyncState.ACTIVE.value
                        break

            audit = AuditLog(
                action="code_activated",
                lock_id=lock.id if lock else None,
                booking_id=booking.id if booking else None,
                slot_number=slot_number,
                code=code,
                details=details,
                success=True,
            )
            session.add(audit)

    async def _on_code_deactivate(
        self, lock_entity_id: str, slot_number: int, booking_uid: str
    ) -> None:
        """Deactivate a code on a lock."""
        async with self._activation_lock:
            await self._do_code_deactivate(lock_entity_id, slot_number, booking_uid)

    async def _do_code_deactivate(
        self, lock_entity_id: str, slot_number: int, booking_uid: str
    ) -> None:
        """Internal deactivation logic, must be called under _activation_lock."""
        logger.info(
            f"Deactivating code on {lock_entity_id} slot {slot_number} "
            f"for booking {booking_uid}"
        )

        if self._sync_manager:
            await self._sync_manager.clear_code(lock_entity_id, slot_number, booking_uid)

        # Update DB CodeSlot and log to audit
        async with get_session_context() as session:
            booking_result = await session.execute(
                select(Booking)
                .options(selectinload(Booking.calendar))
                .where(Booking.uid == booking_uid)
            )
            booking = booking_result.scalar_one_or_none()
            details = self._booking_details(booking)

            result = await session.execute(
                select(Lock).options(selectinload(Lock.code_slots))
                .where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()

            if lock:
                for slot in lock.code_slots:
                    if slot.slot_number == slot_number:
                        slot.current_code = None
                        slot.sync_state = CodeSyncState.IDLE.value
                        break

            audit = AuditLog(
                action="code_deactivated",
                lock_id=lock.id if lock else None,
                booking_id=booking.id if booking else None,
                slot_number=slot_number,
                details=details,
                success=True,
            )
            session.add(audit)

    async def _on_code_finalize(self, booking_uid: str, calendar_id_str: str, booking_id: int = 0) -> None:
        """Finalize the code for a booking at 11am the day before check-in.

        Re-fetches the calendar to get the latest phone number, generates the
        definitive code, and locks it in. Any scheduled activations are updated.
        """
        logger.info(f"Finalizing code for booking {booking_uid} (calendar {calendar_id_str}, id={booking_id})")

        async with get_session_context() as session:
            # Look up by DB id first (stable), fall back to UID (unstable with HostTools)
            if booking_id:
                booking_result = await session.execute(
                    select(Booking)
                    .options(selectinload(Booking.calendar))
                    .where(Booking.id == booking_id)
                )
            else:
                booking_result = await session.execute(
                    select(Booking)
                    .options(selectinload(Booking.calendar))
                    .where(Booking.uid == booking_uid)
                    .join(Calendar)
                    .where(Calendar.calendar_id == calendar_id_str)
                )
            booking = booking_result.scalar_one_or_none()
            if not booking:
                logger.warning(f"Booking {booking_uid} (id={booking_id}) not found for finalization")
                return

            if booking.locked_code:
                logger.info(f"Booking {booking_uid} already has locked code {booking.locked_code}")
                return

            # Re-fetch the calendar to get latest data (phone number may have been added)
            calendar = booking.calendar
            try:
                parsed_bookings = await self._fetch_calendar_bookings(calendar)
                if parsed_bookings:
                    # Match by content key (guest_name + dates), not UID
                    match_key = self._booking_match_key(
                        booking.guest_name, booking.check_in_date, booking.check_out_date
                    )
                    for parsed in parsed_bookings:
                        parsed_key = self._booking_match_key(
                            parsed.guest_name, parsed.check_in_date, parsed.check_out_date
                        )
                        if parsed_key == match_key:
                            if parsed.phone and parsed.phone != booking.phone:
                                logger.info(
                                    f"Phone updated for {booking.guest_name}: "
                                    f"{booking.phone} -> {parsed.phone}"
                                )
                                booking.phone = parsed.phone
                            break
            except Exception as e:
                logger.error(f"Error re-fetching calendar for finalization: {e}")
                # Continue with existing phone number

            # Generate and lock the code
            code = generate_code_from_phone(booking.phone)
            if code:
                booking.locked_code = code
                booking.code_locked_at = datetime.utcnow()
                logger.info(f"Locked code {code} for booking {booking_uid} ({booking.guest_name})")

                # Log to audit
                audit = AuditLog(
                    action="code_finalized",
                    booking_id=booking.id,
                    code=code,
                    details=f"Code locked for {booking.guest_name} (phone: {booking.phone})",
                    success=True,
                )
                session.add(audit)
            else:
                logger.warning(f"Could not generate code for booking {booking_uid} - no phone")

            await session.commit()

    async def _on_emergency_rotate(self) -> None:
        """Weekly rotation of all emergency codes."""
        logger.info("Rotating emergency codes (weekly)")
        result = await self.randomize_emergency_codes()
        logger.info(
            f"Emergency codes rotated: {result['success_count']}/{result['total_locks']} locks"
        )

    async def _poll_calendars(self) -> None:
        """Poll all calendars for updates.

        For each calendar, tries HostTools API first (if listing ID configured),
        then HA calendar entity, then iCal URL.
        """
        if self._polling:
            logger.info("Calendar poll already in progress, skipping")
            return
        self._polling = True
        logger.info("Polling calendars...")

        async with get_session_context() as session:
            # Get all calendars
            result = await session.execute(select(Calendar))
            calendars = result.scalars().all()

            for calendar in calendars:
                try:
                    bookings = await self._fetch_calendar_bookings(calendar)
                    if bookings is None:
                        continue  # No source configured
                    await self._process_calendar_bookings(session, calendar, bookings)
                    calendar.last_fetched = datetime.utcnow()
                    calendar.last_fetch_error = None
                except Exception as e:
                    logger.error(f"Error fetching calendar {calendar.calendar_id}: {e}")
                    calendar.last_fetch_error = str(e)

            # Check for upcoming bookings without codes
            await self._check_upcoming_no_code_bookings(session)

            # Validate and fix assignment times that don't match expected timing
            await self._validate_assignment_times(session)

            await session.commit()

        self._polling = False
        logger.info("Calendar poll complete")

    async def _fetch_calendar_bookings(
        self, calendar: Calendar
    ) -> list[ParsedBooking] | None:
        """Fetch bookings from a calendar via HostTools API.

        Returns None if HostTools is not configured for this calendar.
        """
        if not self._hosttools_client or not calendar.hosttools_listing_id:
            return None

        reservations = await self._hosttools_client.get_reservations(
            calendar.hosttools_listing_id
        )
        bookings = parse_hosttools_reservations(reservations)
        logger.debug(
            f"Fetched {len(bookings)} bookings from HostTools for {calendar.calendar_id}"
        )
        return bookings

    @staticmethod
    def _booking_match_key(guest_name: str, check_in: date, check_out: date) -> str:
        """Create a dedup key for a booking based on content, not UID.

        HostTools generates new random UIDs on every fetch, so we match
        on (guest_name, check_in_date, check_out_date) instead.
        """
        return f"{guest_name}|{check_in.isoformat()}|{check_out.isoformat()}"

    async def _process_calendar_bookings(
        self, session: AsyncSession, calendar: Calendar, parsed_bookings: list[ParsedBooking]
    ) -> None:
        """Process bookings from a calendar fetch."""
        # Get existing bookings for this calendar, keyed by content match
        result = await session.execute(
            select(Booking).where(Booking.calendar_id == calendar.id)
        )
        existing_by_key: dict[str, Booking] = {}
        for b in result.scalars().all():
            key = self._booking_match_key(b.guest_name, b.check_in_date, b.check_out_date)
            existing_by_key[key] = b

        # Track keys we've already processed in this batch
        processed_keys: set[str] = set()

        for parsed in parsed_bookings:
            key = self._booking_match_key(
                parsed.guest_name, parsed.check_in_date, parsed.check_out_date
            )

            # Skip if we've already processed this in this batch
            if key in processed_keys:
                continue
            processed_keys.add(key)

            if key in existing_by_key:
                # Update existing booking
                booking = existing_by_key[key]
                booking.uid = parsed.uid  # Update UID since HostTools changes it
                booking.phone = parsed.phone
                booking.channel = parsed.channel
                booking.reservation_id = parsed.reservation_id
                booking.is_blocked = parsed.is_blocked
            else:
                # Create new booking
                booking = Booking(
                    calendar_id=calendar.id,
                    uid=parsed.uid,
                    guest_name=parsed.guest_name,
                    phone=parsed.phone,
                    channel=parsed.channel,
                    reservation_id=parsed.reservation_id,
                    check_in_date=parsed.check_in_date,
                    check_out_date=parsed.check_out_date,
                    is_blocked=parsed.is_blocked,
                )
                session.add(booking)
                await session.flush()
                existing_by_key[key] = booking

                # Schedule codes for this booking if not blocked
                if not parsed.is_blocked:
                    await self._schedule_booking_codes(session, calendar, booking)

                    # Schedule code finalization at 11am day before check-in
                    if parsed.phone and self._scheduler:
                        finalize_at = datetime.combine(
                            parsed.check_in_date - timedelta(days=1),
                            time(11, 0),
                        )
                        self._scheduler.schedule_finalization(
                            booking_uid=parsed.uid,
                            calendar_id=calendar.calendar_id,
                            finalize_at=finalize_at,
                            booking_id=booking.id,
                        )

                    # Schedule whole-house auto-lock toggle for whole_house/both_houses calendars
                    if calendar.calendar_type in ("whole_house", "both_houses") and self._scheduler:
                        self._scheduler.schedule_whole_house(
                            booking_uid=parsed.uid,
                            check_in_date=parsed.check_in_date,
                            check_out_date=parsed.check_out_date,
                        )
                        logger.info(
                            f"Scheduled whole-house auto-lock toggle for {parsed.guest_name} "
                            f"check-in {parsed.check_in_date} 14:30, "
                            f"check-out {parsed.check_out_date} 11:30"
                        )

                    # Notify if booking has no phone (can't generate code)
                    if not parsed.phone:
                        await self._notify_no_code(
                            parsed.guest_name, calendar.name,
                            parsed.check_in_date, parsed.check_out_date,
                        )

        # Remove stale bookings that no longer appear in the feed
        stale_keys = set(existing_by_key.keys()) - processed_keys
        for stale_key in stale_keys:
            booking = existing_by_key[stale_key]
            await self._remove_stale_booking(session, booking)

    async def _remove_stale_booking(
        self, session: AsyncSession, booking: Booking
    ) -> None:
        """Remove a booking that no longer appears in the calendar feed.

        Clears active codes from locks, cancels scheduler jobs,
        deletes code assignments, and deletes the booking.
        """
        logger.info(
            f"Removing stale booking: {booking.guest_name} "
            f"({booking.check_in_date}–{booking.check_out_date}) "
            f"from calendar {booking.calendar_id}"
        )

        # Cancel all scheduler jobs for this booking
        if self._scheduler:
            jobs = self._scheduler.get_jobs_for_booking(booking.uid)
            for job in jobs:
                self._scheduler.cancel_job(job.job_id)
            if jobs:
                logger.info(f"Cancelled {len(jobs)} scheduler jobs for stale booking {booking.uid}")

        # Clear active codes from locks and delete assignments
        now = datetime.utcnow()
        assignments = await session.execute(
            select(CodeAssignment)
            .options(joinedload(CodeAssignment.code_slot).joinedload(CodeSlot.lock))
            .where(CodeAssignment.booking_id == booking.id)
        )
        for assignment in assignments.unique().scalars().all():
            # If this code is currently active on a lock, clear it
            if assignment.activate_at <= now < assignment.deactivate_at:
                lock = assignment.code_slot.lock
                slot = assignment.code_slot
                if lock and self._sync_manager:
                    logger.info(
                        f"Clearing active code from {lock.entity_id} slot "
                        f"{slot.slot_number} (cancelled booking)"
                    )
                    await self._sync_manager.clear_code(
                        lock.entity_id,
                        slot.slot_number,
                        booking.uid,
                    )
                    slot.current_code = None
                    slot.sync_state = CodeSyncState.IDLE.value
            await session.delete(assignment)

        # Delete time overrides
        overrides = await session.execute(
            select(TimeOverride).where(TimeOverride.booking_id == booking.id)
        )
        for override in overrides.scalars().all():
            await session.delete(override)

        # Delete the booking itself
        await session.delete(booking)
        logger.info(f"Deleted stale booking {booking.guest_name} (id={booking.id})")

    async def _notify_no_code(
        self, guest_name: str, calendar_name: str, check_in: date, check_out: date
    ) -> None:
        """Send HA notification for a booking with no code."""
        message = (
            f"Booking without code: {guest_name} "
            f"({calendar_name}, {check_in} to {check_out}). "
            f"No phone number available to generate a code."
        )
        logger.warning(message)
        try:
            await self._ha_client.send_notification(
                message, title="Rental Manager — Missing Code"
            )
        except Exception as e:
            logger.error(f"Failed to send no-code notification: {e}")

    async def _check_upcoming_no_code_bookings(self, session: AsyncSession) -> None:
        """Check for upcoming bookings (within 48h) that still have no code. Dedup via AuditLog."""
        cutoff = date.today() + timedelta(days=2)
        today = date.today()

        result = await session.execute(
            select(Booking)
            .options(selectinload(Booking.calendar))
            .where(
                Booking.is_blocked == False,
                Booking.check_in_date <= cutoff,
                Booking.check_out_date >= today,
                Booking.phone == None,
                Booking.locked_code == None,
            )
        )
        bookings = result.scalars().all()

        for booking in bookings:
            # Check if we already notified for this booking
            audit_result = await session.execute(
                select(AuditLog).where(
                    AuditLog.action == "no_code_warning",
                    AuditLog.booking_id == booking.id,
                ).limit(1)
            )
            if audit_result.scalar_one_or_none():
                continue

            await self._notify_no_code(
                booking.guest_name,
                booking.calendar.name if booking.calendar else "Unknown",
                booking.check_in_date, booking.check_out_date,
            )
            session.add(AuditLog(
                action="no_code_warning",
                booking_id=booking.id,
                details=f"No code for {booking.guest_name} checking in {booking.check_in_date}",
                success=True,
            ))

    async def _validate_assignment_times(self, session: AsyncSession) -> None:
        """Validate and fix CodeAssignment times that don't match expected lock timing.

        Detects assignments created with wrong lock_type timing (e.g. bathroom
        getting kitchen's 15:00 instead of 12:00) and corrects them. Skips
        assignments that have manual TimeOverrides.
        """
        now = datetime.utcnow()
        today = date.today()

        # Get all future/active assignments with their relationships
        result = await session.execute(
            select(CodeAssignment)
            .options(
                joinedload(CodeAssignment.code_slot).joinedload(CodeSlot.lock),
                joinedload(CodeAssignment.booking).joinedload(Booking.calendar),
            )
            .where(CodeAssignment.deactivate_at > now)
        )
        assignments = result.unique().scalars().all()

        fixed_count = 0
        for assignment in assignments:
            slot = assignment.code_slot
            lock = slot.lock if slot else None
            booking = assignment.booking
            if not lock or not booking:
                continue

            lock_type = LockType(lock.lock_type)

            # Check if a manual TimeOverride exists for this lock+booking
            override_result = await session.execute(
                select(TimeOverride).where(
                    TimeOverride.booking_id == booking.id,
                    TimeOverride.lock_id == lock.id,
                )
            )
            override = override_result.scalar_one_or_none()

            expected_activate, expected_deactivate = calculate_code_times(
                lock_type=lock_type,
                check_in_date=booking.check_in_date,
                check_out_date=booking.check_out_date,
                stagger_minutes=lock.stagger_minutes,
                override_activate=override.activate_at if override else None,
                override_deactivate=override.deactivate_at if override else None,
            )

            needs_fix = False
            if assignment.activate_at != expected_activate:
                logger.warning(
                    "Assignment time mismatch: %s slot %d for %s — "
                    "activate_at=%s expected=%s (lock_type=%s)",
                    lock.entity_id, slot.slot_number, booking.guest_name,
                    assignment.activate_at, expected_activate, lock.lock_type,
                )
                assignment.activate_at = expected_activate
                needs_fix = True

            if assignment.deactivate_at != expected_deactivate:
                logger.warning(
                    "Assignment time mismatch: %s slot %d for %s — "
                    "deactivate_at=%s expected=%s (lock_type=%s)",
                    lock.entity_id, slot.slot_number, booking.guest_name,
                    assignment.deactivate_at, expected_deactivate, lock.lock_type,
                )
                assignment.deactivate_at = expected_deactivate
                needs_fix = True

            if needs_fix:
                fixed_count += 1
                # Reschedule if scheduler is running
                if self._scheduler and booking.calendar:
                    code = booking.locked_code or generate_code_from_phone(booking.phone)
                    if code:
                        # If code is already on the lock, only reschedule deactivation
                        # to avoid redundant Z-Wave commands that drain battery
                        code_already_on_lock = (
                            slot.current_code == code
                            and slot.sync_state == CodeSyncState.ACTIVE.value
                        )
                        if code_already_on_lock:
                            self._scheduler.schedule_deactivation_only(
                                lock_entity_id=lock.entity_id,
                                slot_number=slot.slot_number,
                                booking_uid=booking.uid,
                                deactivate_at=expected_deactivate,
                            )
                        else:
                            entry = CodeScheduleEntry(
                                lock_entity_id=lock.entity_id,
                                slot_number=slot.slot_number,
                                code=code,
                                activate_at=expected_activate,
                                deactivate_at=expected_deactivate,
                                booking_uid=booking.uid,
                                calendar_id=booking.calendar.calendar_id,
                                guest_name=booking.guest_name,
                            )
                            self._scheduler.schedule_code(entry)

        if fixed_count:
            logger.info("Fixed %d assignment time mismatches", fixed_count)

    async def _create_code_assignments(
        self, session: AsyncSession, booking: Booking, code: str
    ) -> list[CodeAssignment]:
        """Create code assignments for a booking across all applicable locks.

        Handles lock querying, time overrides, slot allocation, assignment
        creation, and scheduler integration. Returns newly created assignments
        with code_slot.lock relationships loaded.
        """
        calendar = booking.calendar
        if not calendar:
            logger.warning(
                f"Booking {booking.id} has no calendar, cannot create assignments"
            )
            return []

        # Get locks that this calendar grants access to
        result = await session.execute(
            select(Lock)
            .join(LockCalendar)
            .where(LockCalendar.calendar_id == calendar.id)
            .options(
                selectinload(Lock.code_slots)
                .selectinload(CodeSlot.assignments)
                .joinedload(CodeAssignment.booking)
            )
        )
        locks = result.unique().scalars().all()
        new_assignments = []
        now = datetime.utcnow()

        for lock in locks:
            lock_type = LockType(lock.lock_type)

            # Check for time override
            override_result = await session.execute(
                select(TimeOverride).where(
                    TimeOverride.booking_id == booking.id,
                    TimeOverride.lock_id == lock.id,
                )
            )
            override = override_result.scalar_one_or_none()

            activate_at, deactivate_at = calculate_code_times(
                lock_type=lock_type,
                check_in_date=booking.check_in_date,
                check_out_date=booking.check_out_date,
                stagger_minutes=lock.stagger_minutes,
                override_activate=override.activate_at if override else None,
                override_deactivate=override.deactivate_at if override else None,
            )

            # Allocate slot — use time window to find active occupants
            slot_a, slot_b = get_slot_for_calendar(calendar.calendar_id)
            existing_assignments = [
                a for slot in lock.code_slots
                for a in slot.assignments
                if slot.slot_number in (slot_a, slot_b)
                and a.activate_at <= now < a.deactivate_at
            ]
            existing_uids = {a.booking.uid for a in existing_assignments}

            slot_number = self._slot_allocator.allocate_slot_for_booking(
                lock.entity_id, calendar.calendar_id, booking.uid, existing_uids
            )

            code_slot = next(
                (s for s in lock.code_slots if s.slot_number == slot_number), None
            )
            if not code_slot:
                continue

            assignment = CodeAssignment(
                code_slot_id=code_slot.id,
                booking_id=booking.id,
                code=code,
                activate_at=activate_at,
                deactivate_at=deactivate_at,
                is_active=activate_at <= now < deactivate_at,
            )
            session.add(assignment)
            new_assignments.append(assignment)

            if self._scheduler:
                entry = CodeScheduleEntry(
                    lock_entity_id=lock.entity_id,
                    slot_number=slot_number,
                    code=code,
                    activate_at=activate_at,
                    deactivate_at=deactivate_at,
                    booking_uid=booking.uid,
                    calendar_id=calendar.calendar_id,
                    guest_name=booking.guest_name,
                )
                self._scheduler.schedule_code(entry)

        if new_assignments:
            await session.flush()
            # Reload with code_slot.lock relationships for callers
            ids = [a.id for a in new_assignments]
            reloaded = await session.execute(
                select(CodeAssignment)
                .options(
                    joinedload(CodeAssignment.code_slot)
                    .joinedload(CodeSlot.lock)
                )
                .where(CodeAssignment.id.in_(ids))
            )
            new_assignments = reloaded.unique().scalars().all()
            logger.info(
                f"Created {len(new_assignments)} code assignments for booking "
                f"{booking.guest_name} (id={booking.id})"
            )

        return new_assignments

    async def _schedule_booking_codes(
        self, session: AsyncSession, calendar: Calendar, booking: Booking
    ) -> None:
        """Schedule code activations for a booking (called during calendar sync)."""
        if booking.is_blocked:
            return
        if not booking.phone and not booking.locked_code:
            return

        code = booking.locked_code or generate_code_from_phone(booking.phone)
        if not code:
            logger.warning(f"Could not generate code for booking {booking.uid}")
            return

        await self._create_code_assignments(session, booking, code)

    # Public API methods

    async def get_locks(self) -> list[dict]:
        """Get all locks for this house (DB only — no HA polling)."""
        async with get_session_context() as session:
            query = select(Lock).options(
                selectinload(Lock.house),
                selectinload(Lock.code_slots).selectinload(
                    CodeSlot.assignments
                ).selectinload(
                    CodeAssignment.booking
                ).selectinload(Booking.calendar),
            )

            result = await session.execute(query)
            locks = result.scalars().all()

            now = datetime.utcnow()

            def _slot_info(slot: CodeSlot) -> dict:
                info: dict = {
                    "slot_number": slot.slot_number,
                    "current_code": slot.current_code,
                    "sync_state": slot.sync_state,
                }
                # Find the active or upcoming assignment for this slot
                active = None
                for a in slot.assignments:
                    if a.booking and a.activate_at <= now < a.deactivate_at:
                        active = a
                        break
                # Fallback: find the next upcoming assignment
                if not active:
                    upcoming = [
                        a for a in slot.assignments
                        if a.booking and a.activate_at > now
                    ]
                    if upcoming:
                        active = min(upcoming, key=lambda a: a.activate_at)

                if active and active.booking:
                    b = active.booking
                    is_now_active = active.activate_at <= now < active.deactivate_at
                    info["guest_name"] = b.guest_name
                    info["booking_id"] = b.id
                    info["check_in"] = b.check_in_date.isoformat() if b.check_in_date else None
                    info["check_out"] = b.check_out_date.isoformat() if b.check_out_date else None
                    info["calendar_id"] = b.calendar.calendar_id if b.calendar else None
                    info["is_active"] = is_now_active
                    info["assigned_code"] = active.code if is_now_active else None
                return info

            return [
                {
                    "id": lock.id,
                    "entity_id": lock.entity_id,
                    "name": lock.name,
                    "house_code": lock.house.code,
                    "lock_type": lock.lock_type,
                    "master_code": lock.master_code,
                    "emergency_code": lock.emergency_code,
                    "auto_lock_enabled": lock.auto_lock_enabled,
                    "volume_level": lock.volume_level,
                    "slots": [
                        _slot_info(slot)
                        for slot in sorted(lock.code_slots, key=lambda s: s.slot_number)
                    ],
                }
                for lock in locks
            ]

    async def get_bookings(
        self,
        calendar_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> list[dict]:
        """Get bookings, optionally filtered.

        Defaults to showing bookings with check-out in the last 30 days or later.
        """
        if from_date is None:
            from_date = date.today() - timedelta(days=30)

        async with get_session_context() as session:
            query = select(Booking).options(selectinload(Booking.calendar))

            if calendar_id:
                query = query.join(Calendar).where(
                    Calendar.calendar_id == calendar_id
                )

            query = query.where(Booking.check_out_date >= from_date)

            if to_date:
                query = query.where(Booking.check_in_date <= to_date)

            result = await session.execute(query)
            bookings = result.scalars().all()

            return [
                {
                    "id": b.id,
                    "uid": b.uid,
                    "calendar_id": b.calendar.calendar_id,
                    "guest_name": b.guest_name,
                    "phone": b.phone,
                    "channel": b.channel,
                    "check_in_date": b.check_in_date.isoformat(),
                    "check_out_date": b.check_out_date.isoformat(),
                    "is_blocked": b.is_blocked,
                    "code": b.locked_code or (generate_code_from_phone(b.phone) if b.phone else None),
                    "code_locked": b.locked_code is not None,
                    "code_disabled": b.code_disabled,
                }
                for b in bookings
            ]

    async def get_booking_lock_times(self, booking_id: int) -> list[dict]:
        """Get the computed activation/deactivation times for a booking on each relevant lock.

        Returns default times and any existing overrides for each lock.
        """
        async with get_session_context() as session:
            # Get the booking with its calendar
            booking_result = await session.execute(
                select(Booking)
                .options(selectinload(Booking.calendar))
                .where(Booking.id == booking_id)
            )
            booking = booking_result.scalar_one_or_none()
            if not booking:
                return []

            # Get locks associated with this booking's calendar
            lock_result = await session.execute(
                select(Lock)
                .join(LockCalendar)
                .where(LockCalendar.calendar_id == booking.calendar.id)
            )
            locks = lock_result.scalars().all()

            # Get existing overrides for this booking
            override_result = await session.execute(
                select(TimeOverride).where(TimeOverride.booking_id == booking_id)
            )
            overrides = {o.lock_id: o for o in override_result.scalars().all()}

            result = []
            for lock in locks:
                lock_type = LockType(lock.lock_type)
                override = overrides.get(lock.id)

                # Calculate the default times (without override)
                default_activate, default_deactivate = calculate_code_times(
                    lock_type=lock_type,
                    check_in_date=booking.check_in_date,
                    check_out_date=booking.check_out_date,
                    stagger_minutes=lock.stagger_minutes,
                )

                # Calculate the effective times (with override if present)
                effective_activate, effective_deactivate = calculate_code_times(
                    lock_type=lock_type,
                    check_in_date=booking.check_in_date,
                    check_out_date=booking.check_out_date,
                    stagger_minutes=lock.stagger_minutes,
                    override_activate=override.activate_at if override else None,
                    override_deactivate=override.deactivate_at if override else None,
                )

                result.append({
                    "lock_id": lock.id,
                    "lock_name": lock.name,
                    "lock_type": lock.lock_type,
                    "default_activate": default_activate.isoformat(),
                    "default_deactivate": default_deactivate.isoformat(),
                    "effective_activate": effective_activate.isoformat(),
                    "effective_deactivate": effective_deactivate.isoformat(),
                    "has_override": override is not None,
                    "override_notes": override.notes if override else None,
                })

            return result

    async def _set_code_with_retry(
        self, lock_entity_id: str, slot_number: int, code: str, max_retries: int = 3
    ) -> None:
        """Set a code on a lock with retry logic and stagger delay."""
        for attempt in range(max_retries + 1):
            try:
                await self._ha_set_code(lock_entity_id, slot_number, code)
                return
            except Exception:
                if attempt < max_retries:
                    await asyncio.sleep(5 * (attempt + 1))  # 5s, 10s, 15s backoff
                else:
                    raise

    async def _notify_failure(self, message: str) -> None:
        """Send a failure notification via HA."""
        logger.error(f"NOTIFICATION: {message}")
        try:
            await self._ha_client.send_notification(message, title="Rental Manager Alert")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")

    async def set_master_code(self, code: str) -> dict:
        """Set master code on all locks with stagger delays and retries."""
        async with get_session_context() as session:
            result = await session.execute(select(Lock))
            locks = result.scalars().all()

            success_count = 0
            errors = []

            for i, lock in enumerate(locks):
                # Stagger: 3 second delay between locks
                if i > 0:
                    await asyncio.sleep(3)

                try:
                    await self._set_code_with_retry(lock.entity_id, MASTER_CODE_SLOT, code)
                    lock.master_code = code
                    success_count += 1

                    audit = AuditLog(
                        action="master_code_set",
                        lock_id=lock.id,
                        slot_number=MASTER_CODE_SLOT,
                        code=code,
                        success=True,
                    )
                    session.add(audit)
                except Exception as e:
                    errors.append({"lock": lock.entity_id, "error": str(e)})
                    lock.master_code = code  # Save to DB anyway

            await session.commit()

            if errors:
                failed_locks = ", ".join(e["lock"] for e in errors)
                await self._notify_failure(
                    f"Master code failed on {len(errors)} lock(s) after retries: {failed_locks}"
                )

            return {
                "success_count": success_count,
                "total_locks": len(locks),
                "errors": errors,
            }

    @staticmethod
    def _generate_random_code() -> str:
        """Generate a random 5-digit emergency code (10000-99999)."""
        return str(random.randint(10000, 99999))

    async def get_emergency_codes(self) -> list[dict]:
        """Get all emergency codes grouped by lock."""
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).options(selectinload(Lock.house))
            )
            locks = result.scalars().all()

            return [
                {
                    "lock_id": lock.id,
                    "entity_id": lock.entity_id,
                    "lock_name": lock.name,
                    "house_code": lock.house.code,
                    "lock_type": lock.lock_type,
                    "emergency_code": lock.emergency_code,
                }
                for lock in locks
            ]

    async def randomize_emergency_codes(
        self, lock_ids: Optional[list[int]] = None
    ) -> dict:
        """Randomize emergency codes — each lock gets a unique random code."""
        async with get_session_context() as session:
            query = select(Lock)
            if lock_ids:
                query = query.where(Lock.id.in_(lock_ids))

            result = await session.execute(query)
            locks = result.scalars().all()

            success_count = 0
            errors = []
            codes = {}
            bid = uuid.uuid4().hex[:12]

            for i, lock in enumerate(locks):
                # Stagger: 3 second delay between locks
                if i > 0:
                    await asyncio.sleep(3)

                code = self._generate_random_code()
                codes[lock.entity_id] = code
                lock.emergency_code = code

                try:
                    await self._set_code_with_retry(
                        lock.entity_id, EMERGENCY_CODE_SLOT, code
                    )
                    success_count += 1

                    audit = AuditLog(
                        action="emergency_code_randomized",
                        lock_id=lock.id,
                        slot_number=EMERGENCY_CODE_SLOT,
                        code=code,
                        success=True,
                        batch_id=bid,
                    )
                    session.add(audit)
                except Exception as e:
                    errors.append({"lock": lock.entity_id, "error": str(e)})

            await session.commit()

            if errors:
                failed_locks = ", ".join(e["lock"] for e in errors)
                await self._notify_failure(
                    f"Emergency code rotation failed on {len(errors)} lock(s) after retries: {failed_locks}"
                )

            # Backup to Google Sheets
            await self._backup_emergency_codes()

            return {
                "success_count": success_count,
                "total_locks": len(locks),
                "codes": codes,
                "errors": errors,
            }

    async def set_emergency_code(
        self, lock_id: int, code: str
    ) -> dict:
        """Set a specific emergency code on a single lock."""
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).where(Lock.id == lock_id)
            )
            lock = result.scalar_one_or_none()
            if not lock:
                raise ValueError(f"Lock {lock_id} not found")

            success = True
            error_msg = None
            try:
                await self._set_code_with_retry(lock.entity_id, EMERGENCY_CODE_SLOT, code)
            except Exception as e:
                success = False
                error_msg = str(e)
                logger.error(f"Emergency code failed on {lock.entity_id}: {e}")
                await self._notify_failure(
                    f"Emergency code failed on {lock.entity_id} after retries: {e}"
                )

            lock.emergency_code = code
            audit = AuditLog(
                action="emergency_code_set",
                lock_id=lock.id,
                slot_number=EMERGENCY_CODE_SLOT,
                code=code,
                success=success,
                error_message=error_msg,
            )
            session.add(audit)
            await session.commit()

            # Backup to Google Sheets
            await self._backup_emergency_codes()

            return {
                "lock_id": lock.id,
                "entity_id": lock.entity_id,
                "emergency_code": code,
            }

    async def clear_all_codes(self, lock_entity_id: str) -> dict:
        """Clear ALL code slots (1-20) on a lock. For setup use.

        Each call to _ha_clear_code is serialized through the Z-Wave command
        lock with a built-in delay, so no additional stagger is needed here.
        """
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).options(selectinload(Lock.code_slots))
                .where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()
            if not lock:
                raise ValueError(f"Lock {lock_entity_id} not found")

            errors = []
            cleared = 0
            cleared_slots = set()
            for slot_num in range(1, 21):
                try:
                    await self._ha_clear_code(lock.entity_id, slot_num)
                    cleared += 1
                    cleared_slots.add(slot_num)
                    logger.debug(f"Cleared slot {slot_num} on {lock.entity_id}")
                except Exception as e:
                    logger.error(
                        f"Failed to clear slot {slot_num} on {lock.entity_id}: {e}"
                    )
                    errors.append(f"Slot {slot_num}: {e}")

            # Update DB — only for slots that were successfully cleared
            if MASTER_CODE_SLOT in cleared_slots:
                lock.master_code = None
            if EMERGENCY_CODE_SLOT in cleared_slots:
                lock.emergency_code = None
            for slot in lock.code_slots:
                if slot.slot_number in cleared_slots:
                    slot.current_code = None
                    slot.sync_state = CodeSyncState.IDLE.value

            session.add(AuditLog(
                action="clear_all_codes",
                lock_id=lock.id,
                success=len(errors) == 0,
                error_message="; ".join(errors) if errors else None,
                details=f"Cleared {cleared}/20 slots",
            ))
            await session.commit()

            return {
                "entity_id": lock.entity_id,
                "cleared": cleared,
                "errors": errors,
            }

    async def set_slot_code(
        self, lock_entity_id: str, slot_number: int, code: str
    ) -> dict:
        """Set a code on a specific slot.

        If the slot has an active CodeAssignment, the assignment's code is
        also updated so the scheduler won't revert the manual override.
        """
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).options(
                    selectinload(Lock.code_slots).selectinload(CodeSlot.assignments)
                )
                .where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()
            if not lock:
                raise ValueError(f"Lock {lock_entity_id} not found")

            success = True
            error_msg = None
            try:
                await self._set_code_with_retry(lock.entity_id, slot_number, code)
            except Exception as e:
                success = False
                error_msg = str(e)
                logger.error(f"Failed to set code on {lock.entity_id} slot {slot_number}: {e}")

            # Update DB
            slot = next((s for s in lock.code_slots if s.slot_number == slot_number), None)
            if slot:
                slot.current_code = code if success else slot.current_code
                # Also update any active assignment so the scheduler doesn't revert
                if success:
                    now = datetime.utcnow()
                    for assignment in slot.assignments:
                        if assignment.is_active or (
                            assignment.activate_at <= now < assignment.deactivate_at
                        ):
                            logger.info(
                                f"Manual override on {lock.entity_id} slot {slot_number}: "
                                f"updating assignment {assignment.id} code "
                                f"{assignment.code} -> {code}"
                            )
                            assignment.code = code

            if slot_number == MASTER_CODE_SLOT and success:
                lock.master_code = code
            elif slot_number == EMERGENCY_CODE_SLOT and success:
                lock.emergency_code = code

            session.add(AuditLog(
                action="set_slot_code",
                lock_id=lock.id,
                slot_number=slot_number,
                code=code,
                success=success,
                error_message=error_msg,
                details="manual override" if slot and any(
                    a.is_active for a in (slot.assignments if slot else [])
                ) else None,
            ))
            await session.commit()

            if not success:
                raise ValueError(f"Failed to set code: {error_msg}")

            return {
                "entity_id": lock.entity_id,
                "slot_number": slot_number,
                "code": code,
            }

    async def clear_slot_code(self, lock_entity_id: str, slot_number: int) -> dict:
        """Clear a specific slot."""
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).options(selectinload(Lock.code_slots))
                .where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()
            if not lock:
                raise ValueError(f"Lock {lock_entity_id} not found")

            success = True
            error_msg = None
            try:
                await self._ha_clear_code(lock.entity_id, slot_number)
            except Exception as e:
                success = False
                error_msg = str(e)
                logger.error(f"Failed to clear slot {slot_number} on {lock.entity_id}: {e}")

            # Update DB
            slot = next((s for s in lock.code_slots if s.slot_number == slot_number), None)
            if slot and success:
                slot.current_code = None
            if slot_number == MASTER_CODE_SLOT and success:
                lock.master_code = None
            elif slot_number == EMERGENCY_CODE_SLOT and success:
                lock.emergency_code = None

            session.add(AuditLog(
                action="clear_slot_code",
                lock_id=lock.id,
                slot_number=slot_number,
                success=success,
                error_message=error_msg,
            ))
            await session.commit()

            if not success:
                raise ValueError(f"Failed to clear slot: {error_msg}")

            return {
                "entity_id": lock.entity_id,
                "slot_number": slot_number,
            }

    async def disable_booking_code(self, booking_id: int) -> dict:
        """Disable a guest's code — clear from all locks immediately."""
        from rental_manager.scheduler.scheduler import JobType

        async with get_session_context() as session:
            result = await session.execute(
                select(Booking)
                .options(
                    selectinload(Booking.code_assignments)
                    .joinedload(CodeAssignment.code_slot)
                    .joinedload(CodeSlot.lock),
                )
                .where(Booking.id == booking_id)
            )
            booking = result.scalar_one_or_none()
            if not booking:
                raise ValueError(f"Booking {booking_id} not found")
            if booking.code_disabled:
                return {"booking_id": booking_id, "status": "already_disabled"}

            booking.code_disabled = True
            booking.code_disabled_at = datetime.utcnow()

            cleared_count = 0
            for assignment in booking.code_assignments:
                lock = assignment.code_slot.lock
                slot = assignment.code_slot
                slot_num = slot.slot_number
                if assignment.is_active:
                    try:
                        await self._ha_clear_code(lock.entity_id, slot_num)
                        slot.current_code = None
                        slot.sync_state = CodeSyncState.IDLE.value
                        assignment.is_active = False
                        cleared_count += 1
                    except Exception as e:
                        logger.error(
                            f"Failed to clear slot {slot_num} on {lock.entity_id} "
                            f"while disabling booking {booking_id}: {e}"
                        )

            # Cancel pending activation jobs
            if self._scheduler:
                jobs = self._scheduler.get_jobs_for_booking(booking.uid)
                for job in jobs:
                    if job.job_type == JobType.ACTIVATE_CODE:
                        self._scheduler.cancel_job(job.job_id)

            session.add(AuditLog(
                action="code_disabled",
                booking_id=booking.id,
                details=f"Disabled code for {booking.guest_name}, cleared {cleared_count} lock(s)",
                success=True,
            ))
            await session.commit()

        return {
            "booking_id": booking_id,
            "status": "disabled",
            "locks_cleared": cleared_count,
        }

    async def enable_booking_code(self, booking_id: int) -> dict:
        """Re-enable a guest's code — set on all locks if within active window."""
        async with get_session_context() as session:
            result = await session.execute(
                select(Booking)
                .options(
                    selectinload(Booking.calendar),
                    selectinload(Booking.code_assignments)
                    .joinedload(CodeAssignment.code_slot)
                    .joinedload(CodeSlot.lock),
                )
                .where(Booking.id == booking_id)
            )
            booking = result.scalar_one_or_none()
            if not booking:
                raise ValueError(f"Booking {booking_id} not found")
            if not booking.code_disabled:
                return {"booking_id": booking_id, "status": "already_enabled"}

            booking.code_disabled = False
            booking.code_disabled_at = None

            code = booking.locked_code or generate_code_from_phone(booking.phone)
            if not code:
                await session.commit()
                return {
                    "booking_id": booking_id,
                    "status": "enabled_no_code",
                    "message": "No code available (no phone number)",
                }

            now = datetime.utcnow()
            activated_count = 0
            rescheduled_count = 0

            for assignment in booking.code_assignments:
                lock = assignment.code_slot.lock
                slot = assignment.code_slot
                slot_num = slot.slot_number

                if now >= assignment.activate_at and now < assignment.deactivate_at:
                    # Within active window — re-set immediately
                    try:
                        await self._set_code_with_retry(lock.entity_id, slot_num, code)
                        assignment.is_active = True
                        slot.current_code = code
                        slot.sync_state = CodeSyncState.ACTIVE.value
                        activated_count += 1
                    except Exception as e:
                        logger.error(
                            f"Failed to re-set slot {slot_num} on {lock.entity_id} "
                            f"while enabling booking {booking_id}: {e}"
                        )
                elif now < assignment.activate_at:
                    # Future activation — reschedule
                    if self._scheduler:
                        self._scheduler.reschedule_activation(
                            lock.entity_id, slot_num, booking.uid,
                            assignment.activate_at, code,
                        )
                        rescheduled_count += 1

            session.add(AuditLog(
                action="code_enabled",
                booking_id=booking.id,
                details=(
                    f"Re-enabled code for {booking.guest_name}, "
                    f"activated {activated_count}, rescheduled {rescheduled_count}"
                ),
                success=True,
            ))
            await session.commit()

        return {
            "booking_id": booking_id,
            "status": "enabled",
            "locks_activated": activated_count,
            "locks_rescheduled": rescheduled_count,
        }

    async def _load_booking_with_assignments(
        self, session: AsyncSession, booking_id: int
    ) -> Booking | None:
        """Load a booking with code_assignments (including code_slot.lock) and calendar."""
        result = await session.execute(
            select(Booking)
            .options(
                selectinload(Booking.code_assignments)
                .joinedload(CodeAssignment.code_slot)
                .joinedload(CodeSlot.lock),
                selectinload(Booking.calendar),
            )
            .where(Booking.id == booking_id)
        )
        return result.scalar_one_or_none()

    async def _ensure_code_assignments(
        self, session: AsyncSession, booking: Booking, code: str
    ) -> list[CodeAssignment]:
        """Create code assignments if none exist, then ensure they're loaded.

        After calling this, booking.code_assignments is guaranteed to be
        populated with code_slot.lock relationships loaded.
        """
        if booking.code_assignments:
            return []

        new_assignments = await self._create_code_assignments(session, booking, code)
        if new_assignments:
            # Refresh the collection so booking.code_assignments includes new ones
            await session.refresh(booking, ["code_assignments"])
            # The new assignments already have code_slot.lock loaded from
            # _create_code_assignments, but the refresh replaces the collection
            # with lazy proxies. Re-load them properly.
            for a in booking.code_assignments:
                await session.refresh(a, ["code_slot"])
                await session.refresh(a.code_slot, ["lock"])
        return new_assignments

    async def set_booking_code(self, booking_id: int, code: str) -> dict:
        """Manually set (override) the PIN code for a booking.

        Updates the locked_code on the booking and re-sets the code on all
        currently active lock slots. Creates code assignments if none exist
        (e.g. booking had no phone number initially).
        """
        async with get_session_context() as session:
            booking = await self._load_booking_with_assignments(session, booking_id)
            if not booking:
                raise ValueError(f"Booking {booking_id} not found")

            old_code = booking.locked_code or generate_code_from_phone(booking.phone)
            booking.locked_code = code
            booking.code_locked_at = datetime.utcnow()

            await self._ensure_code_assignments(session, booking, code)

            updated_count = 0
            rescheduled_count = 0
            now = datetime.utcnow()

            for assignment in booking.code_assignments:
                lock = assignment.code_slot.lock
                slot = assignment.code_slot
                slot_num = slot.slot_number

                assignment.code = code

                if now >= assignment.activate_at and now < assignment.deactivate_at:
                    # Within active window — re-set on lock immediately
                    if not booking.code_disabled:
                        try:
                            await self._set_code_with_retry(lock.entity_id, slot_num, code)
                            assignment.is_active = True
                            slot.current_code = code
                            slot.sync_state = CodeSyncState.ACTIVE.value
                            updated_count += 1
                        except Exception as e:
                            logger.error(
                                f"Failed to update code on {lock.entity_id} slot {slot_num} "
                                f"for booking {booking_id}: {e}"
                            )
                elif now < assignment.activate_at:
                    # Future activation — reschedule with new code
                    if self._scheduler and not booking.code_disabled:
                        self._scheduler.reschedule_activation(
                            lock.entity_id, slot_num, booking.uid,
                            assignment.activate_at, code,
                        )
                        rescheduled_count += 1

            session.add(AuditLog(
                action="booking_code_set",
                booking_id=booking.id,
                code=code,
                details=f"Manual code set for {booking.guest_name} (was: {old_code})",
                success=True,
            ))
            await session.commit()

        return {
            "booking_id": booking_id,
            "code": code,
            "locks_updated": updated_count,
            "locks_rescheduled": rescheduled_count,
        }

    async def recode_booking(self, booking_id: int) -> dict:
        """Re-send codes to all assigned locks for a booking.

        Only works if the booking is within its active time window.
        Creates code assignments if none exist.
        """
        async with get_session_context() as session:
            booking = await self._load_booking_with_assignments(session, booking_id)
            if not booking:
                raise ValueError(f"Booking {booking_id} not found")

            if booking.code_disabled:
                return {
                    "booking_id": booking_id,
                    "status": "disabled",
                    "message": "Cannot recode — booking code is disabled. Enable it first.",
                }

            code = booking.locked_code or generate_code_from_phone(booking.phone)
            if not code:
                return {
                    "booking_id": booking_id,
                    "status": "no_code",
                    "message": "No code available (no phone number and no manual code set).",
                }

            await self._ensure_code_assignments(session, booking, code)

            now = datetime.utcnow()
            recoded_count = 0
            skipped_count = 0
            errors = []

            for assignment in booking.code_assignments:
                lock = assignment.code_slot.lock
                slot = assignment.code_slot
                slot_num = slot.slot_number

                if now >= assignment.activate_at and now < assignment.deactivate_at:
                    try:
                        await self._set_code_with_retry(lock.entity_id, slot_num, code)
                        assignment.is_active = True
                        assignment.code = code
                        slot.current_code = code
                        slot.sync_state = CodeSyncState.ACTIVE.value
                        recoded_count += 1
                        logger.info(
                            f"Recoded {lock.entity_id} slot {slot_num} "
                            f"for booking {booking.guest_name}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to recode {lock.entity_id} slot {slot_num} "
                            f"for booking {booking_id}: {e}"
                        )
                        errors.append(f"{lock.name} slot {slot_num}: {e}")
                else:
                    skipped_count += 1

            if recoded_count == 0 and skipped_count > 0 and not errors:
                return {
                    "booking_id": booking_id,
                    "status": "outside_window",
                    "message": "No locks are within the active time window for this booking.",
                }

            session.add(AuditLog(
                action="booking_recoded",
                booking_id=booking.id,
                code=code,
                details=(
                    f"Recoded {recoded_count} lock(s) for {booking.guest_name}"
                    + (f", {len(errors)} failed" if errors else "")
                    + (f", {skipped_count} outside window" if skipped_count else "")
                ),
                success=len(errors) == 0,
                error_message="; ".join(errors) if errors else None,
            ))
            await session.commit()

        return {
            "booking_id": booking_id,
            "status": "recoded",
            "locks_recoded": recoded_count,
            "locks_skipped": skipped_count,
            "errors": errors,
        }

    async def _backup_emergency_codes(self) -> None:
        """Backup emergency codes to /share/ (synced by Google Drive Backup) and optionally Google Sheets."""
        import json
        from pathlib import Path

        codes = await self.get_emergency_codes()

        # Always save to /share/ for Google Drive Backup add-on
        try:
            backup_dir = Path("/share/rental_manager")
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_file = backup_dir / "emergency_codes.json"
            backup_data = {
                "updated_at": datetime.utcnow().isoformat() + "Z",
                "house_code": self.settings.house_code,
                "codes": codes,
            }
            backup_file.write_text(json.dumps(backup_data, indent=2))
            logger.info("Emergency codes saved to %s", backup_file)
        except Exception as e:
            logger.error(f"Failed to save emergency codes to /share/: {e}")

        # Optionally sync to Google Sheets
        try:
            from rental_manager.core.sheets_backup import SheetsBackup
            creds_path = self.settings.google_sheets_credentials
            spreadsheet_id = self.settings.google_sheets_spreadsheet_id
            if not creds_path or not spreadsheet_id:
                return
            backup = SheetsBackup(creds_path, spreadsheet_id)
            backup.update_emergency_codes(codes)
            logger.info("Emergency codes backed up to Google Sheets")
        except Exception as e:
            logger.error(f"Failed to backup emergency codes to Google Sheets: {e}")

    async def _upsert_time_override(
        self,
        session: AsyncSession,
        booking_id: int,
        lock_id: int,
        activate_at: Optional[datetime] = None,
        deactivate_at: Optional[datetime] = None,
        notes: Optional[str] = None,
    ) -> TimeOverride:
        """Create or update a time override for a single lock (internal helper)."""
        result = await session.execute(
            select(TimeOverride).where(
                TimeOverride.booking_id == booking_id,
                TimeOverride.lock_id == lock_id,
            )
        )
        override = result.scalar_one_or_none()

        if override:
            if activate_at:
                override.activate_at = activate_at
            if deactivate_at:
                override.deactivate_at = deactivate_at
            if notes:
                override.notes = notes
        else:
            override = TimeOverride(
                booking_id=booking_id,
                lock_id=lock_id,
                activate_at=activate_at,
                deactivate_at=deactivate_at,
                notes=notes,
            )
            session.add(override)

        return override

    async def set_time_override(
        self,
        booking_id: int,
        lock_id: int,
        activate_at: Optional[datetime] = None,
        deactivate_at: Optional[datetime] = None,
        notes: Optional[str] = None,
    ) -> dict:
        """Set a time override for a booking on a specific lock.

        If the lock is a room lock, bathroom locks for the same booking
        are automatically updated to match (pegged timing).
        """
        # Strip timezone info — the system uses naive datetimes (UTC)
        if activate_at and activate_at.tzinfo is not None:
            activate_at = activate_at.replace(tzinfo=None)
        if deactivate_at and deactivate_at.tzinfo is not None:
            deactivate_at = deactivate_at.replace(tzinfo=None)
        async with get_session_context() as session:
            override = await self._upsert_time_override(
                session, booking_id, lock_id, activate_at, deactivate_at, notes,
            )
            await session.flush()

            # If this is a room lock, sync bathroom locks to the same times
            lock_result = await session.execute(select(Lock).where(Lock.id == lock_id))
            target_lock = lock_result.scalar_one_or_none()

            if target_lock and target_lock.lock_type == LockType.ROOM.value:
                # Find bathroom locks that share a calendar with this booking
                booking_result = await session.execute(
                    select(Booking)
                    .options(selectinload(Booking.calendar))
                    .where(Booking.id == booking_id)
                )
                booking = booking_result.scalar_one_or_none()
                if booking:
                    bath_locks = await session.execute(
                        select(Lock)
                        .join(LockCalendar)
                        .where(
                            LockCalendar.calendar_id == booking.calendar.id,
                            Lock.lock_type == LockType.BATHROOM.value,
                        )
                    )
                    for bath_lock in bath_locks.scalars().all():
                        await self._upsert_time_override(
                            session, booking_id, bath_lock.id,
                            activate_at, deactivate_at, notes,
                        )
                        logger.info(
                            f"Synced bathroom lock {bath_lock.entity_id} override "
                            f"to match room lock {target_lock.entity_id}"
                        )

            # Reschedule if scheduler is running
            if self._scheduler:
                booking_result = await session.execute(
                    select(Booking)
                    .options(selectinload(Booking.calendar))
                    .where(Booking.id == booking_id)
                )
                booking = booking_result.scalar_one_or_none()

                # Collect all locks to reschedule (target + synced bathrooms)
                locks_to_reschedule = [target_lock] if target_lock else []
                if target_lock and target_lock.lock_type == LockType.ROOM.value and booking:
                    bath_result = await session.execute(
                        select(Lock)
                        .join(LockCalendar)
                        .where(
                            LockCalendar.calendar_id == booking.calendar.id,
                            Lock.lock_type == LockType.BATHROOM.value,
                        )
                    )
                    locks_to_reschedule.extend(bath_result.scalars().all())

                if booking and (booking.phone or booking.locked_code):
                    code = booking.locked_code or generate_code_from_phone(booking.phone)
                    slot_a, slot_b = get_slot_for_calendar(booking.calendar.calendar_id)
                    slot_number = slot_a

                    for lock in locks_to_reschedule:
                        if code and activate_at:
                            self._scheduler.reschedule_activation(
                                lock.entity_id, slot_number, booking.uid, activate_at, code
                            )
                        if deactivate_at:
                            self._scheduler.reschedule_deactivation(
                                lock.entity_id, slot_number, booking.uid, deactivate_at
                            )

            # Update CodeAssignment activate_at/deactivate_at to match override
            lock_ids = (
                [lk.id for lk in locks_to_reschedule]
                if locks_to_reschedule
                else ([lock_id] if target_lock else [])
            )
            if lock_ids:
                assign_result = await session.execute(
                    select(CodeAssignment)
                    .join(CodeSlot)
                    .where(
                        CodeAssignment.booking_id == booking_id,
                        CodeSlot.lock_id.in_(lock_ids),
                    )
                )
                for assignment in assign_result.scalars().all():
                    if activate_at:
                        assignment.activate_at = activate_at
                    if deactivate_at:
                        assignment.deactivate_at = deactivate_at

            await session.commit()

            return {
                "id": override.id,
                "booking_id": booking_id,
                "lock_id": lock_id,
                "activate_at": override.activate_at.isoformat() if override.activate_at else None,
                "deactivate_at": override.deactivate_at.isoformat() if override.deactivate_at else None,
                "notes": override.notes,
            }

    async def resync_all_codes(self) -> dict:
        """Re-sync all lock codes: set codes that should be active, clear those that shouldn't.

        Ensures the physical Z-Wave locks match the expected state from assignments.
        """
        now = datetime.utcnow()
        set_count = 0
        clear_count = 0
        errors = []

        async with get_session_context() as session:
            # Get all locks with their slots and assignments
            result = await session.execute(
                select(Lock).options(
                    selectinload(Lock.code_slots).selectinload(CodeSlot.assignments).joinedload(CodeAssignment.booking)
                )
            )
            locks = result.unique().scalars().all()

            for lock in locks:
                for slot in lock.code_slots:
                    if slot.slot_number in (MASTER_CODE_SLOT, EMERGENCY_CODE_SLOT):
                        continue

                    # Find active assignment for this slot
                    active_assignment = None
                    for a in slot.assignments:
                        if a.code and a.activate_at <= now < a.deactivate_at:
                            if a.booking and not a.booking.code_disabled:
                                active_assignment = a
                                break

                    if active_assignment:
                        # Should be coded — send set_code
                        code = active_assignment.code
                        try:
                            if self._sync_manager:
                                await self._sync_manager.set_code(
                                    lock.entity_id, slot.slot_number, code,
                                    active_assignment.booking.uid if active_assignment.booking else ""
                                )
                            slot.current_code = code
                            slot.sync_state = CodeSyncState.ACTIVE.value
                            set_count += 1
                            logger.info(f"Re-sync: set {code} on {lock.entity_id} slot {slot.slot_number}")
                        except Exception as e:
                            errors.append(f"{lock.entity_id} slot {slot.slot_number}: {e}")
                    else:
                        # Should be empty — send clear
                        try:
                            if self._sync_manager:
                                await self._sync_manager.clear_code(
                                    lock.entity_id, slot.slot_number, ""
                                )
                            slot.current_code = None
                            slot.sync_state = CodeSyncState.IDLE.value
                            clear_count += 1
                            logger.info(f"Re-sync: cleared {lock.entity_id} slot {slot.slot_number}")
                        except Exception as e:
                            errors.append(f"{lock.entity_id} slot {slot.slot_number}: {e}")

        return {"set": set_count, "cleared": clear_count, "errors": errors}

    async def lock_action(self, lock_entity_id: str, action: str) -> dict:
        """Perform a lock/unlock action."""
        if action == "lock":
            await self._ha_client.lock(lock_entity_id)
        elif action == "unlock":
            await self._ha_client.unlock(lock_entity_id)
        else:
            raise ValueError(f"Unknown action: {action}")

        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()

            audit = AuditLog(
                action=f"lock_{action}",
                lock_id=lock.id if lock else None,
                success=True,
            )
            session.add(audit)
            await session.commit()

        return {"entity_id": lock_entity_id, "action": action, "success": True}

    async def set_auto_lock(self, lock_entity_id: str, enabled: bool) -> dict:
        """Enable or disable auto-lock on a lock."""
        await self._ha_client.set_auto_lock(lock_entity_id, enabled)
        # Persist to DB
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()
            if lock:
                lock.auto_lock_enabled = enabled
                session.add(AuditLog(
                    action="auto_lock_changed",
                    lock_id=lock.id,
                    details=f"Auto-lock {'enabled' if enabled else 'disabled'} on {lock.name}",
                    success=True,
                ))
                await session.commit()
        return {"entity_id": lock_entity_id, "auto_lock": enabled}

    async def set_volume(self, lock_entity_id: str, level: str) -> dict:
        """Set the volume level on a lock."""
        await self._ha_client.set_volume(lock_entity_id, level)
        # Persist to DB
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()
            if lock:
                lock.volume_level = level
                session.add(AuditLog(
                    action="volume_changed",
                    lock_id=lock.id,
                    details=f"Volume set to '{level}' on {lock.name}",
                    success=True,
                ))
                await session.commit()
        return {"entity_id": lock_entity_id, "volume": level}

    async def set_volume_all(self, level: str) -> dict:
        """Set volume on ALL locks for this house."""
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).join(HouseModel).where(HouseModel.code == self.settings.house_code)
            )
            locks = result.scalars().all()
            results = []
            errors = []
            for lock in locks:
                try:
                    await self._ha_client.set_volume(lock.entity_id, level)
                    lock.volume_level = level
                    session.add(AuditLog(
                        action="volume_changed",
                        lock_id=lock.id,
                        details=f"Volume set to '{level}' on {lock.name} (bulk)",
                        success=True,
                    ))
                    results.append(lock.entity_id)
                except Exception as e:
                    logger.error("Failed to set volume on %s: %s", lock.entity_id, e)
                    errors.append({"entity_id": lock.entity_id, "error": str(e)})
            await session.commit()
        return {"level": level, "set": len(results), "errors": errors}

    async def set_auto_lock_all(self, enabled: bool) -> dict:
        """Set auto-lock on all internal locks (not front/back) for this house."""
        INTERNAL_TYPES = ("room", "bathroom", "kitchen", "storage")
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).join(HouseModel).where(HouseModel.code == self.settings.house_code)
            )
            locks = [l for l in result.scalars().all() if l.lock_type in INTERNAL_TYPES]
            results = []
            errors = []
            for lock in locks:
                try:
                    await self._ha_client.set_auto_lock(lock.entity_id, enabled)
                    lock.auto_lock_enabled = enabled
                    session.add(AuditLog(
                        action="auto_lock_changed",
                        lock_id=lock.id,
                        details=f"Auto-lock {'enabled' if enabled else 'disabled'} on {lock.name} (bulk)",
                        success=True,
                    ))
                    results.append(lock.entity_id)
                except Exception as e:
                    logger.error("Failed to set auto-lock on %s: %s", lock.entity_id, e)
                    errors.append({"entity_id": lock.entity_id, "error": str(e)})
            await session.commit()
        return {"enabled": enabled, "set": len(results), "errors": errors}

    async def get_sync_status(self) -> dict:
        """Get the current sync status of all slots."""
        if not self._sync_manager:
            return {"error": "Sync manager not initialized"}

        states = self._sync_manager.get_all_states()
        failed = self._sync_manager.get_failed_slots()
        syncing = self._sync_manager.get_syncing_slots()

        # Look up booking names for failed slots
        failed_info = []
        for s in failed:
            info = {
                "lock_entity_id": s.lock_entity_id,
                "slot_number": s.slot_number,
                "error": s.last_error,
                "retry_count": s.retry_count,
                "target_code": s.target_code,
                "booking_uid": s.booking_uid,
            }
            # Try to get guest name from booking
            if s.booking_uid:
                try:
                    async with get_session_context() as session:
                        result = await session.execute(
                            select(Booking).where(Booking.uid == s.booking_uid)
                        )
                        booking = result.scalar_one_or_none()
                        if booking:
                            info["guest_name"] = booking.guest_name
                            info["booking_id"] = booking.id
                except Exception:
                    pass
            failed_info.append(info)

        return {
            "total_slots": len(states),
            "failed_count": len(failed) + len(self._failed_ops),
            "syncing_count": len(syncing),
            "failed_slots": failed_info,
            "failed_ops": list(self._failed_ops),
            "syncing_slots": [
                {
                    "lock_entity_id": s.lock_entity_id,
                    "slot_number": s.slot_number,
                    "state": s.state.value,
                    "started_at": s.started_at.isoformat() if s.started_at else None,
                }
                for s in syncing
            ],
        }

    async def retry_failed_slot(
        self, lock_entity_id: str, slot_number: int
    ) -> dict:
        """Retry a failed sync on a specific slot.

        Resets the failed state and re-attempts the set/clear operation.
        """
        if not self._sync_manager:
            raise ValueError("Sync manager not initialized")

        slot = self._sync_manager.get_slot_state(lock_entity_id, slot_number)
        if slot.state != SyncState.FAILED:
            raise ValueError(
                f"Slot {lock_entity_id}:{slot_number} is not in failed state "
                f"(current: {slot.state.value})"
            )

        target_code = slot.target_code
        booking_uid = slot.booking_uid

        # Reset the failed slot
        self._sync_manager.reset_failed_slot(lock_entity_id, slot_number)

        # Re-attempt the operation
        if target_code:
            logger.info(
                f"Manual retry: setting code on {lock_entity_id} slot {slot_number}"
            )
            result = await self._sync_manager.set_code(
                lock_entity_id, slot_number, target_code, booking_uid or ""
            )
        else:
            logger.info(
                f"Manual retry: clearing code on {lock_entity_id} slot {slot_number}"
            )
            result = await self._sync_manager.clear_code(
                lock_entity_id, slot_number, booking_uid or ""
            )

        return {
            "lock_entity_id": lock_entity_id,
            "slot_number": slot_number,
            "success": result.success,
            "state": result.state.value,
            "error": result.error,
        }

    async def retry_all_failed(self) -> dict:
        """Retry all failed sync slots."""
        if not self._sync_manager:
            raise ValueError("Sync manager not initialized")

        failed = self._sync_manager.get_failed_slots()
        results = []
        for s in failed:
            try:
                result = await self.retry_failed_slot(s.lock_entity_id, s.slot_number)
                results.append(result)
            except Exception as e:
                results.append({
                    "lock_entity_id": s.lock_entity_id,
                    "slot_number": s.slot_number,
                    "success": False,
                    "error": str(e),
                })

        return {
            "retried": len(results),
            "results": results,
        }

    # --- Failed operations tracker (auto-lock, lock/unlock) ---

    def _record_failed_op(
        self,
        lock_entity_id: str,
        lock_name: str,
        action: str,
        error: str,
        reason: str,
    ) -> None:
        """Record a failed non-code operation for display in the UI."""
        self._failed_ops_counter += 1
        self._failed_ops.append({
            "id": self._failed_ops_counter,
            "lock_entity_id": lock_entity_id,
            "lock_name": lock_name,
            "action": action,  # "auto-lock on", "auto-lock off", "lock", "unlock"
            "error": error,
            "retry_count": 0,
            "reason": reason,
            "failed_at": datetime.utcnow().isoformat(),
        })
        logger.info(
            f"Recorded failed op #{self._failed_ops_counter}: "
            f"{action} on {lock_entity_id} — {error}"
        )

    async def retry_failed_op(self, op_id: int) -> dict:
        """Retry a failed non-code operation (auto-lock, lock, unlock)."""
        op = next((o for o in self._failed_ops if o["id"] == op_id), None)
        if not op:
            raise ValueError(f"Failed operation {op_id} not found")

        lock_entity_id = op["lock_entity_id"]
        action = op["action"]
        op["retry_count"] += 1

        try:
            if action.startswith("auto-lock"):
                enabled = action == "auto-lock on"
                await self._ha_client.set_auto_lock(lock_entity_id, enabled)
            elif action == "lock":
                await self._ha_client.lock(lock_entity_id)
            elif action == "unlock":
                await self._ha_client.unlock(lock_entity_id)
            else:
                raise ValueError(f"Unknown action: {action}")

            logger.info(f"Retry succeeded for op #{op_id}: {action} on {lock_entity_id}")

            # Log success to audit and update DB state
            async with get_session_context() as session:
                result = await session.execute(
                    select(Lock).where(Lock.entity_id == lock_entity_id)
                )
                lock = result.scalar_one_or_none()
                if lock and action.startswith("auto-lock"):
                    lock.auto_lock_enabled = (action == "auto-lock on")
                session.add(AuditLog(
                    action=f"retry_{action.replace(' ', '_').replace('-', '_')}",
                    lock_id=lock.id if lock else None,
                    details=f"Manual retry succeeded (attempt {op['retry_count']})",
                    success=True,
                ))

            # Remove from failed list on success
            self._failed_ops = [o for o in self._failed_ops if o["id"] != op_id]

            return {"op_id": op_id, "success": True}

        except Exception as e:
            op["error"] = str(e)
            logger.error(f"Retry failed for op #{op_id}: {action} on {lock_entity_id}: {e}")
            return {"op_id": op_id, "success": False, "error": str(e)}

    async def retry_all_failed_ops(self) -> dict:
        """Retry all failed non-code operations."""
        results = []
        for op in list(self._failed_ops):
            result = await self.retry_failed_op(op["id"])
            results.append(result)
        return {
            "retried": len(results),
            "results": results,
        }

    def dismiss_failed_op(self, op_id: int) -> dict:
        """Dismiss a failed operation from the UI without retrying."""
        op = next((o for o in self._failed_ops if o["id"] == op_id), None)
        if not op:
            raise ValueError(f"Failed operation {op_id} not found")
        self._failed_ops = [o for o in self._failed_ops if o["id"] != op_id]
        return {"op_id": op_id, "dismissed": True}

    async def _on_ws_lock_event(
        self,
        entity_id: str,
        code_slot: Optional[int],
        method: str,
        event_label: str,
    ) -> None:
        """Callback from the HA websocket event listener for Z-Wave lock events."""
        logger.info(
            "WS lock event: entity=%s slot=%s method=%s label=%s",
            entity_id, code_slot, method, event_label,
        )
        try:
            await self.record_unlock_event(
                entity_id=entity_id,
                code_slot=code_slot,
                method=method,
            )
        except Exception as e:
            logger.error("Failed to record WS lock event: %s", e)

    async def record_unlock_event(
        self,
        entity_id: str,
        code_slot: Optional[int] = None,
        method: str = "unknown",
        timestamp: Optional[datetime] = None,
        raw_details: Optional[str] = None,
    ) -> dict:
        """Record a lock unlock event and correlate to a guest booking.

        Called by the webhook endpoint when HA fires a lock unlock event.
        """
        import json as json_mod

        ts = timestamp or datetime.utcnow()

        async with get_session_context() as session:
            # Look up lock
            result = await session.execute(
                select(Lock).where(Lock.entity_id == entity_id)
            )
            lock = result.scalar_one_or_none()
            if not lock:
                logger.warning(f"Unlock event for unknown lock: {entity_id}")
                return {"error": f"Unknown lock: {entity_id}"}

            # Correlate to a booking via CodeAssignment
            booking_id = None
            guest_name = None

            if code_slot is not None:
                if code_slot == MASTER_CODE_SLOT:
                    guest_name = "Master Code"
                elif code_slot == EMERGENCY_CODE_SLOT:
                    guest_name = "Emergency Code"
                else:
                    # Find active CodeAssignment for this lock + slot
                    assignment_result = await session.execute(
                        select(CodeAssignment)
                        .join(CodeSlot)
                        .options(joinedload(CodeAssignment.booking))
                        .where(
                            CodeSlot.lock_id == lock.id,
                            CodeSlot.slot_number == code_slot,
                            CodeAssignment.is_active == True,
                        )
                    )
                    assignment = assignment_result.scalar_one_or_none()
                    if assignment and assignment.booking:
                        booking_id = assignment.booking.id
                        guest_name = assignment.booking.guest_name

            event = UnlockEvent(
                timestamp=ts,
                lock_id=lock.id,
                slot_number=code_slot,
                booking_id=booking_id,
                guest_name=guest_name,
                method=method,
                details=raw_details,
            )
            session.add(event)
            await session.flush()

            logger.info(
                f"Unlock event recorded: {entity_id} slot={code_slot} "
                f"guest={guest_name or 'unknown'} method={method}"
            )

            return {
                "id": event.id,
                "timestamp": event.timestamp.isoformat(),
                "lock_entity_id": entity_id,
                "lock_name": lock.name,
                "slot_number": code_slot,
                "booking_id": booking_id,
                "guest_name": guest_name,
                "method": method,
            }

    async def get_unlock_history(
        self,
        lock_entity_id: Optional[str] = None,
        booking_id: Optional[int] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Get unlock event history, optionally filtered."""
        async with get_session_context() as session:
            query = (
                select(UnlockEvent)
                .join(Lock)
                .order_by(UnlockEvent.timestamp.desc())
            )

            if lock_entity_id:
                query = query.where(Lock.entity_id == lock_entity_id)
            if booking_id:
                query = query.where(UnlockEvent.booking_id == booking_id)
            if from_date:
                query = query.where(UnlockEvent.timestamp >= datetime.combine(from_date, time(0, 0)))
            if to_date:
                query = query.where(UnlockEvent.timestamp <= datetime.combine(to_date, time(23, 59, 59)))

            query = query.offset(offset).limit(limit)
            result = await session.execute(query)
            events = result.scalars().all()

            # We need lock names; load them
            lock_ids = {e.lock_id for e in events}
            lock_result = await session.execute(
                select(Lock).where(Lock.id.in_(lock_ids))
            )
            lock_map = {l.id: l for l in lock_result.scalars().all()}

            return [
                {
                    "id": e.id,
                    "timestamp": e.timestamp.isoformat(),
                    "lock_entity_id": lock_map[e.lock_id].entity_id if e.lock_id in lock_map else None,
                    "lock_name": lock_map[e.lock_id].name if e.lock_id in lock_map else None,
                    "slot_number": e.slot_number,
                    "booking_id": e.booking_id,
                    "guest_name": e.guest_name,
                    "method": e.method,
                }
                for e in events
            ]

    async def health_check(self) -> dict:
        """Perform a health check on all components."""
        ha_healthy = await self._ha_client.health_check()

        return {
            "running": self._running,
            "house_code": self.settings.house_code,
            "home_assistant": ha_healthy,
            "scheduler_running": self._scheduler is not None,
            "sync_manager_running": self._sync_manager is not None,
        }
