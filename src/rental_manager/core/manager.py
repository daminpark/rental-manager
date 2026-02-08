"""Main rental manager that orchestrates all components."""

import asyncio
import random
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
    MASTER_CODE_SLOT,
    EMERGENCY_CODE_SLOT,
)
from rental_manager.core.code_manager import (
    SlotAllocator,
    calculate_code_times,
    generate_code_from_phone,
)
from rental_manager.core.ical_parser import ICalFetcher, ParsedBooking
from rental_manager.core.sync_manager import SyncManager, SyncState
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
)
from rental_manager.ha.client import HomeAssistantClient
from rental_manager.scheduler.scheduler import CodeScheduler, CodeScheduleEntry

logger = logging.getLogger(__name__)


class RentalManager:
    """Main rental manager coordinating all operations."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._ical_fetcher = ICalFetcher()
        self._ha_client = HomeAssistantClient(settings.ha_url, settings.ha_token)
        self._slot_allocator = SlotAllocator()
        self._scheduler: Optional[CodeScheduler] = None
        self._sync_manager: Optional[SyncManager] = None
        self._running = False
        self._polling = False

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

        # Initial calendar poll
        await self._poll_calendars()

        logger.info("Rental manager started")

    async def stop(self) -> None:
        """Stop the manager."""
        self._running = False

        if self._scheduler:
            self._scheduler.stop()

        if self._sync_manager:
            self._sync_manager.stop()

        await self._ha_client.close()
        await self._ical_fetcher.close()

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
        """Set a code on a lock via Home Assistant."""
        await self._ha_client.set_lock_usercode(lock_entity_id, slot_number, code)

    async def _ha_clear_code(self, lock_entity_id: str, slot_number: int) -> None:
        """Clear a code from a lock via Home Assistant."""
        await self._ha_client.clear_lock_usercode(lock_entity_id, slot_number)

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

        # Log to audit
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()

            audit = AuditLog(
                action="code_sync_failed",
                lock_id=lock.id if lock else None,
                slot_number=slot_number,
                code=code,
                success=False,
                error_message=error,
            )
            session.add(audit)

        await self._notify_failure(
            f"Code sync FAILED on {lock_entity_id} slot {slot_number} "
            f"after all retries. Code: {code}. Error: {error}"
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

        Retries each lock up to 3 times. Notifies on failure.
        """
        action_desc = f"auto-lock {'on' if auto_lock else 'off'} + {lock_action}"
        logger.info(f"Internal locks: {action_desc} — {reason}")

        INTERNAL_TYPES = ("room", "bathroom", "kitchen", "storage")

        async with get_session_context() as session:
            result = await session.execute(select(Lock))
            locks = [l for l in result.scalars().all() if l.lock_type in INTERNAL_TYPES]

            failures = []
            for lock in locks:
                # Set auto-lock
                al_success = False
                al_error = None
                for attempt in range(4):
                    try:
                        await self._ha_client.set_auto_lock(lock.entity_id, auto_lock)
                        al_success = True
                        break
                    except Exception as e:
                        al_error = str(e)
                        if attempt < 3:
                            await asyncio.sleep(5 * (attempt + 1))

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
                        break
                    except Exception as e:
                        la_error = str(e)
                        if attempt < 3:
                            await asyncio.sleep(5 * (attempt + 1))

                if not al_success:
                    failures.append((lock.entity_id, f"auto-lock: {al_error}"))
                if not la_success:
                    failures.append((lock.entity_id, f"{lock_action}: {la_error}"))

                audit = AuditLog(
                    action=f"whole_house_{lock_action}",
                    lock_id=lock.id,
                    details=reason,
                    success=al_success and la_success,
                    error_message=al_error or la_error,
                )
                session.add(audit)

            await session.commit()

        if failures:
            failed_desc = "; ".join(f"{f[0]}: {f[1]}" for f in failures)
            await self._notify_failure(
                f"Whole-house lock routine failed — {failed_desc}. Reason: {reason}"
            )

    async def _on_code_activate(
        self, lock_entity_id: str, slot_number: int, code: str, booking_uid: str
    ) -> None:
        """Activate a code on a lock."""
        logger.info(
            f"Activating code on {lock_entity_id} slot {slot_number} "
            f"for booking {booking_uid}"
        )

        if self._sync_manager:
            await self._sync_manager.set_code(
                lock_entity_id, slot_number, code, booking_uid
            )

        # Log to audit
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()

            audit = AuditLog(
                action="code_activated",
                lock_id=lock.id if lock else None,
                slot_number=slot_number,
                code=code,
                details=f"Booking: {booking_uid}",
                success=True,
            )
            session.add(audit)

    async def _on_code_deactivate(
        self, lock_entity_id: str, slot_number: int, booking_uid: str
    ) -> None:
        """Deactivate a code on a lock."""
        logger.info(
            f"Deactivating code on {lock_entity_id} slot {slot_number} "
            f"for booking {booking_uid}"
        )

        if self._sync_manager:
            await self._sync_manager.clear_code(lock_entity_id, slot_number, booking_uid)

        # Log to audit
        async with get_session_context() as session:
            result = await session.execute(
                select(Lock).where(Lock.entity_id == lock_entity_id)
            )
            lock = result.scalar_one_or_none()

            audit = AuditLog(
                action="code_deactivated",
                lock_id=lock.id if lock else None,
                slot_number=slot_number,
                details=f"Booking: {booking_uid}",
                success=True,
            )
            session.add(audit)

    async def _on_code_finalize(self, booking_uid: str, calendar_id_str: str) -> None:
        """Finalize the code for a booking at 11am the day before check-in.

        Re-fetches the calendar to get the latest phone number, generates the
        definitive code, and locks it in. Any scheduled activations are updated.
        """
        logger.info(f"Finalizing code for booking {booking_uid} (calendar {calendar_id_str})")

        async with get_session_context() as session:
            # Get the booking
            booking_result = await session.execute(
                select(Booking)
                .options(selectinload(Booking.calendar))
                .where(Booking.uid == booking_uid)
                .join(Calendar)
                .where(Calendar.calendar_id == calendar_id_str)
            )
            booking = booking_result.scalar_one_or_none()
            if not booking:
                logger.warning(f"Booking {booking_uid} not found for finalization")
                return

            if booking.locked_code:
                logger.info(f"Booking {booking_uid} already has locked code {booking.locked_code}")
                return

            # Re-fetch the calendar to get latest data
            calendar = booking.calendar
            if calendar.ical_url:
                try:
                    parsed_bookings = await self._ical_fetcher.fetch_and_parse(calendar.ical_url)
                    # Find this booking in the fresh data
                    for parsed in parsed_bookings:
                        if parsed.uid == booking_uid:
                            if parsed.phone and parsed.phone != booking.phone:
                                logger.info(
                                    f"Phone updated for {booking_uid}: "
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
        """Poll all calendars for updates."""
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
                if not calendar.ical_url:
                    continue

                try:
                    bookings = await self._ical_fetcher.fetch_and_parse(
                        calendar.ical_url
                    )
                    await self._process_calendar_bookings(session, calendar, bookings)
                    calendar.last_fetched = datetime.utcnow()
                    calendar.last_fetch_error = None
                except Exception as e:
                    logger.error(f"Error fetching calendar {calendar.calendar_id}: {e}")
                    calendar.last_fetch_error = str(e)

            await session.commit()

        self._polling = False
        logger.info("Calendar poll complete")

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

    async def _schedule_booking_codes(
        self, session: AsyncSession, calendar: Calendar, booking: Booking
    ) -> None:
        """Schedule code activations for a booking."""
        if booking.is_blocked or not booking.phone:
            return

        # Use locked code if finalized, otherwise generate from current phone
        code = booking.locked_code or generate_code_from_phone(booking.phone)
        if not code:
            logger.warning(f"Could not generate code for booking {booking.uid}")
            return

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

            # Calculate activation/deactivation times
            activate_at, deactivate_at = calculate_code_times(
                lock_type=lock_type,
                check_in_date=booking.check_in_date,
                check_out_date=booking.check_out_date,
                stagger_minutes=lock.stagger_minutes,
                override_activate=override.activate_at if override else None,
                override_deactivate=override.deactivate_at if override else None,
            )

            # Allocate slot
            slot_a, slot_b = get_slot_for_calendar(calendar.calendar_id)

            # Find available slot
            existing_assignments = [
                a for slot in lock.code_slots
                for a in slot.assignments
                if slot.slot_number in (slot_a, slot_b) and a.is_active
            ]
            existing_uids = {a.booking.uid for a in existing_assignments}

            slot_number = self._slot_allocator.allocate_slot_for_booking(
                lock.entity_id, calendar.calendar_id, booking.uid, existing_uids
            )

            # Find the code slot
            code_slot = next(
                (s for s in lock.code_slots if s.slot_number == slot_number), None
            )
            if not code_slot:
                continue

            # Create or update assignment
            assignment = CodeAssignment(
                code_slot_id=code_slot.id,
                booking_id=booking.id,
                code=code,
                activate_at=activate_at,
                deactivate_at=deactivate_at,
                is_active=False,
            )
            session.add(assignment)

            # Schedule with scheduler
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

    # Public API methods

    async def get_locks(self) -> list[dict]:
        """Get all locks for this house."""
        async with get_session_context() as session:
            query = select(Lock).options(
                selectinload(Lock.house),
                selectinload(Lock.code_slots),
            )

            result = await session.execute(query)
            locks = result.scalars().all()

            return [
                {
                    "id": lock.id,
                    "entity_id": lock.entity_id,
                    "name": lock.name,
                    "house_code": lock.house.code,
                    "lock_type": lock.lock_type,
                    "master_code": lock.master_code,
                    "emergency_code": lock.emergency_code,
                    "slots": [
                        {
                            "slot_number": slot.slot_number,
                            "current_code": slot.current_code,
                            "sync_state": slot.sync_state,
                        }
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
        """Get bookings, optionally filtered."""
        async with get_session_context() as session:
            query = select(Booking).options(selectinload(Booking.calendar))

            if calendar_id:
                query = query.join(Calendar).where(
                    Calendar.calendar_id == calendar_id
                )

            if from_date:
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
        """Generate a random 4-digit code (1000-9999, no leading zeros)."""
        return str(random.randint(1000, 9999))

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

            try:
                await self._set_code_with_retry(lock.entity_id, EMERGENCY_CODE_SLOT, code)
            except Exception as e:
                await self._notify_failure(
                    f"Emergency code failed on {lock.entity_id} after retries: {e}"
                )

            lock.emergency_code = code
            audit = AuditLog(
                action="emergency_code_set",
                lock_id=lock.id,
                slot_number=EMERGENCY_CODE_SLOT,
                code=code,
                success=True,
            )
            session.add(audit)
            await session.commit()

            return {
                "lock_id": lock.id,
                "entity_id": lock.entity_id,
                "emergency_code": code,
            }

    async def set_time_override(
        self,
        booking_id: int,
        lock_id: int,
        activate_at: Optional[datetime] = None,
        deactivate_at: Optional[datetime] = None,
        notes: Optional[str] = None,
    ) -> dict:
        """Set a time override for a booking on a specific lock."""
        async with get_session_context() as session:
            # Get or create override
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

            await session.flush()

            # Reschedule if scheduler is running
            if self._scheduler:
                # Get booking and lock details
                booking_result = await session.execute(
                    select(Booking)
                    .options(selectinload(Booking.calendar))
                    .where(Booking.id == booking_id)
                )
                booking = booking_result.scalar_one_or_none()

                lock_result = await session.execute(
                    select(Lock).where(Lock.id == lock_id)
                )
                lock = lock_result.scalar_one_or_none()

                if booking and lock and booking.phone:
                    code = generate_code_from_phone(booking.phone)
                    slot_a, slot_b = get_slot_for_calendar(booking.calendar.calendar_id)
                    # Use slot_a by default for overrides
                    slot_number = slot_a

                    if code and activate_at:
                        self._scheduler.reschedule_activation(
                            lock.entity_id, slot_number, booking.uid, activate_at, code
                        )

                    if deactivate_at:
                        self._scheduler.reschedule_deactivation(
                            lock.entity_id, slot_number, booking.uid, deactivate_at
                        )

            await session.commit()

            return {
                "id": override.id,
                "booking_id": booking_id,
                "lock_id": lock_id,
                "activate_at": override.activate_at.isoformat() if override.activate_at else None,
                "deactivate_at": override.deactivate_at.isoformat() if override.deactivate_at else None,
                "notes": override.notes,
            }

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
        return {"entity_id": lock_entity_id, "auto_lock": enabled}

    async def set_volume(self, lock_entity_id: str, level: str) -> dict:
        """Set the volume level on a lock."""
        await self._ha_client.set_volume(lock_entity_id, level)
        return {"entity_id": lock_entity_id, "volume": level}

    async def get_sync_status(self) -> dict:
        """Get the current sync status of all slots."""
        if not self._sync_manager:
            return {"error": "Sync manager not initialized"}

        states = self._sync_manager.get_all_states()
        failed = self._sync_manager.get_failed_slots()
        syncing = self._sync_manager.get_syncing_slots()

        return {
            "total_slots": len(states),
            "failed_count": len(failed),
            "syncing_count": len(syncing),
            "failed_slots": [
                {
                    "lock_entity_id": s.lock_entity_id,
                    "slot_number": s.slot_number,
                    "error": s.last_error,
                    "retry_count": s.retry_count,
                }
                for s in failed
            ],
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
