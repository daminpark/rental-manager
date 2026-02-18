"""Time scheduler for code activation and deactivation."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Optional, Awaitable
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


class JobType(str, Enum):
    """Type of scheduled job."""

    ACTIVATE_CODE = "activate_code"
    DEACTIVATE_CODE = "deactivate_code"
    CALENDAR_POLL = "calendar_poll"
    SYNC_CHECK = "sync_check"
    WHOLE_HOUSE_CHECKIN = "whole_house_checkin"
    WHOLE_HOUSE_CHECKOUT = "whole_house_checkout"


@dataclass
class ScheduledJob:
    """Information about a scheduled job."""

    job_id: str
    job_type: JobType
    run_at: datetime
    lock_entity_id: Optional[str] = None
    slot_number: Optional[int] = None
    code: Optional[str] = None
    booking_uid: Optional[str] = None
    calendar_id: Optional[str] = None


@dataclass
class CodeScheduleEntry:
    """Entry for a scheduled code activation or deactivation."""

    lock_entity_id: str
    slot_number: int
    code: str
    activate_at: datetime
    deactivate_at: datetime
    booking_uid: str
    calendar_id: str
    guest_name: str


class CodeScheduler:
    """Manages scheduling of code activations and deactivations."""

    # Delay between consecutive catch-up Z-Wave operations (seconds).
    CATCHUP_STAGGER = 8

    def __init__(
        self,
        on_activate: Callable[[str, int, str, str], Awaitable[None]],
        on_deactivate: Callable[[str, int, str], Awaitable[None]],
        on_calendar_poll: Callable[[], Awaitable[None]],
        on_code_finalize: Optional[Callable[[str, str], Awaitable[None]]] = None,
        on_emergency_rotate: Optional[Callable[[], Awaitable[None]]] = None,
        on_whole_house_checkin: Optional[Callable[[str], Awaitable[None]]] = None,
        on_whole_house_checkout: Optional[Callable[[str], Awaitable[None]]] = None,
        poll_interval_seconds: int = 120,
    ):
        """Initialize the scheduler.

        Args:
            on_activate: Callback when a code should be activated.
                Args: (lock_entity_id, slot_number, code, booking_uid)
            on_deactivate: Callback when a code should be deactivated.
                Args: (lock_entity_id, slot_number, booking_uid)
            on_calendar_poll: Callback to poll calendars for updates.
            on_code_finalize: Callback to finalize a code at 11am day before check-in.
                Args: (booking_uid, calendar_id)
            on_emergency_rotate: Callback to rotate all emergency codes weekly.
            on_whole_house_checkin: Callback at 14:30 on whole-house check-in day.
                Args: (booking_uid)
            on_whole_house_checkout: Callback at 11:30 on whole-house check-out day.
                Args: (booking_uid)
            poll_interval_seconds: How often to poll calendars.
        """
        self._on_activate = on_activate
        self._on_deactivate = on_deactivate
        self._on_calendar_poll = on_calendar_poll
        self._on_code_finalize = on_code_finalize
        self._on_emergency_rotate = on_emergency_rotate
        self._on_whole_house_checkin = on_whole_house_checkin
        self._on_whole_house_checkout = on_whole_house_checkout
        self._poll_interval = poll_interval_seconds

        self._scheduler = AsyncIOScheduler()
        self._scheduled_jobs: dict[str, ScheduledJob] = {}

        # Queue for catch-up operations processed sequentially with stagger.
        self._catchup_queue: asyncio.Queue[tuple] = asyncio.Queue()
        self._catchup_task: Optional[asyncio.Task] = None

    def start(self) -> None:
        """Start the scheduler."""
        # Add recurring calendar poll job
        self._scheduler.add_job(
            self._handle_calendar_poll,
            IntervalTrigger(seconds=self._poll_interval),
            id="calendar_poll",
            replace_existing=True,
        )

        # Weekly emergency code rotation - every Monday at 3am
        if self._on_emergency_rotate:
            self._scheduler.add_job(
                self._handle_emergency_rotate,
                CronTrigger(day_of_week="mon", hour=3, minute=0),
                id="emergency_rotate",
                replace_existing=True,
            )

        self._scheduler.start()

        # Start catch-up queue processor
        self._catchup_task = asyncio.create_task(self._process_catchup_queue())

        logger.info("Scheduler started")

    def stop(self) -> None:
        """Stop the scheduler."""
        self._scheduler.shutdown(wait=False)
        if self._catchup_task:
            self._catchup_task.cancel()
        logger.info("Scheduler stopped")

    async def _process_catchup_queue(self) -> None:
        """Process catch-up operations sequentially with stagger delays.

        Past-due activations/deactivations are queued here instead of being
        fired concurrently, preventing Z-Wave mesh flooding on startup.
        """
        while True:
            try:
                op_type, args = await self._catchup_queue.get()
                if op_type == "activate":
                    await self._handle_activate(*args)
                elif op_type == "deactivate":
                    await self._handle_deactivate(*args)
                elif op_type == "finalize":
                    await self._handle_finalize(*args)
                elif op_type == "wh_checkin":
                    await self._handle_whole_house_checkin(*args)
                elif op_type == "wh_checkout":
                    await self._handle_whole_house_checkout(*args)
                self._catchup_queue.task_done()
                # Stagger between operations to avoid overwhelming Z-Wave
                if not self._catchup_queue.empty():
                    await asyncio.sleep(self.CATCHUP_STAGGER)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in catch-up queue: {e}")

    async def _handle_emergency_rotate(self) -> None:
        """Handle weekly emergency code rotation."""
        if self._on_emergency_rotate:
            try:
                await self._on_emergency_rotate()
                logger.info("Emergency codes rotated")
            except Exception as e:
                logger.error(f"Error rotating emergency codes: {e}")

    async def _handle_calendar_poll(self) -> None:
        """Handle calendar poll."""
        try:
            await self._on_calendar_poll()
        except Exception as e:
            logger.error(f"Error polling calendars: {e}")

    async def _handle_activate(
        self, lock_entity_id: str, slot_number: int, code: str, booking_uid: str
    ) -> None:
        """Handle code activation."""
        job_id = f"activate_{lock_entity_id}_{slot_number}_{booking_uid}"
        if job_id in self._scheduled_jobs:
            del self._scheduled_jobs[job_id]

        try:
            await self._on_activate(lock_entity_id, slot_number, code, booking_uid)
        except Exception as e:
            logger.error(
                f"Error activating code on {lock_entity_id} slot {slot_number}: {e}"
            )

    async def _handle_deactivate(
        self, lock_entity_id: str, slot_number: int, booking_uid: str
    ) -> None:
        """Handle code deactivation."""
        job_id = f"deactivate_{lock_entity_id}_{slot_number}_{booking_uid}"
        if job_id in self._scheduled_jobs:
            del self._scheduled_jobs[job_id]

        try:
            await self._on_deactivate(lock_entity_id, slot_number, booking_uid)
        except Exception as e:
            logger.error(
                f"Error deactivating code on {lock_entity_id} slot {slot_number}: {e}"
            )

    async def _handle_finalize(self, booking_uid: str, calendar_id: str) -> None:
        """Handle code finalization at 11am day before check-in."""
        job_id = f"finalize_{booking_uid}"
        if job_id in self._scheduled_jobs:
            del self._scheduled_jobs[job_id]

        if self._on_code_finalize:
            try:
                await self._on_code_finalize(booking_uid, calendar_id)
            except Exception as e:
                logger.error(f"Error finalizing code for booking {booking_uid}: {e}")

    async def _handle_whole_house_checkin(self, booking_uid: str) -> None:
        """Handle whole-house check-in (14:30 on check-in day)."""
        job_id = f"wh_checkin_{booking_uid}"
        if job_id in self._scheduled_jobs:
            del self._scheduled_jobs[job_id]

        if self._on_whole_house_checkin:
            try:
                await self._on_whole_house_checkin(booking_uid)
            except Exception as e:
                logger.error(f"Error in whole-house check-in routine for {booking_uid}: {e}")

    async def _handle_whole_house_checkout(self, booking_uid: str) -> None:
        """Handle whole-house check-out (11:30 on check-out day)."""
        job_id = f"wh_checkout_{booking_uid}"
        if job_id in self._scheduled_jobs:
            del self._scheduled_jobs[job_id]

        if self._on_whole_house_checkout:
            try:
                await self._on_whole_house_checkout(booking_uid)
            except Exception as e:
                logger.error(f"Error in whole-house check-out routine for {booking_uid}: {e}")

    def schedule_whole_house(
        self, booking_uid: str, check_in_date: "date", check_out_date: "date"
    ) -> tuple[str, str]:
        """Schedule whole-house check-in (14:30) and check-out (11:30) routines.

        Returns:
            Tuple of (checkin_job_id, checkout_job_id)
        """
        from datetime import date as date_type

        now = datetime.now()

        # Check-in: 14:30 on check-in day
        checkin_job_id = f"wh_checkin_{booking_uid}"
        checkin_time = datetime.combine(check_in_date, datetime.min.time().replace(hour=14, minute=30))

        if checkin_time <= now:
            self._catchup_queue.put_nowait(("wh_checkin", (booking_uid,)))
        else:
            self._scheduler.add_job(
                self._handle_whole_house_checkin,
                DateTrigger(run_date=checkin_time),
                args=[booking_uid],
                id=checkin_job_id,
                replace_existing=True,
            )
            self._scheduled_jobs[checkin_job_id] = ScheduledJob(
                job_id=checkin_job_id,
                job_type=JobType.WHOLE_HOUSE_CHECKIN,
                run_at=checkin_time,
                booking_uid=booking_uid,
            )

        # Check-out: 11:30 on check-out day
        checkout_job_id = f"wh_checkout_{booking_uid}"
        checkout_time = datetime.combine(check_out_date, datetime.min.time().replace(hour=11, minute=30))

        if checkout_time <= now:
            self._catchup_queue.put_nowait(("wh_checkout", (booking_uid,)))
        else:
            self._scheduler.add_job(
                self._handle_whole_house_checkout,
                DateTrigger(run_date=checkout_time),
                args=[booking_uid],
                id=checkout_job_id,
                replace_existing=True,
            )
            self._scheduled_jobs[checkout_job_id] = ScheduledJob(
                job_id=checkout_job_id,
                job_type=JobType.WHOLE_HOUSE_CHECKOUT,
                run_at=checkout_time,
                booking_uid=booking_uid,
            )

        return checkin_job_id, checkout_job_id

    def schedule_finalization(
        self, booking_uid: str, calendar_id: str, finalize_at: datetime
    ) -> str:
        """Schedule code finalization at 11am the day before check-in."""
        job_id = f"finalize_{booking_uid}"
        now = datetime.now()

        if finalize_at <= now:
            # Already past finalization time, queue for catch-up
            self._catchup_queue.put_nowait((
                "finalize", (booking_uid, calendar_id),
            ))
        else:
            self._scheduler.add_job(
                self._handle_finalize,
                DateTrigger(run_date=finalize_at),
                args=[booking_uid, calendar_id],
                id=job_id,
                replace_existing=True,
            )
            self._scheduled_jobs[job_id] = ScheduledJob(
                job_id=job_id,
                job_type=JobType.SYNC_CHECK,
                run_at=finalize_at,
                booking_uid=booking_uid,
                calendar_id=calendar_id,
            )

        return job_id

    def schedule_code(self, entry: CodeScheduleEntry) -> tuple[str, str]:
        """Schedule a code activation and deactivation.

        Args:
            entry: The schedule entry

        Returns:
            Tuple of (activate_job_id, deactivate_job_id)
        """
        now = datetime.now()

        # Schedule activation
        activate_job_id = (
            f"activate_{entry.lock_entity_id}_{entry.slot_number}_{entry.booking_uid}"
        )

        if entry.activate_at <= now:
            # Past-due: queue for sequential catch-up (avoids Z-Wave flooding)
            self._catchup_queue.put_nowait((
                "activate",
                (entry.lock_entity_id, entry.slot_number, entry.code, entry.booking_uid),
            ))
        else:
            self._scheduler.add_job(
                self._handle_activate,
                DateTrigger(run_date=entry.activate_at),
                args=[
                    entry.lock_entity_id,
                    entry.slot_number,
                    entry.code,
                    entry.booking_uid,
                ],
                id=activate_job_id,
                replace_existing=True,
            )
            self._scheduled_jobs[activate_job_id] = ScheduledJob(
                job_id=activate_job_id,
                job_type=JobType.ACTIVATE_CODE,
                run_at=entry.activate_at,
                lock_entity_id=entry.lock_entity_id,
                slot_number=entry.slot_number,
                code=entry.code,
                booking_uid=entry.booking_uid,
                calendar_id=entry.calendar_id,
            )

        # Schedule deactivation
        deactivate_job_id = (
            f"deactivate_{entry.lock_entity_id}_{entry.slot_number}_{entry.booking_uid}"
        )

        if entry.deactivate_at <= now:
            # Past-due deactivation: queue for sequential catch-up
            self._catchup_queue.put_nowait((
                "deactivate",
                (entry.lock_entity_id, entry.slot_number, entry.booking_uid),
            ))
        else:
            self._scheduler.add_job(
                self._handle_deactivate,
                DateTrigger(run_date=entry.deactivate_at),
                args=[entry.lock_entity_id, entry.slot_number, entry.booking_uid],
                id=deactivate_job_id,
                replace_existing=True,
            )
            self._scheduled_jobs[deactivate_job_id] = ScheduledJob(
                job_id=deactivate_job_id,
                job_type=JobType.DEACTIVATE_CODE,
                run_at=entry.deactivate_at,
                lock_entity_id=entry.lock_entity_id,
                slot_number=entry.slot_number,
                booking_uid=entry.booking_uid,
                calendar_id=entry.calendar_id,
            )

        return activate_job_id, deactivate_job_id

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a scheduled job.

        Args:
            job_id: The job ID to cancel

        Returns:
            True if the job was cancelled, False if not found
        """
        try:
            self._scheduler.remove_job(job_id)
            if job_id in self._scheduled_jobs:
                del self._scheduled_jobs[job_id]
            return True
        except Exception:
            return False

    def reschedule_activation(
        self,
        lock_entity_id: str,
        slot_number: int,
        booking_uid: str,
        new_time: datetime,
        code: str,
    ) -> str:
        """Reschedule an activation to a new time.

        This is used for manual time overrides.

        Args:
            lock_entity_id: Lock entity ID
            slot_number: Slot number
            booking_uid: Booking UID
            new_time: New activation time
            code: The code to set

        Returns:
            The job ID
        """
        job_id = f"activate_{lock_entity_id}_{slot_number}_{booking_uid}"

        # Remove existing job if present
        self.cancel_job(job_id)

        now = datetime.now()
        if new_time <= now:
            # Past-due: queue for sequential catch-up
            self._catchup_queue.put_nowait((
                "activate",
                (lock_entity_id, slot_number, code, booking_uid),
            ))
        else:
            self._scheduler.add_job(
                self._handle_activate,
                DateTrigger(run_date=new_time),
                args=[lock_entity_id, slot_number, code, booking_uid],
                id=job_id,
                replace_existing=True,
            )
            self._scheduled_jobs[job_id] = ScheduledJob(
                job_id=job_id,
                job_type=JobType.ACTIVATE_CODE,
                run_at=new_time,
                lock_entity_id=lock_entity_id,
                slot_number=slot_number,
                code=code,
                booking_uid=booking_uid,
            )

        return job_id

    def reschedule_deactivation(
        self, lock_entity_id: str, slot_number: int, booking_uid: str, new_time: datetime
    ) -> str:
        """Reschedule a deactivation to a new time.

        Args:
            lock_entity_id: Lock entity ID
            slot_number: Slot number
            booking_uid: Booking UID
            new_time: New deactivation time

        Returns:
            The job ID
        """
        job_id = f"deactivate_{lock_entity_id}_{slot_number}_{booking_uid}"

        # Remove existing job if present
        self.cancel_job(job_id)

        now = datetime.now()
        if new_time <= now:
            # Past-due: queue for sequential catch-up
            self._catchup_queue.put_nowait((
                "deactivate",
                (lock_entity_id, slot_number, booking_uid),
            ))
        else:
            self._scheduler.add_job(
                self._handle_deactivate,
                DateTrigger(run_date=new_time),
                args=[lock_entity_id, slot_number, booking_uid],
                id=job_id,
                replace_existing=True,
            )
            self._scheduled_jobs[job_id] = ScheduledJob(
                job_id=job_id,
                job_type=JobType.DEACTIVATE_CODE,
                run_at=new_time,
                lock_entity_id=lock_entity_id,
                slot_number=slot_number,
                booking_uid=booking_uid,
            )

        return job_id

    def get_scheduled_jobs(self) -> list[ScheduledJob]:
        """Get all scheduled jobs.

        Returns:
            List of ScheduledJob objects
        """
        return list(self._scheduled_jobs.values())

    def get_jobs_for_lock(self, lock_entity_id: str) -> list[ScheduledJob]:
        """Get scheduled jobs for a specific lock.

        Args:
            lock_entity_id: Lock entity ID

        Returns:
            List of ScheduledJob objects for that lock
        """
        return [
            job
            for job in self._scheduled_jobs.values()
            if job.lock_entity_id == lock_entity_id
        ]

    def get_jobs_for_booking(self, booking_uid: str) -> list[ScheduledJob]:
        """Get scheduled jobs for a specific booking.

        Args:
            booking_uid: Booking UID

        Returns:
            List of ScheduledJob objects for that booking
        """
        return [
            job
            for job in self._scheduled_jobs.values()
            if job.booking_uid == booking_uid
        ]
