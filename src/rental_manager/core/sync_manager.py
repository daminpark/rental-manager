"""Code synchronization state machine and retry logic."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Optional, Awaitable
import logging

logger = logging.getLogger(__name__)


class SyncState(str, Enum):
    """State of code synchronization for a slot."""

    IDLE = "idle"
    SETTING = "setting"
    CONFIRMING = "confirming"
    ACTIVE = "active"
    CLEARING = "clearing"
    RETRYING = "retrying"
    FAILED = "failed"


@dataclass
class SlotSync:
    """Synchronization state for a single slot."""

    lock_entity_id: str
    slot_number: int
    state: SyncState = SyncState.IDLE
    target_code: Optional[str] = None
    current_code: Optional[str] = None
    booking_uid: Optional[str] = None
    started_at: Optional[datetime] = None
    retry_count: int = 0
    last_error: Optional[str] = None


@dataclass
class SyncResult:
    """Result of a sync operation."""

    success: bool
    state: SyncState
    error: Optional[str] = None
    retry_count: int = 0


class SyncManager:
    """Manages code synchronization with retry logic."""

    def __init__(
        self,
        set_code: Callable[[str, int, str], Awaitable[None]],
        clear_code: Callable[[str, int], Awaitable[None]],
        ping_lock: Callable[[str], Awaitable[bool]],
        on_sync_failed: Callable[[str, int, str, str], Awaitable[None]],
        timeout_seconds: int = 120,
        max_retries: int = 3,
    ):
        """Initialize the sync manager.

        Args:
            set_code: Callback to set a code. Args: (lock_entity_id, slot_number, code)
            clear_code: Callback to clear a code. Args: (lock_entity_id, slot_number)
            ping_lock: Callback to ping a lock. Args: (lock_entity_id) Returns: success
            on_sync_failed: Callback when sync fails after all retries.
                Args: (lock_entity_id, slot_number, code, error)
            timeout_seconds: Timeout before considering a sync stuck
            max_retries: Maximum retry attempts
        """
        self._set_code = set_code
        self._clear_code = clear_code
        self._ping_lock = ping_lock
        self._on_sync_failed = on_sync_failed
        self._timeout = timedelta(seconds=timeout_seconds)
        self._max_retries = max_retries

        # Track sync state per slot: {(lock_entity_id, slot_number): SlotSync}
        self._slots: dict[tuple[str, int], SlotSync] = {}

        # Background task for checking timeouts
        self._check_task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        """Start the sync manager background task."""
        self._running = True
        self._check_task = asyncio.create_task(self._check_loop())
        logger.info("Sync manager started")

    def stop(self) -> None:
        """Stop the sync manager."""
        self._running = False
        if self._check_task:
            self._check_task.cancel()
        logger.info("Sync manager stopped")

    async def _check_loop(self) -> None:
        """Background loop to check for stuck syncs."""
        while self._running:
            try:
                await self._check_timeouts()
            except Exception as e:
                logger.error(f"Error in sync check loop: {e}")
            await asyncio.sleep(30)  # Check every 30 seconds

    async def _check_timeouts(self) -> None:
        """Check for stuck sync operations and retry."""
        now = datetime.now()

        for key, slot in list(self._slots.items()):
            if slot.state not in (SyncState.SETTING, SyncState.CLEARING, SyncState.CONFIRMING):
                continue

            if slot.started_at is None:
                continue

            elapsed = now - slot.started_at

            if elapsed > self._timeout:
                logger.warning(
                    f"Sync timeout on {slot.lock_entity_id} slot {slot.slot_number} "
                    f"(state={slot.state}, elapsed={elapsed})"
                )
                await self._handle_timeout(slot)

    async def _handle_timeout(self, slot: SlotSync) -> None:
        """Handle a timed-out sync operation."""
        if slot.retry_count >= self._max_retries:
            # Max retries exceeded, mark as failed
            slot.state = SyncState.FAILED
            slot.last_error = f"Max retries ({self._max_retries}) exceeded"
            logger.error(
                f"Sync failed on {slot.lock_entity_id} slot {slot.slot_number}: "
                f"{slot.last_error}"
            )
            await self._on_sync_failed(
                slot.lock_entity_id,
                slot.slot_number,
                slot.target_code or "",
                slot.last_error,
            )
            return

        # Attempt retry
        slot.state = SyncState.RETRYING
        slot.retry_count += 1
        logger.info(
            f"Retrying sync on {slot.lock_entity_id} slot {slot.slot_number} "
            f"(attempt {slot.retry_count}/{self._max_retries})"
        )

        try:
            # Step 1: Ping the lock
            ping_success = await self._ping_lock(slot.lock_entity_id)
            if not ping_success:
                slot.last_error = "Lock not responding to ping"
                logger.warning(f"Lock {slot.lock_entity_id} not responding")
                slot.started_at = datetime.now()
                return

            # Step 2: Clear the slot
            await self._clear_code(slot.lock_entity_id, slot.slot_number)
            await asyncio.sleep(2)  # Brief pause

            # Step 3: Re-set the code if we have a target
            if slot.target_code:
                await self._set_code(
                    slot.lock_entity_id, slot.slot_number, slot.target_code
                )
                slot.state = SyncState.SETTING
                slot.started_at = datetime.now()
            else:
                # We were clearing, and it should be clear now
                slot.state = SyncState.IDLE
                slot.current_code = None
                slot.started_at = None

        except Exception as e:
            slot.last_error = str(e)
            slot.started_at = datetime.now()
            logger.error(
                f"Error during retry on {slot.lock_entity_id} slot {slot.slot_number}: {e}"
            )

    def get_slot_state(self, lock_entity_id: str, slot_number: int) -> SlotSync:
        """Get the sync state for a slot.

        Args:
            lock_entity_id: Lock entity ID
            slot_number: Slot number

        Returns:
            SlotSync state object
        """
        key = (lock_entity_id, slot_number)
        if key not in self._slots:
            self._slots[key] = SlotSync(
                lock_entity_id=lock_entity_id, slot_number=slot_number
            )
        return self._slots[key]

    async def set_code(
        self, lock_entity_id: str, slot_number: int, code: str, booking_uid: str
    ) -> SyncResult:
        """Set a code on a lock slot.

        Args:
            lock_entity_id: Lock entity ID
            slot_number: Slot number
            code: Code to set
            booking_uid: Associated booking UID

        Returns:
            SyncResult indicating success/failure
        """
        slot = self.get_slot_state(lock_entity_id, slot_number)

        # Update state
        slot.state = SyncState.SETTING
        slot.target_code = code
        slot.booking_uid = booking_uid
        slot.started_at = datetime.now()
        slot.retry_count = 0
        slot.last_error = None

        try:
            await self._set_code(lock_entity_id, slot_number, code)
            # Move to confirming state - we'll wait for confirmation or timeout
            slot.state = SyncState.CONFIRMING
            return SyncResult(success=True, state=slot.state)
        except Exception as e:
            slot.last_error = str(e)
            logger.error(
                f"Error setting code on {lock_entity_id} slot {slot_number}: {e}"
            )
            return SyncResult(success=False, state=slot.state, error=str(e))

    async def clear_code(
        self, lock_entity_id: str, slot_number: int, booking_uid: str
    ) -> SyncResult:
        """Clear a code from a lock slot.

        Args:
            lock_entity_id: Lock entity ID
            slot_number: Slot number
            booking_uid: Associated booking UID

        Returns:
            SyncResult indicating success/failure
        """
        slot = self.get_slot_state(lock_entity_id, slot_number)

        # Update state
        slot.state = SyncState.CLEARING
        slot.target_code = None
        slot.booking_uid = booking_uid
        slot.started_at = datetime.now()
        slot.retry_count = 0
        slot.last_error = None

        try:
            await self._clear_code(lock_entity_id, slot_number)
            # Move to idle - clearing is typically confirmed immediately
            slot.state = SyncState.IDLE
            slot.current_code = None
            slot.started_at = None
            return SyncResult(success=True, state=slot.state)
        except Exception as e:
            slot.last_error = str(e)
            logger.error(
                f"Error clearing code on {lock_entity_id} slot {slot_number}: {e}"
            )
            return SyncResult(success=False, state=slot.state, error=str(e))

    def confirm_code_set(self, lock_entity_id: str, slot_number: int) -> None:
        """Confirm that a code was successfully set.

        This is called when we receive confirmation from the lock
        (e.g., via Z-Wave event or state update).

        Args:
            lock_entity_id: Lock entity ID
            slot_number: Slot number
        """
        slot = self.get_slot_state(lock_entity_id, slot_number)

        if slot.state in (SyncState.SETTING, SyncState.CONFIRMING, SyncState.RETRYING):
            slot.state = SyncState.ACTIVE
            slot.current_code = slot.target_code
            slot.started_at = None
            slot.last_error = None
            logger.info(
                f"Code confirmed on {lock_entity_id} slot {slot_number}"
            )

    def get_all_states(self) -> dict[tuple[str, int], SlotSync]:
        """Get all slot sync states.

        Returns:
            Dictionary mapping (lock_entity_id, slot_number) to SlotSync
        """
        return dict(self._slots)

    def get_failed_slots(self) -> list[SlotSync]:
        """Get all slots in failed state.

        Returns:
            List of SlotSync objects in FAILED state
        """
        return [slot for slot in self._slots.values() if slot.state == SyncState.FAILED]

    def get_syncing_slots(self) -> list[SlotSync]:
        """Get all slots currently syncing.

        Returns:
            List of SlotSync objects in SETTING, CONFIRMING, or RETRYING state
        """
        return [
            slot
            for slot in self._slots.values()
            if slot.state in (SyncState.SETTING, SyncState.CONFIRMING, SyncState.RETRYING)
        ]

    def reset_failed_slot(self, lock_entity_id: str, slot_number: int) -> None:
        """Reset a failed slot to idle state.

        Args:
            lock_entity_id: Lock entity ID
            slot_number: Slot number
        """
        slot = self.get_slot_state(lock_entity_id, slot_number)
        if slot.state == SyncState.FAILED:
            slot.state = SyncState.IDLE
            slot.retry_count = 0
            slot.last_error = None
            slot.started_at = None
