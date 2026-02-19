"""Home Assistant WebSocket event listener for Z-Wave lock events."""

import asyncio
import json
import logging
from typing import Any, Callable, Coroutine, Optional

import websockets

logger = logging.getLogger(__name__)

# Z-Wave Notification CC Event types for locks
# Type 6 = Access Control
ACCESS_CONTROL_EVENT_MAP = {
    1: ("manual", "Manual Lock"),
    2: ("manual", "Manual Unlock"),
    3: ("rf", "RF Lock"),
    4: ("rf", "RF Unlock"),
    5: ("keypad", "Keypad Lock"),
    6: ("keypad", "Keypad Unlock"),
    9: ("auto_lock", "Auto Lock"),
    10: ("auto_lock", "Auto Unlock"),
    11: ("keypad", "Keypad Lock (limited)"),
    12: ("keypad", "Keypad Unlock (limited)"),
}


class HAEventListener:
    """Listens to Home Assistant websocket API for Z-Wave lock events."""

    def __init__(
        self,
        ha_url: str,
        ha_token: str,
        on_lock_event: Callable[..., Coroutine],
    ):
        self._ws_url = ha_url.replace("http://", "ws://").replace("https://", "wss://") + "/api/websocket"
        self._token = ha_token
        self._on_lock_event = on_lock_event
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._msg_id = 0

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    async def start(self) -> None:
        """Start listening for events in a background task."""
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._listen_loop())
        logger.info("HA event listener started")

    async def stop(self) -> None:
        """Stop listening."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("HA event listener stopped")

    async def _listen_loop(self) -> None:
        """Reconnecting listen loop."""
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    logger.warning("HA websocket disconnected: %s, reconnecting in 10s...", e)
                    await asyncio.sleep(10)

    async def _connect_and_listen(self) -> None:
        """Connect to HA websocket, authenticate, and subscribe to events."""
        logger.info("Connecting to HA websocket at %s", self._ws_url)

        async with websockets.connect(self._ws_url, ping_interval=30, ping_timeout=10) as ws:
            # HA sends auth_required
            msg = json.loads(await ws.recv())
            if msg.get("type") != "auth_required":
                logger.error("Unexpected first message: %s", msg)
                return

            # Authenticate
            await ws.send(json.dumps({
                "type": "auth",
                "access_token": self._token,
            }))
            msg = json.loads(await ws.recv())
            if msg.get("type") != "auth_ok":
                logger.error("HA auth failed: %s", msg)
                return
            logger.info("HA websocket authenticated")

            # Subscribe to zwave_js_notification events
            sub_id = self._next_id()
            await ws.send(json.dumps({
                "id": sub_id,
                "type": "subscribe_events",
                "event_type": "zwave_js_notification",
            }))
            result = json.loads(await ws.recv())
            if not result.get("success"):
                logger.error("Failed to subscribe to zwave_js_notification: %s", result)
                return
            logger.info("Subscribed to zwave_js_notification events")

            # Also subscribe to state_changed for lock entities (catches all lock/unlock)
            state_sub_id = self._next_id()
            await ws.send(json.dumps({
                "id": state_sub_id,
                "type": "subscribe_events",
                "event_type": "state_changed",
            }))
            result = json.loads(await ws.recv())
            if not result.get("success"):
                logger.warning("Failed to subscribe to state_changed: %s", result)

            # Listen for events
            while self._running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    msg = json.loads(raw)
                    if msg.get("type") == "event":
                        await self._handle_event(msg.get("event", {}))
                except asyncio.TimeoutError:
                    # Send a ping to keep connection alive
                    pong = self._next_id()
                    await ws.send(json.dumps({"id": pong, "type": "ping"}))
                    await asyncio.wait_for(ws.recv(), timeout=10)

    async def _handle_event(self, event: dict[str, Any]) -> None:
        """Handle an event from HA."""
        event_type = event.get("event_type", "")

        if event_type == "zwave_js_notification":
            await self._handle_zwave_notification(event)
        elif event_type == "state_changed":
            await self._handle_state_changed(event)

    async def _handle_zwave_notification(self, event: dict[str, Any]) -> None:
        """Handle a Z-Wave JS notification event (lock access control)."""
        data = event.get("data", {})

        # Only care about Notification CC, type 6 (Access Control)
        command_class = data.get("command_class")
        cc_type = data.get("type")

        if command_class != 6:
            return

        event_code = data.get("event")
        event_info = ACCESS_CONTROL_EVENT_MAP.get(event_code)
        if not event_info:
            return

        method, label = event_info

        # Get entity_id from device_id or node_id
        entity_id = data.get("entity_id")
        node_id = data.get("node_id")
        device_id = data.get("device_id")

        # Extract code slot from parameters if available
        code_slot = data.get("parameters", {}).get("userId")

        if not entity_id:
            logger.debug("Z-Wave notification without entity_id: node=%s event=%s", node_id, event_code)
            return

        # Only process unlock events (even codes = unlock)
        # But also track lock events for completeness
        logger.info(
            "Z-Wave lock event: entity=%s event=%d (%s) slot=%s method=%s",
            entity_id, event_code, label, code_slot, method,
        )

        try:
            await self._on_lock_event(
                entity_id=entity_id,
                code_slot=code_slot,
                method=method,
                event_label=label,
            )
        except Exception as e:
            logger.error("Error processing lock event: %s", e)

    async def _handle_state_changed(self, event: dict[str, Any]) -> None:
        """Handle state_changed events for lock entities (fallback)."""
        data = event.get("data", {})
        entity_id = data.get("entity_id", "")

        # Only care about lock entities
        if not entity_id.startswith("lock."):
            return

        old_state = data.get("old_state", {})
        new_state = data.get("new_state", {})

        if not old_state or not new_state:
            return

        old_val = old_state.get("state")
        new_val = new_state.get("state")

        # Only process state transitions
        if old_val == new_val:
            return

        # We only use state_changed as a fallback if zwave_js_notification
        # doesn't fire (shouldn't happen for Z-Wave locks, but just in case)
        # Don't log state_changed for lock entities to avoid duplicate events
        # since zwave_js_notification is the primary source
        pass
