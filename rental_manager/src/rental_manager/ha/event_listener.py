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
        # Maps device_id -> lock entity_id (built at connect time)
        self._device_to_entity: dict[str, str] = {}

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

            # Build device_id -> entity_id mapping for lock entities
            await self._build_device_map(ws)

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

    async def _build_device_map(self, ws) -> None:
        """Fetch entity registry to build device_id -> lock entity_id mapping."""
        reg_id = self._next_id()
        await ws.send(json.dumps({
            "id": reg_id,
            "type": "config/entity_registry/list",
        }))
        result = json.loads(await ws.recv())
        if not result.get("success"):
            logger.warning("Failed to fetch entity registry: %s", result)
            return

        self._device_to_entity.clear()
        for entry in result.get("result", []):
            entity_id = entry.get("entity_id", "")
            device_id = entry.get("device_id")
            if entity_id.startswith("lock.") and device_id:
                self._device_to_entity[device_id] = entity_id

        logger.info("Built device map: %d lock entities", len(self._device_to_entity))

    async def _handle_event(self, event: dict[str, Any]) -> None:
        """Handle an event from HA."""
        event_type = event.get("event_type", "")

        if event_type == "zwave_js_notification":
            logger.debug("Raw zwave_js_notification: %s", json.dumps(event.get("data", {}))[:500])
            await self._handle_zwave_notification(event)
        elif event_type == "state_changed":
            await self._handle_state_changed(event)

    async def _handle_zwave_notification(self, event: dict[str, Any]) -> None:
        """Handle a Z-Wave JS notification event (lock access control)."""
        data = event.get("data", {})

        command_class = data.get("command_class")
        command_class_name = data.get("command_class_name", "")
        cc_type = data.get("type")

        logger.info(
            "Z-Wave notification: cc=%s cc_name=%s type=%s event=%s params=%s",
            command_class, command_class_name, cc_type,
            data.get("event"), data.get("parameters"),
        )

        # Notification CC = 113 (0x71), Access Control type = 6
        # Also accept command_class == 6 for backwards compat
        if command_class not in (6, 113):
            return

        # If command_class is 113, check that type is 6 (Access Control)
        if command_class == 113 and cc_type != 6:
            return

        event_code = data.get("event")
        event_info = ACCESS_CONTROL_EVENT_MAP.get(event_code)
        if not event_info:
            logger.debug("Unknown access control event code: %s", event_code)
            return

        method, label = event_info

        # Get entity_id - resolve from device_id if needed
        entity_id = data.get("entity_id")
        node_id = data.get("node_id")
        device_id = data.get("device_id")

        if not entity_id and device_id:
            entity_id = self._device_to_entity.get(device_id)

        # Extract code slot from parameters if available
        params = data.get("parameters", {})
        code_slot = params.get("userId")

        if not entity_id:
            logger.warning("Z-Wave notification: could not resolve entity_id (node=%s device=%s event=%s)", node_id, device_id, event_code)
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

        logger.info(
            "Lock state changed: %s %s -> %s",
            entity_id, old_val, new_val,
        )
