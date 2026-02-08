"""Home Assistant API client."""

from dataclasses import dataclass
from typing import Any, Optional

import httpx


@dataclass
class LockState:
    """State of a lock."""

    entity_id: str
    state: str  # "locked", "unlocked", "jammed", "unknown"
    friendly_name: Optional[str] = None
    auto_lock: Optional[bool] = None
    volume: Optional[str] = None  # "low", "high", "off"


@dataclass
class CodeSlotState:
    """State of a code slot on a lock."""

    slot_number: int
    code: Optional[str]
    is_enabled: bool
    status: str  # "set", "unset", "adding", "deleting"


class HomeAssistantClient:
    """Client for communicating with Home Assistant API."""

    def __init__(self, url: str, token: str, timeout: float = 30.0):
        """Initialize the client.

        Args:
            url: Home Assistant URL (e.g., "http://192.168.1.100:8123")
            token: Long-lived access token
            timeout: Request timeout in seconds
        """
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _call_service(
        self, domain: str, service: str, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Call a Home Assistant service.

        Args:
            domain: Service domain (e.g., "lock", "zwave_js")
            service: Service name (e.g., "lock", "set_lock_usercode")
            data: Service data

        Returns:
            Response data
        """
        client = await self._get_client()
        response = await client.post(
            f"{self.url}/api/services/{domain}/{service}",
            json=data,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    async def _get_state(self, entity_id: str) -> dict[str, Any]:
        """Get the state of an entity.

        Args:
            entity_id: Entity ID

        Returns:
            Entity state data
        """
        client = await self._get_client()
        response = await client.get(f"{self.url}/api/states/{entity_id}")
        response.raise_for_status()
        return response.json()

    async def _get_states(self) -> list[dict[str, Any]]:
        """Get all entity states.

        Returns:
            List of entity state data
        """
        client = await self._get_client()
        response = await client.get(f"{self.url}/api/states")
        response.raise_for_status()
        return response.json()

    # Lock operations

    async def get_lock_state(self, entity_id: str) -> LockState:
        """Get the state of a lock.

        Args:
            entity_id: Lock entity ID

        Returns:
            LockState object
        """
        data = await self._get_state(entity_id)
        attrs = data.get("attributes", {})

        return LockState(
            entity_id=entity_id,
            state=data.get("state", "unknown"),
            friendly_name=attrs.get("friendly_name"),
            auto_lock=attrs.get("auto_lock"),
            volume=attrs.get("volume_level"),
        )

    async def lock(self, entity_id: str) -> None:
        """Lock a lock.

        Args:
            entity_id: Lock entity ID
        """
        await self._call_service("lock", "lock", {"entity_id": entity_id})

    async def unlock(self, entity_id: str) -> None:
        """Unlock a lock.

        Args:
            entity_id: Lock entity ID
        """
        await self._call_service("lock", "unlock", {"entity_id": entity_id})

    # Z-Wave JS code operations

    async def set_lock_usercode(
        self, entity_id: str, code_slot: int, usercode: str
    ) -> None:
        """Set a user code on a lock.

        Args:
            entity_id: Lock entity ID
            code_slot: Slot number (1-20)
            usercode: The code to set
        """
        await self._call_service(
            "zwave_js",
            "set_lock_usercode",
            {
                "entity_id": entity_id,
                "code_slot": code_slot,
                "usercode": usercode,
            },
        )

    async def clear_lock_usercode(self, entity_id: str, code_slot: int) -> None:
        """Clear a user code from a lock.

        Args:
            entity_id: Lock entity ID
            code_slot: Slot number (1-20)
        """
        await self._call_service(
            "zwave_js",
            "clear_lock_usercode",
            {
                "entity_id": entity_id,
                "code_slot": code_slot,
            },
        )

    async def refresh_lock_usercodes(self, entity_id: str) -> None:
        """Refresh the user codes from a lock.

        Args:
            entity_id: Lock entity ID
        """
        # This pings the lock to get current code states
        await self._call_service(
            "zwave_js",
            "refresh_value",
            {
                "entity_id": entity_id,
                "refresh_all_values": False,
            },
        )

    # Z-Wave configuration (auto-lock, volume)

    async def set_config_parameter(
        self, entity_id: str, parameter: int, value: int
    ) -> None:
        """Set a Z-Wave configuration parameter.

        Args:
            entity_id: Lock entity ID
            parameter: Parameter number
            value: Value to set
        """
        await self._call_service(
            "zwave_js",
            "set_config_parameter",
            {
                "entity_id": entity_id,
                "parameter": parameter,
                "value": value,
            },
        )

    async def set_auto_lock(self, entity_id: str, enabled: bool) -> None:
        """Enable or disable auto-lock on a lock.

        Note: Parameter number may vary by lock model. Yale Keyless Connected
        typically uses parameter 1 or 2 for auto-lock.

        Args:
            entity_id: Lock entity ID
            enabled: True to enable, False to disable
        """
        # Yale Keyless Connected auto-lock parameter
        # Parameter 1: Auto-lock (0 = off, 255 = on for some models, or 1 = on)
        # This may need adjustment based on actual lock configuration
        await self.set_config_parameter(entity_id, 1, 255 if enabled else 0)

    async def set_volume(self, entity_id: str, level: str) -> None:
        """Set the volume level on a lock.

        Args:
            entity_id: Lock entity ID
            level: "low", "high", or "off"
        """
        # Yale Keyless Connected volume parameter
        # Parameter may vary; common values:
        # 0 = off, 1 = low, 2 = high
        value_map = {"off": 0, "low": 1, "high": 2}
        value = value_map.get(level.lower(), 1)
        # Volume is often parameter 4 or similar
        await self.set_config_parameter(entity_id, 4, value)

    # Utility methods

    async def send_notification(
        self, message: str, title: str = "Rental Manager"
    ) -> None:
        """Send a notification via HA's mobile app notification service.

        Args:
            message: Notification message
            title: Notification title
        """
        await self._call_service(
            "notify",
            "notify",
            {"message": message, "title": title},
        )

    async def ping_lock(self, entity_id: str) -> bool:
        """Ping a lock to check if it's responsive.

        Args:
            entity_id: Lock entity ID

        Returns:
            True if the lock responded
        """
        try:
            await self.refresh_lock_usercodes(entity_id)
            return True
        except Exception:
            return False

    async def health_check(self) -> bool:
        """Check if the Home Assistant instance is reachable.

        Returns:
            True if HA is reachable
        """
        try:
            client = await self._get_client()
            response = await client.get(f"{self.url}/api/")
            return response.status_code == 200
        except Exception:
            return False


