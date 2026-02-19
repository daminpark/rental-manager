"""HostTools API client for fetching reservations directly."""

import re
from datetime import date, timedelta
from typing import Any, Optional
import logging

import httpx

from rental_manager.core.ical_parser import ParsedBooking

logger = logging.getLogger(__name__)

BASE_URL = "https://app.hosttools.com/api"


class HostToolsClient:
    """Client for the HostTools public API."""

    def __init__(self, auth_token: str):
        self._auth_token = auth_token
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers={"authToken": self._auth_token},
                timeout=30.0,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get_listings(self) -> list[dict[str, Any]]:
        """Get all listings for this account."""
        client = await self._get_client()
        response = await client.get(f"{BASE_URL}/getlistings")
        response.raise_for_status()
        return response.json().get("listings", [])

    async def get_reservations(
        self,
        listing_id: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> list[dict[str, Any]]:
        """Get reservations for a listing within a date range.

        Args:
            listing_id: HostTools listing ID
            start: Start date (defaults to today - 30 days)
            end: End date (defaults to today + 365 days)

        Returns:
            List of reservation dicts from the HostTools API.
        """
        if start is None:
            start = date.today() - timedelta(days=30)
        if end is None:
            end = date.today() + timedelta(days=365)

        client = await self._get_client()
        url = f"{BASE_URL}/getreservations/{listing_id}/{start.isoformat()}/{end.isoformat()}"
        response = await client.get(url)
        response.raise_for_status()
        return response.json().get("reservations", [])


def parse_hosttools_reservations(
    reservations: list[dict[str, Any]],
) -> list[ParsedBooking]:
    """Convert HostTools reservation dicts to ParsedBooking objects.

    Args:
        reservations: Raw reservation dicts from the HostTools API.

    Returns:
        List of ParsedBooking objects compatible with the existing pipeline.
    """
    bookings: list[ParsedBooking] = []

    for res in reservations:
        status = res.get("status", "")
        if status not in ("accepted", "pending"):
            continue  # Skip cancelled/declined reservations

        first = (res.get("firstName") or "").strip()
        last = (res.get("lastName") or "").strip()
        guest_name = f"{first} {last}".strip() or "Unknown Guest"

        # Check for blocked periods
        is_blocked = guest_name.lower() in ("blocked", "not available", "")

        # Phone — strip to digits only
        raw_phone = res.get("phone") or ""
        phone = re.sub(r"[^\d]", "", raw_phone) or None

        # Channel / source
        channel = res.get("source")  # e.g. "Airbnb", "internal", "Booking.com"

        # Reservation ID
        reservation_id = res.get("confirmationCode")

        # Dates — HostTools returns ISO strings like "2026-02-19T00:00:00.000Z"
        start_str = res.get("startDate", "")
        end_str = res.get("endDate", "")
        try:
            check_in = date.fromisoformat(start_str[:10])
            check_out = date.fromisoformat(end_str[:10])
        except (ValueError, IndexError):
            logger.warning(f"Skipping reservation with invalid dates: {start_str} - {end_str}")
            continue

        bookings.append(ParsedBooking(
            uid=res.get("_id", ""),
            guest_name=guest_name,
            phone=phone,
            channel=channel,
            reservation_id=reservation_id,
            check_in_date=check_in,
            check_out_date=check_out,
            is_blocked=is_blocked,
        ))

    return bookings
