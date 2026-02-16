"""iCal feed fetcher and parser."""

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import httpx
from icalendar import Calendar


@dataclass
class ParsedBooking:
    """A parsed booking from an iCal event."""

    uid: str
    guest_name: str
    phone: Optional[str]
    channel: Optional[str]
    reservation_id: Optional[str]
    check_in_date: date
    check_out_date: date
    is_blocked: bool

    def __repr__(self) -> str:
        if self.is_blocked:
            return f"<ParsedBooking BLOCKED {self.check_in_date}-{self.check_out_date}>"
        return f"<ParsedBooking {self.guest_name} {self.check_in_date}-{self.check_out_date}>"


def parse_description(description: str) -> dict[str, Optional[str]]:
    """Parse the DESCRIPTION field to extract guest details.

    HostTools format - all on one line with spaces between fields:
    Name: Adam Cotterill Phone: 16046121213 Channel: Airbnb Listing: 5.1 · Room 1 ReservationID: ABC123
    """
    result: dict[str, Optional[str]] = {
        "phone": None,
        "channel": None,
        "reservation_id": None,
    }

    if not description:
        return result

    # Use regex to extract fields since they're all on one line
    phone_match = re.search(r"Phone:\s*(\+?[\d\s-]+)", description)
    if phone_match:
        result["phone"] = re.sub(r"[^\d]", "", phone_match.group(1))

    channel_match = re.search(r"Channel:\s*(\S+)", description)
    if channel_match:
        result["channel"] = channel_match.group(1)

    reservation_match = re.search(r"ReservationID:\s*(\S+)", description)
    if reservation_match:
        result["reservation_id"] = reservation_match.group(1)

    return result


def extract_date(dt_value) -> date:
    """Extract a date from an iCal datetime value.

    iCal events can have either DATE or DATETIME values.
    """
    if isinstance(dt_value, datetime):
        return dt_value.date()
    elif isinstance(dt_value, date):
        return dt_value
    else:
        # Try to parse as string
        return datetime.fromisoformat(str(dt_value)).date()


def parse_ical_feed(ical_content: str) -> list[ParsedBooking]:
    """Parse an iCal feed and return a list of bookings.

    Args:
        ical_content: Raw iCal content as string

    Returns:
        List of ParsedBooking objects
    """
    bookings: list[ParsedBooking] = []

    try:
        cal = Calendar.from_ical(ical_content)
    except Exception as e:
        raise ValueError(f"Failed to parse iCal content: {e}")

    for component in cal.walk():
        if component.name != "VEVENT":
            continue

        # Extract basic fields
        uid = str(component.get("UID", ""))
        summary = str(component.get("SUMMARY", "")).strip()
        description = str(component.get("DESCRIPTION", "")).strip()

        # Get dates
        dtstart = component.get("DTSTART")
        dtend = component.get("DTEND")

        if not dtstart or not dtend:
            continue

        check_in_date = extract_date(dtstart.dt)
        check_out_date = extract_date(dtend.dt)

        # Determine if this is a blocked period
        is_blocked = summary.lower() == "blocked" or not summary

        if is_blocked:
            bookings.append(
                ParsedBooking(
                    uid=uid,
                    guest_name="Blocked",
                    phone=None,
                    channel=None,
                    reservation_id=None,
                    check_in_date=check_in_date,
                    check_out_date=check_out_date,
                    is_blocked=True,
                )
            )
        else:
            # Parse guest details from description
            details = parse_description(description)

            bookings.append(
                ParsedBooking(
                    uid=uid,
                    guest_name=summary,
                    phone=details["phone"],
                    channel=details["channel"],
                    reservation_id=details["reservation_id"],
                    check_in_date=check_in_date,
                    check_out_date=check_out_date,
                    is_blocked=False,
                )
            )

    return bookings


def parse_ha_calendar_events(events: list[dict]) -> list[ParsedBooking]:
    """Parse events from the HA calendar API into ParsedBooking objects.

    HA calendar API returns events like:
    {
        "start": {"date": "2026-02-20"} or {"dateTime": "2026-02-20T14:00:00+00:00"},
        "end": {"date": "2026-02-22"} or {"dateTime": "..."},
        "summary": "Guest Name",
        "description": "Name: ... Phone: ... Channel: ...",
        "uid": "abc123",
    }
    """
    bookings: list[ParsedBooking] = []

    for event in events:
        summary = (event.get("summary") or "").strip()
        description = event.get("description") or ""
        uid = event.get("uid") or event.get("recurrence_id") or f"ha_{hash(summary + str(event.get('start')))}"

        # Extract dates — HA can return date or dateTime
        start_info = event.get("start", {})
        end_info = event.get("end", {})

        if "date" in start_info:
            check_in_date = date.fromisoformat(start_info["date"])
        elif "dateTime" in start_info:
            check_in_date = datetime.fromisoformat(start_info["dateTime"]).date()
        else:
            continue

        if "date" in end_info:
            check_out_date = date.fromisoformat(end_info["date"])
        elif "dateTime" in end_info:
            check_out_date = datetime.fromisoformat(end_info["dateTime"]).date()
        else:
            continue

        is_blocked = summary.lower() == "blocked" or not summary

        if is_blocked:
            bookings.append(ParsedBooking(
                uid=uid, guest_name="Blocked",
                phone=None, channel=None, reservation_id=None,
                check_in_date=check_in_date, check_out_date=check_out_date,
                is_blocked=True,
            ))
        else:
            details = parse_description(description)
            bookings.append(ParsedBooking(
                uid=uid, guest_name=summary,
                phone=details["phone"], channel=details["channel"],
                reservation_id=details["reservation_id"],
                check_in_date=check_in_date, check_out_date=check_out_date,
                is_blocked=False,
            ))

    return bookings


class ICalFetcher:
    """Fetches and parses iCal feeds."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def fetch_and_parse(self, url: str) -> list[ParsedBooking]:
        """Fetch an iCal feed and parse it.

        Args:
            url: The iCal URL to fetch

        Returns:
            List of ParsedBooking objects

        Raises:
            httpx.HTTPError: If the fetch fails
            ValueError: If the content cannot be parsed
        """
        if not url:
            return []

        client = await self._get_client()
        response = await client.get(url)
        response.raise_for_status()

        content = response.text
        return parse_ical_feed(content)

    async def fetch_multiple(
        self, urls: dict[str, str]
    ) -> dict[str, list[ParsedBooking] | Exception]:
        """Fetch multiple iCal feeds.

        Args:
            urls: Dictionary mapping calendar_id to URL

        Returns:
            Dictionary mapping calendar_id to list of bookings or exception
        """
        results: dict[str, list[ParsedBooking] | Exception] = {}

        for calendar_id, url in urls.items():
            if not url:
                results[calendar_id] = []
                continue

            try:
                bookings = await self.fetch_and_parse(url)
                results[calendar_id] = bookings
            except Exception as e:
                results[calendar_id] = e

        return results
