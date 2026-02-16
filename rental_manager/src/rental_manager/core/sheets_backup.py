"""Google Sheets backup for emergency codes."""

import logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetsBackup:
    """Backup emergency codes to a Google Sheet."""

    def __init__(self, credentials_path: str, spreadsheet_id: str):
        self._spreadsheet_id = spreadsheet_id
        creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        self._client = gspread.authorize(creds)

    def update_emergency_codes(self, codes: list[dict]) -> None:
        """Overwrite the sheet with current emergency codes.

        Args:
            codes: List of dicts with keys: lock_name, entity_id, emergency_code, lock_type
        """
        sheet = self._client.open_by_key(self._spreadsheet_id).sheet1
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        # Build rows: header + data
        rows = [["Lock Name", "Entity ID", "Type", "Emergency Code", "Last Updated"]]
        for lock in codes:
            rows.append([
                lock.get("lock_name", ""),
                lock.get("entity_id", ""),
                lock.get("lock_type", ""),
                lock.get("emergency_code", "----"),
                now,
            ])

        # Clear and write
        sheet.clear()
        sheet.update(range_name="A1", values=rows)
        logger.info("Updated %d emergency codes in Google Sheet", len(codes))
