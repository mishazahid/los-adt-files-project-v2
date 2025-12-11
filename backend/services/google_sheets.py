"""
Google Sheets Service - Handles updating Google Sheets with processed data
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import os
import logging
import datetime
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """Service for interacting with Google Sheets"""
    
    def __init__(self):
        self.service = None
        self.sheets_service = None
        self._initialize_service()
        self.sheet_id = os.getenv("GOOGLE_SHEET_ID", "1CWV6su2PZUrP372Vd19N6sZzXcEFxZb_NNZwT0af0Wo")
        self.sheet_tab = os.getenv("GOOGLE_SHEET_TAB", "Summary")

    @staticmethod
    def _to_number(value):
        """
        Convert common string representations to numeric values so Sheets
        stores numbers instead of text or time formats.
        - "h:mm:ss", "m:ss" or "mm:ss" -> minutes as decimal (e.g., 2:13 -> 2.2167)
        - plain numeric strings -> float
        - pandas/NumPy timedeltas or datetime/time -> minutes as decimal
        Otherwise returns the original value.
        """
        try:
            # Keep blanks as-is
            if value is None or value == "":
                return value

            # Timedelta handling
            if isinstance(value, pd.Timedelta):
                return value.total_seconds() / 60.0
            if isinstance(value, datetime.timedelta):
                return value.total_seconds() / 60.0
            if isinstance(value, np.timedelta64):
                return value.astype('timedelta64[s]').astype(float) / 60.0

            # datetime/time -> minutes
            if isinstance(value, datetime.datetime):
                return value.hour * 60 + value.minute + value.second / 60.0
            if isinstance(value, datetime.time):
                return value.hour * 60 + value.minute + value.second / 60.0

            if isinstance(value, str):
                parts = value.split(":")
                if len(parts) == 3 and all(p.strip().isdigit() for p in parts):
                    hours = int(parts[0].strip())
                    minutes = int(parts[1].strip())
                    seconds = int(parts[2].strip())
                    return hours * 60 + minutes + seconds / 60.0
                if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
                    minutes = int(parts[0].strip())
                    seconds = int(parts[1].strip())
                    return minutes + seconds / 60.0
                # plain numeric string
                return float(value)

            # Already numeric
            return float(value)
        except Exception:
            return value

    @staticmethod
    def _normalize_numeric_columns(values, start_col: int = 1, end_col: int = 28):
        """
        Convert columns (1-indexed in Sheets terms; 0-indexed in Python lists)
        within the provided range to numeric using _to_number, skipping the header row.
        Default range: columns B (index 1) through AC (index 28).
        """
        for r_idx, row in enumerate(values):
            if r_idx == 0:
                continue  # header
            for c_idx in range(start_col, min(len(row), end_col + 1)):
                row[c_idx] = GoogleSheetsService._to_number(row[c_idx])
        return values

    def _get_sheet_id(self, spreadsheet_id: str, tab_title: str) -> Optional[int]:
        """Fetch sheetId for a given tab title."""
        try:
            meta = self.sheets_service.get(spreadsheetId=spreadsheet_id).execute()
            for sheet in meta.get("sheets", []):
                props = sheet.get("properties", {})
                if props.get("title") == tab_title:
                    return props.get("sheetId")
        except Exception as e:
            logger.warning(f"Could not get sheetId for {tab_title}: {e}")
        return None

    def _set_number_format(self, spreadsheet_id: str, tab_title: str, start_row: int = 2):
        """
        Force columns B:AC to plain number format to avoid time/date display.
        Applies from start_row to end of sheet.
        """
        sheet_id = self._get_sheet_id(spreadsheet_id, tab_title)
        if sheet_id is None:
            return
        try:
            requests = [{
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,  # zero-based
                        "startColumnIndex": 1,  # column B
                        "endColumnIndex": 29    # column AC (exclusive)
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0.00"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            }]
            self.sheets_service.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute()
            logger.info(f"Applied numeric format to {tab_title}!B:AC")
        except Exception as e:
            logger.warning(f"Failed to apply numeric format to {tab_title}: {e}")
    
    def _initialize_service(self):
        """Initialize Google Sheets API service"""
        try:
            import json
            
            SCOPES = [
                'https://www.googleapis.com/auth/spreadsheets'
            ]
            
            # Try to get credentials from environment variable first (for cloud deployments)
            credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
            credentials = None
            
            if credentials_json:
                try:
                    # Parse JSON string from environment variable
                    credentials_dict = json.loads(credentials_json)
                    credentials = service_account.Credentials.from_service_account_info(
                        credentials_dict,
                        scopes=SCOPES
                    )
                    logger.info("Google Sheets service initialized from GOOGLE_CREDENTIALS_JSON")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")
                    logger.error(f"JSON content (first 100 chars): {credentials_json[:100] if credentials_json else 'None'}")
                    credentials = None
                except Exception as e:
                    logger.error(f"Error creating credentials from GOOGLE_CREDENTIALS_JSON: {e}")
                    credentials = None
            
            # Fall back to file path if JSON not available or failed
            if not credentials:
                credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
                
                if not os.path.exists(credentials_path):
                    logger.warning(f"Google credentials not found at {credentials_path}. Sheets features will be disabled.")
                    logger.warning(f"GOOGLE_CREDENTIALS_JSON was: {'SET' if credentials_json else 'NOT SET'}")
                    return
                
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        credentials_path,
                        scopes=SCOPES
                    )
                    logger.info(f"Google Sheets service initialized from file: {credentials_path}")
                except Exception as e:
                    logger.error(f"Error loading credentials from file {credentials_path}: {e}")
                    return
            
            if credentials:
                self.service = build('sheets', 'v4', credentials=credentials)
                self.sheets_service = self.service.spreadsheets()
                logger.info("Google Sheets service initialized successfully")
            else:
                logger.error("Failed to load credentials - both JSON and file methods failed")
                raise ValueError("Failed to load credentials")
        
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")
            self.service = None
            self.sheets_service = None
    
    async def update_sheets(self, csv_file: Path, facility_values: Optional[dict] = None) -> str:
        """
        Update Google Sheets with data from CSV file
        Optionally populate GS, PPS, INC columns based on facility values
        
        Args:
            csv_file: Path to the CSV file
            facility_values: Dict mapping facility names to {GS, PPS, INC} values
        
        Returns:
            Google Sheets URL
        """
        if not self.sheets_service:
            logger.warning("Google Sheets service not available")
            return ""
        
        try:
            # Work on a copy so we don't mutate caller's dict (e.g., _quarter key)
            facility_values = dict(facility_values) if facility_values else {}
            
            # Read CSV file
            df = pd.read_csv(csv_file)

            # Normalize values to numeric where possible to avoid time/text formatting
            df = df.applymap(self._to_number)
            
            # Extract quarter value (for Executive sheet only, not Summary sheet)
            quarter_value = None
            if facility_values and '_quarter' in facility_values:
                quarter_value = facility_values.pop('_quarter')  # Remove from dict after extracting
            
            # Add GS, PPS, INC columns to Summary sheet if facility_values provided
            # Note: Quarter goes to Executive sheet, not Summary sheet
            if facility_values:
                # Ensure columns exist (but NOT Quarter - that goes to Executive sheet)
                if 'GS' not in df.columns:
                    df['GS'] = ''
                if 'PPS' not in df.columns:
                    df['PPS'] = ''
                if 'INC' not in df.columns:
                    df['INC'] = ''
                
                # Explicitly reset values to avoid stale data when facility count shrinks
                df['GS'] = ''
                df['PPS'] = ''
                df['INC'] = ''
                
                # Populate GS, PPS, INC values for each facility
                if 'Facility' in df.columns:
                    for facility_name, values in facility_values.items():
                        # Skip special keys like '_quarter'
                        if facility_name.startswith('_'):
                            continue
                        # Match facility name (case-insensitive, handle variations)
                        mask = df['Facility'].str.contains(facility_name, case=False, na=False)
                        if mask.any():
                            if 'GS' in values:
                                df.loc[mask, 'GS'] = values['GS']
                            if 'PPS' in values:
                                df.loc[mask, 'PPS'] = values['PPS']
                            if 'INC' in values:
                                df.loc[mask, 'INC'] = values['INC']
                            logger.info(f"Populated GS, PPS, INC for {facility_name}: {mask.sum()} rows")
                else:
                    logger.warning("'Facility' column not found in CSV, cannot populate facility-specific values")
            
            # Convert DataFrame to list of lists (values)
            values = [df.columns.tolist()]  # Header row
            values.extend(df.values.tolist())

            # Normalize numeric columns B:AC to ensure numbers, not strings/times
            values = self._normalize_numeric_columns(values, start_col=1, end_col=28)
            
            # Clear existing data
            # 1) Hard-clear GS/PPS/INC columns to avoid stale data even if new payload is smaller
            self.sheets_service.values().batchClear(
                spreadsheetId=self.sheet_id,
                body={"ranges": [f"{self.sheet_tab}!AA:AC"]}
            ).execute()
            # 2) Clear main data range to remove extra rows
            range_name = f"{self.sheet_tab}!A1:Z10000"
            self.sheets_service.values().clear(
                spreadsheetId=self.sheet_id,
                range=range_name
            ).execute()
            
            # Update with new data
            body = {
                'values': values
            }
            
            result = self.sheets_service.values().update(
                spreadsheetId=self.sheet_id,
                range=f"{self.sheet_tab}!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"Updated {self.sheet_tab} sheet: {result.get('updatedCells')} cells updated")

            # Force numeric format on Summary sheet columns B:AC
            self._set_number_format(self.sheet_id, self.sheet_tab, start_row=2)
            
            # Update Executive sheet with Quarter value if provided
            if quarter_value:
                await self.update_executive_sheet_quarter(quarter_value)
            
            # Copy data to Test sheet (Raw_Data tab)
            test_sheet_link = await self.copy_to_test_sheet(quarter_value)
            
            # Return both sheet URLs as a dict
            return {
                "facility_summary": f"https://docs.google.com/spreadsheets/d/{self.sheet_id}",
                "test_fac": test_sheet_link
            }
        
        except HttpError as e:
            logger.error(f"Error updating Google Sheets: {e}")
            return ""
        except Exception as e:
            logger.error(f"Error processing CSV for Sheets: {e}")
            return ""
    
    async def update_executive_sheet_quarter(self, quarter_value: str) -> bool:
        """
        Update the Executive sheet with Quarter value in the first column (A column)
        Preserves existing sheet structure and only updates the Quarter column
        
        Args:
            quarter_value: The quarter value to populate (e.g., "Q3 2025")
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.sheets_service:
            logger.warning("Google Sheets service not available")
            return False
        
        try:
            executive_tab = "Executive"
            
            # First, check if Executive sheet exists, if not create it
            try:
                # Try to read from Executive sheet to check if it exists
                self.sheets_service.values().get(
                    spreadsheetId=self.sheet_id,
                    range=f"{executive_tab}!A1"
                ).execute()
            except HttpError as e:
                if e.resp.status == 400:
                    # Sheet doesn't exist, create it
                    logger.info(f"Executive sheet not found, creating it...")
                    requests = [{
                        'addSheet': {
                            'properties': {
                                'title': executive_tab
                            }
                        }
                    }]
                    self.sheets_service.batchUpdate(
                        spreadsheetId=self.sheet_id,
                        body={'requests': requests}
                    ).execute()
                    logger.info(f"Created Executive sheet")
                else:
                    raise
            
            # Get existing data from Executive sheet to preserve structure
            try:
                existing_data = self.sheets_service.values().get(
                    spreadsheetId=self.sheet_id,
                    range=f"{executive_tab}!A:Z"
                ).execute()
                existing_values = existing_data.get('values', [])
            except:
                existing_values = []
            
            # Update or add Quarter in first column (column A)
            if existing_values:
                # Update first column header to "Quarter" if needed
                if len(existing_values) > 0:
                    if len(existing_values[0]) == 0 or existing_values[0][0] != "Quarter":
                        if len(existing_values[0]) == 0:
                            existing_values[0] = ["Quarter"]
                        else:
                            existing_values[0][0] = "Quarter"
                
                # Populate Quarter for all data rows (skip header row)
                num_rows = len(existing_values)
                for i in range(1, num_rows):
                    if len(existing_values[i]) == 0:
                        existing_values[i] = [quarter_value]
                    else:
                        existing_values[i][0] = quarter_value
            else:
                # No existing data, create new structure with Quarter header
                existing_values = [
                    ["Quarter"],  # Header
                    [quarter_value]  # First data row
                ]
            
            # Update only column A (first column) with Quarter values
            # Build values array with only first column
            quarter_values = []
            for row in existing_values:
                if len(row) > 0:
                    quarter_values.append([row[0]])  # Only first column
                else:
                    quarter_values.append([quarter_value])
            
            # Update the Executive sheet - only column A
            body = {
                'values': quarter_values
            }
            
            result = self.sheets_service.values().update(
                spreadsheetId=self.sheet_id,
                range=f"{executive_tab}!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"Updated Executive sheet Quarter column (A1): {result.get('updatedCells')} cells updated with value '{quarter_value}'")
            return True
            
        except HttpError as e:
            logger.error(f"Error updating Executive sheet: {e}")
            return False
        except Exception as e:
            logger.error(f"Error updating Executive sheet Quarter: {e}")
            return False
    
    async def copy_to_test_sheet(self, quarter_value: Optional[str] = None) -> str:
        """
        Copy data from Summary sheet to Test sheet's Raw_Data tab
        Clears existing data in Raw_Data tab and pastes Summary data starting at A1
        Also updates Summary tab B2 with quarter value if provided
        
        Args:
            quarter_value: Optional quarter value to write to Summary tab B2
        
        Returns:
            str: Test sheet URL
        """
        if not self.sheets_service:
            logger.warning("Google Sheets service not available")
            return False
        
        try:
            # Test sheet configuration
            test_sheet_id = "1FvZLxUS36JON-O8yY6zvrxxYyfOMHtHzmKAWUd5ytZk"
            test_sheet_tab = "Raw_Data"
            
            # Read data from Summary tab of current sheet
            # Use wider range to include GS, PPS, INC columns (AA, AB, AC)
            logger.info(f"Reading data from Summary tab of sheet {self.sheet_id}")
            source_data = self.sheets_service.values().get(
                spreadsheetId=self.sheet_id,
                range=f"{self.sheet_tab}!A:AC"
            ).execute()
            
            source_values = source_data.get('values', [])
            if not source_values:
                logger.warning("No data found in Summary sheet to copy")
                return False
            
            # Normalize numeric columns B:AC before writing to Test sheet
            source_values = self._normalize_numeric_columns(source_values, start_col=1, end_col=28)
            
            logger.info(f"Found {len(source_values)} rows to copy to Test sheet")
            
            # Clear existing data in Raw_Data tab of Test sheet
            # Clear wider range to include GS, PPS, INC columns
            logger.info(f"Clearing existing data in {test_sheet_tab} tab of Test sheet")
            self.sheets_service.values().clear(
                spreadsheetId=test_sheet_id,
                range=f"{test_sheet_tab}!A1:AC10000"
            ).execute()
            
            # Paste data starting at A1
            body = {
                'values': source_values
            }
            
            result = self.sheets_service.values().update(
                spreadsheetId=test_sheet_id,
                range=f"{test_sheet_tab}!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"Copied {result.get('updatedCells', 0)} cells to Test sheet Raw_Data tab")

            # Force numeric format on Test sheet Raw_Data columns B:AC
            self._set_number_format(test_sheet_id, test_sheet_tab, start_row=2)
            
            # Update Summary tab B2 with quarter value if provided
            if quarter_value:
                try:
                    self.sheets_service.values().update(
                        spreadsheetId=test_sheet_id,
                        range="Summary!B2",
                        valueInputOption='USER_ENTERED',
                        body={'values': [[quarter_value]]}
                    ).execute()
                    logger.info(f"Updated Test sheet Summary tab B2 with quarter value: {quarter_value}")
                except Exception as e:
                    logger.warning(f"Could not update quarter value in Test sheet Summary tab: {e}")
            
            # Return the Test sheet URL
            return f"https://docs.google.com/spreadsheets/d/{test_sheet_id}"
            
        except HttpError as e:
            logger.error(f"HTTP error copying to Test sheet: {e}")
            return False
        except Exception as e:
            logger.error(f"Error copying to Test sheet: {e}")
            return False
    
    async def append_data(self, data: list, sheet_name: Optional[str] = None) -> bool:
        """
        Append data to Google Sheets
        """
        if not self.sheets_service:
            logger.warning("Google Sheets service not available")
            return False
        
        try:
            tab = sheet_name or self.sheet_tab
            range_name = f"{tab}!A:Z"
            
            body = {
                'values': data
            }
            
            self.sheets_service.values().append(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"Appended data to Google Sheets")
            return True
        
        except Exception as e:
            logger.error(f"Error appending data to Sheets: {e}")
            return False

