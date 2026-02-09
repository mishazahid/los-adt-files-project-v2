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
        self.medilodge_q3_sheet_id = os.getenv("MEDILODGE_Q3_DATA_SHEET_ID", "1BlTxrYp5368Ggl5fRDI99O27AECH5auLM-mxJhr_tzw")

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
    def _normalize_numeric_columns(values, start_col: int = 1, end_col: int = 28, skip_columns: list = None):
        """
        Convert columns (1-indexed in Sheets terms; 0-indexed in Python lists)
        within the provided range to numeric using _to_number, skipping the header row.
        Default range: columns B (index 1) through AC (index 28).
        
        Args:
            values: List of rows (each row is a list of values)
            start_col: Starting column index (0-based)
            end_col: Ending column index (0-based)
            skip_columns: List of column names to skip conversion (e.g., ['HD', 'HDN', 'HT'])
        """
        # Get column indices to skip based on header row
        skip_indices = []
        if skip_columns and len(values) > 0:
            header_row = values[0]
            for col_name in skip_columns:
                try:
                    col_idx = header_row.index(col_name)
                    skip_indices.append(col_idx)
                    logger.info(f"Skipping conversion for ratio column '{col_name}' at index {col_idx}")
                except ValueError:
                    pass  # Column not found in header
        
        for r_idx, row in enumerate(values):
            if r_idx == 0:
                continue  # header
            # Ensure row is long enough
            while len(row) <= end_col:
                row.append('')
            for c_idx in range(start_col, min(len(row), end_col + 1)):
                if c_idx < len(row):
                    # Skip if already empty string or None
                    if row[c_idx] != '' and row[c_idx] is not None:
                        # Skip conversion for ratio columns
                        if c_idx not in skip_indices:
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
            # Keep ratio columns as strings to prevent automatic time conversion
            ratio_columns = ['HD', 'HDN', 'HT', 'Ex', 'Cus', 'AL', 'OT', 'SNF']
            dtype_dict = {col: str for col in ratio_columns}
            df = pd.read_csv(csv_file, dtype=dtype_dict)
            
            # Log what we're reading
            logger.info(f"Reading CSV file: {csv_file}")
            logger.info(f"DataFrame shape: {df.shape}, columns: {list(df.columns)}")
            
            # Handle empty dataframe
            if df.empty:
                logger.error(f"CSV file {csv_file} is empty or has no data rows. Cannot update Google Sheets.")
                # Return sheet links even if empty so frontend doesn't break
                try:
                    test_sheet_link = await self.copy_to_test_sheet(None)
                except:
                    test_sheet_link = ""
                return {
                    "facility_summary": f"https://docs.google.com/spreadsheets/d/{self.sheet_id}",
                    "test_fac": test_sheet_link
                }

            # Normalize values to numeric where possible to avoid time/text formatting
            # Use apply with map instead of deprecated applymap, and handle NaN values
            # Skip ratio columns (HD, HDN, HT, Ex, Cus, AL, OT, SNF) to preserve "count:total" format
            ratio_columns = ['HD', 'HDN', 'HT', 'Ex', 'Cus', 'AL', 'OT', 'SNF']
            for col in df.columns:
                if col not in ratio_columns:
                    df[col] = df[col].apply(lambda x: self._to_number(x) if pd.notna(x) else x)
            
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
                    # Log all facility names in CSV for debugging
                    unique_facilities = df['Facility'].unique()
                    logger.info(f"Facilities found in CSV: {list(unique_facilities)}")
                    logger.info(f"Facility values to match: {list(facility_values.keys())}")
                    
                    for facility_name, values in facility_values.items():
                        # Skip special keys like '_quarter'
                        if facility_name.startswith('_'):
                            continue
                        
                        # Try multiple matching strategies
                        # 1. Exact match (case-insensitive)
                        mask_exact = df['Facility'].str.lower().str.strip() == facility_name.lower().strip()
                        # 2. Contains match (facility_name in CSV facility)
                        mask_contains = df['Facility'].str.contains(facility_name, case=False, na=False)
                        # 3. Reverse contains (CSV facility in facility_name) - for partial matches
                        mask_reverse = df['Facility'].apply(
                            lambda x: facility_name.lower() in str(x).lower() if pd.notna(x) else False
                        )
                        
                        # Combine all matches
                        mask = mask_exact | mask_contains | mask_reverse
                        
                        if mask.any():
                            if 'GS' in values:
                                df.loc[mask, 'GS'] = values['GS']
                            if 'PPS' in values:
                                df.loc[mask, 'PPS'] = values['PPS']
                            if 'INC' in values:
                                df.loc[mask, 'INC'] = values['INC']
                            matched_facilities = df.loc[mask, 'Facility'].unique().tolist()
                            logger.info(f"Populated GS, PPS, INC for '{facility_name}': matched {mask.sum()} rows with facilities: {matched_facilities}")
                        else:
                            logger.warning(f"No match found for facility '{facility_name}' in CSV. Available facilities: {list(unique_facilities)}")
                else:
                    logger.warning("'Facility' column not found in CSV, cannot populate facility-specific values")
            
            # Convert DataFrame to list of lists (values)
            # Replace NaN with empty strings for Google Sheets BEFORE converting to list
            df = df.fillna('')
            
            # Convert to list, handling any remaining NaN values
            values = [df.columns.tolist()]  # Header row
            for idx, row in df.iterrows():
                row_list = []
                for val in row:
                    # Convert any remaining NaN/None to empty string
                    if pd.isna(val) or val is None:
                        row_list.append('')
                    else:
                        row_list.append(val)
                values.append(row_list)

            # Log what we're about to write
            logger.info(f"Preparing to write {len(values)} rows to Google Sheets (1 header + {len(values)-1} data rows)")
            if len(values) > 1:
                logger.info(f"First data row sample: {values[1][:5]}...")  # Log first 5 columns of first data row

            # Normalize numeric columns B:AC to ensure numbers, not strings/times
            # Skip ratio columns (HD, HDN, HT, Ex, Cus, AL, OT, SNF) to preserve "count:total" format
            ratio_columns = ['HD', 'HDN', 'HT', 'Ex', 'Cus', 'AL', 'OT', 'SNF']
            values = self._normalize_numeric_columns(values, start_col=1, end_col=28, skip_columns=ratio_columns)
            
            # Prepend single quote to ratio columns to force Google Sheets to treat as text
            header = values[0]
            ratio_col_indices = [i for i, col in enumerate(header) if col in ratio_columns]
            for row_idx in range(1, len(values)):  # Skip header row
                for col_idx in ratio_col_indices:
                    if col_idx < len(values[row_idx]) and values[row_idx][col_idx]:
                        # Add single quote prefix to force text format in Google Sheets
                        values[row_idx][col_idx] = f"'{values[row_idx][col_idx]}"
            
            # Clear existing data
            # 1) Hard-clear GS/PPS/INC columns to avoid stale data even if new payload is smaller
            try:
                self.sheets_service.values().batchClear(
                    spreadsheetId=self.sheet_id,
                    body={"ranges": [f"{self.sheet_tab}!AA:AC"]}
                ).execute()
            except Exception as e:
                logger.warning(f"Could not clear AA:AC columns: {e}")
            
            # 2) Clear main data range to remove extra rows
            try:
                range_name = f"{self.sheet_tab}!A1:Z10000"
                self.sheets_service.values().clear(
                    spreadsheetId=self.sheet_id,
                    range=range_name
                ).execute()
                logger.info(f"Cleared existing data from {range_name}")
            except Exception as e:
                logger.warning(f"Could not clear existing data: {e}")
            
            # Update with new data
            body = {
                'values': values
            }
            
            try:
                result = self.sheets_service.values().update(
                    spreadsheetId=self.sheet_id,
                    range=f"{self.sheet_tab}!A1",
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
                
                updated_cells = result.get('updatedCells', 0)
                updated_rows = result.get('updatedRows', 0)
                updated_cols = result.get('updatedColumns', 0)
                logger.info(f"Updated {self.sheet_tab} sheet: {updated_cells} cells, {updated_rows} rows, {updated_cols} columns")
                
                if updated_cells == 0 or updated_rows == 0:
                    logger.error(f"ERROR: No cells/rows were updated in Google Sheets!")
                    logger.error(f"Attempted to write {len(values)} rows")
                    logger.error(f"First row: {values[0] if values else 'EMPTY'}")
                    logger.error(f"Second row: {values[1] if len(values) > 1 else 'NO DATA'}")
                    raise ValueError(f"Failed to write data to Google Sheets - no cells updated")
            except Exception as e:
                logger.error(f"Error writing to Google Sheets: {e}")
                logger.error(f"Attempted to write {len(values)} rows")
                raise

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
            # Return sheet links even on error (sheets might still exist)
            try:
                test_sheet_link = await self.copy_to_test_sheet(None)
            except:
                test_sheet_link = ""
            return {
                "facility_summary": f"https://docs.google.com/spreadsheets/d/{self.sheet_id}",
                "test_fac": test_sheet_link
            }
        except Exception as e:
            logger.error(f"Error processing CSV for Sheets: {e}")
            # Return sheet links even on error (sheets might still exist)
            try:
                test_sheet_link = await self.copy_to_test_sheet(None)
            except:
                test_sheet_link = ""
            return {
                "facility_summary": f"https://docs.google.com/spreadsheets/d/{self.sheet_id}",
                "test_fac": test_sheet_link
            }
    
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
            return ""
        
        # Test sheet configuration (defined outside try block for error handlers)
        test_sheet_id = "1FvZLxUS36JON-O8yY6zvrxxYyfOMHtHzmKAWUd5ytZk"
        test_sheet_tab = "Raw_Data"
        
        try:
            
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
                # Still return the sheet URL even if no data
                return f"https://docs.google.com/spreadsheets/d/{test_sheet_id}"
            
            # Normalize numeric columns B:AC before writing to Test sheet
            # Skip ratio columns (HD, HDN, HT, Ex, Cus, AL, OT, SNF) to preserve "count:total" format
            ratio_columns = ['HD', 'HDN', 'HT', 'Ex', 'Cus', 'AL', 'OT', 'SNF']
            source_values = self._normalize_numeric_columns(source_values, start_col=1, end_col=28, skip_columns=ratio_columns)
            
            # Prepend single quote to ratio columns to force Google Sheets to treat as text
            if len(source_values) > 0:
                header = source_values[0]
                ratio_col_indices = [i for i, col in enumerate(header) if col in ratio_columns]
                for row_idx in range(1, len(source_values)):  # Skip header row
                    for col_idx in ratio_col_indices:
                        if col_idx < len(source_values[row_idx]) and source_values[row_idx][col_idx]:
                            # Add single quote prefix to force text format in Google Sheets
                            source_values[row_idx][col_idx] = f"'{source_values[row_idx][col_idx]}"
            
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
            # Still return the sheet URL even on error
            return f"https://docs.google.com/spreadsheets/d/{test_sheet_id}"
        except Exception as e:
            logger.error(f"Error copying to Test sheet: {e}")
            # Still return the sheet URL even on error
            return f"https://docs.google.com/spreadsheets/d/{test_sheet_id}"
    
    async def copy_raw_data_to_facility_data(self) -> bool:
        """
        Copy 6 columns from Raw_Data tab to Facility_Data tab in Test Fac sheet.
        Column structure in Facility_Data:
        - Column A: Facilities (facility names from Raw_Data)
        - Column B: Managed Care Average LOS (from Raw_Data "LOS Man Avg")
        - Column C: Medicare A Average LOS (from Raw_Data "LOS Med Avg")
        - Column D: Section GG Improv (from Raw_Data "INC")
        - Column E: 5 Day Mean (from Raw_Data "GS")
        - Column F: End of PPS Mean (from Raw_Data "PPS")
        
        Clears only A1:A9 and F1:F9 in Facility_Data tab (preserves graphs).
        Copies all facilities from Raw_Data.
        Adds "Network Average" row at the end with averages for columns B-F.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.sheets_service:
            logger.warning("Google Sheets service not available")
            return False
        
        try:
            # Test sheet configuration
            test_sheet_id = "1FvZLxUS36JON-O8yY6zvrxxYyfOMHtHzmKAWUd5ytZk"
            raw_data_tab = "Raw_Data"
            facility_data_tab = "Facility_Data"
            
            # Column mapping: (source_col_name_in_raw_data, dest_column_letter)
            column_mapping = [
                ("LOS Man Avg", "B"),  # Managed Care Average LOS
                ("LOS Med Avg", "C"),  # Medicare A Average LOS
                ("INC", "D"),          # Section GG Improv
                ("GS", "E"),          # 5 Day Mean
                ("PPS", "F")          # End of PPS Mean
            ]
            
            # Read Raw_Data tab
            logger.info(f"Reading data from {raw_data_tab} tab")
            raw_data = self.sheets_service.values().get(
                spreadsheetId=test_sheet_id,
                range=f"{raw_data_tab}!A:AC"
            ).execute()
            
            raw_values = raw_data.get('values', [])
            if not raw_values or len(raw_values) < 2:
                logger.warning(f"No data found in {raw_data_tab} tab (need at least header + 1 row)")
                return False
            
            # Get headers from Raw_Data
            raw_headers = raw_values[0]
            raw_data_rows = raw_values[1:]  # Skip header
            
            # Find Facility column index in Raw_Data
            try:
                facility_col_idx = raw_headers.index("Facility")
            except ValueError:
                logger.error("'Facility' column not found in Raw_Data")
                return False
            
            # Find source column indices for the mapped columns
            source_col_indices = {}
            for source_col, _ in column_mapping:
                try:
                    idx = raw_headers.index(source_col)
                    source_col_indices[source_col] = idx
                    logger.info(f"Found source column '{source_col}' at index {idx}")
                except ValueError:
                    logger.warning(f"Source column '{source_col}' not found in Raw_Data headers")
                    return False
            
            # Clear A2:F100 in Facility_Data tab (preserve header row 1, clear all old data)
            # This ensures ALL previous data is removed before writing new data
            logger.info(f"Clearing data range A2:F100 in {facility_data_tab} tab (preserving header and graphs)")
            self.sheets_service.values().clear(
                spreadsheetId=test_sheet_id,
                range=f"{facility_data_tab}!A2:F100"
            ).execute()
            logger.info(f"Successfully cleared old data from {facility_data_tab} tab")
            
            # Write headers in row 1 if needed (or update them)
            headers = ["Facilities", "Managed Care Average LOS", "Medicare A Average LOS", 
                      "Section GG Improv", "5 Day Mean", "End of PPS Mean"]
            self.sheets_service.values().update(
                spreadsheetId=test_sheet_id,
                range=f"{facility_data_tab}!A1:F1",
                valueInputOption='USER_ENTERED',
                body={'values': [headers]}
            ).execute()
            logger.info("Updated headers in Facility_Data tab")
            
            # Prepare data rows: collect all facility data from Raw_Data
            data_rows = []
            numeric_values = {dest_col: [] for _, dest_col in column_mapping}
            
            for row in raw_data_rows:
                if len(row) <= facility_col_idx:
                    continue
                
                facility_name = row[facility_col_idx] if len(row) > facility_col_idx else ""
                if not facility_name or str(facility_name).strip() == "":
                    continue
                
                # Build row data: [Facility, LOS Man Avg, LOS Med Avg, INC, GS, PPS]
                row_data = [facility_name]
                
                for source_col, dest_col in column_mapping:
                    source_idx = source_col_indices[source_col]
                    value = ""
                    if len(row) > source_idx:
                        value = row[source_idx]
                        # Convert to number if possible for averaging
                        try:
                            if value and str(value).strip() != "":
                                num_value = float(value)
                                numeric_values[dest_col].append(num_value)
                        except (ValueError, TypeError):
                            pass
                    row_data.append(value)
                
                data_rows.append(row_data)
            
            if not data_rows:
                logger.warning("No facility data to copy from Raw_Data")
                return False
            
            logger.info(f"Prepared {len(data_rows)} facility rows to copy")
            
            # Write all data rows starting from row 2 (row 1 is headers)
            # Write in batches to avoid API limits
            batch_size = 100
            for i in range(0, len(data_rows), batch_size):
                batch = data_rows[i:i + batch_size]
                start_row = i + 2  # Row 2, 3, 4, etc.
                end_row = start_row + len(batch) - 1
                
                self.sheets_service.values().update(
                    spreadsheetId=test_sheet_id,
                    range=f"{facility_data_tab}!A{start_row}:F{end_row}",
                    valueInputOption='USER_ENTERED',
                    body={'values': batch}
                ).execute()
            
            logger.info(f"Copied {len(data_rows)} facilities to {facility_data_tab} tab")
            
            # Add "Network Average" row at the end
            network_avg_row = len(data_rows) + 2  # After all data rows, row 1 is header
            
            # Write "Network Average" label in column A
            self.sheets_service.values().update(
                spreadsheetId=test_sheet_id,
                range=f"{facility_data_tab}!A{network_avg_row}",
                valueInputOption='USER_ENTERED',
                body={'values': [["Network Average"]]}
            ).execute()
            
            # Calculate and write averages for columns B-F
            avg_row_data = ["Network Average"]
            for source_col, dest_col in column_mapping:
                if dest_col in numeric_values and numeric_values[dest_col]:
                    avg_value = sum(numeric_values[dest_col]) / len(numeric_values[dest_col])
                    avg_row_data.append(avg_value)
                    logger.info(f"Network Average for '{dest_col}': {avg_value:.2f}")
                else:
                    avg_row_data.append("")
            
            # Write averages to columns B-F
            self.sheets_service.values().update(
                spreadsheetId=test_sheet_id,
                range=f"{facility_data_tab}!B{network_avg_row}:F{network_avg_row}",
                valueInputOption='USER_ENTERED',
                body={'values': [avg_row_data[1:]]}  # Skip first element (label)
            ).execute()
            
            logger.info(f"Added Network Average row at row {network_avg_row}")
            return True
            
        except HttpError as e:
            logger.error(f"HTTP error copying Raw_Data to Facility_Data: {e}")
            return False
        except Exception as e:
            logger.error(f"Error copying Raw_Data to Facility_Data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _column_index_to_letter(self, col_idx: int) -> str:
        """Convert 0-based column index to Excel column letter (A, B, ..., Z, AA, AB, ...)"""
        result = ""
        col_idx += 1  # Convert to 1-based
        while col_idx > 0:
            col_idx -= 1
            result = chr(65 + (col_idx % 26)) + result
            col_idx //= 26
        return result
    
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
    
    def _map_facility_to_tab_name(self, facility_name: str) -> Optional[str]:
        """
        Map facility name from ADT files to Google Sheet tab name.
        
        Args:
            facility_name: Facility name from ADT file (e.g., "Medilodge of Farmington")
        
        Returns:
            Tab name in Medilodge Q3 Data sheet, or None if not found
        """
        # Normalize facility name for matching
        facility_lower = facility_name.lower().strip()
        logger.info(f"Mapping facility: '{facility_name}' (lowercase: '{facility_lower}')")
        
        # Remove "medilodge of" or "of" prefix for better matching
        if facility_lower.startswith('medilodge of '):
            facility_lower = facility_lower.replace('medilodge of ', '', 1).strip()
            logger.info(f"  Removed 'medilodge of' prefix -> '{facility_lower}'")
        elif facility_lower.startswith('of '):
            facility_lower = facility_lower.replace('of ', '', 1).strip()
            logger.info(f"  Removed 'of' prefix -> '{facility_lower}'")
        
        # Mapping of facility names to tab names
        # Tab names: Clare, Mt Pleasant, Holland, at the Shore, Ludington, Grand Rapids, 
        # Wyoming, Sault Ste. Marie, Autumn Woods, Farmington, Grand Blanc, Howell, 
        # Monroe, Livingston, Montrose, Shoreline, Sterling Heights
        facility_mapping = {
            'clare': 'Clare',
            'mt pleasant': 'Mt. Pleasant',
            'mt. pleasant': 'Mt. Pleasant',
            'holland': 'Holland',
            'at the shore': 'at the Shore',
            'ludington': 'Ludington',
            'grand rapids': 'Grand Rapids',
            'wyoming': 'Wyoming',
            'sault ste. marie': 'Sault Ste. Marie',
            'sault st. marie': 'Sault Ste. Marie',
            'autumn woods': 'Autumn Woods',
            'autumn woods residential': 'Autumn Woods',
            'farmington': 'Farmington',
            'grand blanc': 'Grand Blanc',
            'howell': 'Howell',
            'monroe': 'Monroe',
            'livingston': 'Livingston',
            'montrose': 'Montrose',
            'shoreline': 'Shoreline',
            'sterling heights': 'Sterling Heights',
            'sterling': 'Sterling Heights',
        }
        
        # Try exact match first
        for key, tab_name in facility_mapping.items():
            if key == facility_lower or key in facility_lower:
                logger.info(f"  Matched '{facility_lower}' to tab '{tab_name}' via key '{key}'")
                return tab_name
        
        # Try partial matches (check if any key is a substring of facility_lower)
        for key, tab_name in facility_mapping.items():
            if key in facility_lower or facility_lower in key:
                logger.info(f"  Matched '{facility_lower}' to tab '{tab_name}' via partial key '{key}'")
                return tab_name
        
        logger.warning(f"Could not map facility '{facility_name}' (normalized: '{facility_lower}') to a tab name. Available keys: {list(facility_mapping.keys())}")
        return None
    
    def fetch_facility_metrics(self, facility_names: list, sheet_id: Optional[str] = None) -> dict:
        """
        Fetch GS, PPS, and INC values from Google Sheet for given facilities.
        
        Args:
            facility_names: List of facility names (e.g., ["Medilodge of Farmington", "Medilodge of Clare"])
            sheet_id: Optional Google Sheet ID. If not provided, uses default from env var.
        
        Returns:
            Dictionary mapping facility names to their metrics:
            {
                "Medilodge of Farmington": {"GS": 15.5, "PPS": 18.2, "INC": 2.7},
                "Medilodge of Clare": {"GS": 12.3, "PPS": 14.8, "INC": 2.5},
                ...
            }
        """
        if not self.sheets_service:
            logger.error("Google Sheets service not initialized")
            return {}
        
        # Use provided sheet_id or fall back to default
        target_sheet_id = sheet_id if sheet_id else self.medilodge_q3_sheet_id
        
        # Verify sheet is accessible
        try:
            sheet_metadata = self.sheets_service.get(spreadsheetId=target_sheet_id).execute()
            sheet_title = sheet_metadata.get('properties', {}).get('title', 'Unknown')
            logger.info(f"Accessing Google Sheet: '{sheet_title}' (ID: {target_sheet_id})")
            
            # List available tabs
            tabs = [sheet.get('properties', {}).get('title', '') for sheet in sheet_metadata.get('sheets', [])]
            logger.info(f"Available tabs in sheet: {tabs}")
        except Exception as e:
            logger.error(f"Error accessing Google Sheet (ID: {target_sheet_id}): {e}")
            return {}
        
        results = {}
        
        for facility_name in facility_names:
            tab_name_mapped = self._map_facility_to_tab_name(facility_name)
            if not tab_name_mapped:
                logger.warning(f"Skipping facility '{facility_name}' - no matching tab found in mapping")
                continue
            
            # Find the exact tab name (case-insensitive) from available tabs
            tab_name = None
            for available_tab in tabs:
                if available_tab.lower() == tab_name_mapped.lower():
                    tab_name = available_tab  # Use the exact case from the sheet
                    logger.info(f"Mapped facility '{facility_name}' to tab '{tab_name}'")
                    break
            
            if not tab_name:
                logger.warning(f"Tab '{tab_name_mapped}' not found in sheet for facility '{facility_name}'. Available tabs: {tabs}")
                continue
            
            try:
                # Read columns L-Y and AA-AN separately to ensure proper alignment
                # Columns L-Y (columns 12-25, 0-indexed: 11-24) 
                # Columns AA-AN (columns 27-40, 0-indexed: 26-39)
                # Rows 4-250 (1-indexed, so 0-indexed: 3-249)
                range_l_y = f"{tab_name}!L4:Y250"
                range_aa_an = f"{tab_name}!AA4:AN250"
                
                logger.info(f"Reading data from tab '{tab_name}' for facility '{facility_name}'")
                
                result_l_y = self.sheets_service.values().get(
                    spreadsheetId=target_sheet_id,
                    range=range_l_y
                ).execute()
                
                result_aa_an = self.sheets_service.values().get(
                    spreadsheetId=target_sheet_id,
                    range=range_aa_an
                ).execute()
                
                values_l_y = result_l_y.get('values', [])
                values_aa_an = result_aa_an.get('values', [])
                
                logger.info(f"Read {len(values_l_y)} rows from L-Y and {len(values_aa_an)} rows from AA-AN for '{facility_name}'")
                
                if not values_l_y or not values_aa_an:
                    logger.warning(f"No data found in tab '{tab_name}' for facility '{facility_name}' (L-Y: {len(values_l_y)} rows, AA-AN: {len(values_aa_an)} rows)")
                    continue
                
                # Column Y is the 14th column in L-Y range (index 13)
                # Column AN is the 14th column in AA-AN range (index 13)
                col_y_index = 13  # Y is the 14th column (L=0, M=1, ..., Y=13)
                col_an_index = 13  # AN is the 14th column (AA=0, AB=1, ..., AN=13)
                
                complete_rows_y = []
                complete_rows_an = []
                
                # Find rows where all columns L-Y and AA-AN are populated
                max_rows = min(len(values_l_y), len(values_aa_an))
                
                for row_idx in range(max_rows):
                    row_l_y = values_l_y[row_idx] if row_idx < len(values_l_y) else []
                    row_aa_an = values_aa_an[row_idx] if row_idx < len(values_aa_an) else []
                    
                    # Check if all columns L-Y are populated (14 columns: L through Y)
                    l_y_complete = len(row_l_y) >= 14 and all(
                        cell and str(cell).strip() != '' 
                        for cell in row_l_y[:14]
                    )
                    
                    # Check if all columns AA-AN are populated (14 columns: AA through AN)
                    aa_an_complete = len(row_aa_an) >= 14 and all(
                        cell and str(cell).strip() != '' 
                        for cell in row_aa_an[:14]
                    )
                    
                    # Row is complete only if BOTH sections are fully populated
                    if l_y_complete and aa_an_complete:
                        # Get column Y value (index 13 in L-Y range)
                        if len(row_l_y) > col_y_index:
                            y_value = row_l_y[col_y_index]
                            try:
                                y_num = float(y_value)
                                complete_rows_y.append(y_num)
                            except (ValueError, TypeError):
                                pass
                        
                        # Get column AN value (index 13 in AA-AN range)
                        if len(row_aa_an) > col_an_index:
                            an_value = row_aa_an[col_an_index]
                            try:
                                an_num = float(an_value)
                                complete_rows_an.append(an_num)
                            except (ValueError, TypeError):
                                pass
                
                # Calculate averages
                if complete_rows_y and complete_rows_an and len(complete_rows_y) == len(complete_rows_an):
                    gs = sum(complete_rows_y) / len(complete_rows_y)
                    pps = sum(complete_rows_an) / len(complete_rows_an)
                    inc = pps - gs
                    
                    results[facility_name] = {
                        "GS": round(gs, 2),
                        "PPS": round(pps, 2),
                        "INC": round(inc, 2)
                    }
                    
                    logger.info(f"Calculated metrics for '{facility_name}' (tab: {tab_name}): "
                              f"GS={gs:.2f}, PPS={pps:.2f}, INC={inc:.2f} "
                              f"(from {len(complete_rows_y)} complete rows)")
                else:
                    logger.warning(f"Insufficient complete rows for '{facility_name}' (tab: {tab_name}): "
                                 f"Y values: {len(complete_rows_y)}, AN values: {len(complete_rows_an)}")
                    # Log more details for debugging
                    logger.info(f"Total rows checked: {max_rows}, L-Y rows: {len(values_l_y)}, AA-AN rows: {len(values_aa_an)}")
                    
            except HttpError as e:
                error_details = str(e)
                logger.error(f"HTTP Error fetching data from tab '{tab_name}' for facility '{facility_name}': {error_details}")
                # Check if it's a permission or not found error
                if '404' in error_details or 'not found' in error_details.lower():
                    logger.error(f"Tab '{tab_name}' may not exist in the sheet")
                elif '403' in error_details or 'permission' in error_details.lower():
                    logger.error(f"Permission denied accessing tab '{tab_name}' - check sheet permissions")
            except Exception as e:
                import traceback
                error_traceback = traceback.format_exc()
                logger.error(f"Unexpected error processing facility '{facility_name}': {e}")
                logger.error(f"Traceback: {error_traceback}")
        
        return results
    
    def fetch_facility_metrics_from_file(self, facility_names: list, file_path: str) -> dict:
        """
        Fetch GS, PPS, and INC values from an uploaded Excel/CSV file for given facilities.
        The file should have multiple sheets/tabs, one for each facility.
        
        Args:
            facility_names: List of facility names (e.g., ["Medilodge of Farmington", "Medilodge of Clare"])
            file_path: Path to the uploaded Excel/CSV file
        
        Returns:
            Dictionary mapping facility names to their metrics:
            {
                "Medilodge of Farmington": {"GS": 15.5, "PPS": 18.2, "INC": 2.7},
                "Medilodge of Clare": {"GS": 12.3, "PPS": 14.8, "INC": 2.5},
                ...
            }
        """
        results = {}
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            logger.error(f"File not found: {file_path}")
            return {}
        
        try:
            # Read the file - try Excel first, then CSV
            if file_path_obj.suffix.lower() in ['.xlsx', '.xls']:
                # Read all sheets from Excel file
                excel_file = pd.ExcelFile(file_path_obj)
                sheet_names = excel_file.sheet_names
                logger.info(f"Reading Excel file with {len(sheet_names)} sheets: {sheet_names}")
            else:
                # For CSV, we'll treat it as a single sheet
                sheet_names = ['Sheet1']  # Default name for CSV
                logger.info(f"Reading CSV file: {file_path}")
            
            for facility_name in facility_names:
                tab_name_mapped = self._map_facility_to_tab_name(facility_name)
                if not tab_name_mapped:
                    logger.warning(f"Skipping facility '{facility_name}' - no matching tab found in mapping")
                    continue
                
                # Find the exact tab name (case-insensitive) from available sheets
                logger.info(f"Looking for tab '{tab_name_mapped}' (case-insensitive) in available sheets: {sheet_names}")
                tab_name = None
                for available_tab in sheet_names:
                    logger.debug(f"  Comparing: '{available_tab.lower()}' == '{tab_name_mapped.lower()}' -> {available_tab.lower() == tab_name_mapped.lower()}")
                    if available_tab.lower() == tab_name_mapped.lower():
                        tab_name = available_tab  # Use the exact case from the file
                        logger.info(f"Mapped facility '{facility_name}' to sheet '{tab_name}'")
                        break
                
                if not tab_name:
                    logger.warning(f"Sheet '{tab_name_mapped}' not found in file for facility '{facility_name}'. Available sheets: {sheet_names}")
                    continue
                
                try:
                    # Read the specific sheet
                    if file_path_obj.suffix.lower() in ['.xlsx', '.xls']:
                        df = pd.read_excel(file_path_obj, sheet_name=tab_name, header=None)
                    else:
                        df = pd.read_csv(file_path_obj, header=None)
                    
                    # Columns L-Y are indices 11-24 (0-indexed), AA-AN are indices 26-39
                    # Rows 4-250 (0-indexed: 3-249)
                    # Read columns L-Y (11-24) and AA-AN (26-39) from rows 3-249
                    if len(df) < 4:
                        logger.warning(f"Sheet '{tab_name}' has insufficient rows (need at least 4, got {len(df)})")
                        continue
                    
                    # Get data from rows 3-249 (0-indexed), columns 11-24 (L-Y) and 26-39 (AA-AN)
                    max_row = min(250, len(df))
                    l_y_data = df.iloc[3:max_row, 11:25].values  # Columns L-Y (11-24, exclusive end is 25)
                    aa_an_data = df.iloc[3:max_row, 26:40].values  # Columns AA-AN (26-39, exclusive end is 40)
                    
                    # Column Y is index 13 in L-Y range, Column AN is index 13 in AA-AN range
                    col_y_index = 13
                    col_an_index = 13
                    
                    complete_rows_y = []
                    complete_rows_an = []
                    
                    # Find rows where all columns L-Y and AA-AN are populated
                    for row_idx in range(len(l_y_data)):
                        row_l_y = l_y_data[row_idx]
                        row_aa_an = aa_an_data[row_idx] if row_idx < len(aa_an_data) else []
                        
                        # Check if all columns L-Y are populated (14 columns)
                        l_y_complete = len(row_l_y) >= 14 and all(
                            pd.notna(cell) and str(cell).strip() != '' 
                            for cell in row_l_y[:14]
                        )
                        
                        # Check if all columns AA-AN are populated (14 columns)
                        aa_an_complete = len(row_aa_an) >= 14 and all(
                            pd.notna(cell) and str(cell).strip() != '' 
                            for cell in row_aa_an[:14]
                        )
                        
                        # Row is complete only if BOTH sections are fully populated
                        if l_y_complete and aa_an_complete:
                            # Get column Y value (index 13 in L-Y range)
                            if len(row_l_y) > col_y_index:
                                y_value = row_l_y[col_y_index]
                                try:
                                    y_num = float(y_value)
                                    complete_rows_y.append(y_num)
                                except (ValueError, TypeError):
                                    pass
                            
                            # Get column AN value (index 13 in AA-AN range)
                            if len(row_aa_an) > col_an_index:
                                an_value = row_aa_an[col_an_index]
                                try:
                                    an_num = float(an_value)
                                    complete_rows_an.append(an_num)
                                except (ValueError, TypeError):
                                    pass
                    
                    # Calculate averages
                    if complete_rows_y and complete_rows_an and len(complete_rows_y) == len(complete_rows_an):
                        gs = sum(complete_rows_y) / len(complete_rows_y)
                        pps = sum(complete_rows_an) / len(complete_rows_an)
                        inc = pps - gs
                        
                        results[facility_name] = {
                            "GS": round(gs, 2),
                            "PPS": round(pps, 2),
                            "INC": round(inc, 2)
                        }
                        
                        logger.info(f"Calculated metrics for '{facility_name}' (sheet: {tab_name}): "
                                  f"GS={gs:.2f}, PPS={pps:.2f}, INC={inc:.2f} "
                                  f"(from {len(complete_rows_y)} complete rows)")
                    else:
                        logger.warning(f"Insufficient complete rows for '{facility_name}' (sheet: {tab_name}): "
                                     f"Y values: {len(complete_rows_y)}, AN values: {len(complete_rows_an)}")
                        
                except Exception as e:
                    import traceback
                    error_traceback = traceback.format_exc()
                    logger.error(f"Error reading sheet '{tab_name}' for facility '{facility_name}': {e}")
                    logger.error(f"Traceback: {error_traceback}")
                    
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            logger.error(f"Error reading file '{file_path}': {e}")
            logger.error(f"Traceback: {error_traceback}")
        
        return results
