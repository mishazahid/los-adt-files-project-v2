"""
Google Sheets Service - Handles updating Google Sheets with processed data
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import os
import logging
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
            # Read CSV file
            df = pd.read_csv(csv_file)
            
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
            
            # Clear existing data
            range_name = f"{self.sheet_tab}!A:Z"
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
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"Updated {self.sheet_tab} sheet: {result.get('updatedCells')} cells updated")
            
            # Update Executive sheet with Quarter value if provided
            if quarter_value:
                await self.update_executive_sheet_quarter(quarter_value)
            
            # Return the sheet URL
            return f"https://docs.google.com/spreadsheets/d/{self.sheet_id}"
        
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
                valueInputOption='RAW',
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
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"Appended data to Google Sheets")
            return True
        
        except Exception as e:
            logger.error(f"Error appending data to Sheets: {e}")
            return False

