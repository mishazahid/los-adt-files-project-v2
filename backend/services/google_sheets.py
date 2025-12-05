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
                    credentials = None
            else:
                # Fall back to file path
                credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
                
                if not os.path.exists(credentials_path):
                    logger.warning(f"Google credentials not found at {credentials_path}. Sheets features will be disabled.")
                    return
                
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=SCOPES
                )
                logger.info(f"Google Sheets service initialized from file: {credentials_path}")
            
            if credentials:
                self.service = build('sheets', 'v4', credentials=credentials)
                self.sheets_service = self.service.spreadsheets()
                logger.info("Google Sheets service initialized successfully")
            else:
                raise ValueError("Failed to load credentials")
        
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")
            self.service = None
            self.sheets_service = None
    
    async def update_sheets(self, csv_file: Path) -> str:
        """
        Update Google Sheets with data from CSV file
        Returns the Google Sheets URL
        """
        if not self.sheets_service:
            logger.warning("Google Sheets service not available")
            return ""
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
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
            
            logger.info(f"Updated Google Sheets: {result.get('updatedCells')} cells updated")
            
            # Return the sheet URL
            return f"https://docs.google.com/spreadsheets/d/{self.sheet_id}"
        
        except HttpError as e:
            logger.error(f"Error updating Google Sheets: {e}")
            return ""
        except Exception as e:
            logger.error(f"Error processing CSV for Sheets: {e}")
            return ""
    
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

