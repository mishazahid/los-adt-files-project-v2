"""
Google Slides Service - Handles creating and exporting Google Slides reports
"""

from pathlib import Path
from typing import Optional
import os
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

logger = logging.getLogger(__name__)


class GoogleSlidesService:
    """Service for interacting with Google Slides"""
    
    def __init__(self):
        self.service = None
        self._initialize_service()
    
    def _initialize_service(self):
        """Initialize Google Slides API service"""
        try:
            import json
            
            SCOPES = [
                'https://www.googleapis.com/auth/presentations'
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
                    logger.info("Google Slides service initialized from GOOGLE_CREDENTIALS_JSON")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")
                    credentials = None
            else:
                # Fall back to file path
                credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
                
                if not os.path.exists(credentials_path):
                    logger.warning(f"Google credentials not found at {credentials_path}. Slides features will be disabled.")
                    return
                
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=SCOPES
                )
                logger.info(f"Google Slides service initialized from file: {credentials_path}")
            
            if credentials:
                self.service = build('slides', 'v1', credentials=credentials)
                logger.info("Google Slides service initialized successfully")
            else:
                raise ValueError("Failed to load credentials")
        
        except Exception as e:
            logger.error(f"Failed to initialize Google Slides service: {e}")
            self.service = None
    
    async def create_report(self, job_id: str, summary_csv: Optional[Path] = None, 
                           all_patients_csv: Optional[Path] = None) -> str:
        """
        Create a Google Slides report from the processed data
        Returns the presentation ID
        """
        if not self.service:
            logger.warning("Google Slides service not available")
            return ""
        
        try:
            # Create a new presentation
            presentation = self.service.presentations().create(
                body={
                    'title': f'Facility Report - {job_id}'
                }
            ).execute()
            
            presentation_id = presentation.get('presentationId')
            logger.info(f"Created Google Slides presentation: {presentation_id}")
            
            # Create slides with data
            requests = []
            
            # Slide 1: Title slide
            requests.append({
                'createSlide': {
                    'slideLayoutReference': {
                        'predefinedLayout': 'TITLE'
                    },
                    'placeholderIdMappings': [{
                        'layoutPlaceholder': {
                            'type': 'TITLE',
                            'index': 0
                        },
                        'objectId': 'title_id'
                    }]
                }
            })
            
            requests.append({
                'insertText': {
                    'objectId': 'title_id',
                    'text': 'Facility Report'
                }
            })
            
            # Add summary slide if CSV is provided
            if summary_csv and summary_csv.exists():
                await self._add_summary_slide(requests, summary_csv)
            
            # Add patient details slide if CSV is provided
            if all_patients_csv and all_patients_csv.exists():
                await self._add_patients_slide(requests, all_patients_csv)
            
            # Execute all requests
            if requests:
                self.service.presentations().batchUpdate(
                    presentationId=presentation_id,
                    body={'requests': requests}
                ).execute()
            
            logger.info(f"Added content to Google Slides presentation")
            return presentation_id
        
        except HttpError as e:
            logger.error(f"Error creating Google Slides: {e}")
            return ""
        except Exception as e:
            logger.error(f"Error building slides content: {e}")
            return ""
    
    async def _add_summary_slide(self, requests: list, csv_file: Path):
        """Add a summary slide with data from CSV"""
        try:
            df = pd.read_csv(csv_file)
            
            # Create a new slide
            slide_id = 'summary_slide_id'
            requests.append({
                'createSlide': {
                    'slideLayoutReference': {
                        'predefinedLayout': 'TITLE_AND_BODY'
                    },
                    'objectId': slide_id
                }
            })
            
            # Add title
            requests.append({
                'insertText': {
                    'objectId': slide_id,
                    'insertionIndex': 0,
                    'text': 'Summary Statistics\n'
                }
            })
            
            # Add summary text
            summary_text = "\n".join([
                f"{col}: {df[col].iloc[0] if col in df.columns else 'N/A'}"
                for col in df.columns[:10]  # Limit to first 10 columns
            ])
            
            requests.append({
                'insertText': {
                    'objectId': slide_id,
                    'insertionIndex': 1,
                    'text': summary_text
                }
            })
        
        except Exception as e:
            logger.error(f"Error adding summary slide: {e}")
    
    async def _add_patients_slide(self, requests: list, csv_file: Path):
        """Add a patients slide with data from CSV"""
        try:
            df = pd.read_csv(csv_file)
            
            # Create a new slide
            slide_id = 'patients_slide_id'
            requests.append({
                'createSlide': {
                    'slideLayoutReference': {
                        'predefinedLayout': 'TITLE_AND_BODY'
                    },
                    'objectId': slide_id
                }
            })
            
            # Add title
            requests.append({
                'insertText': {
                    'objectId': slide_id,
                    'insertionIndex': 0,
                    'text': f'Patient Details ({len(df)} patients)\n'
                }
            })
        
        except Exception as e:
            logger.error(f"Error adding patients slide: {e}")
    
    async def update_slide_content(self, presentation_id: str, slide_id: str, content: str) -> bool:
        """
        Update content of a specific slide
        """
        if not self.service:
            logger.warning("Google Slides service not available")
            return False
        
        try:
            requests = [{
                'insertText': {
                    'objectId': slide_id,
                    'text': content
                }
            }]
            
            self.service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={'requests': requests}
            ).execute()
            
            return True
        
        except Exception as e:
            logger.error(f"Error updating slide content: {e}")
            return False

