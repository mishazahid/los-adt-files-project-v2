"""
Google Apps Script Service - Triggers Apps Script functions to generate PDFs
"""

import os
import logging
import json
from typing import Optional
import httpx
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleAppsScriptService:
    """Service for executing Google Apps Script functions"""
    
    def __init__(self):
        self.service = None
        self.script_id = os.getenv("GOOGLE_APPS_SCRIPT_ID", "")
        self.web_app_url = os.getenv("GOOGLE_APPS_SCRIPT_WEB_APP_URL", "")
        self._initialize_service()
    
    def _initialize_service(self):
        """Initialize Google Apps Script API service"""
        try:
            import json
            
            SCOPES = [
                'https://www.googleapis.com/auth/script.scriptapp',
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/presentations'
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
                    logger.info("Google Apps Script service initialized from GOOGLE_CREDENTIALS_JSON")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")
                    credentials = None
                except Exception as e:
                    logger.error(f"Error creating credentials from GOOGLE_CREDENTIALS_JSON: {e}")
                    credentials = None
            
            # Fall back to file path if JSON not available or failed
            if not credentials:
                credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
                
                if not os.path.exists(credentials_path):
                    logger.warning(f"Google credentials not found at {credentials_path}. Apps Script features will be disabled.")
                    return
                
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        credentials_path,
                        scopes=SCOPES
                    )
                    logger.info(f"Google Apps Script service initialized from file: {credentials_path}")
                except Exception as e:
                    logger.error(f"Error loading credentials from file {credentials_path}: {e}")
                    return
            
            if credentials:
                self.service = build('script', 'v1', credentials=credentials)
                logger.info("Google Apps Script service initialized successfully")
            else:
                logger.error("Failed to load credentials - both JSON and file methods failed")
                raise ValueError("Failed to load credentials")
        
        except Exception as e:
            logger.error(f"Failed to initialize Google Apps Script service: {e}")
            self.service = None
    
    async def execute_function(self, function_name: str, parameters: Optional[list] = None) -> dict:
        """
        Execute a Google Apps Script function
        
        Args:
            function_name: Name of the function to execute (e.g., 'generatePDF')
            parameters: Optional list of parameters to pass to the function
        
        Returns:
            Dictionary with execution results
        """
        if not self.service:
            logger.warning("Google Apps Script service not available")
            return {"success": False, "error": "Service not initialized"}
        
        if not self.script_id:
            logger.warning("GOOGLE_APPS_SCRIPT_ID not set")
            return {"success": False, "error": "Script ID not configured"}
        
        try:
            # Prepare the request
            request = {
                'function': function_name
            }
            
            if parameters:
                request['parameters'] = parameters
            
            # Execute the function
            logger.info(f"Executing Apps Script function: {function_name}")
            response = self.service.scripts().run(
                body=request,
                scriptId=self.script_id
            ).execute()
            
            # Check for errors
            if 'error' in response:
                error = response['error']
                error_details = error.get('details', [])
                error_message = error.get('message', 'Unknown error')
                
                logger.error(f"Apps Script execution error: {error_message}")
                if error_details:
                    for detail in error_details:
                        logger.error(f"  Detail: {detail}")
                
                return {
                    "success": False,
                    "error": error_message,
                    "details": error_details
                }
            
            # Success
            result = response.get('response', {}).get('result', {})
            logger.info(f"Apps Script function {function_name} executed successfully")
            
            return {
                "success": True,
                "result": result
            }
        
        except HttpError as e:
            error_msg = f"HTTP error executing Apps Script: {e}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"Error executing Apps Script: {e}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    async def generate_pdf(self) -> dict:
        """
        Execute the generatePDF() function in Apps Script
        Tries Web App URL first (simpler), falls back to API if available
        
        Returns:
            Dictionary with success status and PDF link if available
        """
        # Try Web App URL first (simpler, no special permissions needed)
        if self.web_app_url:
            logger.info(f"Using Web App URL: {self.web_app_url[:50]}...")
            return await self._execute_via_web_app('generatePDF')
        
        # Fall back to API method
        logger.warning(f"Web App URL not set, falling back to API method. GOOGLE_APPS_SCRIPT_WEB_APP_URL is: {os.getenv('GOOGLE_APPS_SCRIPT_WEB_APP_URL', 'NOT SET')}")
        return await self.execute_function('generatePDF')
    
    async def _execute_via_web_app(self, function_name: str) -> dict:
        """
        Execute Apps Script function via Web App deployment (HTTP POST)
        This method doesn't require special service account permissions
        """
        try:
            # Ensure URL ends with /exec (not /dev)
            web_app_url = self.web_app_url.rstrip('/')
            if not web_app_url.endswith('/exec'):
                if web_app_url.endswith('/dev'):
                    web_app_url = web_app_url.replace('/dev', '/exec')
                else:
                    web_app_url = web_app_url + '/exec'
            
            logger.info(f"Calling Web App URL: {web_app_url[:80]}...")
            
            # Call the web app URL with the function name as a parameter
            async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
                response = await client.post(
                    web_app_url,
                    json={"function": function_name},
                    headers={"Content-Type": "application/json"}
                )
                
                logger.info(f"Web App response status: {response.status_code}")
                logger.info(f"Web App response headers: {dict(response.headers)}")
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        logger.info(f"Apps Script function {function_name} executed successfully via Web App")
                        logger.info(f"Apps Script response: {json.dumps(result, indent=2)}")
                        return {
                            "success": True,
                            "result": result
                        }
                    except Exception as json_error:
                        # Sometimes Web Apps return text instead of JSON
                        logger.warning(f"Response is not JSON, treating as success: {json_error}")
                        return {
                            "success": True,
                            "result": {"message": response.text[:200]}
                        }
                else:
                    # Log first 500 chars of error response
                    error_text = response.text[:500] if response.text else "No response body"
                    error_msg = f"Web App returned status {response.status_code}: {error_text}"
                    logger.error(error_msg)
                    return {"success": False, "error": error_msg}
        
        except Exception as e:
            error_msg = f"Error calling Web App: {e}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    async def generate_facility_slides(self) -> dict:
        """
        Execute the generateFacilitySlides() function in Apps Script
        
        Returns:
            Dictionary with success status
        """
        return await self.execute_function('generateFacilitySlides')
    
    async def generate_executive_summary(self) -> dict:
        """
        Execute the generateExecutiveSummarySlide() function in Apps Script
        
        Returns:
            Dictionary with success status
        """
        return await self.execute_function('generateExecutiveSummarySlide')

