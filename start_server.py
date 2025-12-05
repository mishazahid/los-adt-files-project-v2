#!/usr/bin/env python3
"""
Startup script for Facility Report Generator
"""

import os
import sys
from pathlib import Path

def check_env_file():
    """Check if .env file exists"""
    env_file = Path(".env")
    if not env_file.exists():
        print("⚠️  Warning: .env file not found!")
        print("Please create a .env file with the following variables:")
        print("  - OPENAI_API_KEY=your_key")
        print("  - GOOGLE_CREDENTIALS_PATH=credentials.json")
        print("  - GOOGLE_SHEET_ID=your_sheet_id (optional)")
        print("  - GOOGLE_SHEET_TAB=Summary (optional)")
        print("\nSee SETUP.md for detailed instructions.")
        response = input("\nContinue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)

def check_credentials():
    """Check if Google credentials file exists"""
    credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    creds_file = Path(credentials_path)
    if not creds_file.exists():
        print(f"⚠️  Warning: {credentials_path} not found!")
        print("Google Drive/Sheets/Slides features will be disabled.")
        print("See SETUP.md for instructions on setting up Google credentials.")

def check_directories():
    """Create necessary directories"""
    directories = ["uploads", "outputs", "logs", "jobs"]
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)

def main():
    print("=" * 60)
    print("Facility Report Generator - Starting Server")
    print("=" * 60)
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Run checks
    check_env_file()
    check_credentials()
    check_directories()
    
    print("\n✅ Pre-flight checks complete!")
    print("Starting server on http://0.0.0.0:8000")
    print("Press Ctrl+C to stop the server\n")
    
    # Change to backend directory and import app
    backend_path = Path(__file__).parent / "backend"
    sys.path.insert(0, str(backend_path.parent))
    
    # Start the server
    import uvicorn
    uvicorn.run(
        "backend.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped. Goodbye!")
    except Exception as e:
        print(f"\n❌ Error starting server: {e}")
        sys.exit(1)

