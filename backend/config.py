"""
Configuration and shared state for the application
"""
from pathlib import Path

# Directory paths
UPLOAD_DIR = Path("uploads")
OUTPUT_DIR = Path("outputs")
LOGS_DIR = Path("logs")
JOBS_DIR = Path("jobs")

# Create directories
for directory in [UPLOAD_DIR, OUTPUT_DIR, LOGS_DIR, JOBS_DIR]:
    directory.mkdir(exist_ok=True)

# Job status storage (in production, use Redis or database)
job_status = {}

