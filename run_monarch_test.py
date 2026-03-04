"""
Direct pipeline runner for Monarch test files - bypasses HTTP server.
Picks 4 ADT+LOS pairs from the new Monarch files and runs the full pipeline.
"""

import asyncio
import sys
import os
import uuid
import shutil
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load env vars
load_dotenv()

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from backend.services.pipeline import PipelineService
from backend.config import job_status

# File pairs to test (4 Monarch facilities)
NEW_FILES_DIR = Path(__file__).parent.parent / "new - files"

ADT_FILES = [
    "ADT Report The Villas at Brookview Q4.pdf",
    "ADT Report Maplewood Rehab Center Q4.pdf",
    "ADT Report The Estates at Roseville Q4.pdf",
    "ADT Report The Villas at New Brighton Q4.pdf",
]

LOS_FILES = [
    "LOS The Villas at Brookview Q4.pdf",
    "LOS Maplewood Rehabilitation Center Q4.pdf",
    "LOS The Estates at Roseville Q4.pdf",
    "LOS The Villas at New Brighton Q4.pdf",
]

EXCEL_FILE = "Monarch Q4.xlsx"

# Charge Capture / Visit file (same one used in all previous runs)
CHARGE_CAPTURE_FILE = Path(__file__).parent.parent / "Charge Capture - Puzzle Healthcare (Nov 01 - Nov 30) (2).xlsx"


async def main():
    # Create a job
    job_id = str(uuid.uuid4())
    print(f"\n{'='*60}")
    print(f"JOB ID: {job_id}")
    print(f"{'='*60}")

    # Set up directories
    uploads_dir = Path(__file__).parent / "uploads" / job_id
    adt_dir = uploads_dir / "ADT"
    los_dir = uploads_dir / "LOS"
    visits_dir = uploads_dir / "VISITS"

    for d in [adt_dir, los_dir, visits_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Copy ADT files
    print("\nCopying ADT files...")
    for fname in ADT_FILES:
        src = NEW_FILES_DIR / fname
        if not src.exists():
            print(f"  [ERROR] Not found: {src}")
            continue
        shutil.copy2(src, adt_dir / fname)
        print(f"  [OK] {fname}")

    # Copy LOS files
    print("\nCopying LOS files...")
    for fname in LOS_FILES:
        src = NEW_FILES_DIR / fname
        if not src.exists():
            print(f"  [ERROR] Not found: {src}")
            continue
        shutil.copy2(src, los_dir / fname)
        print(f"  [OK] {fname}")

    # Copy Charge Capture file into VISITS
    if CHARGE_CAPTURE_FILE.exists():
        shutil.copy2(CHARGE_CAPTURE_FILE, visits_dir / CHARGE_CAPTURE_FILE.name)
        print(f"\nCopied Charge Capture: {CHARGE_CAPTURE_FILE.name}")
    else:
        print(f"\n[WARNING] Charge Capture file not found: {CHARGE_CAPTURE_FILE}")

    # Copy Excel file to job dir
    excel_src = NEW_FILES_DIR / EXCEL_FILE
    if not excel_src.exists():
        print(f"\n[ERROR] Excel file not found: {excel_src}")
        return

    excel_dest = uploads_dir / f"google_sheet_file.xlsx"
    shutil.copy2(excel_src, excel_dest)
    print(f"\nCopied Excel: {EXCEL_FILE}")

    # Initialize job status
    job_status[job_id] = {
        "status": "processing",
        "created_at": datetime.now().isoformat(),
        "progress": 10,
        "message": "Starting pipeline...",
        "files": {
            "adt": ADT_FILES,
            "los": LOS_FILES,
            "visits": []
        },
        "outputs": {},
        "errors": [],
        "google_sheet_file": str(excel_dest),
        "comparison_mode": False,
    }

    print(f"\n{'='*60}")
    print("Starting pipeline...")
    print(f"{'='*60}\n")

    start = datetime.now()
    pipeline = PipelineService()

    try:
        results = await pipeline.run_pipeline(job_id, str(uploads_dir))

        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n{'='*60}")
        print(f"PIPELINE COMPLETE in {elapsed:.1f}s")
        print(f"{'='*60}")
        print(f"\nSteps completed: {results['steps_completed']}")
        print(f"\nLinks:")
        for k, v in results.get("links", {}).items():
            print(f"  {k}: {v}")
        if results.get("errors"):
            print(f"\nErrors:")
            for e in results["errors"]:
                print(f"  - {e}")
    except Exception as e:
        import traceback
        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n{'='*60}")
        print(f"PIPELINE FAILED after {elapsed:.1f}s")
        print(f"Error: {e}")
        print(traceback.format_exc())
        print(f"{'='*60}")

        # Print log if it exists
        log_file = Path(__file__).parent / "logs" / f"{job_id}.log"
        if log_file.exists():
            print(f"\n--- LOG FILE ({log_file}) ---")
            print(log_file.read_text(encoding="utf-8", errors="replace")[-5000:])


if __name__ == "__main__":
    asyncio.run(main())
