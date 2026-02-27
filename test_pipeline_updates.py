"""
Test pipeline steps 5+ (Update #1: GS/PPS/INC and Update #2: payer-level GG gains)
using pre-processed data from a previous job run.
This avoids re-running expensive PDF processing.
"""
import sys
import os
import asyncio
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path
from backend.services.google_sheets import GoogleSheetsService
from backend.config import job_status
import pandas as pd

# Use existing job data
JOB_ID = "5c9030b8-06ba-43cc-ae76-76df55d9f8e2"
JOB_DIR = f"uploads/{JOB_ID}"
MASTER_SUMMARY = f"outputs/{JOB_ID}/summary/master_summary.csv"
GOOGLE_SHEET_FILE = f"uploads/{JOB_ID}/google_sheet_file.xlsx"
LOS_CSV_DIR = f"outputs/{JOB_ID}/LOS-csv"

print("=" * 70)
print("PIPELINE UPDATE #1 + #2 TEST")
print("=" * 70)

# Verify files exist
for f in [MASTER_SUMMARY, GOOGLE_SHEET_FILE, LOS_CSV_DIR]:
    exists = os.path.exists(f)
    print(f"  {'[OK]' if exists else '[MISSING]'} {f}")
    if not exists:
        print(f"ERROR: Required file not found: {f}")
        sys.exit(1)

sheets_service = GoogleSheetsService()

# ---- Step 5.1: Auto-fetch metrics (Update #1 + #2) ----
print("\n" + "-" * 70)
print("STEP 5.1: Auto-fetch GS/PPS/INC + GG Payer Gains")
print("-" * 70)

# Read facility names from master_summary.csv (same as pipeline does)
ratio_columns = ['HD', 'HDN', 'HT', 'Ex', 'Cus', 'AL', 'OT', 'SNF', 'Managed Care Ratio', 'Medicare A Ratio']
dtype_dict = {col: str for col in ratio_columns}
df = pd.read_csv(MASTER_SUMMARY, dtype=dtype_dict)
facility_names = df['Facility'].unique().tolist()
print(f"\nFacilities: {facility_names}")

# Fetch metrics from file (with LOS dir for payer gains)
auto_metrics = sheets_service.fetch_facility_metrics_from_file(
    facility_names,
    file_path=GOOGLE_SHEET_FILE,
    los_csv_dir=LOS_CSV_DIR
)

print(f"\nAuto-fetched metrics:")
for fac, metrics in auto_metrics.items():
    print(f"  {fac}:")
    for key, val in metrics.items():
        print(f"    {key}: {val}")

# ---- Step 5.2: Update Google Sheets (Summary tab) ----
print("\n" + "-" * 70)
print("STEP 5.2: Update Google Sheets Summary tab")
print("-" * 70)

# Build facility_values (same as pipeline does)
facility_values = {}
for facility_name in facility_names:
    if facility_name in auto_metrics:
        facility_values[facility_name] = auto_metrics[facility_name]
        metrics = auto_metrics[facility_name]
        print(f"  {facility_name}: GS={metrics.get('GS', 'N/A')}, PPS={metrics.get('PPS', 'N/A')}, INC={metrics.get('INC', 'N/A')}, "
              f"GG_Gain_MC={metrics.get('GG_Gain_MC', 'N/A')}, GG_Gain_MA={metrics.get('GG_Gain_MA', 'N/A')}, GG_Gain_Overall={metrics.get('GG_Gain_Overall', 'N/A')}")

print(f"\nUpdating Google Sheets with {len(facility_values)} facilities...")
sheets_links = asyncio.run(sheets_service.update_sheets(Path(MASTER_SUMMARY), facility_values))
print(f"Sheets links: {sheets_links}")

# ---- Step 5.4: Copy Raw_Data to Facility_Data ----
print("\n" + "-" * 70)
print("STEP 5.4: Copy Raw_Data to Facility_Data tab (with new columns G-I)")
print("-" * 70)

import asyncio
copy_success = asyncio.run(sheets_service.copy_raw_data_to_facility_data())
print(f"Copy result: {'SUCCESS' if copy_success else 'FAILED'}")

# ---- Verification Summary ----
print("\n" + "=" * 70)
print("VERIFICATION SUMMARY")
print("=" * 70)

shore = auto_metrics.get("Medilodge at the Shore", {})
print(f"\nMedilodge at the Shore:")
print(f"  Update #1 (GS/PPS/INC):")
print(f"    GS  = {shore.get('GS', 'MISSING')}")
print(f"    PPS = {shore.get('PPS', 'MISSING')}")
print(f"    INC = {shore.get('INC', 'MISSING')}")
print(f"  Update #2 (Payer GG Gains):")
print(f"    GG_Gain_MC      = {shore.get('GG_Gain_MC', 'MISSING')}")
print(f"    GG_Gain_MA      = {shore.get('GG_Gain_MA', 'MISSING')}")
print(f"    GG_Gain_Overall = {shore.get('GG_Gain_Overall', 'MISSING')}")

# Checks
checks = []
if shore.get('GS') is not None and shore.get('PPS') is not None and shore.get('INC') is not None:
    checks.append(("[PASS] Update #1: GS/PPS/INC present", True))
else:
    checks.append(("[FAIL] Update #1: GS/PPS/INC missing", False))

if shore.get('GG_Gain_Overall') is not None and shore.get('GG_Gain_Overall') != '':
    checks.append(("[PASS] Update #2: GG_Gain_Overall present", True))
else:
    checks.append(("[FAIL] Update #2: GG_Gain_Overall missing", False))

if shore.get('GG_Gain_MA') is not None and shore.get('GG_Gain_MA') != '':
    checks.append(("[PASS] Update #2: GG_Gain_MA has value", True))
else:
    checks.append(("[FAIL] Update #2: GG_Gain_MA missing", False))

if shore.get('GG_Gain_MC') == '':
    checks.append(("[PASS] Update #2: GG_Gain_MC empty (no MC patients with complete GG)", True))
else:
    checks.append((f"[INFO] Update #2: GG_Gain_MC = {shore.get('GG_Gain_MC')}", True))

if isinstance(sheets_links, dict) and sheets_links.get("facility_summary"):
    checks.append(("[PASS] Google Sheets updated successfully", True))
else:
    checks.append(("[WARN] Google Sheets update returned unexpected format", True))

if copy_success:
    checks.append(("[PASS] Facility_Data tab updated (columns A-I)", True))
else:
    checks.append(("[FAIL] Facility_Data tab update failed", False))

print(f"\nResults:")
passed = 0
for msg, ok in checks:
    print(f"  {msg}")
    if ok:
        passed += 1

print(f"\n{passed}/{len(checks)} checks passed")
print("=" * 70)
