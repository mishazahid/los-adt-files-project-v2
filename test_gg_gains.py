"""
Quick standalone test of the payer-level GG gains calculation.
Tests Update #2 changes against the "at the Shore" facility data.
"""
import sys
import os
import logging

# Set up logging to see all output
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.services.google_sheets import GoogleSheetsService

# Use existing test data
JOB_ID = "5c9030b8-06ba-43cc-ae76-76df55d9f8e2"
GOOGLE_SHEET_FILE = f"uploads/{JOB_ID}/google_sheet_file.xlsx"
LOS_CSV_DIR = f"outputs/{JOB_ID}/LOS-csv"

print("=" * 60)
print("TEST: Section GG Payer-Level Gains (Update #2)")
print("=" * 60)

# Check files exist
if not os.path.exists(GOOGLE_SHEET_FILE):
    print(f"ERROR: Google Sheet file not found: {GOOGLE_SHEET_FILE}")
    sys.exit(1)
if not os.path.exists(LOS_CSV_DIR):
    print(f"ERROR: LOS CSV directory not found: {LOS_CSV_DIR}")
    sys.exit(1)

print(f"\nGoogle Sheet file: {GOOGLE_SHEET_FILE}")
print(f"LOS CSV directory: {LOS_CSV_DIR}")
print(f"LOS CSV files: {os.listdir(LOS_CSV_DIR)}")

# Create service (Google API init may fail but that's OK - we only need file methods)
try:
    service = GoogleSheetsService()
except Exception as e:
    print(f"Note: Google API init warning (expected in test): {e}")
    # Create a minimal instance without API
    service = object.__new__(GoogleSheetsService)

facility_names = ["Medilodge at the Shore"]

print(f"\nFacility names: {facility_names}")
print("-" * 60)

# Test 1: fetch_facility_metrics_from_file WITHOUT los_csv_dir (baseline - Update #1)
print("\n--- Test 1: Baseline (no LOS dir) ---")
results_baseline = service.fetch_facility_metrics_from_file(
    facility_names,
    file_path=GOOGLE_SHEET_FILE
)
for fac, metrics in results_baseline.items():
    print(f"  {fac}: {metrics}")

# Test 2: fetch_facility_metrics_from_file WITH los_csv_dir (Update #2)
print("\n--- Test 2: With LOS dir (payer gains) ---")
results_payer = service.fetch_facility_metrics_from_file(
    facility_names,
    file_path=GOOGLE_SHEET_FILE,
    los_csv_dir=LOS_CSV_DIR
)
for fac, metrics in results_payer.items():
    print(f"  {fac}:")
    for key, val in metrics.items():
        print(f"    {key}: {val}")

# Verification
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

shore = results_payer.get("Medilodge at the Shore", {})
gs = shore.get("GS")
pps = shore.get("PPS")
inc = shore.get("INC")
gg_mc = shore.get("GG_Gain_MC")
gg_ma = shore.get("GG_Gain_MA")
gg_overall = shore.get("GG_Gain_Overall")

print(f"\nGS (5-Day Average):     {gs}")
print(f"PPS (End-of-PPS Average): {pps}")
print(f"INC (Score Increase):    {inc}")
print(f"GG_Gain_MC:              {gg_mc}")
print(f"GG_Gain_MA:              {gg_ma}")
print(f"GG_Gain_Overall:         {gg_overall}")

# Expected from plan:
# GG_Gain_Overall = 3.00 (same as INC)
# GG_Gain_MA ≈ 3.14 (7 Medicare A patients matched)
# GG_Gain_MC = "" (no MC patients with complete GG data, or some MC patients)
print(f"\nExpected: GG_Gain_Overall ~ INC ({inc})")
print(f"Expected: GG_Gain_MA ~ 3.14 (from plan)")
print(f"Expected: GG_Gain_MC = '' or numeric if MC patients found")

checks_passed = 0
checks_total = 3

# Check 1: GG_Gain_Overall should equal INC
if gg_overall is not None and inc is not None and abs(float(gg_overall) - float(inc)) < 0.01:
    print(f"\n[PASS] GG_Gain_Overall ({gg_overall}) == INC ({inc})")
    checks_passed += 1
else:
    print(f"\n[FAIL] GG_Gain_Overall ({gg_overall}) != INC ({inc})")

# Check 2: GG_Gain_MA should be a number
if gg_ma != "" and gg_ma is not None:
    print(f"[PASS] GG_Gain_MA has a value: {gg_ma}")
    checks_passed += 1
else:
    print(f"[FAIL] GG_Gain_MA is empty: {gg_ma}")

# Check 3: All baseline metrics still present
if gs is not None and pps is not None and inc is not None:
    print(f"[PASS] Baseline metrics (GS, PPS, INC) present")
    checks_passed += 1
else:
    print(f"[FAIL] Missing baseline metrics")

print(f"\n{'=' * 60}")
print(f"Results: {checks_passed}/{checks_total} checks passed")
print(f"{'=' * 60}")
