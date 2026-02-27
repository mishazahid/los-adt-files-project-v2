# Dashboard Update Request - Implementation Checklist

**Source:** `Dashboard_Update_Request.docx`
**Date:** February 2026
**Project:** Puzzle Physiatry - Medilodge Facility Reporting Dashboard

---

## Update 1: Long-Term Care (LTC) Encounter Metrics

**Status: COMPLETE**

### Data Source
- [x] Pull LTC data from Charge Detail Report (Charge Capture file)
- [x] Filter on POS (Place of Service) column = "32"

### Logic & Calculations
- [x] **Gross Encounters:** Count all rows where POS = "32" per facility
- [x] **Unique Patients:** De-duplicate patients across months using Patient ID (fallback: first + last name)

### Display Requirements
- [x] Add "Total Gross LTC Encounters" field to summarized output
- [x] Add "Patients Served (LTC)" field to summarized output
- [x] New fields appended at END of existing columns (no disruption to current flow)
- [x] Per-facility calculations (not global)

### Pipeline Integration
- [x] `csv_combiner-test.py` - `calculate_ltc_metrics()` function added
- [x] `csv_combiner-test.py` - `export_summarized_data()` updated with new columns
- [x] `csv_combiner-test.py` - `process_file_combination()` calls LTC metrics
- [x] `google_sheets.py` - Column ranges widened (AG -> AZ) to accommodate new columns
- [x] `summary_combiner.py` - No changes needed (pd.concat handles automatically)
- [x] Facility name matching/normalization handles suffixes: (M), - SNF, LLC, Ste./St.

### Verified
- [x] Medilodge of Wyoming: 65 gross LTC encounters, 26 unique patients
- [x] Medilodge of Farmington: 0 gross LTC encounters (expected)
- [x] Medilodge at the Shore: 62 gross LTC encounters, 23 unique patients
- [x] Columns appear in Google Sheets Summary tab
- [x] Columns flow through to Test sheet Raw_Data tab
- [x] Columns flow through to Facility_Data tab

---

## Update 2: Section GG Functional Gains - Payer-Level Averages

**Status: COMPLETE**

### Background
- [x] Currently displays: 5-Day Average, End-of-PPS Average, Section GG Score Increase (Sum)
- [x] Need to add average functional gains broken down by payer source

### Data Source
- [x] Payer source from LOS (Length of Stay) Report
- [x] GG Excel file: Col H (First Name), Col I (Last Name), Col Y (5-Day Global Score), Col AN (End-of-PPS Global Score)
- [x] LOS CSV: `first_name`, `last_name`, `payer_type` (Managed Care / Medicare A)
- [x] Cross-referenced via patient name matching (exact case-insensitive, then partial last-name first 3 chars)

### New Metrics to Add
| Metric | Payer Source | Description |
|--------|-------------|-------------|
| Avg. Section GG Gain | Managed Care | Average functional gain score for all Managed Care patients |
| Avg. Section GG Gain | Medicare A | Average functional gain score for all Medicare A patients |
| Avg. Section GG Gain | Overall | Average functional gain score across all payer types combined |

### Implementation Tasks
- [x] Cross-reference Section GG data with LOS Report payer classifications
- [x] Calculate per-payer average GG gains (End-of-PPS minus 5-Day) for Managed Care
- [x] Calculate per-payer average GG gains for Medicare A
- [x] Calculate overall average GG gains across all payers
- [x] Add new columns to summarized output
- [x] Update Google Sheets pipeline to include new columns
- [x] Display below existing GG metrics, clearly labeled by payer
- [x] Fields with no data write 0 (not empty)

### Pipeline Integration
- [x] `google_sheets.py` - `_find_los_csv_for_facility()` helper added (normalized name matching)
- [x] `google_sheets.py` - `_calculate_payer_gg_gains()` helper added (patient name matching + payer grouping)
- [x] `google_sheets.py` - `fetch_facility_metrics_from_file()` extended with `los_csv_dir` parameter
- [x] `google_sheets.py` - `update_sheets()` extended with GG_Gain_MC, GG_Gain_MA, GG_Gain_Overall columns
- [x] `google_sheets.py` - `copy_raw_data_to_facility_data()` extended with columns G-I, ranges updated to A2:I100
- [x] `pipeline.py` - Passes `los_csv_dir=str(los_output_dir)` when calling `fetch_facility_metrics_from_file()`

### Verified
- [x] Medilodge at the Shore: 9 complete GG rows, 9/9 patients matched (all Medicare A)
- [x] GG_Gain_MA = 3.0, GG_Gain_MC = 0, GG_Gain_Overall = 3.0 (matches INC)
- [x] Columns appear in Google Sheets Summary tab
- [x] Columns flow through to Test sheet Raw_Data tab
- [x] Columns flow through to Facility_Data tab (headers: Avg GG Gain - MC/MA/Overall)
- [x] Network Average row includes new columns

---

## Update 3: Performed Injections by Building (New Section)

**Status: NOT STARTED**

### Background
- [ ] New section to track injection procedures performed across facilities
- [ ] Does not currently exist - build from scratch

### Data Source
- [ ] CPT Codes column within Charge Capture Report
- [ ] Filter for six specific CPT codes, aggregate by facility

### CPT Codes to Include
| CPT Code | Description | Notes |
|----------|-------------|-------|
| 20600 | Small joint/bursa injection - without ultrasound | e.g., fingers, toes |
| 20604 | Small joint/bursa injection - with ultrasound | e.g., fingers, toes |
| 20605 | Medium/intermediate joint injection - without ultrasound | e.g., wrists, elbows |
| 20606 | Medium/intermediate joint injection - with ultrasound | e.g., wrists, elbows |
| 20610 | Major joint injection - without ultrasound | e.g., shoulders, hips, knees |
| 20611 | Major joint injection - with ultrasound | e.g., shoulders, hips, knees |

### Display Requirements
- [ ] Results displayed by building/facility
- [ ] **Overall Total Injections Performed** - sum of all six CPT codes
- [ ] **Breakdown by CPT Code** - individual count per code
- [ ] Layout mirrors existing facility-by-facility card/block format
- [ ] Appears as new subsection within each facility's page

### Implementation Tasks
- [ ] Parse CPT Codes column from Charge Capture data (may contain multiple comma-separated codes)
- [ ] Filter for the six injection CPT codes
- [ ] Aggregate counts by facility and CPT code
- [ ] Add injection metrics to summarized output
- [ ] Update Google Sheets pipeline
- [ ] Design display format matching existing dashboard style

---

## Update 4: Payer Patient Counts in Stay Duration Analysis

**Status: COMPLETE** (implemented by user)

### Background
- [x] Currently displays avg LOS by payer type (Overall, Managed Care, Medicare A)
- [x] Need to add patient count ratios next to payer labels

### Requested Display Format
Format: `[Payer Type] [Payer Patient Count]:[Total Facility Patients]`

| Payer Type | Current Display | Updated Display |
|------------|----------------|-----------------|
| Overall | Overall - 19.4 days | Overall - 19.4 days (no count needed) |
| Managed Care | Managed Care - 16.07 days | Managed Care 14:38 - 16.07 days |
| Medicare A | Medicare A - 22.27 days | Medicare A 5:38 - 22.27 days |

### Data Source
- [x] LOS Report - payer patient count & total patients

### Logic Requirements
- [x] **Payer Patient Count:** Unique patients at facility for that payer type
- [x] **Total Facility Patients:** All unique patients at facility (same denominator for all payers)
- [x] Ratio format: `[payer count]:[total patients]` (consistent with discharge destination format)

### Implementation Tasks
- [x] Calculate payer-specific patient counts from LOS data
- [x] Format as ratio strings (already have `Managed Care Ratio` and `Medicare A Ratio` in summarized data)
- [x] Verify existing ratio columns match this requirement
- [x] Update display in Google Sheets/Slides output

---

## Update 5: Comparison Report - Puzzle vs. Non-Puzzle Providers

**Status: NOT STARTED**

### Background
- [ ] Currently filters all data to Puzzle Physiatry providers only
- [ ] Need side-by-side comparison: Puzzle vs. Non-Puzzle providers
- [ ] Optional/togglable at report level

### Toggle Behavior
- [ ] Single report-level toggle in UI, applies to all buildings
- [ ] **OFF (default):** Puzzle provider data only (current behavior)
- [ ] **ON:** Puzzle + Non-Puzzle data side by side per building
- [ ] Defaults to OFF each time report is generated

### Data Sources
| Metric Section | Puzzle Data (Current) | Non-Puzzle Data (Comparison) |
|---------------|----------------------|------------------------------|
| Encounter Metrics | Charge Detail Report - Puzzle only | ADT Report - excluding Puzzle |
| Functional Gains (Section GG) | Section GG Report - Puzzle only | Section GG Report - excluding Puzzle |
| Length of Stay by Payer | LOS Report - Puzzle only | LOS Report - excluding Puzzle |
| Discharge Destinations | LOS Report - Puzzle only | LOS Report - excluding Puzzle |
| Payer Patient Counts | LOS Report - Puzzle only | LOS Report - excluding Puzzle |
| Injections by Building | Charge Capture - Puzzle only | **NOT INCLUDED** in comparison mode |

### Metrics Included in Comparison
| Metric | Included? |
|--------|-----------|
| Patients Served (Short Term) | Yes |
| Patients Served (Long Term Care) | Yes |
| Section GG Functional Gains (5-Day, End-of-PPS, Score Increase) | Yes |
| Section GG Gains by Payer (Managed Care, Medicare A, Overall) | Yes |
| Length of Stay by Payer | Yes |
| Payer Patient Counts | Yes |
| Discharge Destinations | Yes |
| Injections by Building | No - Puzzle-specific only |

### Additional Notes
- [ ] Two data sets must be mutually exclusive (encounter in Puzzle OR Non-Puzzle, never both)
- [ ] Charge Detail Report = source of truth for Puzzle provider identification
- [ ] Column labels: "Puzzle Physiatry" vs. "Non-Puzzle Providers"
- [ ] Injection section stays Puzzle-only regardless of toggle state
- [ ] No changes to existing Puzzle-only pipeline; comparison adds parallel inverted-filter pull

### Implementation Tasks
- [ ] Add UI toggle for comparison mode
- [ ] Implement inverted provider filter logic using Charge Detail Report
- [ ] Generate Non-Puzzle metrics for all applicable sections
- [ ] Design side-by-side display layout (two columns/panels per facility)
- [ ] Ensure mutual exclusivity of patient encounters
- [ ] Handle edge cases (patients with both Puzzle and Non-Puzzle encounters)

---

## Summary

| # | Update | Status |
|---|--------|--------|
| 1 | LTC Encounter Metrics | COMPLETE |
| 2 | Section GG Payer Averages | COMPLETE |
| 3 | Injections by Building (New) | NOT STARTED |
| 4 | Payer Patient Counts in LOS | COMPLETE |
| 5 | Comparison Report Toggle | NOT STARTED |
