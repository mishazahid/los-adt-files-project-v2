#!/usr/bin/env python3
"""
CSV Combiner Script

This script combines CSV files in two modes:

1. INDIVIDUAL FILE MODE: Combines three specific CSV files:
   - ADT cycles data (with patient admission/discharge/transfer information)
   - Patient data (with payer type and length of stay)
   - Visit data (with patient visit counts)

2. FOLDER MODE: Processes folders containing multiple CSV files and matches
   ADT files with corresponding patient files based on facility names, using
   a single Change Capture file for all combinations.

The script merges these files on patient names and outputs combined CSV files
with comprehensive healthcare facility metrics.

Usage:
    # Individual file processing:
    python csv_combiner.py <adt_file> <patient_file> <visit_file> <output_file>

    # Folder batch processing:
    python csv_combiner.py --folders <adt_folder> <patient_folder> <visit_folder> <output_folder>

Examples:
    # Process individual files:
    python csv_combiner.py ADT-Example_cycles.csv Medilodge_of_Farmington.csv Change_Capture.csv output.csv
    
    # Process folders with name matching:
    # ADT files: ADT Medilodge at the Shore_cycles.csv, ADT Medilodge Sterling Heights_cycles.csv
    # Patient files: Medilodge_at_the_Shore.csv, Medilodge_of_Sterling_Heights.csv  
    # Visit file: Change_Capture.csv (same for all)
    python csv_combiner.py --folders adt_folder/ patient_folder/ visit_folder/ output_folder/
    python csv_combiner.py --folders adt_folder/ patient_folder/ visit_folder/ output_folder/ --facility-name "Medilodge Facility"
"""

import pandas as pd
import argparse
import sys
import os
from pathlib import Path
import glob
import re
from typing import List, Tuple, Dict


def load_csv_file(file_path, description):
    """Load a CSV file and return the DataFrame."""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{description} file not found: {file_path}")
        
        # Try to read as CSV first
        try:
            df = pd.read_csv(file_path)
        except Exception:
            # If CSV fails, try Excel
            try:
                df = pd.read_excel(file_path)
            except Exception as e:
                raise ValueError(f"Could not read {description} file as CSV or Excel: {e}")
        
        print(f"[OK] Loaded {description}: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    
    except Exception as e:
        print(f"[FAILED] Error loading {description} file: {e}")
        sys.exit(1)


def validate_required_columns(df, required_cols, file_description):
    """Validate that the DataFrame has the required columns."""
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"[FAILED] {file_description} is missing required columns: {missing_cols}")
        print(f"  Available columns: {list(df.columns)}")
        sys.exit(1)


def process_adt_data(adt_df):
    """Process ADT cycles data."""
    print("\n--- Processing ADT Data ---")
    
    # Required columns for ADT data
    required_cols = ['first_name', 'last_name']
    validate_required_columns(adt_df, required_cols, "ADT file")
    
    print(f"ADT data shape: {adt_df.shape}")
    print(f"ADT columns: {list(adt_df.columns)}")
    
    return adt_df


def process_patient_data(patient_df):
    """Process patient data with payer type and length of stay."""
    print("\n--- Processing Patient Data ---")
    
    # Required columns for patient data
    required_cols = ['first_name', 'last_name']
    validate_required_columns(patient_df, required_cols, "Patient file")
    
    # Rename 'days' to 'LOS' if it exists
    if 'days' in patient_df.columns:
        patient_df = patient_df.rename(columns={'days': 'LOS'})
        print("[OK] Renamed 'days' column to 'LOS'")
    
    print(f"Patient data shape: {patient_df.shape}")
    print(f"Patient columns: {list(patient_df.columns)}")
    
    return patient_df


def process_visit_data(visit_df):
    """Process visit data to count visits per patient."""
    print("\n--- Processing Visit Data ---")
    
    # Check for different possible column names for patient names
    first_name_cols = [col for col in visit_df.columns if 'first' in col.lower() and 'name' in col.lower()]
    last_name_cols = [col for col in visit_df.columns if 'last' in col.lower() and 'name' in col.lower()]
    
    if not first_name_cols or not last_name_cols:
        print(f"[FAILED] Visit file must contain columns with 'first name' and 'last name'")
        print(f"  Available columns: {list(visit_df.columns)}")
        sys.exit(1)
    
    first_name_col = first_name_cols[0]
    last_name_col = last_name_cols[0]
    
    print(f"Using columns: '{first_name_col}' and '{last_name_col}' for patient names")
    
    # Count visits per patient
    visit_counts = visit_df.groupby([first_name_col, last_name_col]).size().reset_index(name='visit_count')
    
    # Rename columns to match the expected format
    visit_counts = visit_counts.rename(columns={
        first_name_col: 'First Name',
        last_name_col: 'Last Name'
    })
    
    print(f"[OK] Calculated visit counts for {len(visit_counts)} unique patients")
    print(f"  Total visits: {visit_counts['visit_count'].sum()}")
    print(f"  Average visits per patient: {visit_counts['visit_count'].mean():.2f}")
    
    return visit_counts


def merge_dataframes(adt_df, patient_df, visit_counts):
    """Merge all dataframes together."""
    print("\n--- Merging Data ---")
    
    # First merge: ADT data with patient data on first_name and last_name
    print("Merging ADT data with patient data...")
    merged_df = pd.merge(
        patient_df, 
        adt_df, 
        on=['first_name', 'last_name'], 
        how='inner', 
        suffixes=('_patient', '_adt')
    )
    
    print(f"[OK] Initial merge result: {merged_df.shape[0]} rows, {merged_df.shape[1]} columns")
    
    # Rename columns to more readable names
    column_mapping = {
        'first_name': 'First Name',
        'last_name': 'Last Name', 
        'payer_type': 'Payer Type',
        'LOS': 'LOS',  # Already renamed in process_patient_data
        'resident_id': 'Resident ID',
        'admission_date': 'Admission Date',
        'discharge_date': 'Discharge Date'
    }
    
    # Apply column mapping (only for columns that exist)
    existing_mapping = {k: v for k, v in column_mapping.items() if k in merged_df.columns}
    merged_df = merged_df.rename(columns=existing_mapping)
    
    print(f"[OK] Renamed columns: {list(existing_mapping.keys())} -> {list(existing_mapping.values())}")
    
    # Second merge: Add visit counts
    print("Adding visit counts...")
    merged_df_with_visits = pd.merge(
        merged_df, 
        visit_counts, 
        on=['First Name', 'Last Name'], 
        how='left'
    )
    
    # Fill NaN values with 0 and convert to integer
    merged_df_with_visits['visit_count'] = merged_df_with_visits['visit_count'].fillna(0).astype(int)
    
    # Rename visit_count column
    merged_df_with_visits = merged_df_with_visits.rename(columns={
        'visit_count': 'Number of Visits by Puzzle Provider'
    })
    
    # Add Puzzle Patient column based on visit count
    merged_df_with_visits['Puzzle Patient'] = merged_df_with_visits['Number of Visits by Puzzle Provider'] > 0
    
    print(f"[OK] Final merge result: {merged_df_with_visits.shape[0]} rows, {merged_df_with_visits.shape[1]} columns")
    print(f"[OK] Added 'Puzzle Patient' column: {merged_df_with_visits['Puzzle Patient'].sum()} patients with visits")
    
    return merged_df_with_visits


def export_summarized_data(df, output_path, facility_name):
    """Export summarized data CSV with key metrics."""
    print(f"\n--- Exporting Summarized Data ---")
    
    try:
        # Create output directory if it doesn't exist
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate basic metrics
        patients_served = len(df)
        total_visits = df['Number of Visits by Puzzle Provider'].sum() if 'Number of Visits by Puzzle Provider' in df.columns else 0
        avg_visits_per_patient = total_visits / patients_served if patients_served > 0 else 0
        
        # Calculate LOS metrics
        los_avg = df['LOS'].mean() if 'LOS' in df.columns else 0
        
        # Calculate LOS by payer type
        los_managed_avg = 0
        los_medicare_avg = 0
        
        if 'LOS' in df.columns and 'Payer Type' in df.columns:
            managed_care_data = df[df['Payer Type'] == 'Managed Care']['LOS']
            medicare_data = df[df['Payer Type'] == 'Medicare A']['LOS']
            
            los_managed_avg = managed_care_data.mean() if len(managed_care_data) > 0 else 0
            los_medicare_avg = medicare_data.mean() if len(medicare_data) > 0 else 0
        
        # Create discharge mapping
        discharge_mapping = []
        if 'to_type' in df.columns:
            for to_type in df['to_type'].fillna(''):
                to_type_str = str(to_type).lower()
                if 'hospital' in to_type_str:
                    discharge_mapping.append('Hospital Transfer')
                elif 'funeral' in to_type_str:
                    discharge_mapping.append('Expired')
                elif 'home' in to_type_str and 'funeral' not in to_type_str:
                    discharge_mapping.append('Home Discharge')
                elif 'other' in to_type_str:
                    discharge_mapping.append('Other')
                else:
                    discharge_mapping.append('Other')
        else:
            discharge_mapping = ['Other'] * len(df)
        
        # Calculate discharge counts and ratios
        total_home_discharge = discharge_mapping.count('Home Discharge')
        total_hospital_transfer = discharge_mapping.count('Hospital Transfer')
        total_expired = discharge_mapping.count('Expired')
        
        # Create ratio format KPIs (always show count:total_patients)
        hd_ratio = f"{total_home_discharge}:{patients_served}" if patients_served > 0 else "0:0"
        ht_ratio = f"{total_hospital_transfer}:{patients_served}" if patients_served > 0 else "0:0"
        ex_ratio = f"{total_expired}:{patients_served}" if patients_served > 0 else "0:0"
        
        pct_home_discharge = (total_home_discharge / patients_served * 100) if patients_served > 0 else 0
        pct_hospital_transfer = (total_hospital_transfer / patients_served * 100) if patients_served > 0 else 0
        pct_expired = (total_expired / patients_served * 100) if patients_served > 0 else 0
        
        # Create summarized data
        summarized_data = {
            'Facility': [facility_name],
            'Patients Served': [patients_served],
            'Total Visits': [total_visits],
            'Avg Visits per Patient': [round(avg_visits_per_patient, 2)],
            'LOS Overall Avg': [round(los_avg, 2)],
            'LOS Man Avg': [round(los_managed_avg, 2)],
            'LOS Med Avg': [round(los_medicare_avg, 2)],
            'HD': [hd_ratio],
            'HT': [ht_ratio],
            'Ex': [ex_ratio],
            '%Home Discharge': [round(pct_home_discharge, 2)],
            '%Hospital Transfer': [round(pct_hospital_transfer, 2)],
            '%Expired': [round(pct_expired, 2)]
        }
        
        summarized_df = pd.DataFrame(summarized_data)
        
        # Save to CSV
        summarized_df.to_csv(output_path, index=False)
        
        print(f"[OK] Summarized data saved to: {output_path}")
        print(f"  Summary metrics:")
        print(f"    Facility: {facility_name}")
        print(f"    Patients Served: {patients_served}")
        print(f"    Total Visits: {total_visits}")
        print(f"    Avg Visits per Patient: {avg_visits_per_patient:.2f}")
        print(f"    LOS Overall Avg: {los_avg:.2f}")
        print(f"    LOS Man Avg: {los_managed_avg:.2f}")
        print(f"    LOS Med Avg: {los_medicare_avg:.2f}")
        print(f"    HD: {hd_ratio}")
        print(f"    HT: {ht_ratio}")
        print(f"    Ex: {ex_ratio}")
        print(f"    %Home Discharge: {pct_home_discharge:.2f}%")
        print(f"    %Hospital Transfer: {pct_hospital_transfer:.2f}%")
        print(f"    %Expired: {pct_expired:.2f}%")
        
        return summarized_df
        
    except Exception as e:
        print(f"[FAILED] Error exporting summarized data: {e}")
        sys.exit(1)


def save_output(df, output_path):
    """Save the final DataFrame to CSV."""
    print(f"\n--- Saving Output ---")
    
    try:
        # Create output directory if it doesn't exist
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        
        print(f"[OK] Output saved to: {output_path}")
        print(f"  Final shape: {df.shape[0]} rows, {df.shape[1]} columns")
        
        # Display summary
        print(f"\n--- Summary ---")
        print(f"Columns in output: {list(df.columns)}")
        
        if 'Number of Visits by Puzzle Provider' in df.columns:
            total_visits = df['Number of Visits by Puzzle Provider'].sum()
            patients_with_visits = (df['Number of Visits by Puzzle Provider'] > 0).sum()
            print(f"Total visits recorded: {total_visits}")
            print(f"Patients with visits: {patients_with_visits}/{len(df)}")
        
        if 'Puzzle Patient' in df.columns:
            puzzle_patients = df['Puzzle Patient'].sum()
            print(f"Puzzle patients (True): {puzzle_patients}/{len(df)}")
        
        print(f"\nFirst few rows:")
        print(df.head().to_string(index=False))
        
    except Exception as e:
        print(f"[FAILED] Error saving output: {e}")
        sys.exit(1)


def find_csv_files_in_folder(folder_path: str, file_patterns: List[str]) -> Dict[str, List[str]]:
    """
    Find CSV files in a folder that match specific patterns.
    
    Args:
        folder_path: Path to the folder to search
        file_patterns: List of patterns to match (e.g., ['*adt*', '*patient*', '*visit*'])
    
    Returns:
        Dictionary mapping pattern names to list of matching file paths
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    
    found_files = {}
    
    for pattern in file_patterns:
        # Search for files matching the pattern
        matching_files = []
        for ext in ['*.csv', '*.xlsx', '*.xls']:
            search_pattern = folder / f"*{pattern}*{ext}"
            matching_files.extend(glob.glob(str(search_pattern), recursive=False))
        
        # Also search in subdirectories
        for ext in ['*.csv', '*.xlsx', '*.xls']:
            search_pattern = folder / "**" / f"*{pattern}*{ext}"
            matching_files.extend(glob.glob(str(search_pattern), recursive=True))
        
        # Remove duplicates and sort
        matching_files = sorted(list(set(matching_files)))
        found_files[pattern] = matching_files
    
    return found_files


def extract_facility_name_from_filename(filename: str, file_type: str) -> str:
    """
    Extract facility name from filename for matching purposes.
    
    Args:
        filename: The filename to extract facility name from
        file_type: Type of file ('adt' or 'patient')
    
    Returns:
        Normalized facility name for matching
    """
    name = Path(filename).stem.lower()
    
    if file_type == 'adt':
        # Handle ADT files like "ADT Medilodge at the Shore_cycles" or "ADT-Medilode-Farmington"
        # Remove ADT prefix (handle both space and underscore cases)
        if name.startswith('adt '):
            name = name[4:]  # Remove "adt "
        elif name.startswith('adt-'):
            name = name[4:]  # Remove "adt-"
        elif name.startswith('adt_'):
            name = name[4:]  # Remove "adt_"
        
        # Remove _cycles suffix
        name = name.replace('_cycles', '')
        
        # Remove "medilodge" prefix
        if name.startswith('medilodge '):
            name = name[10:]  # Remove "medilodge "
        elif name.startswith('medilodge-'):
            name = name[10:]  # Remove "medilodge-"
        elif name.startswith('medilodge_'):
            name = name[10:]  # Remove "medilodge_"
        elif name.startswith('medilode-'):
            name = name[9:]   # Remove "medilode-"
        
        # Normalize spaces, dashes, and underscores
        name = name.replace('-', ' ').replace('_', ' ').strip()
        
        # Remove "of" prefix if present (e.g., "of farmington" -> "farmington")
        if name.startswith('of '):
            name = name[3:]  # Remove "of "
        
    elif file_type == 'patient':
        # Handle patient files like "Medilodge_at_the_Shore" or "Medilodge_of_Sterling_Heights"
        # Remove "medilodge" prefix
        if name.startswith('medilodge_'):
            name = name[10:]  # Remove "medilodge_"
        elif name.startswith('medilodge-'):
            name = name[10:]  # Remove "medilodge-"
        
        # Convert underscores to spaces
        name = name.replace('_', ' ').replace('-', ' ').strip()
        
        # Remove "of" prefix if present (e.g., "of sterling heights" -> "sterling heights")
        if name.startswith('of '):
            name = name[3:]  # Remove "of "
    
    # Normalize common facility name variations for matching
    name = name.replace('at the', 'at_the')
    name = name.replace('of the', 'of_the') 
    name = name.replace('sterling heights', 'sterling_heights')
    name = name.replace('farmington', 'farmington')
    name = name.replace('shore', 'shore')
    
    # Special handling for "at the shore" - ensure consistent normalization
    if 'at_the shore' in name:
        name = name.replace('at_the shore', 'at_the_shore')
    
    # Final cleanup - remove any remaining spaces and normalize
    return name.strip()


def normalize_facility_name_for_matching(facility_name: str) -> str:
    """
    Normalize facility name for flexible matching (handles variations like 'of_farmington' vs 'farmington').
    
    Args:
        facility_name: The facility name to normalize
    
    Returns:
        Normalized facility name for matching
    """
    # Convert to lowercase and replace underscores with spaces
    normalized = facility_name.lower().replace('_', ' ').strip()
    
    # Remove "of " prefix if present
    if normalized.startswith('of '):
        normalized = normalized[3:]
    
    # Remove common suffixes that cause mismatches (quarters, periods, etc.)
    # Remove Q1, Q2, Q3, Q4, Quarter 1, etc.
    normalized = re.sub(r'\s+q[1-4]$', '', normalized)  # "holland q3" -> "holland"
    normalized = re.sub(r'\s+quarter\s+[1-4]$', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\s+[qQ][1-4]$', '', normalized)  # Handle "Q3" at end
    
    # Remove leading/trailing whitespace
    normalized = normalized.strip()
    
    return normalized


def format_facility_name_for_display(facility_name: str) -> str:
    """
    Format facility name for display with proper Medilodge prefix.
    
    Args:
        facility_name: The normalized facility name from extraction
    
    Returns:
        Properly formatted facility name with Medilodge prefix
    """
    # Convert back from normalized format to readable format
    display_name = facility_name.replace('_', ' ')
    
    # Handle specific facility name patterns
    if display_name == 'at_the shore':
        return 'Medilodge at the Shore'
    elif display_name == 'at_the_shore':
        return 'Medilodge at the Shore'
    elif display_name == 'sterling_heights':
        return 'Medilodge of Sterling Heights'
    elif display_name == 'farmington':
        return 'Medilodge of Farmington'
    elif display_name == 'sault st marie':
        return 'Medilodge of Sault St. Marie'
    elif display_name == 'clare':
        return 'Medilodge of Clare'
    elif display_name == 'ludington':
        return 'Medilodge of Ludington'
    elif display_name == 'mt pleasant':
        return 'Medilodge of Mt. Pleasant'
    elif display_name == 'holland':
        return 'Medilodge of Holland'
    elif display_name == 'wyoming':
        return 'Medilodge of Wyoming'
    elif display_name == 'grand rapids':
        return 'Medilodge of Grand Rapids'
    else:
        # Default formatting for other facilities
        # Handle special cases for abbreviations
        formatted_name = display_name.title()
        formatted_name = formatted_name.replace('St ', 'St. ')
        formatted_name = formatted_name.replace('Mt ', 'Mt. ')
        
        # Remove "of " prefix if it was left from ADT extraction
        if formatted_name.startswith('Of '):
            formatted_name = formatted_name[3:]
        
        # Special case for "at the" facilities
        if 'At The' in formatted_name:
            return f'Medilodge at the {formatted_name.replace("At The ", "")}'
        
        return f'Medilodge of {formatted_name}'


def find_matching_files(adt_folder: str, patient_folder: str, visit_folder: str) -> List[Tuple[str, str, str]]:
    """
    Find matching ADT and patient files based on facility names, plus visit files.
    
    Args:
        adt_folder: Folder containing ADT cycle CSV files
        patient_folder: Folder containing patient data CSV files
        visit_folder: Folder containing visit data CSV files
    
    Returns:
        List of tuples (adt_file, patient_file, visit_file) for matching files
    """
    print("Searching for matching files...")
    
    # Find all ADT files (deduplicate in case files match multiple patterns)
    adt_files = find_csv_files_in_folder(adt_folder, ['adt', 'cycle'])
    all_adt_files = sorted(list(set(adt_files.get('adt', []) + adt_files.get('cycle', []))))
    
    # Find all patient files (deduplicate in case files match multiple patterns)
    patient_files = find_csv_files_in_folder(patient_folder, ['patient', 'medilodge'])
    all_patient_files = sorted(list(set(patient_files.get('patient', []) + patient_files.get('medilodge', []))))
    
    # Find visit files (should be the same for all) (deduplicate in case files match multiple patterns)
    visit_files = find_csv_files_in_folder(visit_folder, ['visit', 'change', 'capture'])
    all_visit_files = sorted(list(set(visit_files.get('visit', []) + visit_files.get('change', []) + visit_files.get('capture', []))))
    
    print(f"Found {len(all_adt_files)} ADT files")
    print(f"Found {len(all_patient_files)} patient files")
    print(f"Found {len(all_visit_files)} visit files")
    
    # Create facility name mappings
    adt_facilities = {}
    print("\n--- ADT File Processing ---")
    for adt_file in all_adt_files:
        facility_name = extract_facility_name_from_filename(adt_file, 'adt')
        adt_facilities[facility_name] = adt_file
        print(f"ADT file: {Path(adt_file).name}")
        print(f"  -> Extracted facility: '{facility_name}'")
    
    patient_facilities = {}
    print("\n--- Patient File Processing ---")
    for patient_file in all_patient_files:
        facility_name = extract_facility_name_from_filename(patient_file, 'patient')
        patient_facilities[facility_name] = patient_file
        print(f"Patient file: {Path(patient_file).name}")
        print(f"  -> Extracted facility: '{facility_name}'")
    
    # Show all available facility names for debugging
    print(f"\n--- Available Facility Names ---")
    print(f"ADT facilities: {list(adt_facilities.keys())}")
    print(f"Patient facilities: {list(patient_facilities.keys())}")
    
    # Find matches using normalized names
    matches = []
    print(f"\n--- Matching Process ---")
    
    # Create normalized mappings for flexible matching
    normalized_adt_map = {normalize_facility_name_for_matching(name): (name, file) 
                          for name, file in adt_facilities.items()}
    normalized_patient_map = {normalize_facility_name_for_matching(name): (name, file) 
                              for name, file in patient_facilities.items()}
    
    for normalized_name, (adt_facility_name, adt_file) in normalized_adt_map.items():
        if normalized_name in normalized_patient_map:
            patient_facility_name, patient_file = normalized_patient_map[normalized_name]
            # Use the first visit file for all matches (assuming it's the same Change Capture file)
            if all_visit_files:
                visit_file = all_visit_files[0]  # Use the first (and likely only) visit file
                matches.append((adt_file, patient_file, visit_file))
                print(f"[OK] Match found: '{adt_facility_name}' <-> '{patient_facility_name}' (normalized: '{normalized_name}')")
                print(f"  ADT: {Path(adt_file).name}")
                print(f"  Patient: {Path(patient_file).name}")
                print(f"  Visit: {Path(visit_file).name}")
            else:
                print(f"[FAILED] No visit files found for match: '{adt_facility_name}'")
        else:
            print(f"[FAILED] No matching patient file for ADT: '{adt_facility_name}' (normalized: '{normalized_name}')")
            print(f"  Available patient facilities (normalized): {list(normalized_patient_map.keys())}")
    
    print(f"\nTotal matches found: {len(matches)}")
    return matches


def process_folder_batch(adt_folder: str, patient_folder: str, visit_folder: str, output_folder: str, 
                        facility_name: str = None) -> None:
    """
    Process folders containing CSV files and combine matching files based on facility names.
    
    Args:
        adt_folder: Folder containing ADT cycle CSV files
        patient_folder: Folder containing patient data CSV files  
        visit_folder: Folder containing visit data CSV files (Change Capture file)
        output_folder: Folder to save combined output files
        facility_name: Optional facility name for summary data
    """
    print("=" * 80)
    print("FOLDER BATCH PROCESSING - NAME MATCHING")
    print("=" * 80)
    
    # Find matching files based on facility names
    matches = find_matching_files(adt_folder, patient_folder, visit_folder)
    
    if not matches:
        print("[FAILED] No matching files found!")
        return
    
    # Create output directory
    output_dir = Path(output_folder)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each matching combination
    processed_count = 0
    
    for adt_file, patient_file, visit_file in matches:
        try:
            print(f"\n{'='*60}")
            print(f"Processing match {processed_count + 1}:")
            print(f"ADT: {Path(adt_file).name}")
            print(f"Patient: {Path(patient_file).name}")
            print(f"Visit: {Path(visit_file).name}")
            print(f"{'='*60}")
            
            # Generate output filename based on facility name
            facility_name_from_file = extract_facility_name_from_filename(patient_file, 'patient')
            facility_name_clean = format_facility_name_for_display(facility_name_from_file)
            
            output_filename = f"combined_{facility_name_clean.replace(' ', '_').replace('Medilodge_', '')}.csv"
            output_path = output_dir / output_filename
            
            # Use facility name from file if not provided
            current_facility_name = facility_name or facility_name_clean
            
            # Process this combination
            process_file_combination(adt_file, patient_file, visit_file, str(output_path), current_facility_name)
            
            processed_count += 1
            
        except Exception as e:
            print(f"[FAILED] Error processing match: {e}")
            continue
    
    print(f"\n{'='*80}")
    print(f"[OK] BATCH PROCESSING COMPLETE!")
    print(f"Processed {processed_count} matching file combinations")
    print(f"Output saved to: {output_folder}")
    print(f"{'='*80}")


def process_file_combination(adt_file: str, patient_file: str, visit_file: str, 
                           output_file: str, facility_name: str = None) -> None:
    """
    Process a single combination of ADT, patient, and visit files.
    
    Args:
        adt_file: Path to ADT cycles CSV file
        patient_file: Path to patient data CSV file
        visit_file: Path to visit data CSV file
        output_file: Path for the output CSV file
        facility_name: Optional facility name for summary data
    """
    # Load all input files
    adt_df = load_csv_file(adt_file, "ADT cycles")
    patient_df = load_csv_file(patient_file, "Patient data")
    visit_df = load_csv_file(visit_file, "Visit data")
    
    # Process each dataset
    adt_df = process_adt_data(adt_df)
    patient_df = process_patient_data(patient_df)
    visit_counts = process_visit_data(visit_df)
    
    # Merge all data
    final_df = merge_dataframes(adt_df, patient_df, visit_counts)
    
    # Save output
    save_output(final_df, output_file)
    
    # Export summarized data
    if not facility_name:
        # Extract facility name from patient file name
        facility_name_from_file = extract_facility_name_from_filename(patient_file, 'patient')
        facility_name = format_facility_name_for_display(facility_name_from_file)
    
    # Create summarized data output path
    output_dir = Path(output_file).parent
    summarized_filename = f"summarized_{Path(output_file).stem}.csv"
    summarized_output_path = output_dir / summarized_filename
    
    export_summarized_data(final_df, str(summarized_output_path), facility_name)


def main():
    """Main function to orchestrate the CSV combining process."""
    parser = argparse.ArgumentParser(
        description="Combine CSV files (ADT cycles, patient data, and visit data) into combined output CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process individual files:
  python csv_combiner.py adt_data.csv patient_data.csv visit_data.csv output.csv
  python csv_combiner.py ADT-Example_cycles.csv Medilodge_of_Farmington.csv Change_Capture.xlsx combined_output.csv
  
  # Process folders with name matching:
  python csv_combiner.py --folders adt_folder/ patient_folder/ visit_folder/ output_folder/
  python csv_combiner.py --folders adt_folder/ patient_folder/ visit_folder/ output_folder/ --facility-name "Medilodge Facility"
        """
    )
    
    # Add mutually exclusive group for individual files vs folders
    group = parser.add_mutually_exclusive_group(required=True)
    
    # Individual file processing
    group.add_argument('adt_file', nargs='?', help='Path to ADT cycles CSV file (for individual file processing)')
    group.add_argument('patient_file', nargs='?', help='Path to patient data CSV file (for individual file processing)')
    group.add_argument('visit_file', nargs='?', help='Path to visit data CSV/Excel file (for individual file processing)')
    group.add_argument('output_file', nargs='?', help='Path for the output CSV file (for individual file processing)')
    
    # Folder processing
    group.add_argument('--folders', nargs=4, metavar=('ADT_FOLDER', 'PATIENT_FOLDER', 'VISIT_FOLDER', 'OUTPUT_FOLDER'),
                      help='Process folders of CSV files: ADT_FOLDER PATIENT_FOLDER VISIT_FOLDER OUTPUT_FOLDER')
    
    parser.add_argument('--facility-name', help='Name of the facility for summarized data (default: extracted from patient file name)')
    
    args = parser.parse_args()
    
    # Check which mode to use
    if args.folders:
        # Folder processing mode
        adt_folder, patient_folder, visit_folder, output_folder = args.folders
        
        print("=" * 60)
        print("CSV COMBINER - FOLDER MODE")
        print("=" * 60)
        print(f"ADT folder: {adt_folder}")
        print(f"Patient folder: {patient_folder}")
        print(f"Visit folder: {visit_folder}")
        print(f"Output folder: {output_folder}")
        print("=" * 60)
        
        # Process folders
        process_folder_batch(adt_folder, patient_folder, visit_folder, output_folder, args.facility_name)
        
    else:
        # Individual file processing mode
        if not all([args.adt_file, args.patient_file, args.visit_file, args.output_file]):
            parser.error("All four arguments (adt_file, patient_file, visit_file, output_file) are required for individual file processing")
        
        print("=" * 60)
        print("CSV COMBINER - INDIVIDUAL FILE MODE")
        print("=" * 60)
        print(f"ADT file: {args.adt_file}")
        print(f"Patient file: {args.patient_file}")
        print(f"Visit file: {args.visit_file}")
        print(f"Output file: {args.output_file}")
        print("=" * 60)
        
        # Process individual files
        process_file_combination(args.adt_file, args.patient_file, args.visit_file, args.output_file, args.facility_name)
    
    print("\n" + "=" * 60)
    print("[OK] PROCESSING COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    main()