#!/usr/bin/env python3
"""
Unified PDF to CSV Converter
Combines PDF text extraction, GPT parsing, patient grouping, and CSV conversion into one pipeline.

This script takes a PDF file as input and outputs a CSV file with patient cycles.
Optionally saves intermediate JSON files for debugging/analysis.
"""

import PyPDF2
import sys
import os
import json
import csv
import openai
import argparse
import glob
import re
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed


def extract_text_from_pdf(pdf_path, save_text_file=None):
    """
    Extract all text from a PDF file using PyPDF2.
    
    Args:
        pdf_path (str): Path to the input PDF file
        save_text_file (str, optional): Path to save the extracted text
    
    Returns:
        str: Combined text from all pages
    """
    try:
        # Check if PDF file exists
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        # Open the PDF file
        with open(pdf_path, 'rb') as file:
            # Create a PDF reader object
            pdf_reader = PyPDF2.PdfReader(file)
            
            # Get number of pages
            num_pages = len(pdf_reader.pages)
            print(f"Processing PDF with {num_pages} pages...")
            
            # Extract text from all pages
            all_text = ""
            for page_num, page in enumerate(pdf_reader.pages, 1):
                try:
                    page_text = page.extract_text()
                    all_text += page_text + "\n"
                    print(f"Extracted text from page {page_num} ({len(page_text)} characters)")
                except Exception as e:
                    print(f"Warning: Could not extract text from page {page_num}: {e}")
                    all_text += f"\n[Error extracting text from page {page_num}: {e}]\n"
            
            print(f"Total characters extracted: {len(all_text)}")
            
            # Save extracted text to file if requested
            if save_text_file:
                try:
                    with open(save_text_file, 'w', encoding='utf-8') as text_file:
                        text_file.write(all_text)
                    print(f"Saved extracted text to: {save_text_file}")
                except Exception as e:
                    print(f"Warning: Could not save extracted text to file: {e}")
            
            return all_text
            
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return None


def clean_total_lines(text):
    """
    Remove all instances of 'Total: {some number}' except the last one.
    
    Args:
        text (str): Raw text extracted from PDF
    
    Returns:
        str: Cleaned text with all but the last 'Total: {number}' line removed
    """
    # Pattern to match 'Total: {number}' lines
    # This will match variations like "Total: 5", "Total: 123", etc.
    total_pattern = r'^Total:\s*\d+.*$'
    
    # Split text into lines
    lines = text.split('\n')
    
    # Find all lines that match the pattern
    total_lines = []
    for i, line in enumerate(lines):
        if re.match(total_pattern, line.strip(), re.IGNORECASE):
            total_lines.append((i, line))
    
    # If we found total lines, remove all except the last one
    if len(total_lines) > 1:
        print(f"Found {len(total_lines)} 'Total: X' lines, keeping only the last one")
        
        # Remove all total lines except the last one
        for line_index, _ in total_lines[:-1]:  # All except the last
            lines[line_index] = ''  # Replace with empty string
        
        # Join the lines back together
        cleaned_text = '\n'.join(lines)
        
        print(f"Removed {len(total_lines) - 1} 'Total: X' lines from text")
        return cleaned_text
    elif len(total_lines) == 1:
        print("Found 1 'Total: X' line, keeping it")
        return text
    else:
        print("No 'Total: X' lines found")
        return text


def parse_with_gpt4o_mini(text, api_key=None):
    """
    Use GPT-4o-mini to parse the extracted text into structured JSON.
    
    Args:
        text (str): Raw text extracted from PDF
        api_key (str): OpenAI API key
    
    Returns:
        dict: Structured JSON data
    """
    if not api_key:
        print("Error: OpenAI API key is required")
        return None
    
    # Check if text is too large (roughly 20,000 characters might be too much)
    if len(text) > 20000:
        print(f"Warning: Text is very large ({len(text)} characters). This might cause token limit issues.")
        print("Attempting to process with increased token limit...")
    
    # Set up OpenAI client
    client = openai.OpenAI(api_key=api_key)
    
    # Create the prompt for GPT-4o-mini
    prompt = f"""
You are a data extraction specialist. Convert the following Admission/Discharge report text into a SINGLE valid JSON object that strictly conforms to the schema below. Return ONLY the JSON (no prose, no markdown, no comments).

CRITICAL NAME PARSING RULE: When you see names like "Anastasia, Zedna (13715)", the resident_name must be "Anastasia, Zedna" (both last and first name). NEVER truncate names to just the last name.

############################
# OUTPUT CONSTRAINTS
############################
- Output must be valid JSON and parse without errors.
- Use null for unknown fields; never invent data.
- Dates must be ISO 8601 (YYYY-MM-DD).
- Time must be HH:MM:SS (no timezone suffix).
- Preserve source capitalization for names and locations (e.g., "FELIX, TERRY").
- Trim extra spaces; collapse internal runs of spaces to single spaces.

############################
# SCOPE OF EXTRACTION
############################
Extract:
1) Report metadata (facility name, report title, generated date/time, user, page info, and the date ranges for admissions/discharges).
2) Admissions section entries (if present).
3) Discharges section entries - PROCESS ALL PAGES AND ALL BLOCKS.
4) Subtotals for each facility block found in detail and in the Summary section.
5) The grand total (e.g., "Total Discharges: 74") if present.

IMPORTANT: This document may span multiple pages. Continue processing ALL data throughout the entire document, including data that appears after "Summary" sections.

############################
# JSON SHAPE (REQUIRED)
############################
{{
  "report_metadata": {{
    "facility": string|null,
    "title": string|null,
    "generated_date": "YYYY-MM-DD"|null,
    "generated_time_et": "HH:MM:SS"|null,
    "user": string|null,
    "admissions_range": {{"from": "YYYY-MM-DD"|null, "to": "YYYY-MM-DD"|null}},
    "discharges_range": {{"from": "YYYY-MM-DD"|null, "to": "YYYY-MM-DD"|null}},
    "page": {{"number": integer|null, "of": integer|null}}
  }},
  "sections": [
    {{
      "name": "Admissions",
      "entries": [
        {{
          "from_type": string|null,
          "from_location": string|null,
          "resident_name": "Last, First"|null,
          "resident_id": string|null,
          "effective_date": "YYYY-MM-DD"|null
        }}
      ],
      "subtotals": [
        {{
          "from_type": string|null,
          "from_location": string|null,
          "count": integer,
          "note": string|null,
          "source": "detail"|"summary"
        }}
      ],
      "total": integer|null
    }},
    {{
      "name": "Discharges",
      "entries": [
        {{
          "to_type": string|null,
          "to_location": string|null,
          "resident_name": "Last, First"|null,
          "resident_id": string|null,
          "effective_date": "YYYY-MM-DD"|null
        }}
      ],
      "subtotals": [
        {{
          "to_type": string|null,
          "to_location": string|null,
          "count": integer,
          "note": string|null,
          "source": "detail"|"summary"
        }}
      ],
      "total": integer|null
    }}
  ],
  "grand_totals": {{
    "admissions": integer|null,
    "discharges": integer|null
  }},
  "validation": {{
    "notes": [string],
    "warnings": [string]
  }}
}}

############################
# PARSING RULES
############################
A) GENERAL
- Treat the document as multi-page; metadata may repeat. Use the FIRST occurrence of facility/title/date/time/user as canonical report_metadata.
- CRITICAL: Process the ENTIRE document from start to finish. Do not stop at "Summary" sections or intermediate totals.
- Page info: parse from patterns like "Page X of Y". Use X=1 and Y from the first page header if available.
- Date normalization:
  • "Date: Oct 16, 2025" → generated_date = 2025-10-16
  • Ranges like "Discharges 5/1/2025 To 7/31/2025" → discharges_range.from=2025-05-01, to=2025-07-31
  • If "No Admissions selected" appears, keep admissions_range values as null unless an explicit range is shown.

B) TABLE HEADERS & GROUPS
- Headers may appear as: "To Type  To Location  Resident  Effective Date" or "From Type  From Location  Resident  Effective Date".
- Rows are grouped by facility type/location blocks. A new block begins when a new Type (e.g., "Acute care hospital", "Funeral Home", "Private home/apt. ...", "Board and care/assisted living/group home", "Other Health Facility", etc.) appears.
- Continuation rows: If a line contains only "Resident (ID) Date" without repeating type/location, INHERIT the most recent type and location from the current block.
- Subtotal lines: "Total: N" immediately following a block belong to that block.

C) RESIDENT & ID DETECTION (CRITICAL)
- Resident pattern: (?P<last>[A-Z][A-Za-z'\\- ]+), (?P<first>[A-Z][A-Za-z'\\- ]+) \\((?P<id>\\d{{6}}|\\d{{5}}|\\d{{4,}})\\)
  • Allow spaces/hyphens/apostrophes in names (e.g., "Hitsman Whelpley, Tina").
  • Preserve capitalization as in source (including all-caps last names like "FELIX, TERRY").
- CRITICAL: resident_name MUST be exactly "Last, First" from the source (before the parenthetical ID).
  • Example: "Anastasia, Zedna (13715)" → resident_name="Anastasia, Zedna", resident_id="13715"
  • Example: "Smith, John (12345)" → resident_name="Smith, John", resident_id="12345"
  • NEVER truncate names - always include both last and first name parts
- resident_id is the number inside parentheses.

D) TYPE vs LOCATION DISAMBIGUATION (CRITICAL)
- After detecting a Type token at the start of a row (e.g., "Acute care hospital", "Funeral Home", "Other Health Facility", "Nursing home", "Home with Hospice", "Private home/apt. with home health services", "Private home/apt. with no home health services", "Board and care/assisted living/group home", "Other"), parse any LOCATION TEXT that appears BETWEEN the Type and the Resident.
- LOCATION is ALL text after the Type and BEFORE the resident pattern; trim it.
- If the resident pattern immediately follows the Type (i.e., there is NO intervening location), set location = null.
  • Example: "Other Health Facility Barclay, Paul (202605) 07/09/2025" → to_type="Other Health Facility"; to_location=null; resident_name="Barclay, Paul".
  • Example: "Acute care hospital Blodgett Sloan, Thomas (202644) 06/30/2025" → to_type="Acute care hospital"; to_location="Blodgett"; resident_name="Sloan, Thomas".
- NEVER treat the resident last name as a location.

E) EFFECTIVE DATE
- Effective date is the final date on the row; normalize to YYYY-MM-DD.

F) SUBTOTALS & SUMMARY
- "Total: N" directly under a block → record as a subtotal for that block with source="detail".
- The "Summary" section lists location-level counts; attach those as subtotals with source="summary". Use the Type inferred from the summary heading context or leave as null if unclear.
- IMPORTANT: The "Summary" section is NOT the end of the document. Continue processing all data after the Summary section.
- "Total Discharges: N" (or similar) → set sections[].total and grand_totals.discharges accordingly.
- If detail and summary counts disagree, keep both subtotals and add a validation.warning describing the mismatch.

G) SECTIONS
- Use "from_*" keys for Admissions blocks; "to_*" keys for Discharges blocks.
- If there are no Admissions entries, return an empty entries array and empty subtotals array for Admissions; total may be null.

H) NORMALIZATION & SANITY CHECKS
- Whitespace: collapse runs; trim ends.
- IDs: numeric string only (no parentheses).
- Add validation.notes for assumptions (e.g., inherited location=null when absent).
- Add validation.warnings for anomalies (e.g., name not matching pattern, missing ID, bad date).

############################
# ENUM HINTS (do NOT force; use as soft match)
############################
Known Type starters (case-sensitive as seen in source lines):
- "Acute care hospital"
- "Funeral Home"
- "Nursing home"
- "Private home/apt. with home health services"
- "Private home/apt. with no home health services"
- "Home with Hospice"
- "Board and care/assisted living/group home"
- "Other Health Facility"
- "Other"

############################
# INPUT
############################
{text}

############################
# OUTPUT
############################
Return ONLY the JSON object described above.
"""


    try:
        print("Sending text to GPT-4o-mini for parsing...")
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a data extraction specialist that converts unstructured text into structured JSON format."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=16000
        )
        
        # Extract the JSON from the response
        json_text = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if present
        if json_text.startswith('```json'):
            json_text = json_text[7:]  # Remove ```json
        if json_text.startswith('```'):
            json_text = json_text[3:]   # Remove ```
        if json_text.endswith('```'):
            json_text = json_text[:-3]  # Remove closing ```
        
        json_text = json_text.strip()
        
        # Try to parse the JSON
        try:
            structured_data = json.loads(json_text)
            print("Successfully parsed JSON from GPT-4o-mini")
            return structured_data
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            print(f"Raw response length: {len(json_text)} characters")
            print(f"Raw response (first 500 chars): {json_text[:500]}")
            
            # Check if the response appears to be truncated
            if json_text.count('{') > json_text.count('}'):
                print("Response appears to be truncated (unmatched braces)")
                print("Attempting to fix truncated JSON...")
                
                # Try to fix truncated JSON by adding missing closing braces
                try:
                    # Count missing closing braces
                    open_braces = json_text.count('{')
                    close_braces = json_text.count('}')
                    missing_braces = open_braces - close_braces
                    
                    if missing_braces > 0:
                        # Add missing closing braces
                        fixed_json = json_text + '}' * missing_braces
                        print(f"Added {missing_braces} missing closing braces")
                        
                        # Try to parse the fixed JSON
                        structured_data = json.loads(fixed_json)
                        print("Successfully parsed fixed JSON from GPT-4o-mini")
                        return structured_data
                except json.JSONDecodeError as fix_error:
                    print(f"Failed to fix truncated JSON: {fix_error}")
            
            return None
            
    except Exception as e:
        print(f"Error calling GPT-4o-mini: {e}")
        return None


def parse_name(full_name):
    """Parse full name into first and last name."""
    if not full_name:
        return "", ""
    
    # Remove quotes if present
    full_name = full_name.strip('"')
    
    # Split by comma and clean up
    parts = [part.strip() for part in full_name.split(',')]
    
    if len(parts) >= 2:
        last_name = parts[0]
        first_name = parts[1]
    elif len(parts) == 1:
        # If no comma, assume it's just last name
        last_name = parts[0]
        first_name = ""
    else:
        last_name = ""
        first_name = ""
    
    return first_name, last_name


def fix_empty_first_names(data):
    """
    Fix cases where first_name is empty but last_name contains multiple names.
    The last_name field contains "First Name Middle Name Last Name" format when first_name is empty.
    Splits the names so that the first part becomes first_name and all subsequent parts become last_name.
    
    Args:
        data (dict): Patient grouped data
    
    Returns:
        dict: Updated data with fixed names
    """
    print("Checking for empty first names that need fixing...")
    
    fixed_count = 0
    
    for patient_id, patient in data.get('patients', {}).items():
        first_name = patient.get('first_name', '').strip()
        last_name = patient.get('last_name', '').strip()
        
        # If first_name is empty but last_name contains spaces, split it
        if not first_name and last_name and ' ' in last_name:
            # Split by space and clean up
            parts = [part.strip() for part in last_name.split(' ') if part.strip()]
            
            if len(parts) >= 2:
                # Update the names - first part goes to first_name, all subsequent parts become last_name
                patient['first_name'] = parts[0]  # First part becomes first name
                patient['last_name'] = ' '.join(parts[1:])  # All remaining parts become last name
                patient['full_name'] = f"{' '.join(parts[1:])}, {parts[0]}"  # Update full name to "Last, First" format
                
                fixed_count += 1
                print(f"Fixed name for patient {patient_id}: '{last_name}' → first='{parts[0]}', last='{' '.join(parts[1:])}'")
    
    print(f"Fixed {fixed_count} names with empty first names")
    return data


def create_patient_grouped_json(data):
    """Create JSON with all patient entries grouped by patient ID."""
    
    # Dictionary to store patient data by resident_id
    patients = defaultdict(lambda: {
        'resident_id': '',
        'first_name': '',
        'last_name': '',
        'full_name': '',
        'admissions': [],
        'discharges': [],
        'total_admissions': 0,
        'total_discharges': 0
    })
    
    # Process admissions
    for section in data.get('sections', []):
        if section.get('name') == 'Admissions':
            for entry in section.get('entries', []):
                resident_id = entry.get('resident_id', '')
                full_name = entry.get('resident_name', '')
                first_name, last_name = parse_name(full_name)
                
                patient = patients[resident_id]
                patient['resident_id'] = resident_id
                patient['first_name'] = first_name
                patient['last_name'] = last_name
                patient['full_name'] = full_name
                
                # Add admission entry
                admission_entry = {
                    'effective_date': entry.get('effective_date', ''),
                    'from_type': entry.get('from_type', ''),
                    'from_location': entry.get('from_location', '')
                }
                patient['admissions'].append(admission_entry)
                patient['total_admissions'] += 1
    
    # Process discharges
    for section in data.get('sections', []):
        if section.get('name') == 'Discharges':
            for entry in section.get('entries', []):
                resident_id = entry.get('resident_id', '')
                full_name = entry.get('resident_name', '')
                first_name, last_name = parse_name(full_name)
                
                patient = patients[resident_id]
                patient['resident_id'] = resident_id
                patient['first_name'] = first_name
                patient['last_name'] = last_name
                patient['full_name'] = full_name
                
                # Add discharge entry
                discharge_entry = {
                    'effective_date': entry.get('effective_date', ''),
                    'to_type': entry.get('to_type', ''),
                    'to_location': entry.get('to_location', '')
                }
                patient['discharges'].append(discharge_entry)
                patient['total_discharges'] += 1
    
    # Sort admissions and discharges by date for each patient
    for patient in patients.values():
        patient['admissions'].sort(key=lambda x: x['effective_date'])
        patient['discharges'].sort(key=lambda x: x['effective_date'])
    
    # Create the final JSON structure
    result = {
        'metadata': {
            'generated_date': datetime.now().strftime('%Y-%m-%d'),
            'generated_time': datetime.now().strftime('%H:%M:%S'),
            'source_file': 'PDF_structured_data',
            'total_patients': len(patients),
            'description': 'Patient data grouped by resident ID with all admissions and discharges'
        },
        'report_info': data.get('report_metadata', {}),
        'patients': {}
    }
    
    # Convert patients to regular dict and sort by resident_id
    sorted_patients = dict(sorted(patients.items(), key=lambda x: x[0]))
    result['patients'] = sorted_patients
    
    # Add summary statistics
    result['summary'] = {
        'total_patients': len(patients),
        'patients_with_admissions_only': sum(1 for p in patients.values() if p['total_admissions'] > 0 and p['total_discharges'] == 0),
        'patients_with_discharges_only': sum(1 for p in patients.values() if p['total_admissions'] == 0 and p['total_discharges'] > 0),
        'patients_with_both': sum(1 for p in patients.values() if p['total_admissions'] > 0 and p['total_discharges'] > 0),
        'total_admission_entries': sum(p['total_admissions'] for p in patients.values()),
        'total_discharge_entries': sum(p['total_discharges'] for p in patients.values())
    }
    
    return result


def parse_date(date_str):
    """Parse date string to datetime object for comparison."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def find_subsequent_cycles(admissions, discharges):
    """
    Find subsequent admission-discharge cycles.
    Returns list of tuples: (admission_entry, discharge_entry)
    """
    cycles = []
    
    # Sort both lists by date
    sorted_admissions = sorted(admissions, key=lambda x: parse_date(x.get('effective_date', '')) or datetime.min)
    sorted_discharges = sorted(discharges, key=lambda x: parse_date(x.get('effective_date', '')) or datetime.min)
    
    # If no admissions or no discharges, return empty cycles
    if not sorted_admissions or not sorted_discharges:
        return cycles
    
    # Find subsequent pairs
    admission_index = 0
    discharge_index = 0
    
    while admission_index < len(sorted_admissions) and discharge_index < len(sorted_discharges):
        admission = sorted_admissions[admission_index]
        discharge = sorted_discharges[discharge_index]
        
        admission_date = parse_date(admission.get('effective_date', ''))
        discharge_date = parse_date(discharge.get('effective_date', ''))
        
        # If discharge is after admission, it's a valid cycle
        if admission_date and discharge_date and discharge_date >= admission_date:
            cycles.append((admission, discharge))
            admission_index += 1
            discharge_index += 1
        elif admission_date and discharge_date and discharge_date < admission_date:
            # Discharge is before admission, skip this discharge
            discharge_index += 1
        else:
            # Handle cases where dates can't be parsed
            if not admission_date:
                admission_index += 1
            if not discharge_date:
                discharge_index += 1
    
    return cycles


def create_cycles_csv(data, output_file):
    """Create CSV with admission-discharge cycles."""
    
    cycles_data = []
    
    # Process each patient
    for patient_id, patient in data.get('patients', {}).items():
        first_name = patient.get('first_name', '')
        last_name = patient.get('last_name', '')
        admissions = patient.get('admissions', [])
        discharges = patient.get('discharges', [])
        
        # Find subsequent cycles
        cycles = find_subsequent_cycles(admissions, discharges)
        
        if cycles:
            # Add rows for each cycle
            for admission, discharge in cycles:
                cycle_row = {
                    'first_name': first_name,
                    'last_name': last_name,
                    'resident_id': patient_id,
                    'admission_date': admission.get('effective_date', ''),
                    'discharge_date': discharge.get('effective_date', ''),
                    'from_location': admission.get('from_location', ''),
                    'to_location': discharge.get('to_location', ''),
                    'from_type': admission.get('from_type', ''),
                    'to_type': discharge.get('to_type', '')
                    # Commented out fields as requested:
                    # 'cycle_number': i,
                    # 'total_cycles_for_patient': len(cycles),
                    # 'patient_total_admissions': total_admissions,
                    # 'patient_total_discharges': total_discharges
                }
                cycles_data.append(cycle_row)
        else:
            # Handle cases with only admissions or only discharges
            if admissions and not discharges:
                # Only admissions, no discharges
                for admission in admissions:
                    cycle_row = {
                        'first_name': first_name,
                        'last_name': last_name,
                        'resident_id': patient_id,
                        'admission_date': admission.get('effective_date', ''),
                        'discharge_date': '',  # No discharge
                        'from_location': admission.get('from_location', ''),
                        'to_location': '',  # No discharge location
                        'from_type': admission.get('from_type', ''),
                        'to_type': ''  # No discharge type
                    }
                    cycles_data.append(cycle_row)
            
            elif discharges and not admissions:
                # Only discharges, no admissions
                for discharge in discharges:
                    cycle_row = {
                        'first_name': first_name,
                        'last_name': last_name,
                        'resident_id': patient_id,
                        'admission_date': '',  # No admission
                        'discharge_date': discharge.get('effective_date', ''),
                        'from_location': '',  # No admission location
                        'to_location': discharge.get('to_location', ''),
                        'from_type': '',  # No admission type
                        'to_type': discharge.get('to_type', '')
                    }
                    cycles_data.append(cycle_row)
    
    # Sort by patient name and then by admission date
    cycles_data.sort(key=lambda x: (x['last_name'], x['first_name'], x['admission_date'] or '9999-12-31'))
    
    # Define CSV headers (excluding commented fields)
    headers = [
        'first_name',
        'last_name', 
        'resident_id',
        'admission_date',
        'discharge_date',
        'from_location',
        'to_location',
        'from_type',
        'to_type'
    ]
    
    # Write to CSV
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for cycle in cycles_data:
                writer.writerow(cycle)
        
        print(f"Successfully created cycles CSV: {output_file}")
        print(f"  - Total cycles/rows: {len(cycles_data)}")
        
        # Count different types of cycles
        complete_cycles = sum(1 for c in cycles_data if c['admission_date'] and c['discharge_date'])
        admission_only = sum(1 for c in cycles_data if c['admission_date'] and not c['discharge_date'])
        discharge_only = sum(1 for c in cycles_data if not c['admission_date'] and c['discharge_date'])
        
        print(f"  - Complete cycles (admission + discharge): {complete_cycles}")
        print(f"  - Admission only: {admission_only}")
        print(f"  - Discharge only: {discharge_only}")
        
        return True
        
    except Exception as e:
        print(f"Error writing CSV file: {e}")
        return False


def process_single_pdf(pdf_file, api_key, output_dir, save_json=False, save_text=False):
    """
    Process a single PDF file through the complete pipeline.
    
    Args:
        pdf_file (str): Path to the PDF file
        api_key (str): OpenAI API key
        output_dir (str): Output directory
        save_json (bool): Whether to save intermediate JSON files
        save_text (bool): Whether to save extracted text file
    
    Returns:
        dict: Processing results with success status and statistics
    """
    print(f"\nProcessing: {pdf_file}")
    print("-" * 40)
    
    # Step 1: Extract text from PDF
    print("Step 1: Extracting text from PDF...")
    
    # Determine text file path if saving is requested
    text_file_path = None
    if save_text:
        text_file_path = os.path.join(output_dir, f"{Path(pdf_file).stem}_extracted_text.txt")
    
    extracted_text = extract_text_from_pdf(pdf_file, text_file_path)
    
    if not extracted_text:
        return {
            'success': False,
            'error': 'Text extraction failed',
            'file': pdf_file
        }
    
    # Step 1.5: Clean the extracted text (remove all 'Total: X' except the last one)
    print("Step 1.5: Cleaning extracted text...")
    cleaned_text = clean_total_lines(extracted_text)
    
    # Step 2: Parse with GPT-4o-mini
    print("Step 2: Parsing with GPT-4o-mini...")
    structured_data = parse_with_gpt4o_mini(cleaned_text, api_key)
    
    if not structured_data:
        return {
            'success': False,
            'error': 'Parsing with GPT-4o-mini failed',
            'file': pdf_file
        }
    
    # Save structured JSON if requested
    if save_json:
        structured_json_file = os.path.join(output_dir, f"{Path(pdf_file).stem}_structured.json")
        with open(structured_json_file, 'w', encoding='utf-8') as f:
            json.dump(structured_data, f, indent=2, ensure_ascii=False)
        print(f"Saved structured JSON: {structured_json_file}")
    
    # Step 3: Group by patients
    print("Step 3: Grouping data by patients...")
    patient_grouped_data = create_patient_grouped_json(structured_data)
    
    # Step 3.5: Fix empty first names
    print("Step 3.5: Fixing empty first names...")
    patient_grouped_data = fix_empty_first_names(patient_grouped_data)
    
    # Save patient-grouped JSON if requested
    if save_json:
        patient_json_file = os.path.join(output_dir, f"{Path(pdf_file).stem}_patients_grouped.json")
        with open(patient_json_file, 'w', encoding='utf-8') as f:
            json.dump(patient_grouped_data, f, indent=2, ensure_ascii=False)
        print(f"Saved patient-grouped JSON: {patient_json_file}")
    
    # Step 4: Create CSV
    print("Step 4: Creating CSV output...")
    csv_file = os.path.join(output_dir, f"{Path(pdf_file).stem}_cycles.csv")
    success = create_cycles_csv(patient_grouped_data, csv_file)
    
    if not success:
        return {
            'success': False,
            'error': 'CSV creation failed',
            'file': pdf_file
        }
    
    # Get summary statistics
    summary = patient_grouped_data.get('summary', {})
    
    return {
        'success': True,
        'file': pdf_file,
        'csv_file': csv_file,
        'summary': summary,
        'file_size': os.path.getsize(csv_file) if os.path.exists(csv_file) else 0
    }


def process_folder(folder_path, api_key, output_dir, save_json=False, save_text=False, parallel=False, max_workers=3):
    """
    Process all PDF files in a folder.
    
    Args:
        folder_path (str): Path to the folder containing PDF files
        api_key (str): OpenAI API key
        output_dir (str): Output directory
        save_json (bool): Whether to save intermediate JSON files
        save_text (bool): Whether to save extracted text files
        parallel (bool): Whether to process files in parallel
        max_workers (int): Maximum number of parallel workers (if parallel=True)
    
    Returns:
        dict: Batch processing results
    """
    # Find all PDF files in the folder
    pdf_pattern = os.path.join(folder_path, "*.pdf")
    pdf_files = glob.glob(pdf_pattern)
    
    if not pdf_files:
        return {
            'success': False,
            'error': f'No PDF files found in folder: {folder_path}',
            'processed': 0,
            'failed': 0
        }
    
    print(f"Found {len(pdf_files)} PDF files in folder: {folder_path}")
    if parallel:
        print(f"Processing {len(pdf_files)} files in parallel (max workers: {max_workers})")
    print("=" * 60)
    
    # Process each PDF file
    results = []
    successful = 0
    failed = 0
    
    if parallel and len(pdf_files) > 1:
        # Parallel processing
        def process_with_index(pdf_file):
            """Wrapper to process file and return with index for ordering"""
            index = pdf_files.index(pdf_file) + 1
            print(f"\n[Starting {index}/{len(pdf_files)}] Processing: {os.path.basename(pdf_file)}")
            result = process_single_pdf(pdf_file, api_key, output_dir, save_json, save_text)
            result['_index'] = index
            result['_filename'] = os.path.basename(pdf_file)
            return result
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(process_with_index, pdf_file): pdf_file for pdf_file in pdf_files}
            
            # Collect results as they complete
            completed_results = []
            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    completed_results.append(result)
                except Exception as exc:
                    pdf_file = future_to_file[future]
                    print(f"[FAILED] {os.path.basename(pdf_file)} generated an exception: {exc}")
                    completed_results.append({
                        'success': False,
                        'error': str(exc),
                        'file': pdf_file,
                        '_index': pdf_files.index(pdf_file) + 1,
                        '_filename': os.path.basename(pdf_file)
                    })
            
            # Sort results by original index to maintain order in output
            completed_results.sort(key=lambda x: x.get('_index', 999))
            
            # Process and log results
            for result in completed_results:
                results.append({k: v for k, v in result.items() if not k.startswith('_')})
                if result['success']:
                    successful += 1
                    print(f"[OK] Successfully processed: {result.get('_filename', result.get('file', 'unknown'))}")
                else:
                    failed += 1
                    print(f"[FAILED] Failed to process: {result.get('_filename', result.get('file', 'unknown'))} - {result.get('error', 'Unknown error')}")
    else:
        # Sequential processing (original behavior)
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"\n[{i}/{len(pdf_files)}] Processing: {os.path.basename(pdf_file)}")
            
            result = process_single_pdf(pdf_file, api_key, output_dir, save_json, save_text)
            results.append(result)
            
            if result['success']:
                successful += 1
                print(f"[OK] Successfully processed: {os.path.basename(pdf_file)}")
            else:
                failed += 1
                print(f"[FAILED] Failed to process: {os.path.basename(pdf_file)} - {result['error']}")
    
    # Create batch summary
    batch_summary = {
        'success': True,  # Always True if we found files to process
        'total_files': len(pdf_files),
        'successful': successful,
        'failed': failed,
        'results': results
    }
    
    # Save batch summary
    batch_summary_file = os.path.join(output_dir, 'batch_processing_summary.json')
    with open(batch_summary_file, 'w', encoding='utf-8') as f:
        json.dump(batch_summary, f, indent=2, ensure_ascii=False)
    
    return batch_summary


def main():
    """Main function to run the unified PDF to CSV pipeline."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Unified PDF to CSV Converter - Process PDF files or entire folders',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python unified_pdf_to_csv.py document.pdf
  python unified_pdf_to_csv.py document.pdf --save-json
  python unified_pdf_to_csv.py document.pdf --save-text
  python unified_pdf_to_csv.py document.pdf --save-json --save-text
  python unified_pdf_to_csv.py /path/to/folder --folder
  python unified_pdf_to_csv.py /path/to/folder --folder --save-json
  python unified_pdf_to_csv.py /path/to/folder --folder --save-text
        """
    )
    
    parser.add_argument('input_path', help='Path to PDF file or folder containing PDF files')
    parser.add_argument('--folder', action='store_true', help='Process entire folder instead of single file')
    parser.add_argument('--save-json', action='store_true', help='Save intermediate JSON files for debugging')
    parser.add_argument('--save-text', action='store_true', help='Save extracted text files')
    parser.add_argument('--output-dir', default='unified_output', help='Output directory (default: unified_output)')
    parser.add_argument('--parallel', action='store_true', help='Process files in parallel (folder mode only)')
    parser.add_argument('--max-workers', type=int, default=3, help='Maximum number of parallel workers (default: 3)')
    
    args = parser.parse_args()
    
    # Check for OpenAI API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("Error: OPENAI_API_KEY not found in .env file")
        print("Please create a .env file with your OpenAI API key:")
        print("  OPENAI_API_KEY=your_api_key_here")
        sys.exit(1)
    
    # Validate input path
    if not os.path.exists(args.input_path):
        print(f"Error: Input path does not exist: {args.input_path}")
        sys.exit(1)
    
    # Create output directory
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Unified PDF to CSV Converter")
    print(f"Input: {args.input_path}")
    print(f"Mode: {'Folder processing' if args.folder else 'Single file processing'}")
    print(f"Save intermediate JSON: {args.save_json}")
    print(f"Save extracted text: {args.save_text}")
    print(f"Output directory: {output_dir}")
    if args.folder:
        print(f"Parallel processing: {'Enabled' if args.parallel else 'Disabled'}")
        if args.parallel:
            print(f"Max workers: {args.max_workers}")
    print("=" * 60)
    
    if args.folder:
        # Process entire folder
        if not os.path.isdir(args.input_path):
            print(f"Error: Specified path is not a directory: {args.input_path}")
            sys.exit(1)
        
        batch_results = process_folder(args.input_path, api_key, output_dir, args.save_json, args.save_text, args.parallel, args.max_workers)
        
        if not batch_results['success']:
            print(f"Batch processing failed: {batch_results['error']}")
            sys.exit(1)
        
        # Print batch summary
        print("\n" + "=" * 60)
        print("BATCH PROCESSING COMPLETED")
        print("=" * 60)
        print(f"Total files processed: {batch_results['total_files']}")
        print(f"Successful: {batch_results['successful']}")
        print(f"Failed: {batch_results['failed']}")
        
        if batch_results['successful'] > 0:
            print(f"\nSuccessful files:")
            for result in batch_results['results']:
                if result['success']:
                    summary = result['summary']
                    print(f"  [OK] {os.path.basename(result['file'])}")
                    print(f"    - Patients: {summary.get('total_patients', 0)}")
                    print(f"    - CSV: {os.path.basename(result['csv_file'])}")
                    print(f"    - Size: {result['file_size']:,} bytes")
        
        if batch_results['failed'] > 0:
            print(f"\nFailed files:")
            for result in batch_results['results']:
                if not result['success']:
                    print(f"  [FAILED] {os.path.basename(result['file'])} - {result['error']}")
        
        print(f"\nBatch summary saved: {os.path.join(output_dir, 'batch_processing_summary.json')}")
        
    else:
        # Process single file
        if not os.path.isfile(args.input_path):
            print(f"Error: Specified path is not a file: {args.input_path}")
            sys.exit(1)
        
        result = process_single_pdf(args.input_path, api_key, output_dir, args.save_json, args.save_text)
        
        if not result['success']:
            print(f"Processing failed: {result['error']}")
            sys.exit(1)
        
        # Print single file summary
        print("\n" + "=" * 60)
        print("PROCESSING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Input file: {args.input_path}")
        print(f"Output CSV: {result['csv_file']}")
        
        if args.save_json:
            print(f"Intermediate JSON files saved in: {output_dir}/")
        
        # Show summary statistics
        summary = result['summary']
        print(f"\nSummary Statistics:")
        print(f"  - Total patients: {summary.get('total_patients', 0)}")
        print(f"  - Patients with admissions only: {summary.get('patients_with_admissions_only', 0)}")
        print(f"  - Patients with discharges only: {summary.get('patients_with_discharges_only', 0)}")
        print(f"  - Patients with both: {summary.get('patients_with_both', 0)}")
        print(f"  - Total admission entries: {summary.get('total_admission_entries', 0)}")
        print(f"  - Total discharge entries: {summary.get('total_discharge_entries', 0)}")
        print(f"  - CSV file size: {result['file_size']:,} bytes")


if __name__ == "__main__":
    main()