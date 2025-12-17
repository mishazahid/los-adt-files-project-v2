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
            page_stats = []
            for page_num, page in enumerate(pdf_reader.pages, 1):
                try:
                    page_text = page.extract_text()
                    all_text += page_text + "\n"
                    page_stats.append({
                        'page': page_num,
                        'chars': len(page_text),
                        'has_content': len(page_text.strip()) > 0
                    })
                    print(f"Extracted text from page {page_num} ({len(page_text)} characters)")
                except Exception as e:
                    print(f"Warning: Could not extract text from page {page_num}: {e}")
                    all_text += f"\n[Error extracting text from page {page_num}: {e}]\n"
                    page_stats.append({
                        'page': page_num,
                        'chars': 0,
                        'has_content': False,
                        'error': str(e)
                    })
            
            print(f"Total characters extracted: {len(all_text)}")
            
            # Diagnostic: Check if all pages were extracted
            empty_pages = [p for p in page_stats if not p['has_content']]
            if empty_pages:
                print(f"[WARNING]  WARNING: {len(empty_pages)} pages appear to have no text extracted:")
                for p in empty_pages:
                    print(f"   - Page {p['page']} ({p.get('error', 'No text found')})")
                print("   This may indicate the PDF is image-based and needs OCR instead of PyPDF2.")
            
            # Diagnostic: Check for page markers in text
            page_markers = []
            lines = all_text.split('\n')
            for i, line in enumerate(lines):
                if 'page' in line.lower() and ('of' in line.lower() or any(char.isdigit() for char in line)):
                    page_markers.append((i+1, line.strip()[:100]))
            
            if page_markers:
                print(f"Found {len(page_markers)} potential page markers in text")
                if len(page_markers) < num_pages:
                    print(f"[WARNING]  WARNING: Only {len(page_markers)} page markers found for {num_pages} pages")
            
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


def remove_page_headers(text):
    """
    Remove page header lines that repeat on each page, but keep the first occurrence.
    These headers include facility name, date, time, report title, etc.
    Page headers can interrupt continuation rows and confuse GPT.
    
    Args:
        text (str): Raw text extracted from PDF
    
    Returns:
        str: Text with repeated page headers removed (keeping first occurrence)
    """
    if not text:
        return ""
    
    lines = text.split('\n')
    cleaned_lines = []
    
    # Patterns to identify page headers (exact matches)
    # Each pattern maps to a category key for tracking first occurrence
    header_patterns = [
        (r'^Medilodge (at the Shore|of [A-Za-z\s]+?( - SNF)?)$', 'facility_name'),  # Flexible facility name pattern - matches "Medilodge at the Shore" or "Medilodge of [Name]" or "Medilodge of [Name] - SNF"
        (r'^Date: \w+ \d+, \d{4}$', 'date'),
        (r'^Time: \d{2}:\d{2}:\d{2} ET', 'time'),  # More flexible - matches "Time: ... ET" even if followed by other text
        (r'^Admission/Discharge To/From Report$', 'report_title'),
        (r'^Admissions \d{1,2}/\d{1,2}/\d{4} To \d{1,2}/\d{1,2}/\d{4} - Discharges \d{1,2}/\d{1,2}/\d{4} To \d{1,2}/\d{1,2}/\d{4}User:.*$', 'admissions_discharges'),
        (r'^Page \d+ of \d+$', 'page_number'),
        (r'^Detail$', 'detail'),
        (r'^Facilities:.*$', 'facilities'),
        (r'^Admissions:.*$', 'admissions_label'),
        (r'^Discharges:.*$', 'discharges_label'),
        (r'^Report by:.*$', 'report_by'),
        (r'^Report:.*$', 'report'),
        (r'^Sort by:.*$', 'sort_by'),
    ]
    
    # Track which header categories have been seen (keep first occurrence)
    seen_headers = set()
    
    # Track multi-line header patterns separately
    seen_multi_line_headers = set()
    
    header_count = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        is_header = False
        is_repeat = False
        line_stripped = line.strip()
        
        # Check single-line header patterns
        for pattern, category in header_patterns:
            if re.match(pattern, line_stripped):
                if category in seen_headers:
                    # This is a repeat - remove it
                    is_header = True
                    is_repeat = True
                    header_count += 1
                else:
                    # First occurrence - keep it
                    seen_headers.add(category)
                    # Add the line - it's the first occurrence
                    cleaned_lines.append(line)
                break
        
        # Check for multi-line header content
        if not is_header and line_stripped:
            # Check if line contains header-like content
            header_key = None
            if line_stripped.startswith('Report by:'):
                header_key = 'report_by_multiline'
            elif line_stripped.startswith('To/From Type:'):
                header_key = 'to_from_type'
            elif 'Court/Law Enforcement' in line_stripped:
                header_key = 'court_law_enforcement'
            elif line_stripped.startswith('Facilities:') and 'Medilodge' in line_stripped:
                header_key = 'facilities_multiline'
            
            if header_key:
                is_repeat_header = header_key in seen_multi_line_headers
                
                if is_repeat_header:
                    # This is a repeat - remove it
                    is_header = True
                    is_repeat = True
                    header_count += 1
                    
                    # Check if next line is continuation of header (contains facility types)
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        # If next line contains facility type names (part of header), check if it's a repeat
                        if ('and care' in next_line.lower() or 
                            'nursing home' in next_line.lower() or
                            'rehabilitation' in next_line.lower()):
                            continuation_key = f'{header_key}_continuation'
                            if continuation_key in seen_multi_line_headers:
                                # Repeat continuation - skip it
                                header_count += 1
                                i += 1  # Skip the continuation line
                                continue
                    # Repeat header without continuation - fall through to skip adding
                else:
                    # First occurrence - keep it
                    seen_multi_line_headers.add(header_key)
                    # Add the line - it's the first occurrence
                    cleaned_lines.append(line)
                    
                    # Check if next line is continuation of header (contains facility types)
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        # If next line contains facility type names (part of header), check if it's a repeat
                        if ('and care' in next_line.lower() or 
                            'nursing home' in next_line.lower() or
                            'rehabilitation' in next_line.lower()):
                            continuation_key = f'{header_key}_continuation'
                            if continuation_key in seen_multi_line_headers:
                                # Repeat continuation - skip it
                                header_count += 1
                                i += 1  # Skip the continuation line
                                continue
                            else:
                                # First occurrence - keep it
                                seen_multi_line_headers.add(continuation_key)
                                i += 1  # Include the continuation line
                                cleaned_lines.append(lines[i])
                                i += 1
                                continue
                    # First occurrence without continuation - already added, skip to next iteration
                    i += 1
                    continue
        
        # Only add line if it's not a header, or if it's the first occurrence
        if not is_header:
            cleaned_lines.append(line)
        elif is_repeat:
            # Log that we're removing a repeat
            pass
        
        i += 1
    
    if header_count > 0:
        print(f"Removed {header_count} repeated page header lines (kept first occurrence)")
    
    result = '\n'.join(cleaned_lines)
    
    # Safety check: If we removed too much (more than 50% of lines), something went wrong
    # Return original text with a warning
    removed_percentage = 100 * (len(lines) - len(cleaned_lines)) / len(lines) if len(lines) > 0 else 0
    if len(cleaned_lines) < len(lines) * 0.5:
        print(f"[WARNING]  WARNING: Header removal seems too aggressive!")
        print(f"   Original: {len(lines)} lines, Cleaned: {len(cleaned_lines)} lines")
        print(f"   Removed {len(lines) - len(cleaned_lines)} lines ({removed_percentage:.1f}%)")
        print(f"   Returning original text to prevent data loss.")
        return text
    
    # Additional safety: If result is too short compared to original, return original
    if len(result.strip()) < len(text.strip()) * 0.3:
        print(f"[WARNING]  WARNING: Cleaned text is much shorter than original!")
        print(f"   Original: {len(text.strip())} chars, Cleaned: {len(result.strip())} chars")
        print(f"   Returning original text to prevent data loss.")
        return text
    
    # Final safety: Never return empty or None - always return at least the original text
    if not result or len(result.strip()) == 0:
        print(f"[WARNING]  WARNING: Header removal resulted in empty text!")
        print(f"   Returning original text to prevent data loss.")
        return text
    
    # Debug: Check if result is empty or very short
    if len(result.strip()) < 100:
        print(f"WARNING: Cleaned text is very short ({len(result)} chars). Original had {len(text)} chars.")
        print(f"Cleaned {len(cleaned_lines)} lines from {len(lines)} original lines")
        if cleaned_lines:
            print(f"First 10 lines of cleaned text:")
            for i, line in enumerate(cleaned_lines[:10], 1):
                print(f"  {i}: {line[:80]}")
        # Even if very short, return it (might be valid for very small PDFs)
        # But add a warning that it's short
    
    return result


def parse_with_python(text, metadata_text):
    """
    Parse the cleaned text into structured JSON using Python regex/pattern matching.
    This replaces GPT parsing for more reliable extraction.
    
    Args:
        text (str): Cleaned text from PDF (full text or section text)
        metadata_text (str): Metadata text for extracting report metadata
    
    Returns:
        dict: Structured JSON data matching the GPT output format
    """
    lines = text.split('\n')
    
    # Find section boundaries
    admissions_start = None
    discharges_start = None
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == 'Admissions' and admissions_start is None:
            admissions_start = i
        elif stripped == 'Discharges' and discharges_start is None:
            discharges_start = i
    
    # Extract metadata
    metadata = extract_metadata_from_text(metadata_text)
    
    # Known facility types
    facility_types = [
        'Acute care hospital',
        'Funeral Home',
        'Nursing home',
        'Private home/apt. with home health services',
        'Private home/apt. with no home health services',
        'Home with Hospice',
        'Board and care/assisted living/group home',
        'Other Health Facility',
        'Other',
        'Unknown',
        'Psychiatric hospital, MR/DD facility',
        'Rehabilitation hospital'
    ]
    
    # Parse Admissions section
    admissions_data = parse_section_with_python(
        lines, 
        admissions_start, 
        discharges_start, 
        'Admissions',
        'from_',
        facility_types
    )
    
    # Parse Discharges section
    discharges_data = parse_section_with_python(
        lines,
        discharges_start,
        None,
        'Discharges',
        'to_',
        facility_types
    )
    
    # Build combined structure
    result = {
        'report_metadata': metadata,
        'sections': []
    }
    
    if admissions_data:
        result['sections'].append(admissions_data)
    
    if discharges_data:
        result['sections'].append(discharges_data)
    
    # Add grand totals
    result['grand_totals'] = {
        'admissions': admissions_data.get('total') if admissions_data else None,
        'discharges': discharges_data.get('total') if discharges_data else None
    }
    
    # Add validation
    result['validation'] = {
        'notes': [],
        'warnings': []
    }
    
    return result


def clean_name_punctuation(name):
    """
    Remove unusual punctuation characters from names (like &, =, etc.)
    but keep common name characters like hyphens and apostrophes.
    
    Args:
        name (str): Name to clean
    
    Returns:
        str: Cleaned name
    """
    if not name:
        return ""
    
    # Remove ALL unusual punctuation characters throughout the name
    # Keep: letters, spaces, hyphens, apostrophes, periods (for initials)
    # Remove: &, =, and other special characters from anywhere in the name
    import string
    
    # Characters to keep (letters, digits, spaces, hyphens, apostrophes, periods)
    allowed_chars = set(string.ascii_letters + string.digits + " -'.")
    
    # Build cleaned name character by character
    cleaned_chars = []
    for char in name:
        if char in allowed_chars:
            cleaned_chars.append(char)
        elif char in '&=+*^%$#@!~`|\\/<>[]{}':
            # Skip these unwanted punctuation marks
            continue
        else:
            # For other characters, replace with space if it's whitespace-like
            if char.isspace():
                cleaned_chars.append(' ')
    
    # Join and clean up
    name = ''.join(cleaned_chars)
    
    # Remove leading/trailing punctuation except hyphens and apostrophes
    name = name.strip()
    
    # Remove leading special characters (but keep hyphens/apostrophes)
    while name and name[0] in '&=+*^%$#@!~`|\\/<>[]{}':
        name = name[1:].strip()
    
    # Remove trailing special characters (but keep hyphens/apostrophes)
    while name and name[-1] in '&=+*^%$#@!~`|\\/<>[]{}':
        name = name[:-1].strip()
    
    # Clean up multiple spaces
    name = ' '.join(name.split())
    
    return name


def parse_section_with_python(lines, section_start, section_end, section_name, key_prefix, facility_types):
    """
    Parse a single section (Admissions or Discharges) using Python.
    
    Args:
        lines: List of text lines
        section_start: Index where section starts (None if not found)
        section_end: Index where section ends (None if end of file)
        section_name: "Admissions" or "Discharges"
        key_prefix: "from_" or "to_"
        facility_types: List of known facility types
    
    Returns:
        dict: Section data with entries and subtotals
    """
    if section_start is None:
        return {
            'name': section_name,
            'entries': [],
            'subtotals': [],
            'total': None
        }
    
    # Get section lines
    if section_end is not None:
        section_lines = lines[section_start:section_end]
    else:
        section_lines = lines[section_start:]
    
    entries = []
    subtotals = []
    current_type = None
    current_location = None
    current_block_entries = []
    
    # Pattern to match patient entries: "Last, First (ID) Date"
    # Strategy: Find the ID pattern first, then extract name before it
    # This avoids matching location text as part of the name
    # Updated to support both numeric IDs (e.g., 310072) and alphanumeric IDs (e.g., MGB404400)
    id_pattern = re.compile(r'\(([A-Z0-9]{4,10})\)\s+(\d{1,2}/\d{1,2}/\d{4})$')
    
    # Helper function to extract patient info from a line
    def extract_patient_from_line(line_text):
        """Extract patient info from a line, avoiding location text."""
        # Find ID pattern first (anchored to end)
        id_match = id_pattern.search(line_text)
        if not id_match:
            return None
        
        # Get everything before the ID pattern
        before_id = line_text[:id_match.start()].rstrip()
        
        # Special handling: if "&" appears before the comma, it's likely a separator
        # between location and name (e.g., "MCLAREN VISITING NURSE & Zilska, Patricia")
        # In this case, we should find the comma after the "&"
        if '&' in before_id:
            # Find the last "&" - everything after it is likely the name part
            amp_pos = before_id.rfind('&')
            if amp_pos != -1:
                # Everything after "&" should be the name part
                name_part_after_amp = before_id[amp_pos+1:].strip()
                # Now find the comma in this name part
                comma_positions = [amp_pos + 1 + i for i, char in enumerate(name_part_after_amp) if char == ',']
                if comma_positions:
                    # Use the first comma after "&" as the name separator
                    comma_pos = comma_positions[0]
                else:
                    # No comma after "&" - this shouldn't happen, but fall back to original logic
                    comma_positions = [i for i, char in enumerate(before_id) if char == ',']
            else:
                # Fallback to original logic
                comma_positions = [i for i, char in enumerate(before_id) if char == ',']
        else:
            # No "&" - use original logic
            comma_positions = [i for i, char in enumerate(before_id) if char == ',']
        
        # Try each comma from the end backwards
        for comma_pos in reversed(comma_positions):
            # Get text after comma
            after_comma = before_id[comma_pos+1:].strip()
            # Check if it matches "First Name" pattern (1-2 words)
            # First name should be short (typically 1-2 words, max ~25 chars)
            first_name_match = re.match(r'^([A-Z][a-zA-Z\s\'-]{1,25})\s*$', after_comma)
            if first_name_match:
                # This looks like a valid first name - extract it
                first_name = first_name_match.group(1).strip()
                # Last name is everything before this comma
                last_name = before_id[:comma_pos].strip()
                
                # Verify names are reasonable
                first_words = first_name.split()
                last_words = last_name.split()
                
                # First name should be 1-2 words
                if len(first_words) <= 2 and len(first_name) <= 25:
                    # Clean up names first
                    last_name = ' '.join(last_name.split())
                    first_name = ' '.join(first_name.split())
                    
                    # Remove unusual punctuation
                    last_name = clean_name_punctuation(last_name)
                    first_name = clean_name_punctuation(first_name)
                    
                    # After cleaning, if last name still looks like it starts with location text
                    # (e.g., "MCLAREN VISITING NURSE & Zilska" -> "Zilska" after cleaning)
                    # we need to re-check the location extraction
                    # But actually, the cleaning should have removed the "&" already
                    # So if last name still contains multiple words, it might be location text
                    
                    # Final check: if last name has more than 3 words after cleaning,
                    # it might still contain location text
                    last_words_after_clean = last_name.split()
                    if len(last_words_after_clean) > 3:
                        # Take only the last 1-2 words as the actual last name
                        if len(last_words_after_clean) >= 2:
                            last_name = ' '.join(last_words_after_clean[-2:])
                        else:
                            last_name = last_words_after_clean[-1] if last_words_after_clean else last_name
                    
                    # Check if last name contains location keywords (common location words)
                    last_name_upper = last_name.upper()
                    location_keywords = ['COMMUNITY', 'HOSPITAL', 'CENTER', 'CARE', 'HEALTH', 'PARTNERS', 'HOME', 'ASSISTED', 'LIVING', 'NURSING', 'SUBMIT', 'UNDECIDED', 'FUNERAL']
                    
                    # If last name is suspiciously long (> 3 words) or contains location keywords,
                    # extract only the actual name part (typically last 1-2 words)
                    if len(last_words) > 3 or any(keyword in last_name_upper for keyword in location_keywords):
                        # Take the last 1-2 words as the actual last name
                        # Most last names are 1 word, some are 2 words (e.g., "Van Der Berg")
                        # Location text is usually before the actual name
                        if len(last_words) >= 2:
                            # Try last 2 words first (in case it's a compound name)
                            potential_last_name = ' '.join(last_words[-2:])
                            # If the second-to-last word looks like a location keyword, take only last word
                            if last_words[-2].upper() in location_keywords:
                                last_name = last_words[-1]
                            else:
                                last_name = potential_last_name
                        else:
                            last_name = last_words[-1] if last_words else last_name
                    
                    # Final cleanup
                    last_name = ' '.join(last_name.split())
                    first_name = ' '.join(first_name.split())
                    
                    # Remove unusual punctuation one more time (in case location removal added some)
                    last_name = clean_name_punctuation(last_name)
                    first_name = clean_name_punctuation(first_name)
                    
                    resident_id = id_match.group(1)
                    date_str = id_match.group(2)
                    
                    # Store original text before comma for location extraction
                    original_before_comma = before_id[:comma_pos].strip()
                    
                    return {
                        'last_name': last_name,
                        'first_name': first_name,
                        'resident_id': resident_id,
                        'date_str': date_str,
                        'original_before_comma': original_before_comma  # Original text before comma (for location extraction)
                    }
        
        # If we didn't find a valid pattern, return None
        return None
    
    # Pattern to match totals
    total_pattern = re.compile(r'^Total:\s*(\d+)', re.IGNORECASE)
    
    i = 0
    while i < len(section_lines):
        line = section_lines[i].strip()
        
        # Skip empty lines and section headers
        if not line or line == section_name or 'Type' in line and 'Location' in line and 'Resident' in line:
            i += 1
            continue
        
        # Special case: "Unknown" type (e.g., "Unknown Workman, Rodney (202745) 08/20/2025")
        if line.startswith('Unknown '):
            # Check if it's followed by a patient pattern
            unknown_remaining = line[8:].strip()  # Skip "Unknown "
            patient_info = extract_patient_from_line(unknown_remaining)
            if patient_info:
                # Extract patient info
                effective_date = normalize_date(patient_info['date_str'])
                
                # Clean punctuation from names
                last_name = clean_name_punctuation(patient_info['last_name'])
                first_name = clean_name_punctuation(patient_info['first_name'])
                
                entry = {
                    f'{key_prefix}type': 'Unknown',
                    f'{key_prefix}location': None,
                    'resident_name': f"{last_name}, {first_name}",
                    'resident_id': patient_info['resident_id'],
                    'effective_date': effective_date
                }
                entries.append(entry)
                current_block_entries.append(entry)
                current_type = 'Unknown'
                current_location = None
                i += 1
                continue
        
        # Check if line starts with a facility type
        matched_type = None
        for facility_type in facility_types:
            if line.startswith(facility_type):
                matched_type = facility_type
                break
        
        if matched_type:
            # New facility block - extract type and location
            current_type = matched_type
            # Get text after facility type
            remaining = line[len(matched_type):].strip()
            
            # Try to extract patient entry from same line
            patient_info = extract_patient_from_line(remaining)
            if patient_info:
                # Type and patient on same line - extract location from text before patient
                # Find where the actual last name starts in the original text
                original_before_comma = patient_info.get('original_before_comma', '')
                extracted_last_name = patient_info['last_name']
                
                # Special handling: if original_before_comma contains "&", it's likely a separator
                # between location and name (e.g., "MCLAREN VISITING NURSE & Zilska")
                if '&' in original_before_comma:
                    # Split on "&" - everything before "&" is location, everything after is name
                    parts = original_before_comma.rsplit('&', 1)
                    if len(parts) == 2:
                        location_text = parts[0].strip()
                        # Verify: the name part after "&" should match the extracted name (after cleaning)
                        name_part_after_amp = parts[1].strip()
                        # The extracted name might have had "&" removed by clean_name_punctuation
                        extracted_clean = extracted_last_name.lstrip('& ').strip()
                        if not name_part_after_amp.startswith(extracted_clean.split()[0] if extracted_clean.split() else ''):
                            # Name doesn't match - fallback to word-based extraction
                            words = original_before_comma.split()
                            if len(words) > 1:
                                location_text = ' '.join(words[:-1]).strip()
                            else:
                                location_text = ''
                elif original_before_comma.endswith(extracted_last_name):
                    # Last name is at the end, so location is everything before it
                    location_text = original_before_comma[:len(original_before_comma) - len(extracted_last_name)].strip()
                else:
                    # Find where last name starts (accounting for cleaned name)
                    # Try to find the cleaned name in the original
                    extracted_clean = extracted_last_name.lstrip('& ').strip()
                    last_name_first_word = extracted_clean.split()[0] if extracted_clean.split() else extracted_clean
                    name_start_pos = original_before_comma.rfind(last_name_first_word)
                    if name_start_pos != -1:
                        location_text = original_before_comma[:name_start_pos].strip()
                    else:
                        # Fallback: assume location is everything except last 1-2 words
                        words = original_before_comma.split()
                        if len(words) > 2:
                            location_text = ' '.join(words[:-2]).strip()
                        else:
                            location_text = ''
                
                # Clean up location text (remove common suffixes)
                for suffix in [' - BOM submit ticket to', ' not in list - BOM submit', ' - MERCY']:
                    if suffix in location_text:
                        location_text = location_text[:location_text.index(suffix)].strip()
                
                # If location contains " - ", split and take first part (location name)
                if ' - ' in location_text:
                    parts = location_text.split(' - ', 1)
                    current_location = parts[0].strip() if parts[0].strip() else None
                else:
                    current_location = location_text if location_text else None
                
                # Extract patient info - only the actual name parts
                effective_date = normalize_date(patient_info['date_str'])
                
                # Clean punctuation from names
                last_name = clean_name_punctuation(patient_info['last_name'])
                first_name = clean_name_punctuation(patient_info['first_name'])
                
                entry = {
                    f'{key_prefix}type': current_type,
                    f'{key_prefix}location': current_location,
                    'resident_name': f"{last_name}, {first_name}",
                    'resident_id': patient_info['resident_id'],
                    'effective_date': effective_date
                }
                entries.append(entry)
                current_block_entries.append(entry)
            else:
                # Type and location on same line, patient on next line(s)
                # Extract location (everything before common suffixes)
                location_text = remaining
                for suffix in [' - BOM submit ticket to', ' not in list - BOM submit', ' - MERCY']:
                    if suffix in location_text:
                        location_text = location_text[:location_text.index(suffix)].strip()
                        break
                
                # If location contains " - ", split and take first part
                if ' - ' in location_text:
                    parts = location_text.split(' - ', 1)
                    current_location = parts[0].strip() if parts[0].strip() else None
                else:
                    current_location = location_text.strip() if location_text.strip() else None
        
        # Check for patient entry (continuation row or standalone)
        # Only process if we didn't already process it as part of a facility type line
        if not matched_type:
            patient_info = extract_patient_from_line(line)
            if patient_info:
                effective_date = normalize_date(patient_info['date_str'])
                
                # Clean punctuation from names
                last_name = clean_name_punctuation(patient_info['last_name'])
                first_name = clean_name_punctuation(patient_info['first_name'])
                
                # Use current type/location if available, otherwise this is a standalone entry
                entry = {
                    f'{key_prefix}type': current_type if current_type else 'Unknown',
                    f'{key_prefix}location': current_location if current_location else None,
                    'resident_name': f"{last_name}, {first_name}",
                    'resident_id': patient_info['resident_id'],
                    'effective_date': effective_date
                }
                entries.append(entry)
                current_block_entries.append(entry)
        
        # Check for total
        total_match = total_pattern.search(line)
        if total_match:
            total_count = int(total_match.group(1))
            # Create subtotal for current block
            if current_type:
                subtotal = {
                    f'{key_prefix}type': current_type,
                    f'{key_prefix}location': current_location if current_location else None,
                    'count': total_count,
                    'source': 'detail'
                }
                subtotals.append(subtotal)
                
                # Verify count matches
                if len(current_block_entries) != total_count:
                    pass  # Could add warning here if needed
            
            # Reset for next block
            current_type = None
            current_location = None
            current_block_entries = []
        
        i += 1
    
    return {
        'name': section_name,
        'entries': entries,
        'subtotals': subtotals,
        'total': len(entries)
    }


def normalize_date(date_str):
    """
    Convert date from MM/DD/YYYY to YYYY-MM-DD.
    
    Args:
        date_str: Date string in format MM/DD/YYYY
    
    Returns:
        str: Date string in format YYYY-MM-DD
    """
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            month, day, year = parts
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    except:
        pass
    return date_str


def split_text_by_sections(text):
    """
    Split cleaned text into Admissions and Discharges sections.
    
    Args:
        text (str): Cleaned text from PDF
    
    Returns:
        tuple: (admissions_text, discharges_text, metadata_text)
    """
    lines = text.split('\n')
    
    # Find section boundaries
    admissions_start = None
    discharges_start = None
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        # Look for section headers (standalone lines)
        if stripped == 'Admissions' and admissions_start is None:
            admissions_start = i
        elif stripped == 'Discharges' and discharges_start is None:
            discharges_start = i
    
    # Extract metadata (everything before Admissions)
    metadata_lines = lines[:admissions_start] if admissions_start else []
    metadata_text = '\n'.join(metadata_lines)
    
    # Extract Admissions section
    if admissions_start is not None and discharges_start is not None:
        admissions_lines = lines[admissions_start:discharges_start]
        admissions_text = '\n'.join(admissions_lines)
    elif admissions_start is not None:
        admissions_lines = lines[admissions_start:]
        admissions_text = '\n'.join(admissions_lines)
    else:
        admissions_text = ""
    
    # Extract Discharges section
    if discharges_start is not None:
        discharges_lines = lines[discharges_start:]
        discharges_text = '\n'.join(discharges_lines)
    else:
        discharges_text = ""
    
    return metadata_text, admissions_text, discharges_text


def parse_single_section(text, section_name, metadata_text, api_key=None, model="gpt-4o-mini"):
    """
    Parse a single section (Admissions or Discharges) using GPT.
    
    Args:
        text (str): Text for the section to parse
        section_name (str): "Admissions" or "Discharges"
        metadata_text (str): Metadata text to include for context
        api_key (str): OpenAI API key
        model (str): Model to use
    
    Returns:
        dict: Parsed section data
    """
    if not api_key:
        print(f"Error: OpenAI API key is required for parsing {section_name}")
        return None
    
    if not text.strip():
        print(f"[WARNING]  Warning: {section_name} section is empty")
        return {
            'name': section_name,
            'entries': [],
            'subtotals': [],
            'total': None
        }
    
    # Combine metadata with section text for context
    full_text = f"{metadata_text}\n\n{text}"
    
    text_length = len(full_text)
    print(f"  {section_name} section length: {text_length:,} characters")
    
    # Estimate token count
    estimated_input_tokens = len(full_text) // 4
    
    # Set max output tokens based on model
    if model == "gpt-4o-mini":
        max_output_tokens = 16384
        max_input_tokens = 100000
    elif model in ["gpt-4o", "gpt-4-turbo"]:
        max_output_tokens = 16384
        max_input_tokens = 100000
    else:
        max_output_tokens = 16384
        max_input_tokens = 100000
    
    if estimated_input_tokens > max_input_tokens:
        print(f"  Warning: {section_name} section is very large (~{estimated_input_tokens:,} tokens)")
    
    # Set up OpenAI client
    client = openai.OpenAI(api_key=api_key)
    
    # Create section-specific prompt
    if section_name == "Admissions":
        key_prefix = "from_"
        expected_count = "~82"
        section_description = "Admissions section (entries coming FROM other facilities)"
    else:
        key_prefix = "to_"
        expected_count = "~33"
        section_description = "Discharges section (entries going TO other facilities)"
    
    # Count patient entries in this section
    # Use same pattern as parser: any 4-6 digit ID
    patient_entry_pattern = r'\(\d{4,6}\)'
    section_entry_count = sum(1 for line in text.split('\n') if re.search(patient_entry_pattern, line))
    
    prompt = f"""
You are a data extraction specialist. Extract ONLY the {section_description} from the following text and return a SINGLE valid JSON object conforming to the schema below. Return ONLY the JSON (no prose, no markdown, no comments).

CRITICAL - COMPLETENESS REQUIREMENT: You MUST extract EVERY single patient entry from the {section_name} section. This section contains approximately {section_entry_count} patient entry lines (lines matching "Last, First (ID) Date"). Your output MUST include ALL of them. Do NOT skip any entries, even if they appear to be duplicates or if totals don't match. Extract EVERY line that matches the resident pattern.

CRITICAL - COUNTING METHOD: 
1. Count ALL lines that match the resident pattern: "Last, First (ID) Date"
2. This section has approximately {section_entry_count} such lines - you MUST extract all of them
3. If a block says "Total: X", count the entries you extracted - you MUST have exactly X
4. Continuation rows ARE separate entries - each continuation row is ONE entry
5. Extract entries even if they appear multiple times (same patient, different dates) - each occurrence is a separate entry

CRITICAL CONTINUATION ROW RULE: When you see lines like:
  "Acute care hospital Blodgett Brown, Lawrence (202720) 07/22/2025"
  "Hernandez, Maria (202787) 08/21/2025"
  "Wohlfard, Scott (202798) 08/22/2025"
The second and third lines are CONTINUATION ROWS - they do NOT repeat the type/location. You MUST extract each as a separate entry, inheriting the type/location from the first line. ALL continuation rows must be extracted.

CRITICAL - DO NOT STOP EARLY: Process the ENTIRE {section_name} section from start to finish. Do not stop when you reach what looks like a summary or total. Continue extracting entries until you reach the end of the section.

############################
# JSON SCHEMA
############################
{{
  "name": "{section_name}",
  "entries": [
    {{
      "{key_prefix}type": "Facility Type",
      "{key_prefix}location": "Facility Location or null",
      "resident_name": "Last, First",
      "resident_id": "ID Number",
      "effective_date": "YYYY-MM-DD"
    }}
  ],
  "subtotals": [
    {{
      "{key_prefix}type": "Facility Type",
      "{key_prefix}location": "Facility Location or null",
      "count": 13,
      "source": "detail"
    }}
  ],
  "total": {expected_count}
}}

############################
# RULES
############################
- Use "{key_prefix}type" and "{key_prefix}location" keys
- Extract ALL patient entries from this section only
- Continuation rows inherit type/location from the previous block header
- Dates must be ISO 8601 (YYYY-MM-DD)
- Use null for unknown fields
- Preserve source capitalization
- Each "Total: X" line indicates a subtotal for the block above it

############################
# INPUT TEXT
############################
{full_text}

############################
# OUTPUT
############################
Return ONLY the JSON object for the {section_name} section.
"""
    
    try:
        print(f"  Sending {section_name} section to {model}...")
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a precise data extraction assistant. Return only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=max_output_tokens
        )
        
        json_text = response.choices[0].message.content.strip()
        finish_reason = response.choices[0].finish_reason
        
        # Remove markdown code blocks if present
        if json_text.startswith('```json'):
            json_text = json_text[7:]
        elif json_text.startswith('```'):
            json_text = json_text[3:]
        if json_text.endswith('```'):
            json_text = json_text[:-3]
        json_text = json_text.strip()
        
        print(f"  Response length: {len(json_text):,} characters")
        print(f"  Finish reason: {finish_reason}")
        
        if finish_reason == "length":
            print(f"  [WARNING]  WARNING: {section_name} response was truncated!")
        
        # Parse JSON
        try:
            section_data = json.loads(json_text)
            entry_count = len(section_data.get('entries', []))
            print(f"  [OK] Successfully parsed {section_name}: {entry_count} entries extracted")
            
            if finish_reason == "length":
                if 'validation' not in section_data:
                    section_data['validation'] = {'warnings': []}
                if 'warnings' not in section_data['validation']:
                    section_data['validation']['warnings'] = []
                section_data['validation']['warnings'].append(
                    f"Response was truncated. Some {section_name.lower()} entries may be missing."
                )
            
            return section_data
        except json.JSONDecodeError as e:
            print(f"  Error parsing {section_name} JSON: {e}")
            print(f"  Raw response (first 500 chars): {json_text[:500]}")
            return None
            
    except Exception as e:
        print(f"  Error calling {model} for {section_name}: {e}")
        return None


def combine_sections(admissions_data, discharges_data, metadata_text):
    """
    Combine Admissions and Discharges section data into a single structured JSON.
    
    Args:
        admissions_data (dict): Parsed Admissions section
        discharges_data (dict): Parsed Discharges section
        metadata_text (str): Metadata text for extracting report metadata
    
    Returns:
        dict: Combined structured JSON
    """
    # Extract metadata from metadata_text
    metadata = extract_metadata_from_text(metadata_text)
    
    # Build combined structure
    combined = {
        'report_metadata': metadata,
        'sections': []
    }
    
    # Add Admissions section
    if admissions_data:
        combined['sections'].append(admissions_data)
    
    # Add Discharges section
    if discharges_data:
        combined['sections'].append(discharges_data)
    
    # Add grand totals
    combined['grand_totals'] = {
        'admissions': admissions_data.get('total') if admissions_data else None,
        'discharges': discharges_data.get('total') if discharges_data else None
    }
    
    # Add validation
    combined['validation'] = {
        'notes': [],
        'warnings': []
    }
    
    # Collect warnings from sections
    if admissions_data and 'validation' in admissions_data:
        warnings = admissions_data['validation'].get('warnings', [])
        combined['validation']['warnings'].extend([f"Admissions: {w}" for w in warnings])
    
    if discharges_data and 'validation' in discharges_data:
        warnings = discharges_data['validation'].get('warnings', [])
        combined['validation']['warnings'].extend([f"Discharges: {w}" for w in warnings])
    
    return combined


def extract_metadata_from_text(metadata_text):
    """
    Extract report metadata from the metadata section.
    
    Args:
        metadata_text (str): Metadata text
    
    Returns:
        dict: Report metadata
    """
    lines = metadata_text.split('\n')
    metadata = {
        'facility': None,
        'title': None,
        'generated_date': None,
        'generated_time_et': None,
        'user': None,
        'admissions_range': {'from': None, 'to': None},
        'discharges_range': {'from': None, 'to': None},
        'page': {'number': None, 'of': None}
    }
    
    for line in lines:
        stripped = line.strip()
        if 'Medilodge' in stripped:
            metadata['facility'] = 'Medilodge at the Shore'
        elif stripped.startswith('Date:'):
            date_match = re.search(r'Date:\s*(\w+)\s+(\d+),\s+(\d{4})', stripped)
            if date_match:
                month, day, year = date_match.groups()
                metadata['generated_date'] = f"{year}-{month}-{day}"  # Will need proper month conversion
        elif stripped.startswith('Time:'):
            time_match = re.search(r'Time:\s*(\d{2}:\d{2}:\d{2})', stripped)
            if time_match:
                metadata['generated_time_et'] = time_match.group(1)
        elif 'Admissions' in stripped and 'Discharges' in stripped:
            # Extract date ranges and user
            adm_match = re.search(r'Admissions\s+(\d{1,2}/\d{1,2}/\d{4})\s+To\s+(\d{1,2}/\d{1,2}/\d{4})', stripped)
            dis_match = re.search(r'Discharges\s+(\d{1,2}/\d{1,2}/\d{4})\s+To\s+(\d{1,2}/\d{1,2}/\d{4})', stripped)
            user_match = re.search(r'User:\s*(.+)', stripped)
            
            if adm_match:
                metadata['admissions_range']['from'] = adm_match.group(1)
                metadata['admissions_range']['to'] = adm_match.group(2)
            if dis_match:
                metadata['discharges_range']['from'] = dis_match.group(1)
                metadata['discharges_range']['to'] = dis_match.group(2)
            if user_match:
                metadata['user'] = user_match.group(1).strip()
    
    return metadata


def clean_total_lines(text):
    """
    Keep all 'Total: {some number}' lines - they are useful for GPT to verify completeness.
    Originally this removed all but the last one, but totals help GPT validate it extracted
    the number of entries per block.
    
    Args:
        text (str): Raw text extracted from PDF
    
    Returns:
        str: Text with all totals preserved (no changes)
    """
    if not text:
        return ""
    
    # Keep all totals - they're useful for GPT validation
    # Check if there are totals to log
    total_pattern = r'^Total:\s*\d+.*$'
    lines = text.split('\n')
    total_lines = [i for i, line in enumerate(lines) if re.match(total_pattern, line.strip(), re.IGNORECASE)]
    
    if total_lines:
        print(f"Found {len(total_lines)} 'Total: X' lines - keeping all for GPT validation")
    else:
        print("No 'Total: X' lines found")
    
    # Return the original text unchanged (we're keeping all totals)
        return text


def parse_with_gpt4o_mini(text, api_key=None, model="gpt-4o-mini"):
    """
    Use GPT model to parse the extracted text into structured JSON.
    
    Args:
        text (str): Raw text extracted from PDF
        api_key (str): OpenAI API key
        model (str): Model to use. Options: "gpt-4o-mini" (max 16k output), "gpt-4o" (max 16k output), "gpt-4-turbo" (max 16k output)
    
    Returns:
        dict: Structured JSON data
    """
    if not api_key:
        print("Error: OpenAI API key is required")
        return None
    
    # Check if text is too large
    text_length = len(text)
    print(f"Text length: {text_length:,} characters")
    
    # Estimate token count (roughly 4 characters per token)
    estimated_input_tokens = len(text) // 4
    
    # Set max output tokens based on model
    if model == "gpt-4o-mini":
        max_output_tokens = 16384  # Maximum for gpt-4o-mini
        max_input_tokens = 100000  # Reserve ~28k tokens for output
    elif model in ["gpt-4o", "gpt-4-turbo"]:
        max_output_tokens = 16384  # Maximum for these models
        max_input_tokens = 100000
    else:
        max_output_tokens = 16384  # Default
        max_input_tokens = 100000
    
    if estimated_input_tokens > max_input_tokens:
        print(f"Warning: Text is very large (~{estimated_input_tokens:,} tokens). May need chunking.")
        print(f"Consider using a model with higher limits or processing in chunks.")
    
    # Set up OpenAI client
    client = openai.OpenAI(api_key=api_key)
    
    # Create the prompt for GPT-4o-mini
    prompt = f"""
You are a data extraction specialist. Convert the following Admission/Discharge report text into a SINGLE valid JSON object that strictly conforms to the schema below. Return ONLY the JSON (no prose, no markdown, no comments).

CRITICAL - COMPLETENESS REQUIREMENT: You MUST extract EVERY single patient entry from the document. The extracted text contains exactly 154 patient entry lines (lines matching "Last, First (ID) Date"). Your output MUST include ALL 154 of them. Do NOT skip any entries, even if they appear to be duplicates or if totals don't match. Extract EVERY line that matches the resident pattern.

CRITICAL - COUNTING METHOD: 
1. Count ALL lines that match the resident pattern: "Last, First (ID) Date"
2. The extracted text has EXACTLY 154 such lines - you MUST extract all 154
3. If a block says "Total: 20", count the entries you extracted - you MUST have exactly 20
4. Continuation rows ARE separate entries - each continuation row is ONE entry
5. If you extract fewer than 154 entries total, you are MISSING entries - go back and extract them
6. Extract entries even if they appear multiple times (same patient, different dates) - each occurrence is a separate entry

CRITICAL - DO NOT STOP EARLY: Process the ENTIRE document from start to finish. Do not stop when you reach what looks like a summary or total. Continue extracting entries until you reach the end of the document or until you have processed all pages.

CRITICAL CONTINUATION ROW RULE: When you see lines like:
  "Acute care hospital Blodgett Brown, Lawrence (202720) 07/22/2025"
  "Hernandez, Maria (202787) 08/21/2025"
  "Wohlfard, Scott (202798) 08/22/2025"
The second and third lines are CONTINUATION ROWS - they do NOT repeat the type/location. You MUST extract each as a separate entry, inheriting the type/location from the first line. ALL continuation rows must be extracted.

CRITICAL PAGE BREAK RULE: When you see page headers like "Page 2 of 5" or "Medilodge at the Shore" between patient entries, these are just metadata. The patient entries that follow STILL inherit the type/location from before the page break. For example:
  Line 58: "Acute care hospital Mercy Hospital Muskegon Linton, James (201480) 07/02/2025"
  Line 59: "Doss, Pamela (202682) 07/03/2025"  ← continuation
  Line 60-64: [Page header metadata]
  Line 65: "Smith, Harold (202504) 07/05/2025"  ← STILL a continuation, inherits same type/location
  Line 66-83: [More continuation rows]
You MUST continue tracking the type/location across page breaks. Page headers do NOT reset the continuation context.

CRITICAL - Another example of page break continuation:
  Line 172: "Private home/apt. with home health services Reynolds, Joyce (202630) 07/03/2025"
  Lines 173-177: [5 continuation rows]
  Lines 178-182: [Page header: "Page 4 of 5", etc.]
  Lines 183-192: [10 MORE continuation rows - these STILL inherit "Private home/apt. with home health services" with null location]
  Line 193: "Total: 20" ← This confirms all 20 entries belong to the same block
You MUST extract all 20 entries from this block, inheriting the same type/location throughout.

CRITICAL CHECKPOINT: After processing, verify:
- TOTAL entries extracted MUST equal 154 (the exact number of patient entry lines in the text)
- Block starting at line 172 should have 20 entries (6 before page break + 10 after page break + 4 more = 20 total)
- Block starting at line 58 should have 21 entries (2 before page break + 19 after page break = 21 total)
- Discharges section should have ~33 entries total
- Admissions section should have ~82 entries total
If your total is less than 154, you are MISSING entries - go back and extract them.

CRITICAL - EXTRACT ALL VISIBLE ENTRIES: Even if you can't find all entries that match a "Total: X", extract EVERY entry line you can see. If a block shows "Total: 20" but you only see 16 entries in the text, extract all 16 entries you can see. Do NOT skip entries because the count doesn't match - extract everything visible.

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
- CRITICAL: You MUST extract ALL patient entries. Count the entries carefully - if you see continuation rows (lines with just "Last, First (ID) Date"), they are separate entries that inherit the type/location from the previous line.
- CRITICAL: Verify completeness - check that the number of entries you extract matches the totals in the document. If totals say "Total: 13", you should extract 13 entries for that block. If totals say "Total: 20", you should extract exactly 20 entries for that block.
- CRITICAL: After extracting, COUNT your entries. If a block shows "Total: 20" and you only extracted 16 entries, you are MISSING 4 entries. Go back and find them - they are likely continuation rows that span page breaks.
- CRITICAL: The document should have approximately 82 admission entries and 33 discharge entries based on the totals. If you extract fewer, you are missing entries.
- IMPORTANT: The "Total: X" lines indicate how many entries belong to the block ABOVE them. Count carefully - if a block shows "Total: 20", you MUST extract all 20 entries, including continuation rows that span page breaks.
- CRITICAL EXAMPLE: If you see:
  Line 118: "Acute care hospital MERCY HEALTH PARTNERS - Davis, Dixie (202752) 08/05/2025"
  Lines 119-123: [Page header metadata]
  Line 124: "Total: 1"
  Then line 118 is a complete block with 1 entry.
  
  But if you see:
  Line 125: "Acute care hospital MERCY HEALTH PARTNERS - MERCY Oja-Tebbe, Nancy (201688) 07/01/2025"
  Lines 126-133: [8 continuation rows]
  Line 134: "Total: 9"
  Then you MUST extract all 9 entries (the one on line 125 + 8 continuation rows = 9 total).
  
  If a "Total: X" appears AFTER a page break, the entries BEFORE the page break that belong to the same block must be counted together with entries AFTER the page break.
- Page info: parse from patterns like "Page X of Y". Use X=1 and Y from the first page header if available.
- Date normalization:
  • "Date: Oct 16, 2025" → generated_date = 2025-10-16
  • Ranges like "Discharges 5/1/2025 To 7/31/2025" → discharges_range.from=2025-05-01, to=2025-07-31
  • If "No Admissions selected" appears, keep admissions_range values as null unless an explicit range is shown.

B) TABLE HEADERS & GROUPS
- Headers may appear as: "To Type  To Location  Resident  Effective Date" or "From Type  From Location  Resident  Effective Date".
- Rows are grouped by facility type/location blocks. A new block begins when a new Type (e.g., "Acute care hospital", "Funeral Home", "Private home/apt. ...", "Board and care/assisted living/group home", "Other Health Facility", etc.) appears.
- CRITICAL - Continuation rows: If a line contains ONLY a resident name pattern (e.g., "Hernandez, Maria (202787) 08/21/2025") WITHOUT repeating the type/location, this is a CONTINUATION ROW. You MUST inherit the most recent type and location from the current block.
  • Example block:
    "Acute care hospital Blodgett Brown, Lawrence (202720) 07/22/2025"
    "Hernandez, Maria (202787) 08/21/2025"  ← This is a continuation, inherits: type="Acute care hospital", location="Blodgett"
    "Wohlfard, Scott (202798) 08/22/2025"  ← This is also a continuation, inherits same type/location
- CRITICAL - Page breaks: Page headers (like "Page 2 of 5", "Medilodge at the Shore", etc.) are metadata and do NOT interrupt continuation rows. If you see patient entries both before and after a page header, they are part of the SAME block and continuation rows after the header still inherit the type/location from before the header.
- CRITICAL - Systematic processing: Process the document LINE BY LINE. For each line:
  1. If it matches the resident pattern ("Last, First (ID) Date"), extract it as an entry
  2. If it's a continuation row (no type/location), inherit from the most recent block
  3. If it's a page header, skip it but DO NOT reset your type/location tracking
  4. If it's "Total: X", verify you extracted X entries for that block
  5. Continue until you reach the end of the document
- IMPORTANT: Process EVERY line that matches the resident pattern, even if it appears to be a continuation row. Do NOT skip any entries.
- Subtotal lines: "Total: N" immediately following a block belong to that block. Use these totals to verify you extracted the correct number of entries. If you see "Total: 20" after a block, count the entries you extracted - you should have exactly 20 entries for that block.
- IMPORTANT: "Total: X" lines are VALIDATION CHECKS, not entries to extract. They tell you how many patient entries should be in the block above them. Count the actual patient entries (lines with "Name, First (ID) Date") and verify the count matches the total.

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
- After detecting a Type token at the start of a row (e.g., "Acute care hospital", "Funeral Home", "Other Health Facility", "Nursing home", "Home with Hospice", "Private home/apt. with home health services", "Private home/apt. with no home health services", "Board and care/assisted living/group home", "Other", "Unknown"), parse any LOCATION TEXT that appears BETWEEN the Type and the Resident.
- LOCATION is ALL text after the Type and BEFORE the resident pattern; trim it.
- If the resident pattern immediately follows the Type (i.e., there is NO intervening location), set location = null.
- SPECIAL CASE - "Unknown" prefix: If you see "Unknown Workman, Rodney (202745)" at the start of a line, "Unknown" is the type (to_type="Unknown"), location is null, and the resident name is "Workman, Rodney".
  • Example: "Unknown Workman, Rodney (202745) 08/20/2025" → to_type="Unknown"; to_location=null; resident_name="Workman, Rodney".
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
- "Unknown"
- "Psychiatric hospital, MR/DD facility"

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
        print(f"Sending text to {model} for parsing...")
        print(f"Using max_tokens={max_output_tokens} (maximum for {model})")
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a data extraction specialist that converts unstructured text into structured JSON format."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=max_output_tokens
        )
        
        # Check if response was truncated
        finish_reason = response.choices[0].finish_reason
        if finish_reason == "length":
            print("[WARNING]  WARNING: Response was truncated due to token limit!")
            print("[WARNING]  The JSON may be incomplete. Consider using a model with higher token limits or chunking the input.")
        
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
        
        # Log response info
        print(f"Response length: {len(json_text):,} characters")
        print(f"Finish reason: {finish_reason}")
        
        # Diagnostic: Check if response was truncated
        if finish_reason == "length":
            print("[WARNING]  WARNING: Response was truncated due to token limit!")
            print("[WARNING]  The JSON may be incomplete. Some patient entries may be missing.")
            print("[WARNING]  Consider:")
            print("    1. Using --save-text to verify all text was extracted")
            print("    2. Checking the extracted text file for completeness")
            print("    3. Using a different extraction method if PyPDF2 is missing pages")
        
        # Try to parse the JSON
        try:
            structured_data = json.loads(json_text)
            print(f"Successfully parsed JSON from {model}")
            
            # Diagnostic: Count entries extracted
            discharges_section = None
            admissions_section = None
            for section in structured_data.get('sections', []):
                if section.get('name') == 'Discharges':
                    discharges_section = section
                elif section.get('name') == 'Admissions':
                    admissions_section = section
            
            if discharges_section:
                discharge_count = len(discharges_section.get('entries', []))
                print(f"Extracted {discharge_count} discharge entries")
                # Expected ~33 based on totals in document
                if discharge_count < 30:
                    print(f"[WARNING]  WARNING: Only {discharge_count} discharge entries extracted. Expected ~33. Some entries may be missing.")
            if admissions_section:
                admission_count = len(admissions_section.get('entries', []))
                print(f"Extracted {admission_count} admission entries")
                # Expected ~82 based on document
                if admission_count < 80:
                    print(f"[WARNING]  WARNING: Only {admission_count} admission entries extracted. Expected ~82. Some entries may be missing.")
            
            total_extracted = (discharge_count if discharges_section else 0) + (admission_count if admissions_section else 0)
            print(f"Total entries extracted: {total_extracted} (target: 154)")
            if total_extracted < 154:
                missing = 154 - total_extracted
                print(f"[WARNING]  WARNING: Only {total_extracted} entries extracted. Missing {missing} entries (target: 154).")
                print(f"[WARNING]  The extracted text contains exactly 154 patient entry lines - all must be extracted.")
                
                # Additional diagnostics: Show where entries might be missing
                print(f"\n📋 Extraction Breakdown:")
                print(f"   - Discharges: {discharge_count if discharges_section else 0} (expected ~33)")
                print(f"   - Admissions: {admission_count if admissions_section else 0} (expected ~82)")
                print(f"   - Missing: {missing} entries")
                
                # Check if we can identify patterns in missing entries
                if missing > 0:
                    print(f"\n💡 Suggestions to find missing entries:")
                    print(f"   1. Check continuation rows - GPT may have missed some")
                    print(f"   2. Check entries near page breaks - context may have been lost")
                    print(f"   3. Check entries near 'Total: X' lines - GPT may have stopped early")
                    print(f"   4. Review the structured JSON file to see which entries are present")
            
            # Check if we might have missing data due to truncation
            if finish_reason == "length":
                # Add warning to validation
                if 'validation' not in structured_data:
                    structured_data['validation'] = {'notes': [], 'warnings': []}
                if 'warnings' not in structured_data['validation']:
                    structured_data['validation']['warnings'] = []
                structured_data['validation']['warnings'].append(
                    "Response was truncated due to token limit. Some patient entries may be missing. "
                    "Consider using a model with higher token limits or processing in chunks."
                )
                print("[WARNING]  Added truncation warning to validation section")
            
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
                        print(f"Successfully parsed fixed JSON from {model}")
                        
                        # Add truncation warning
                        if 'validation' not in structured_data:
                            structured_data['validation'] = {'notes': [], 'warnings': []}
                        if 'warnings' not in structured_data['validation']:
                            structured_data['validation']['warnings'] = []
                        structured_data['validation']['warnings'].append(
                            "Response was truncated and auto-fixed. Some patient entries may be missing."
                        )
                        
                        return structured_data
                except json.JSONDecodeError as fix_error:
                    print(f"Failed to fix truncated JSON: {fix_error}")
            
            return None
            
    except Exception as e:
        print(f"Error calling {model}: {e}")
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
        last_name = clean_name_punctuation(parts[0])
        first_name = clean_name_punctuation(parts[1])
    elif len(parts) == 1:
        # If no comma, assume it's just last name
        last_name = clean_name_punctuation(parts[0])
        first_name = ""
    else:
        last_name = ""
        first_name = ""
    
    return first_name, last_name


def clean_punctuation_from_names(data):
    """
    Clean punctuation marks from first and last names in patient data.
    
    Args:
        data (dict): Patient grouped data
    
    Returns:
        dict: Updated data with cleaned names
    """
    print("Cleaning punctuation from names...")
    
    cleaned_count = 0
    
    for patient_id, patient in data.get('patients', {}).items():
        original_first = patient.get('first_name', '').strip()
        original_last = patient.get('last_name', '').strip()
        
        # Clean both first and last names
        cleaned_first = clean_name_punctuation(original_first)
        cleaned_last = clean_name_punctuation(original_last)
        
        # Update if names changed
        if cleaned_first != original_first or cleaned_last != original_last:
            patient['first_name'] = cleaned_first
            patient['last_name'] = cleaned_last
            # Update full_name to match cleaned names
            if cleaned_first and cleaned_last:
                patient['full_name'] = f"{cleaned_last}, {cleaned_first}"
            elif cleaned_last:
                patient['full_name'] = cleaned_last
            else:
                patient['full_name'] = cleaned_first
            
            cleaned_count += 1
            if cleaned_first != original_first:
                print(f"Cleaned first name for patient {patient_id}: '{original_first}' -> '{cleaned_first}'")
            if cleaned_last != original_last:
                print(f"Cleaned last name for patient {patient_id}: '{original_last}' -> '{cleaned_last}'")
    
    print(f"Cleaned punctuation from {cleaned_count} patient name(s)")
    return data


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
        
        # Clean names first
        first_name = clean_name_punctuation(first_name)
        last_name = clean_name_punctuation(last_name)
        
        # If first_name is empty but last_name contains spaces, split it
        if not first_name and last_name and ' ' in last_name:
            # Split by space and clean up
            parts = [part.strip() for part in last_name.split(' ') if part.strip()]
            
            if len(parts) >= 2:
                # Clean each part
                cleaned_parts = [clean_name_punctuation(part) for part in parts]
                
                # Update the names - first part goes to first_name, all subsequent parts become last_name
                patient['first_name'] = cleaned_parts[0]  # First part becomes first name
                patient['last_name'] = ' '.join(cleaned_parts[1:])  # All remaining parts become last name
                patient['full_name'] = f"{' '.join(cleaned_parts[1:])}, {cleaned_parts[0]}"  # Update full name to "Last, First" format
                
                fixed_count += 1
                print(f"Fixed name for patient {patient_id}: '{last_name}' -> first='{cleaned_parts[0]}', last='{' '.join(cleaned_parts[1:])}'")
        else:
            # Update names even if not splitting (to ensure punctuation is cleaned)
            patient['first_name'] = first_name
            patient['last_name'] = last_name
            if first_name and last_name:
                patient['full_name'] = f"{last_name}, {first_name}"
            elif last_name:
                patient['full_name'] = last_name
            else:
                patient['full_name'] = first_name
    
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


def process_single_pdf(pdf_file, api_key, output_dir, save_json=False, save_text=False, model="gpt-4o-mini"):
    """
    Process a single PDF file through the complete pipeline.
    
    Args:
        pdf_file (str): Path to the PDF file
        api_key (str): OpenAI API key
        output_dir (str): Output directory
        save_json (bool): Whether to save intermediate JSON files
        save_text (bool): Whether to save extracted text file
        model (str): GPT model to use for parsing
    
    Returns:
        dict: Processing results with success status and statistics
    """
    print(f"\nProcessing: {pdf_file}")
    print("-" * 40)
    
    # Step 1: Extract text from PDF
    print("Step 1: Extracting text from PDF...")
    
    # Always save extracted text for diagnostics (unless explicitly disabled)
    text_file_path = os.path.join(output_dir, f"{Path(pdf_file).stem}_extracted_text.txt")
    if not save_text:
        print(f"Note: Extracted text will be saved to {text_file_path} for diagnostics")
    
    extracted_text = extract_text_from_pdf(pdf_file, text_file_path)
    
    if not extracted_text:
        return {
            'success': False,
            'error': 'Text extraction failed',
            'file': pdf_file
        }
    
    # Step 1.5: Clean the extracted text
    print("Step 1.5: Cleaning extracted text...")
    
    # Diagnostic: Check page markers BEFORE cleaning (they'll be removed)
    original_lines = extracted_text.split('\n')
    page_markers_original = []
    page_5_markers_original = []
    for i, line in enumerate(original_lines):
        if 'page' in line.lower():
            page_match = re.search(r'page\s*(\d+)\s*of\s*(\d+)', line.lower())
            if page_match:
                page_num = int(page_match.group(1))
                total_pages = int(page_match.group(2))
                page_markers_original.append((i+1, page_num, total_pages, line.strip()[:80]))
                if page_num >= 5:
                    page_5_markers_original.append((i+1, page_num, total_pages, line.strip()[:80]))
    
    print(f"Page markers found in original text: {len(page_markers_original)}")
    if page_markers_original:
        for line_num, page_num, total_pages, line_text in page_markers_original:
            print(f"   Line {line_num}: Page {page_num} of {total_pages} - '{line_text}'")
    
    # Count patient entries in original text
    # Use same pattern as parser: any 4-6 digit ID
    patient_entry_pattern = r'\(\d{4,6}\)'
    original_entry_count = sum(1 for line in original_lines if re.search(patient_entry_pattern, line))
    print(f"Patient entries in original text: {original_entry_count}")
    
    # Check for page 5+ content and show context
    if page_5_markers_original:
        print(f"Found {len(page_5_markers_original)} page 5+ markers in original text:")
        for line_num, page_num, total_pages, line_text in page_5_markers_original:
            print(f"   Line {line_num}: Page {page_num} of {total_pages}")
            # Show context around page 5 marker (5 lines before and after)
            start_idx = max(0, line_num - 6)
            end_idx = min(len(original_lines), line_num + 5)
            print(f"   Context around line {line_num}:")
            for ctx_line_num in range(start_idx, end_idx):
                marker = ">>> " if ctx_line_num == line_num - 1 else "    "
                ctx_line = original_lines[ctx_line_num].strip()[:100]
                if ctx_line:  # Only show non-empty lines
                    print(f"   {marker}Line {ctx_line_num + 1}: {ctx_line}")
    else:
        print("WARNING: No page 5+ markers found in original extracted text")
        print("   This suggests PyPDF2 may not have extracted page 5+ content")
        print("   Check the extracted text file to verify")
    
    # First remove page headers
    if not extracted_text:
        return {
            'success': False,
            'error': 'No text extracted from PDF',
            'file': pdf_file
        }
    
    cleaned_text = remove_page_headers(extracted_text)
    
    # Debug: Check if cleaned text is empty
    if not cleaned_text or len(cleaned_text.strip()) < 100:
        print(f"ERROR: remove_page_headers returned empty or very short text ({len(cleaned_text) if cleaned_text else 0} chars)")
        print(f"Original text had {len(extracted_text)} chars")
        # Try to see what happened
        original_lines = extracted_text.split('\n')
        cleaned_lines = cleaned_text.split('\n') if cleaned_text else []
        print(f"Original had {len(original_lines)} lines, cleaned has {len(cleaned_lines)} lines")
        if cleaned_lines:
            print(f"First 10 cleaned lines: {cleaned_lines[:10]}")
        return {
            'success': False,
            'error': 'Text cleaning resulted in empty text',
            'file': pdf_file
        }
    
    # Then clean totals (keep all for validation)
    cleaned_text = clean_total_lines(cleaned_text)
    
    # Safety check: ensure clean_total_lines didn't return None
    if cleaned_text is None:
        print(f"[WARNING]  WARNING: clean_total_lines returned None! Using cleaned text from remove_page_headers.")
        cleaned_text = remove_page_headers(extracted_text)  # Fallback to result from remove_page_headers
    
    if not cleaned_text:
        return {
            'success': False,
            'error': 'Text cleaning resulted in empty text',
            'file': pdf_file
        }
    
    # Count patient entries in cleaned text
    cleaned_lines = cleaned_text.split('\n')
    cleaned_entry_count = sum(1 for line in cleaned_lines if re.search(patient_entry_pattern, line))
    print(f"Patient entries in cleaned text: {cleaned_entry_count}")
    if cleaned_entry_count != original_entry_count:
        print(f"WARNING: Entry count changed after cleaning ({original_entry_count} -> {cleaned_entry_count})")
    
    # Diagnostic: Check if page 5+ content still exists after cleaning
    page_5_markers_cleaned = []
    for i, line in enumerate(cleaned_lines):
        if 'page' in line.lower():
            page_match = re.search(r'page\s*(\d+)', line.lower())
            if page_match:
                page_num = int(page_match.group(1))
                if page_num >= 5:
                    page_5_markers_cleaned.append((i+1, line.strip()[:100]))
    
    if page_5_markers_cleaned:
        print(f"Found {len(page_5_markers_cleaned)} references to page 5+ in cleaned text (should be 0)")
    else:
        print("Page headers removed (no page 5+ markers in cleaned text)")
    
    # Diagnostic: Show entries around page boundaries in cleaned text
    # This helps identify if entries are being lost during cleaning or parsing
    if page_5_markers_original:
        print(f"\nChecking entries around page 5 boundary in cleaned text:")
        # Find approximate location where page 5 content would be
        # Look for entries near the end of cleaned text (page 5 is last)
        cleaned_lines_with_indices = [(i+1, line) for i, line in enumerate(cleaned_lines) if line.strip()]
        # Get last 20 patient entries
        patient_entries_near_end = []
        for line_num, line in reversed(cleaned_lines_with_indices):
            if re.search(patient_entry_pattern, line):
                patient_entries_near_end.insert(0, (line_num, line.strip()[:100]))
                if len(patient_entries_near_end) >= 20:
                    break
        
        if patient_entries_near_end:
            print(f"   Last {len(patient_entries_near_end)} patient entries in cleaned text (should include page 5 entries):")
            for line_num, line_text in patient_entries_near_end:
                print(f"   Line {line_num}: {line_text}")
        else:
            print("   WARNING: No patient entries found near end of cleaned text!")
    
    # Step 2: Parse with Python (replacing GPT for more reliable extraction)
    print(f"\nStep 2: Parsing with Python parser...")
    
    # Extract metadata section
    metadata_text, _, _ = split_text_by_sections(cleaned_text)
    
    # Count entries in cleaned text
    admissions_entry_count = 0
    discharges_entry_count = 0
    in_admissions = False
    in_discharges = False
    
    for line in cleaned_lines:
        stripped = line.strip()
        if stripped == 'Admissions':
            in_admissions = True
            in_discharges = False
        elif stripped == 'Discharges':
            in_admissions = False
            in_discharges = True
        
        if re.search(patient_entry_pattern, line):
            if in_admissions:
                admissions_entry_count += 1
            elif in_discharges:
                discharges_entry_count += 1
    
    print(f"  Admissions section: {admissions_entry_count} patient entries")
    print(f"  Discharges section: {discharges_entry_count} patient entries")
    print(f"  Total: {admissions_entry_count + discharges_entry_count} entries")
    
    # Parse with Python
    structured_data = parse_with_python(cleaned_text, metadata_text)
    
    if not structured_data:
        return {
            'success': False,
            'error': 'Python parsing failed',
            'file': pdf_file
        }
    
    # Diagnostic: Count entries extracted
    total_extracted = 0
    admissions_section = None
    discharges_section = None
    
    for section in structured_data.get('sections', []):
        if section.get('name') == 'Admissions':
            admissions_section = section
            admission_count = len(section.get('entries', []))
            total_extracted += admission_count
            print(f"  OK: Admissions: {admission_count} entries extracted (expected {admissions_entry_count})")
        elif section.get('name') == 'Discharges':
            discharges_section = section
            discharge_count = len(section.get('entries', []))
            total_extracted += discharge_count
            print(f"  OK: Discharges: {discharge_count} entries extracted (expected {discharges_entry_count})")
    
    print(f"\n  Total entries extracted: {total_extracted} (target: {admissions_entry_count + discharges_entry_count})")
    if total_extracted < (admissions_entry_count + discharges_entry_count):
        missing = (admissions_entry_count + discharges_entry_count) - total_extracted
        print(f"  WARNING: Missing {missing} entries")
    elif total_extracted == (admissions_entry_count + discharges_entry_count):
        print(f"  Perfect! All entries extracted.")
    
    # Save structured JSON if requested
    if save_json:
        structured_json_file = os.path.join(output_dir, f"{Path(pdf_file).stem}_structured.json")
        with open(structured_json_file, 'w', encoding='utf-8') as f:
            json.dump(structured_data, f, indent=2, ensure_ascii=False)
        print(f"Saved structured JSON: {structured_json_file}")
    
    # Step 3: Group by patients
    print("Step 3: Grouping data by patients...")
    patient_grouped_data = create_patient_grouped_json(structured_data)
    
    # Step 3.5: Clean punctuation from names
    print("Step 3.5: Cleaning punctuation from names...")
    patient_grouped_data = clean_punctuation_from_names(patient_grouped_data)
    
    # Step 3.6: Fix empty first names
    print("Step 3.6: Fixing empty first names...")
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


def process_folder(folder_path, api_key, output_dir, save_json=False, save_text=False,
                   model="gpt-4o-mini", parallel=False, max_workers=3, save_summary=False):
    """
    Process all PDF files in a folder.
    
    Args:
        folder_path (str): Path to the folder containing PDF files
        api_key (str): OpenAI API key
        output_dir (str): Output directory
        save_json (bool): Whether to save intermediate JSON files
        save_text (bool): Whether to save extracted text files
        model (str): GPT model to use for parsing
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
            result = process_single_pdf(pdf_file, api_key, output_dir, save_json, save_text, model=model)
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
            
            result = process_single_pdf(pdf_file, api_key, output_dir, save_json, save_text, model=model)
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
    
    # Optionally save batch summary JSON
    if save_summary:
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
    parser.add_argument('--save-summary', action='store_true', help='Save batch_processing_summary.json (default: False)')
    parser.add_argument('--save-text', action='store_true', help='Save extracted text files')
    parser.add_argument('--output-dir', default='unified_output', help='Output directory (default: unified_output)')
    parser.add_argument('--model', default='gpt-4o-mini', 
                       choices=['gpt-4o-mini', 'gpt-4o', 'gpt-4-turbo'],
                       help='GPT model to use (default: gpt-4o-mini). Use gpt-4o or gpt-4-turbo for larger PDFs.')
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
    print(f"Model: {args.model}")
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
        
        batch_results = process_folder(
            args.input_path,
            api_key,
            output_dir,
            args.save_json,
            args.save_text,
            model=args.model,
            parallel=args.parallel,
            max_workers=args.max_workers,
            save_summary=args.save_summary
        )
        
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
        
        result = process_single_pdf(args.input_path, api_key, output_dir, args.save_json, args.save_text, model=args.model)
        
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
