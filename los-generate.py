#!/usr/bin/env python3
"""
Unified GPT OCR to CSV Converter
Combines PDF OCR extraction, name separation, and CSV conversion into one script
"""

import fitz  # PyMuPDF
import base64
import json
import csv
import sys
import os
import argparse
from pathlib import Path
from dotenv import load_dotenv
import openai
from io import BytesIO
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

def convert_pdf_to_images(pdf_path):
    """
    Convert PDF to images for GPT vision processing using PyMuPDF.
    
    Args:
        pdf_path (str): Path to the input PDF file
    
    Returns:
        list: List of PIL Image objects
    """
    try:
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        print(f"Converting PDF to images: {pdf_path}")
        
        # Open PDF with PyMuPDF
        doc = fitz.open(pdf_path)
        images = []
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            # Convert page to image with high DPI
            mat = fitz.Matrix(2.0, 2.0)  # 2x zoom for higher resolution
            pix = page.get_pixmap(matrix=mat)
            img_data = pix.tobytes("png")
            
            # Convert to PIL Image
            image = Image.open(BytesIO(img_data))
            images.append(image)
            print(f"Converted page {page_num + 1} to image")
        
        doc.close()
        print(f"Converted PDF to {len(images)} images")
        
        return images
        
    except Exception as e:
        print(f"Error converting PDF to images: {e}")
        return None

def encode_image_to_base64(image):
    """
    Encode PIL Image to base64 string for GPT API.
    
    Args:
        image: PIL Image object
    
    Returns:
        str: Base64 encoded image string
    """
    try:
        buffer = BytesIO()
        image.save(buffer, format='PNG')
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        return image_base64
    except Exception as e:
        print(f"Error encoding image to base64: {e}")
        return None

def extract_data_with_gpt_vision(image_base64, api_key):
    """
    Use GPT-4o-mini with vision to extract structured data from image.
    
    Args:
        image_base64 (str): Base64 encoded image
        api_key (str): OpenAI API key
    
    Returns:
        dict: Structured JSON data
    """
    if not api_key:
        print("Error: OpenAI API key is required")
        return None
    
    # Set up OpenAI client
    client = openai.OpenAI(api_key=api_key)
    
    # Create the prompt for GPT-4o-mini vision
    prompt = """
You are a medical data extraction specialist. Please analyze this scanned medical facility report image and extract the following information into a structured JSON format:

REQUIRED EXTRACTION:
1. Report Metadata:
   - Facility name
   - Report title/period (e.g., Q2 2024)
   - Generated date
   - Generated time
   - User name
   - Date ranges for admissions and discharges
   - Page information

2. Patient Information:
   For each patient entry, extract:
   - Patient name (MUST be in "Last, First" format - last name followed by comma, then first name)
   - Payer type (Medicare, Medicaid, Private Insurance, Self Pay, etc.)
   - Days (length of stay or duration)

3. Payer Summary:
   - Summary counts by payer type
   - Total counts for each category

IMPORTANT INSTRUCTIONS:
- Look carefully at the image for all text, even if it's small or in tables
- Extract ALL patient entries you can see
- Convert dates to YYYY-MM-DD format
- Convert times to HH:MM:SS format
- Be thorough - extract every piece of information you can see
- If you see tables or lists, extract all entries
- Pay attention to subtotals and summary sections
- CRITICAL: Patient names MUST be extracted in "Last, First" format (e.g., "Smith, John" not "John Smith")
- If you see names in "First Last" format, convert them to "Last, First" format

Please return ONLY the JSON data in this exact format:

{
  "report_metadata": {
    "facility_name": "Facility Name",
    "report_title": "Report Title/Period",
    "generated_date": "YYYY-MM-DD",
    "generated_time": "HH:MM:SS",
    "user": "User Name",
    "admissions_range": {
      "from": "YYYY-MM-DD",
      "to": "YYYY-MM-DD"
    },
    "discharges_range": {
      "from": "YYYY-MM-DD",
      "to": "YYYY-MM-DD"
    },
    "page_info": "Page X of Y"
  },
  "patients": [
    {
      "patient_name": "Last, First",
      "payer_type": "Payer Type",
      "days": "Number of days"
    }
  ],
  "payer_summary": {
    "Medicare": "count",
    "Medicaid": "count",
    "Private Insurance": "count",
    "Self Pay": "count"
  }
}

Extract ALL information you can see in the image. Be comprehensive and accurate.
"""

    try:
        print("Sending image to GPT-4o-mini for OCR analysis...")
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{image_base64}",
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            temperature=0.1,
            max_tokens=8000
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
            print("Successfully parsed JSON from GPT-4o-mini vision")
            return structured_data
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            print(f"Raw response (first 1000 chars): {json_text[:1000]}")
            return None
            
    except Exception as e:
        print(f"Error calling GPT-4o-mini vision: {e}")
        return None

def convert_patient_names_to_separate_fields(data):
    """
    Convert patient names from "Last, First" format to separate first_name and last_name fields.
    Assumes GPT has extracted names in "Last, First" format.
    
    Args:
        data (dict): Input data with combined patient names
    
    Returns:
        dict: Converted data with separate first and last names
    """
    try:
        # Create the new structure
        converted_data = {
            "report_metadata": data.get("report_metadata", {}),
            "patients": []
        }
        
        # Process each patient
        patients = data.get("patients", [])
        
        for patient in patients:
            patient_name = patient.get("patient_name", "")
            payer_type = patient.get("payer_type", "")
            days = patient.get("days", "")
            
            # Split the name - expecting "Last, First" format
            if "," in patient_name:
                # Format: "Last, First" - split on comma
                parts = patient_name.split(",", 1)
                last_name = parts[0].strip()  # Before comma
                first_name = parts[1].strip() if len(parts) > 1 else ""  # After comma
            else:
                # If no comma found, assume it's already in "First Last" format
                # This is a fallback case - GPT should have converted it
                name_parts = patient_name.strip().split()
                if len(name_parts) >= 2:
                    # Assume last word is last name, everything else is first name
                    last_name = name_parts[-1]
                    first_name = " ".join(name_parts[:-1])
                else:
                    # Single name - treat as first name
                    first_name = patient_name
                    last_name = ""
            
            # Create the converted patient record
            converted_patient = {
                "first_name": first_name,
                "last_name": last_name,
                "payer_type": payer_type,
                "days": days
            }
            
            converted_data["patients"].append(converted_patient)
        
        # Add payer summary
        converted_data["payer_summary"] = data.get("payer_summary", {})
        
        return converted_data
        
    except Exception as e:
        print(f"Error converting patient names: {e}")
        return None

def save_json_data(data, output_file):
    """
    Save data to JSON file.
    
    Args:
        data (dict): Data to save
        output_file (str): Output file path
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"JSON data saved to: {output_file}")
        return True
    except Exception as e:
        print(f"Error saving JSON file: {e}")
        return False

def normalize_payer_types(data):
    """
    Normalize payer types: keep 'Medicare A' as is, change all others to 'Managed Care'.
    
    Args:
        data (dict): Data with patient information
    
    Returns:
        dict: Data with normalized payer types
    """
    try:
        # Create a copy to avoid modifying the original
        normalized_data = {
            "report_metadata": data.get("report_metadata", {}),
            "patients": [],
            "payer_summary": data.get("payer_summary", {})
        }
        
        # Process each patient
        patients = data.get("patients", [])
        medicare_a_count = 0
        managed_care_count = 0
        
        for patient in patients:
            payer_type = patient.get('payer_type', '').strip()
            
            # Keep 'Medicare A' as is (case-insensitive), change all others to 'Managed Care'
            if payer_type.lower() == 'medicare a':
                normalized_payer_type = 'Medicare A'
                medicare_a_count += 1
            else:
                normalized_payer_type = 'Managed Care'
                managed_care_count += 1
            
            # Create normalized patient record
            normalized_patient = {
                "first_name": patient.get('first_name', ''),
                "last_name": patient.get('last_name', ''),
                "payer_type": normalized_payer_type,
                "days": patient.get('days', '')
            }
            
            normalized_data["patients"].append(normalized_patient)
        
        print(f"Normalized payer types: {medicare_a_count} 'Medicare A', {managed_care_count} changed to 'Managed Care'")
        
        return normalized_data
        
    except Exception as e:
        print(f"Error normalizing payer types: {e}")
        return data  # Return original data if normalization fails

def convert_to_csv(data, output_file):
    """
    Convert JSON data to CSV format.
    
    Args:
        data (dict): Data with separated first/last names
        output_file (str): Output CSV file path
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            # Define CSV headers
            fieldnames = ['first_name', 'last_name', 'payer_type', 'days']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write patient data
            patients = data.get("patients", [])
            for patient in patients:
                writer.writerow({
                    'first_name': patient.get('first_name', ''),
                    'last_name': patient.get('last_name', ''),
                    'payer_type': patient.get('payer_type', ''),
                    'days': patient.get('days', '')
                })
        
        print(f"CSV file created: {output_file}")
        print(f"Total records written: {len(patients)}")
        return True
        
    except Exception as e:
        print(f"Error converting to CSV: {e}")
        return False

def process_single_pdf(pdf_file, output_dir, save_json, api_key):
    """
    Process a single PDF file through the complete pipeline.
    
    Args:
        pdf_file (Path): Path to the PDF file
        output_dir (Path): Output directory
        save_json (bool): Whether to save intermediate JSON files
        api_key (str): OpenAI API key
    
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\n{'='*60}")
    print(f"Processing PDF: {pdf_file.name}")
    print(f"{'='*60}")
    
    # Step 1: Convert PDF to images
    images = convert_pdf_to_images(str(pdf_file))
    
    if not images:
        print(f"PDF to image conversion failed for {pdf_file.name}!")
        return False
    
    # Step 2: Process each page with GPT vision
    all_extracted_data = {
        "pages": [],
        "combined_data": {
            "report_metadata": {},
            "patients": [],
            "payer_summary": {}
        }
    }
    
    for page_num, image in enumerate(images, 1):
        print(f"\nProcessing page {page_num}...")
        
        # Encode image to base64
        image_base64 = encode_image_to_base64(image)
        
        if not image_base64:
            print(f"Failed to encode page {page_num}")
            continue
        
        # Extract data using GPT vision
        page_data = extract_data_with_gpt_vision(image_base64, api_key)
        
        if page_data:
            all_extracted_data["pages"].append({
                "page_number": page_num,
                "data": page_data
            })
            
            # Combine data from all pages
            if page_num == 1:  # Use first page metadata
                all_extracted_data["combined_data"]["report_metadata"] = page_data.get("report_metadata", {})
            
            # Combine patients from all pages
            if "patients" in page_data:
                all_extracted_data["combined_data"]["patients"].extend(page_data["patients"])
            
            # Combine payer summary
            if "payer_summary" in page_data:
                for payer, count in page_data["payer_summary"].items():
                    if payer in all_extracted_data["combined_data"]["payer_summary"]:
                        # Add counts if payer already exists
                        try:
                            all_extracted_data["combined_data"]["payer_summary"][payer] = str(
                                int(all_extracted_data["combined_data"]["payer_summary"][payer]) + int(count)
                            )
                        except ValueError:
                            all_extracted_data["combined_data"]["payer_summary"][payer] = count
                    else:
                        all_extracted_data["combined_data"]["payer_summary"][payer] = count
            
            print(f"Page {page_num} processed successfully")
            print(f"  - Patients found: {len(page_data.get('patients', []))}")
            print(f"  - Payer types: {len(page_data.get('payer_summary', {}))}")
        else:
            print(f"Failed to extract data from page {page_num}")
    
    # Step 3: Convert patient names to separate fields
    print("\nConverting patient names to separate fields...")
    converted_data = convert_patient_names_to_separate_fields(all_extracted_data["combined_data"])
    
    if not converted_data:
        print("Name conversion failed!")
        return False
    
    # Step 4: Save intermediate JSON files if requested
    if save_json:
        # Save raw extracted data
        raw_json_file = output_dir / f"{pdf_file.stem}_gpt_ocr_extracted.json"
        save_json_data(all_extracted_data, raw_json_file)
        
        # Save converted data with separated names
        converted_json_file = output_dir / f"{pdf_file.stem}_converted_names.json"
        save_json_data(converted_data, converted_json_file)
    
    # Step 4.5: Normalize payer types (keep 'Medicare A', change others to 'Managed Care')
    print("\nNormalizing payer types...")
    normalized_data = normalize_payer_types(converted_data)
    
    # Step 5: Generate CSV with facility name
    facility_name = normalized_data.get("report_metadata", {}).get("facility_name", "Unknown_Facility")
    clean_facility_name = facility_name.replace(" ", "_").replace(",", "").replace("-", "_")
    csv_file = output_dir / f"{clean_facility_name}.csv"
    
    success = convert_to_csv(normalized_data, csv_file)
    
    if success:
        print(f"\n=== CONVERSION COMPLETE ===")
        print(f"CSV file created: {csv_file}")
        print(f"Total patients: {len(normalized_data['patients'])}")
        
        # Print summary
        if normalized_data["report_metadata"]:
            metadata = normalized_data["report_metadata"]
            print(f"\nReport Metadata:")
            print(f"  Facility: {metadata.get('facility_name', 'N/A')}")
            print(f"  Title: {metadata.get('report_title', 'N/A')}")
            print(f"  Generated: {metadata.get('generated_date', 'N/A')} at {metadata.get('generated_time', 'N/A')}")
        
        if normalized_data["patients"]:
            print(f"\nSample Patients:")
            for i, patient in enumerate(normalized_data["patients"][:5]):
                print(f"  {i+1}. {patient.get('first_name', 'N/A')} {patient.get('last_name', 'N/A')} - {patient.get('payer_type', 'N/A')} - {patient.get('days', 'N/A')} days")
        
        if normalized_data.get("payer_summary"):
            print(f"\nPayer Summary:")
            for payer, count in normalized_data["payer_summary"].items():
                print(f"  {payer}: {count} days")
        
        return True
    else:
        print("CSV conversion failed!")
        return False

def main():
    """Main function to run the unified GPT OCR to CSV converter."""
    parser = argparse.ArgumentParser(description='Unified GPT OCR to CSV Converter')
    parser.add_argument('input_path', help='Input PDF file path or folder containing PDFs')
    parser.add_argument('--save-json', action='store_true', 
                       help='Save intermediate JSON files (default: False)')
    parser.add_argument('--output-dir', default='.', 
                       help='Output directory (default: current directory)')
    parser.add_argument('--parallel', action='store_true',
                       help='Process files in parallel (folder mode only)')
    parser.add_argument('--max-workers', type=int, default=3,
                       help='Maximum number of parallel workers (default: 3)')
    
    args = parser.parse_args()
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Check for OpenAI API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("Error: OPENAI_API_KEY not found in .env file")
        print("Please create a .env file with your OpenAI API key:")
        print("  OPENAI_API_KEY=your_api_key_here")
        sys.exit(1)
    
    input_path = Path(args.input_path)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Determine if input is a file or folder
    if input_path.is_file():
        # Single file processing
        if input_path.suffix.lower() != '.pdf':
            print(f"Error: {input_path} is not a PDF file")
            sys.exit(1)
        
        pdf_files = [input_path]
        print(f"Processing single PDF: {input_path}")
        
    elif input_path.is_dir():
        # Folder processing
        pdf_files = list(input_path.glob("*.pdf"))
        if not pdf_files:
            print(f"No PDF files found in directory: {input_path}")
            sys.exit(1)
        
        print(f"Processing folder: {input_path}")
        print(f"Found {len(pdf_files)} PDF files")
        
    else:
        print(f"Error: {input_path} is neither a file nor a directory")
        sys.exit(1)
    
    print(f"Output directory: {output_dir}")
    print(f"Save intermediate JSON: {args.save_json}")
    if len(pdf_files) > 1:
        print(f"Parallel processing: {'Enabled' if args.parallel else 'Disabled'}")
        if args.parallel:
            print(f"Max workers: {args.max_workers}")
    
    # Process each PDF file
    successful_files = 0
    failed_files = 0
    
    if args.parallel and len(pdf_files) > 1:
        # Parallel processing
        print(f"Processing {len(pdf_files)} files in parallel (max workers: {args.max_workers})")
        
        def process_with_index(pdf_file):
            """Wrapper to process file and return with index for ordering"""
            index = pdf_files.index(pdf_file) + 1
            print(f"\n{'='*80}")
            print(f"[Starting {index}/{len(pdf_files)}] Processing file: {pdf_file.name}")
            print(f"{'='*80}")
            success = process_single_pdf(pdf_file, output_dir, args.save_json, api_key)
            return {
                'success': success,
                'file': pdf_file,
                'index': index
            }
        
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
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
                    print(f"[FAILED] {pdf_file.name} generated an exception: {exc}")
                    completed_results.append({
                        'success': False,
                        'file': pdf_file,
                        'index': pdf_files.index(pdf_file) + 1
                    })
            
            # Sort results by original index to maintain order in output
            completed_results.sort(key=lambda x: x.get('index', 999))
            
            # Process results
            for result in completed_results:
                if result['success']:
                    successful_files += 1
                    print(f"[OK] Successfully processed: {result['file'].name}")
                else:
                    failed_files += 1
                    print(f"[FAILED] Failed to process: {result['file'].name}")
    else:
        # Sequential processing (original behavior)
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"\n{'='*80}")
            print(f"Processing file {i}/{len(pdf_files)}: {pdf_file.name}")
            print(f"{'='*80}")
            
            success = process_single_pdf(pdf_file, output_dir, args.save_json, api_key)
            
            if success:
                successful_files += 1
            else:
                failed_files += 1
    
    # Final summary
    print(f"\n{'='*80}")
    print(f"BATCH PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"Total files processed: {len(pdf_files)}")
    print(f"Successful: {successful_files}")
    print(f"Failed: {failed_files}")
    
    if failed_files > 0:
        print(f"\nSome files failed to process. Check the output above for details.")
        sys.exit(1)
    else:
        print(f"\nAll files processed successfully!")

if __name__ == "__main__":
    main()
