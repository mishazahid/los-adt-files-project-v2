"""
Pipeline Service - Orchestrates the entire processing pipeline
"""

import subprocess
import os
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import asyncio
import logging
import concurrent.futures

from backend.services.google_sheets import GoogleSheetsService
from backend.services.google_slides import GoogleSlidesService
from backend.services.google_apps_script import GoogleAppsScriptService
from backend.config import job_status

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PipelineService:
    """Service to orchestrate the full processing pipeline"""
    
    # Configuration for parallel processing
    MAX_PARALLEL_WORKERS = 3  # Maximum number of files to process in parallel
    
    def __init__(self):
        self.sheets_service = GoogleSheetsService()
        self.slides_service = GoogleSlidesService()
        self.apps_script_service = GoogleAppsScriptService()
        
        # Get script paths (assuming they're in the project root)
        self.project_root = Path(__file__).parent.parent.parent
        self.unified_script = self.project_root / "unified_pdf_to_csv_test.py"
        self.los_script = self.project_root / "los-generate.py"
        self.combiner_script = self.project_root / "csv_combiner-test.py"
        self.summary_script = self.project_root / "summary_combiner.py"
        
        # Thread pool executor for running scripts (Windows compatible)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)

    def _set_progress(self, job_id: str, progress: int, message: str = None):
        """
        Safely update job progress and optional message.
        """
        try:
            if job_id in job_status:
                job_status[job_id]["progress"] = progress
                if message:
                    job_status[job_id]["message"] = message
        except Exception:
            # Avoid breaking pipeline on progress update issues
            pass
    
    async def run_pipeline(self, job_id: str, job_dir: str) -> Dict[str, Any]:
        """
        Run the complete processing pipeline
        
        Steps:
        1-2. Run ADT and LOS processing in parallel (unified_pdf_to_csv_test.py, los-generate.py)
        3. Run combiner (csv_combiner-test.py)
        4. Run summary (summary_combiner.py)
        5. Update Google Sheets
        6. Generate Google Slides report
        """
        log_file = Path("logs") / f"{job_id}.log"
        log_file.parent.mkdir(exist_ok=True)
        
        results = {
            "job_id": job_id,
            "steps_completed": [],
            "outputs": {},
            "links": {},
            "errors": []
        }
        
        # Check if comparison mode is enabled for this job
        comparison_mode = job_status.get(job_id, {}).get("comparison_mode", False)
        if comparison_mode:
            await self._log(log_file, f"[{datetime.now()}] Comparison mode ENABLED (Puzzle vs Non-Puzzle)")

        try:
            # Step 1 & 2: Process ADT and LOS files in parallel
            adt_output_dir = Path("outputs") / job_id / "ADT-csv"
            adt_output_dir.mkdir(parents=True, exist_ok=True)
            los_output_dir = Path("outputs") / job_id / "LOS-csv"
            los_output_dir.mkdir(parents=True, exist_ok=True)
            
            adt_pdfs_dir = Path(job_dir) / "ADT"
            los_pdfs_dir = Path(job_dir) / "LOS"
            
            has_adt = adt_pdfs_dir.exists() and list(adt_pdfs_dir.glob("*.pdf"))
            has_los = los_pdfs_dir.exists() and list(los_pdfs_dir.glob("*.pdf"))
            
            async def process_adt():
                """Process ADT files"""
                await self._log(log_file, f"[{datetime.now()}] Step 1: Processing ADT files...")
                if has_adt:
                    await self._run_script(
                        self.unified_script,
                        [str(adt_pdfs_dir), "--output-dir", str(adt_output_dir), "--folder", "--parallel", "--max-workers", str(self.MAX_PARALLEL_WORKERS)],
                        log_file
                    )
                    results["outputs"]["adt_csv"] = str(adt_output_dir)
                    results["steps_completed"].append("adt_processing")
                    await self._log(log_file, f"[{datetime.now()}] [OK] ADT processing complete")
                return "adt_done"
            
            async def process_los():
                """Process LOS files"""
                await self._log(log_file, f"[{datetime.now()}] Step 2: Processing LOS files...")
                if has_los:
                    await self._run_script(
                        self.los_script,
                        [str(los_pdfs_dir), "--output-dir", str(los_output_dir), "--parallel", "--max-workers", str(self.MAX_PARALLEL_WORKERS)],
                        log_file
                    )
                    results["outputs"]["los_csv"] = str(los_output_dir)
                    results["steps_completed"].append("los_processing")
                    await self._log(log_file, f"[{datetime.now()}] [OK] LOS processing complete")
                return "los_done"
            
            # Run ADT and LOS processing in parallel
            if has_adt and has_los:
                await self._log(log_file, f"[{datetime.now()}] Processing ADT and LOS files in parallel...")
                await asyncio.gather(process_adt(), process_los())
            elif has_adt:
                await process_adt()
            elif has_los:
                await process_los()
            
            # Step 3: Combine data
            await self._log(log_file, f"[{datetime.now()}] Step 3: Combining data...")
            combined_dir = Path("outputs") / job_id / "combined"
            combined_dir.mkdir(parents=True, exist_ok=True)
            
            visits_dir = Path(job_dir) / "VISITS"
            
            # Normalize filenames first (as done in the notebook)
            await self._normalize_filenames(adt_output_dir, log_file)
            await self._normalize_filenames(los_output_dir, log_file)
            
            combiner_args = [
                "--folders",
                str(adt_output_dir),
                str(los_output_dir),
                str(visits_dir),
                str(combined_dir)
            ]
            if comparison_mode:
                combiner_args.append("--comparison-mode")

            await self._run_script(
                self.combiner_script,
                combiner_args,
                log_file
            )
            results["outputs"]["combined"] = str(combined_dir)
            results["steps_completed"].append("combining")
            await self._log(log_file, f"[{datetime.now()}] [OK] Data combination complete")
            self._set_progress(job_id, 40, "Data combined")
            
            # Normalize combined files
            await self._normalize_filenames(combined_dir, log_file)
            
            # Step 4: Generate summaries
            await self._log(log_file, f"[{datetime.now()}] Step 4: Generating summaries...")
            summary_dir = Path("outputs") / job_id / "summary"
            summary_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate all_patients.csv
            await self._run_script(
                self.summary_script,
                [
                    str(combined_dir),
                    "--all-patients",
                    str(summary_dir / "all_patients.csv"),
                    "--add-metrics"
                ],
                log_file
            )
            
            # Generate master_summary.csv
            await self._run_script(
                self.summary_script,
                [
                    str(combined_dir),
                    str(summary_dir / "master_summary.csv"),
                    "--add-metrics"
                ],
                log_file
            )
            
            results["outputs"]["summary"] = str(summary_dir)
            results["steps_completed"].append("summary")
            await self._log(log_file, f"[{datetime.now()}] [OK] Summary generation complete")
            self._set_progress(job_id, 60, "Summary generated")
            
            # Step 5: Update Google Sheets
            await self._log(log_file, f"[{datetime.now()}] Step 5: Updating Google Sheets...")
            master_summary_path = summary_dir / "master_summary.csv"
            if master_summary_path.exists():
                # Get facility values from job status if available (for quarter)
                from backend.config import job_status
                manual_facility_values = job_status.get(job_id, {}).get("facility_values", {})
                
                # Auto-fetch GS, PPS, INC from Google Sheet
                await self._log(log_file, f"[{datetime.now()}] Step 5.1: Auto-fetching GS, PPS, INC from Google Sheet...")
                try:
                    # Read facility names from master_summary.csv
                    import pandas as pd
                    # Keep ratio columns as strings to prevent automatic time conversion
                    ratio_columns = ['HD', 'HDN', 'HT', 'Ex', 'Cus', 'AL', 'OT', 'SNF', 'Managed Care Ratio', 'Medicare A Ratio']
                    dtype_dict = {col: str for col in ratio_columns}
                    df = pd.read_csv(master_summary_path, dtype=dtype_dict)
                    facility_names = df['Facility'].unique().tolist() if 'Facility' in df.columns else []
                    
                    if facility_names:
                        # Get Google Sheet ID or file from job status (user-provided or default)
                        from backend.config import job_status
                        job_data = job_status.get(job_id, {})
                        user_sheet_id = job_data.get("google_sheet_id")
                        google_sheet_file = job_data.get("google_sheet_file")
                        
                        await self._log(log_file, f"[{datetime.now()}] Google Sheet source - ID: {user_sheet_id}, File: {google_sheet_file}")
                        
                        # Fetch metrics from Google Sheet or uploaded file
                        # Determine puzzle_patient_names_file path for comparison mode
                        puzzle_patient_names_file = None
                        if comparison_mode:
                            ppn_path = combined_dir / "puzzle_patient_names.json"
                            if ppn_path.exists():
                                puzzle_patient_names_file = str(ppn_path)
                                await self._log(log_file, f"[{datetime.now()}] Found puzzle_patient_names.json: {puzzle_patient_names_file}")
                            else:
                                await self._log(log_file, f"[{datetime.now()}] [WARNING] puzzle_patient_names.json not found at {ppn_path}")

                        if google_sheet_file:
                            # Read from uploaded file
                            await self._log(log_file, f"[{datetime.now()}] Reading metrics from uploaded file: {google_sheet_file}")
                            auto_metrics = self.sheets_service.fetch_facility_metrics_from_file(
                                facility_names,
                                file_path=google_sheet_file,
                                los_csv_dir=str(los_output_dir),
                                comparison_mode=comparison_mode,
                                puzzle_patient_names_file=puzzle_patient_names_file
                            )
                        elif user_sheet_id:
                            # Fetch from Google Sheet using provided ID
                            await self._log(log_file, f"[{datetime.now()}] Reading metrics from Google Sheet ID: {user_sheet_id}")
                            auto_metrics = self.sheets_service.fetch_facility_metrics(facility_names, sheet_id=user_sheet_id)
                        else:
                            # Use default from env var
                            await self._log(log_file, f"[{datetime.now()}] Using default Google Sheet from env var")
                            auto_metrics = self.sheets_service.fetch_facility_metrics(facility_names, sheet_id=None)
                        
                        # Combine auto-fetched metrics with manual quarter value
                        facility_values = {}
                        for facility_name in facility_names:
                            if facility_name in auto_metrics:
                                facility_values[facility_name] = auto_metrics[facility_name]
                                metrics = auto_metrics[facility_name]
                                await self._log(log_file, f"[{datetime.now()}] Auto-fetched for {facility_name}: GS={metrics.get('GS', 'N/A')}, PPS={metrics.get('PPS', 'N/A')}, INC={metrics.get('INC', 'N/A')}, GG_Gain_MC={metrics.get('GG_Gain_MC', 'N/A')}, GG_Gain_MA={metrics.get('GG_Gain_MA', 'N/A')}, GG_Gain_Overall={metrics.get('GG_Gain_Overall', 'N/A')}")
                                if comparison_mode:
                                    np_keys = [k for k in metrics.keys() if k.startswith('NP_')]
                                    if np_keys:
                                        np_info = ", ".join(f"{k}={metrics[k]}" for k in np_keys)
                                        await self._log(log_file, f"[{datetime.now()}]   NP_ metrics for {facility_name}: {np_info}")
                            else:
                                await self._log(log_file, f"[{datetime.now()}] [WARNING] Could not fetch metrics for {facility_name}")
                        
                        # Add quarter from manual input if provided
                        if manual_facility_values and '_quarter' in manual_facility_values:
                            facility_values['_quarter'] = manual_facility_values['_quarter']
                            await self._log(log_file, f"[{datetime.now()}] Using manual quarter value: {manual_facility_values['_quarter']}")
                    else:
                        await self._log(log_file, f"[{datetime.now()}] [WARNING] No facilities found in master_summary.csv, using manual values if available")
                        facility_values = manual_facility_values
                        
                except Exception as e:
                    await self._log(log_file, f"[{datetime.now()}] [WARNING] Error auto-fetching metrics: {e}, falling back to manual values")
                    facility_values = manual_facility_values
                
                sheets_links = await self.sheets_service.update_sheets(master_summary_path, facility_values)
                # Handle both old format (string) and new format (dict)
                if isinstance(sheets_links, dict):
                    results["links"]["google_sheets"] = sheets_links.get("facility_summary", "")
                    results["links"]["test_fac_sheets"] = sheets_links.get("test_fac", "")
                else:
                    # Backward compatibility
                    results["links"]["google_sheets"] = sheets_links
                    results["links"]["test_fac_sheets"] = ""
                results["steps_completed"].append("sheets_update")
                await self._log(log_file, f"[{datetime.now()}] [OK] Google Sheets updated")
                self._set_progress(job_id, 75, "Sheets updated")
                
                # Wait a few seconds for Test sheet formulas to calculate
                await self._log(log_file, f"[{datetime.now()}] Waiting for Test sheet formulas to calculate...")
                await asyncio.sleep(5)  # Wait 5 seconds for formulas
                
                # Step 5.4: Copy data from Raw_Data to Facility_Data tab
                await self._log(log_file, f"[{datetime.now()}] Step 5.4: Copying data from Raw_Data to Facility_Data tab...")
                copy_success = await self.sheets_service.copy_raw_data_to_facility_data(comparison_mode=comparison_mode)
                if copy_success:
                    await self._log(log_file, f"[{datetime.now()}] [OK] Data copied to Facility_Data tab")
                    results["steps_completed"].append("facility_data_update")
                    self._set_progress(job_id, 80, "Facility Data updated")
                else:
                    await self._log(log_file, f"[{datetime.now()}] [WARNING] Failed to copy data to Facility_Data tab")
                
                # Step 5.5: Generate Test Fac PDF using Apps Script
                await self._log(log_file, f"[{datetime.now()}] Step 5.5: Generating Test Fac PDF via Apps Script...")
                test_fac_pdf_result = await self.apps_script_service.generate_test_fac_pdf()
                if test_fac_pdf_result.get("success"):
                    results["steps_completed"].append("test_fac_pdf_generation")
                    result_data = test_fac_pdf_result.get("result", {})
                    test_fac_pdf_link = None
                    
                    await self._log(log_file, f"[{datetime.now()}] Test Fac Apps Script response: {json.dumps(result_data, indent=2)}")
                    
                    if isinstance(result_data, dict):
                        test_fac_pdf_link = (
                            result_data.get("pdf_link") or 
                            result_data.get("pdfLink") or 
                            result_data.get("url") or
                            result_data.get("fileUrl") or
                            result_data.get("pdf_url") or
                            result_data.get("pdfUrl")
                        )
                    
                    if test_fac_pdf_link:
                        results["links"]["test_fac_pdf"] = test_fac_pdf_link
                        await self._log(log_file, f"[{datetime.now()}] [OK] Test Fac PDF generated: {test_fac_pdf_link}")
                        self._set_progress(job_id, 85, "Test Fac PDF generated")
                    else:
                        # Fallback to Drive folder link
                        results["links"]["test_fac_pdf"] = "https://drive.google.com/drive/folders/1DOThKA_GrOHzDZomzjOxnYfzCjVNWCql"
                        await self._log(log_file, f"[{datetime.now()}] [OK] Test Fac PDF generated via Apps Script (using default Drive folder link)")
                        self._set_progress(job_id, 85, "Test Fac PDF generated")
                else:
                    await self._log(log_file, f"[{datetime.now()}] [WARNING] Test Fac PDF generation failed: {test_fac_pdf_result.get('error', 'Unknown error')}")
            
            # Step 6: Generate Facility Summary PDF using Apps Script
            await self._log(log_file, f"[{datetime.now()}] Step 6: Generating Facility Summary PDF via Apps Script...")
            pdf_result = await self.apps_script_service.generate_pdf()
            if pdf_result.get("success"):
                results["steps_completed"].append("pdf_generation")
                # Extract PDF link from result if available
                result_data = pdf_result.get("result", {})
                pdf_link = None
                
                # Log the full response for debugging
                await self._log(log_file, f"[{datetime.now()}] Apps Script response: {json.dumps(result_data, indent=2)}")
                
                # Check various possible fields for PDF link
                if isinstance(result_data, dict):
                    pdf_link = (
                        result_data.get("pdf_link") or 
                        result_data.get("pdfLink") or 
                        result_data.get("url") or
                        result_data.get("fileUrl") or
                        result_data.get("pdf_url") or
                        result_data.get("pdfUrl")
                    )
                    
                    # If we have a file ID, construct the Drive link
                    if not pdf_link:
                        file_id = (
                            result_data.get("file_id") or 
                            result_data.get("fileId") or 
                            result_data.get("id") or
                            result_data.get("fileID")
                        )
                        if file_id:
                            pdf_link = f"https://drive.google.com/file/d/{file_id}/view"
                
                if pdf_link:
                    results["links"]["generated_pdf"] = pdf_link
                    await self._log(log_file, f"[{datetime.now()}] [OK] PDF generated via Apps Script: {pdf_link}")
                    self._set_progress(job_id, 95, "Facility Summary PDF generated")
                else:
                    # Use the default Google Drive folder link as fallback
                    default_drive_folder = "https://drive.google.com/drive/folders/1DOThKA_GrOHzDZomzjOxnYfzCjVNWCql?usp=drive_link"
                    results["links"]["generated_pdf"] = default_drive_folder
                    await self._log(log_file, f"[{datetime.now()}] [OK] PDF generated via Apps Script (using default Drive folder link)")
                    await self._log(log_file, f"[{datetime.now()}] [DEBUG] Response keys: {list(result_data.keys()) if isinstance(result_data, dict) else 'Not a dict'}")
            else:
                error_msg = pdf_result.get("error", "Unknown error")
                await self._log(log_file, f"[{datetime.now()}] [WARNING] PDF generation failed: {error_msg}")
                results["errors"].append(f"PDF generation: {error_msg}")
                # Continue without PDF - don't fail the entire pipeline
            
            # Step 7: Generate Google Slides report
            await self._log(log_file, f"[{datetime.now()}] Step 7: Generating Google Slides report...")
            slides_id = await self.slides_service.create_report(
                job_id,
                master_summary_path if master_summary_path.exists() else None,
                summary_dir / "all_patients.csv" if (summary_dir / "all_patients.csv").exists() else None
            )
            
            if slides_id:
                results["links"]["google_slides"] = f"https://docs.google.com/presentation/d/{slides_id}"
                results["steps_completed"].append("slides_creation")
                await self._log(log_file, f"[{datetime.now()}] [OK] Google Slides report created (ID: {slides_id})")
            else:
                error_msg = "Google Slides report creation failed - no presentation ID returned"
                await self._log(log_file, f"[{datetime.now()}] [WARNING] {error_msg}")
                results["errors"].append(error_msg)
                # Continue without slides - don't fail the entire pipeline
            
            await self._log(log_file, f"[{datetime.now()}] ===== PIPELINE COMPLETE =====")
            
        except Exception as e:
            import traceback
            error_msg = str(e) if str(e) else repr(e)
            error_traceback = traceback.format_exc()
            
            # Log detailed error
            await self._log(log_file, f"\n{'='*60}")
            await self._log(log_file, f"[{datetime.now()}] ERROR OCCURRED!")
            await self._log(log_file, f"Error Type: {type(e).__name__}")
            await self._log(log_file, f"Error Message: {error_msg}")
            await self._log(log_file, f"{'='*60}")
            await self._log(log_file, "Full Traceback:")
            # Write traceback line by line for better visibility
            for line in error_traceback.split('\n'):
                await self._log(log_file, line)
            await self._log(log_file, f"{'='*60}\n")
            
            results["errors"].append(error_msg)
            results["error_traceback"] = error_traceback
            logger.error(f"Pipeline error for job {job_id}: {error_msg}", exc_info=True)
            raise
        
        return results
    
    async def _run_script(self, script_path: Path, args: list, log_file: Path):
        """Run a Python script and log output - Windows compatible"""
        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script_path}")
        
        # Use absolute path to Python executable
        python_exe = Path(sys.executable).resolve()
        script_path_abs = script_path.resolve()
        
        await self._log(log_file, f"Python executable: {python_exe}")
        
        cmd = [str(python_exe), str(script_path_abs)] + [str(arg) if isinstance(arg, Path) else str(arg) for arg in args]
        
        await self._log(log_file, f"Running command: {' '.join(cmd)}")
        await self._log(log_file, f"Working directory: {self.project_root}")
        
        def run_script_sync():
            """Run script synchronously - Windows compatible"""
            try:
                # Prepare environment with current environment variables
                env = os.environ.copy()
                
                # Ensure PATH includes venv Scripts directory for DLL dependencies
                venv_scripts = self.project_root / "venv" / "Scripts"
                if venv_scripts.exists():
                    current_path = env.get('PATH', '')
                    venv_path = str(venv_scripts)
                    if venv_path not in current_path:
                        env['PATH'] = f"{venv_path};{current_path}"
                
                # On Windows, use shell=True to help with DLL loading
                if os.name == 'nt':  # Windows
                    # Format command properly for Windows cmd.exe - quote paths with spaces
                    def quote_arg(arg):
                        arg_str = str(arg)
                        if ' ' in arg_str or arg_str.startswith('-'):
                            return f'"{arg_str}"'
                        return arg_str
                    shell_cmd = ' '.join(quote_arg(arg) for arg in cmd)
                    result = subprocess.run(
                        shell_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=str(self.project_root.resolve()),
                        env=env,
                        shell=True,
                        text=True,
                        encoding='utf-8',
                        errors='replace',
                        timeout=3600
                    )
                else:
                    # On Unix-like systems, use list format without shell
                    result = subprocess.run(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=str(self.project_root.resolve()),
                        env=env,
                        shell=False,
                        text=True,
                        encoding='utf-8',
                        errors='replace',
                        timeout=3600
                    )
                return result.returncode, result.stdout, result.stderr
            except subprocess.TimeoutExpired:
                return -1, "", "Script execution timed out after 1 hour"
            except Exception as e:
                return -1, "", f"Error running script: {str(e)}"
        
        try:
            # Run in thread executor to avoid blocking and Windows asyncio issues
            loop = asyncio.get_event_loop()
            returncode, stdout_text, stderr_text = await loop.run_in_executor(
                self.executor, run_script_sync
            )
            
            # Split output into lines and log
            stdout_lines = stdout_text.split('\n') if stdout_text else []
            stderr_lines = stderr_text.split('\n') if stderr_text else []
            
            # Log all output (with error handling)
            try:
                for line in stdout_lines:
                    if line.strip():
                        await self._log(log_file, line)
                
                for line in stderr_lines:
                    if line.strip():
                        await self._log(log_file, f"[STDERR] {line}")
            except Exception as log_error:
                await self._log(log_file, f"[WARNING] Error while logging output: {log_error}")
            
            if returncode != 0:
                # Convert Windows error codes to readable messages
                error_code_msg = ""
                if returncode == 3221225794 or returncode == -1073741515:  # 0xC0000135
                    error_code_msg = "\n[WINDOWS ERROR] DLL initialization failed or missing dependency."
                    error_code_msg += "\nThis usually means:"
                    error_code_msg += "\n1. Missing Visual C++ Redistributable"
                    error_code_msg += "\n2. Corrupted Python installation"
                    error_code_msg += "\n3. Virtual environment issues"
                    error_code_msg += "\nFix: Install Visual C++ Redistributable from: https://aka.ms/vs/17/release/vc_redist.x64.exe"
                    error_code_msg += f"\nPython executable: {sys.executable}"
                    error_code_msg += f"\nTry running manually: {' '.join(cmd)}"
                    
                    # Log the error message before raising
                    try:
                        await self._log(log_file, error_code_msg)
                    except:
                        pass  # If logging fails, continue anyway
                
                error_msg = f"Script failed with return code {returncode}"
                if error_code_msg:
                    error_msg += error_code_msg
                
                if stderr_lines:
                    last_stderr = [l.strip() for l in stderr_lines if l.strip()][-20:]
                    if last_stderr:
                        error_msg += f"\n\nSTDERR (last 20 lines):\n" + "\n".join(last_stderr)
                    else:
                        error_msg += f"\n\nNo STDERR output captured (script may have crashed immediately)"
                
                if stdout_lines:
                    last_stdout = [l.strip() for l in stdout_lines if l.strip()][-20:]
                    if last_stdout:
                        error_msg += f"\n\nSTDOUT (last 20 lines):\n" + "\n".join(last_stdout)
                    else:
                        error_msg += f"\n\nNo STDOUT output captured (script may have crashed immediately)"
                
                # Add command info for debugging
                error_msg += f"\n\nCommand that failed: {' '.join(cmd)}"
                error_msg += f"\nWorking directory: {self.project_root}"
                error_msg += f"\nPython executable: {sys.executable}"
                
                # Log the complete error message before raising
                try:
                    await self._log(log_file, f"\n{'='*70}")
                    await self._log(log_file, "SCRIPT EXECUTION FAILED")
                    await self._log(log_file, f"{'='*70}")
                    await self._log(log_file, error_msg)
                    await self._log(log_file, f"{'='*70}\n")
                except Exception as log_err:
                    # If logging fails, at least print it
                    print(f"Error logging failed: {log_err}")
                    print(error_msg)
                
                raise RuntimeError(error_msg)
                
        except Exception as e:
            await self._log(log_file, f"Exception running script: {type(e).__name__}: {str(e)}")
            import traceback
            await self._log(log_file, traceback.format_exc())
            raise
    
    async def _normalize_filenames(self, directory: Path, log_file: Path):
        """Normalize filenames to lowercase underscore style"""
        import re
        for file_path in directory.glob("*"):
            if file_path.is_file():
                new_name = re.sub(r'[^a-z0-9_.]+', '_', file_path.stem.lower()) + file_path.suffix.lower()
                if new_name != file_path.name:
                    new_path = file_path.parent / new_name
                    file_path.rename(new_path)
                    await self._log(log_file, f"Renamed: {file_path.name} → {new_name}")
    
    async def _log(self, log_file: Path, message: str):
        """Append message to log file"""
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(message + "\n")
            f.flush()