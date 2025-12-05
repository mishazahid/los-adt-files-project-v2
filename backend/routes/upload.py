"""
Upload routes for handling file uploads
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Form
from pathlib import Path
import os
import uuid
import shutil
from datetime import datetime
from typing import List, Optional

from backend.services.pipeline import PipelineService
from backend.config import job_status, UPLOAD_DIR, OUTPUT_DIR, LOGS_DIR

router = APIRouter()
pipeline_service = PipelineService()


@router.post("/files")
async def upload_files(
    background_tasks: BackgroundTasks,
    adt_files: Optional[List[UploadFile]] = File(None),
    los_files: Optional[List[UploadFile]] = File(None),
    visit_files: Optional[List[UploadFile]] = File(None)
):
    """
    Upload files for processing
    Accepts files in three categories: ADT, LOS, and VISITS
    """
    try:
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Initialize job status
        job_status[job_id] = {
            "status": "uploading",
            "created_at": datetime.now().isoformat(),
            "progress": 0,
            "message": "Uploading files...",
            "files": {
                "adt": [],
                "los": [],
                "visits": []
            },
            "outputs": {},
            "errors": []
        }
        
        # Create job directory
        job_dir = UPLOAD_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        
        # Create category subdirectories
        adt_dir = job_dir / "ADT"
        los_dir = job_dir / "LOS"
        visits_dir = job_dir / "VISITS"
        
        for directory in [adt_dir, los_dir, visits_dir]:
            directory.mkdir(exist_ok=True)
        
        # Process ADT files
        adt_files_saved = []
        if adt_files:
            for file in adt_files:
                if file.filename:
                    file_path = adt_dir / file.filename
                    with open(file_path, "wb") as buffer:
                        shutil.copyfileobj(file.file, buffer)
                    adt_files_saved.append(file.filename)
                    job_status[job_id]["files"]["adt"].append(file.filename)
        
        # Process LOS files
        los_files_saved = []
        if los_files:
            for file in los_files:
                if file.filename:
                    file_path = los_dir / file.filename
                    with open(file_path, "wb") as buffer:
                        shutil.copyfileobj(file.file, buffer)
                    los_files_saved.append(file.filename)
                    job_status[job_id]["files"]["los"].append(file.filename)
        
        # Process VISIT files
        visit_files_saved = []
        if visit_files:
            for file in visit_files:
                if file.filename:
                    file_path = visits_dir / file.filename
                    with open(file_path, "wb") as buffer:
                        shutil.copyfileobj(file.file, buffer)
                    visit_files_saved.append(file.filename)
                    job_status[job_id]["files"]["visits"].append(file.filename)
        
        # Validate that at least some files were uploaded
        total_files = len(adt_files_saved) + len(los_files_saved) + len(visit_files_saved)
        if total_files == 0:
            job_status[job_id]["status"] = "error"
            job_status[job_id]["message"] = "No files were uploaded"
            raise HTTPException(status_code=400, detail="No files were uploaded")
        
        # Update job status
        job_status[job_id]["status"] = "uploaded"
        job_status[job_id]["progress"] = 10
        job_status[job_id]["message"] = f"Uploaded {total_files} file(s). Starting processing..."
        
        # Start background processing
        background_tasks.add_task(
            process_job,
            job_id,
            str(job_dir)
        )
        
        return {
            "success": True,
            "job_id": job_id,
            "message": f"Successfully uploaded {total_files} file(s)",
            "files": {
                "adt": adt_files_saved,
                "los": los_files_saved,
                "visits": visit_files_saved
            },
            "status_url": f"/api/status/{job_id}"
        }
    
    except Exception as e:
        error_msg = str(e)
        if job_id in job_status:
            job_status[job_id]["status"] = "error"
            job_status[job_id]["message"] = f"Upload failed: {error_msg}"
            job_status[job_id]["errors"].append(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


async def process_job(job_id: str, job_dir: str):
    """
    Background task to process uploaded files
    """
    try:
        job_status[job_id]["status"] = "processing"
        job_status[job_id]["progress"] = 20
        job_status[job_id]["message"] = "Processing files..."
        
        # Run the full pipeline
        results = await pipeline_service.run_pipeline(job_id, job_dir)
        
        # Update job status with results
        job_status[job_id]["status"] = "completed"
        job_status[job_id]["progress"] = 100
        job_status[job_id]["message"] = "Processing completed successfully"
        job_status[job_id]["outputs"] = results
        job_status[job_id]["completed_at"] = datetime.now().isoformat()
        
    except Exception as e:
        import traceback
        error_msg = str(e)
        error_traceback = traceback.format_exc()
        
        # Log the full error
        log_file = LOGS_DIR / f"{job_id}.log"
        log_file.parent.mkdir(exist_ok=True)
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"ERROR: {error_msg}\n")
            f.write(f"{'='*60}\n")
            f.write(error_traceback)
            f.write(f"\n{'='*60}\n")
        
        job_status[job_id]["status"] = "error"
        job_status[job_id]["message"] = f"Processing failed: {error_msg}"
        job_status[job_id]["errors"].append(error_msg)
        job_status[job_id]["error_traceback"] = error_traceback
        job_status[job_id]["progress"] = 0
        print(f"ERROR in job {job_id}: {error_msg}")
        print(error_traceback)

