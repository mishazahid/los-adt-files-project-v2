"""
Download routes for retrieving output files
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
from backend.config import job_status, OUTPUT_DIR

router = APIRouter()


@router.get("/{job_id}")
async def list_outputs(job_id: str):
    """
    List all output files for a job
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = job_status[job_id]
    
    if job_data["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not completed. Current status: {job_data['status']}"
        )
    
    outputs = job_data.get("outputs", {})
    
    # List files in output directory
    output_dir = OUTPUT_DIR / job_id
    files = []
    
    if output_dir.exists():
        for file_path in output_dir.rglob("*"):
            if not file_path.is_file():
                continue
            # Skip raw text diagnostics files (e.g., extracted_text.txt)
            if file_path.suffix.lower() == ".txt":
                continue
            # Only show all_patients.csv files
            if file_path.name != "all_patients.csv":
                continue
            relative_path = file_path.relative_to(output_dir)
            files.append({
                "name": file_path.name,
                "path": str(relative_path),
                "size": file_path.stat().st_size,
                "url": f"/api/download/{job_id}/file/{relative_path.as_posix()}"
            })
    
    return {
        "job_id": job_id,
        "files": files,
        "outputs": outputs
    }


@router.get("/{job_id}/file/{file_path:path}")
async def download_file(job_id: str, file_path: str):
    """
    Download a specific output file
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    output_dir = OUTPUT_DIR / job_id
    requested_file = output_dir / file_path
    
    # Security check - ensure file is within output directory
    try:
        requested_file.resolve().relative_to(output_dir.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")
    
    if not requested_file.exists() or not requested_file.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        requested_file,
        filename=requested_file.name,
        media_type="application/octet-stream"
    )


@router.get("/{job_id}/report")
async def download_report(job_id: str):
    """
    Download the final report (PDF)
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job_data = job_status[job_id]
    
    if job_data["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not completed. Current status: {job_data['status']}"
        )
    
    outputs = job_data.get("outputs", {})
    report_path = outputs.get("report_pdf")
    
    if not report_path:
        raise HTTPException(status_code=404, detail="Report not found")
    
    report_file = Path(report_path)
    
    if not report_file.exists():
        raise HTTPException(status_code=404, detail="Report file not found")
    
    return FileResponse(
        report_file,
        filename=report_file.name,
        media_type="application/pdf"
    )

