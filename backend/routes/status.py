"""
Status routes for checking job progress and logs
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pathlib import Path
import json
from backend.config import job_status, LOGS_DIR

router = APIRouter()


@router.get("/{job_id}")
async def get_job_status(job_id: str):
    """
    Get the status of a processing job
    """
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    status_data = job_status[job_id].copy()
    
    # Add log file path if it exists
    log_file = LOGS_DIR / f"{job_id}.log"
    if log_file.exists():
        status_data["log_file"] = f"/api/status/{job_id}/logs"
    
    return status_data


@router.get("/{job_id}/logs")
async def get_job_logs(job_id: str):
    """
    Stream job logs in real-time
    """
    log_file = LOGS_DIR / f"{job_id}.log"
    
    if not log_file.exists():
        raise HTTPException(status_code=404, detail="Log file not found")
    
    def generate_logs():
        """Generator function to stream log file content"""
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                # Read existing content
                content = f.read()
                yield content
                
                # Stream new content as it's added
                # In a real implementation, you'd use file watching
                # For now, we'll just return what's there
        except Exception as e:
            yield f"Error reading logs: {str(e)}\n"
    
    return StreamingResponse(
        generate_logs(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/{job_id}/logs/tail")
async def get_job_logs_tail(job_id: str, lines: int = 100):
    """
    Get the last N lines of job logs
    """
    log_file = LOGS_DIR / f"{job_id}.log"
    
    if not log_file.exists():
        raise HTTPException(status_code=404, detail="Log file not found")
    
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
            tail_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
            return {
                "job_id": job_id,
                "lines": len(tail_lines),
                "logs": "".join(tail_lines)
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading logs: {str(e)}")

