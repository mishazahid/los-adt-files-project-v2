"""
FastAPI Backend for Facility Report Generator
Handles file uploads, processing pipeline, and report generation
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import uvicorn
from datetime import datetime

from backend.routes import upload, status, download
from backend.config import job_status, UPLOAD_DIR, OUTPUT_DIR, LOGS_DIR

# Initialize FastAPI app
app = FastAPI(
    title="Facility Report Generator API",
    description="API for processing PDF files and generating facility reports",
    version="1.0.0"
)

# CORS middleware - allow all origins for now (can restrict later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(upload.router, prefix="/api/upload", tags=["upload"])
app.include_router(status.router, prefix="/api/status", tags=["status"])
app.include_router(download.router, prefix="/api/download", tags=["download"])

# Mount static files for frontend
frontend_path = Path(__file__).parent.parent / "frontend"
if frontend_path.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_path)), name="static")


@app.get("/")
async def root():
    """Serve the main frontend page"""
    frontend_file = Path(__file__).parent.parent / "frontend" / "index.html"
    if frontend_file.exists():
        return FileResponse(
            frontend_file,
            media_type="text/html"
        )
    return {"message": "Facility Report Generator API", "status": "running"}


@app.get("/health")
async def health_redirect():
    """Redirect /health to /api/health"""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/api/health")


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "pipeline": "ready",
            "google_sheets": "ready",
            "google_slides": "ready",
            "google_apps_script": "ready"
        }
    }


@app.get("/api/jobs")
async def list_jobs():
    """List all processing jobs"""
    return {
        "jobs": [
            {
                "job_id": job_id,
                "status": status.get("status", "unknown"),
                "created_at": status.get("created_at", ""),
                "progress": status.get("progress", 0)
            }
            for job_id, status in job_status.items()
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

