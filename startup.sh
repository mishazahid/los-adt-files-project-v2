#!/bin/bash
# Azure App Service startup script
# Azure uses Gunicorn instead of Uvicorn for production

# Create necessary directories
mkdir -p uploads outputs logs jobs

# Start Gunicorn with Uvicorn workers (for async FastAPI)
# Using uvicorn workers for async support
gunicorn backend.app:app \
  --bind 0.0.0.0:8000 \
  --workers 2 \
  --worker-class uvicorn.workers.UvicornWorker \
  --timeout 600 \
  --access-logfile - \
  --error-logfile - \
  --log-level info

