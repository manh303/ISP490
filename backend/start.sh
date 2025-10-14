#!/bin/bash
# Railway startup script
echo "🚀 Starting FastAPI backend..."

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:/app"
export PORT=${PORT:-8000}

# Change to app directory and start the server
cd /app
exec uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1