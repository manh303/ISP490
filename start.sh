#!/bin/bash
# Railway startup script for backend
echo "🚀 Starting FastAPI backend..."

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:/app/backend"
export PORT=${PORT:-8000}

# Change to backend directory and start the server
cd backend
exec uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1