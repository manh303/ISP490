#!/bin/bash
# Simple Railway startup script

echo "🚀 Starting FastAPI backend for Railway..."

# Install dependencies if needed
if [ ! -d "venv" ]; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

# Start the application
echo "Starting uvicorn server..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1