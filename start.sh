#!/bin/bash
# Railway startup script for backend - Fixed Version
echo "Starting Vietnam E-commerce DSS Backend..."

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:/app/backend:/app"
export PORT=${PORT:-8000}

# Debug information
echo "Current directory: $(pwd)"
echo "Port: $PORT"
echo "Python path: $PYTHONPATH"

# List backend directory contents for debugging
echo "Backend directory contents:"
ls -la backend/ || echo "Backend directory not found"

# Change to backend directory
cd backend || {
    echo "Failed to change to backend directory"
    echo "Current directory contents:"
    ls -la
    exit 1
}

echo "Changed to backend directory"
echo "App directory contents:"
ls -la app/ || echo "App directory not found"

# Change to app directory for correct module resolution
cd app || {
    echo "Failed to change to app directory"
    exit 1
}

echo "Changed to app directory"

# Try to start the server with python main.py first
echo "Attempting to start with python main.py..."
if python main.py; then
    echo "Started successfully with python main.py"
else
    echo "Failed with python main.py, trying uvicorn with app.main..."
    # Go back to backend directory and use full module path
    cd /app/backend
    exec uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1 --log-level info
fi