#!/usr/bin/env python3
"""
Main entry point for Railway deployment
"""
import os
import sys

# Add app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

# Import the FastAPI app
from app.main import app

if __name__ == "__main__":
    import uvicorn

    # Get port from environment (Railway sets this)
    port = int(os.environ.get("PORT", 8000))

    # Run the app
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        workers=1
    )