#!/usr/bin/env python3
"""
Main entry point for Railway deployment - Fixed Version
"""
import os
import sys

# Add app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

# Import the FastAPI app from fixed main
try:
    from app.main import app
    print("Successfully imported fixed FastAPI app")
except ImportError as e:
    print(f"Failed to import app: {e}")
    # Fallback to create a minimal app
    from fastapi import FastAPI
    app = FastAPI(title="Vietnam E-commerce DSS", description="Fallback API")

    @app.get("/")
    async def root():
        return {"message": "Vietnam E-commerce DSS API - Fallback Mode", "status": "running"}

    @app.get("/health")
    async def health():
        return {"status": "healthy", "mode": "fallback"}

if __name__ == "__main__":
    import uvicorn

    # Get port from environment (Railway sets this)
    port = int(os.environ.get("PORT", 8000))

    print(f"Starting server on port {port}")

    # Run the app
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        workers=1,
        log_level="info"
    )