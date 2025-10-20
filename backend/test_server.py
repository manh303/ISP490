#!/usr/bin/env python3
"""
Simple test server to verify basic functionality
"""

import os
import uvicorn
from fastapi import FastAPI

# Create simple app
app = FastAPI(title="Test Server")

@app.get("/")
async def root():
    return {
        "message": "Test server is working!",
        "status": "ok"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8002))
    print(f"Starting test server on http://0.0.0.0:{port}")
    print(f"Access via: http://localhost:{port}")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )