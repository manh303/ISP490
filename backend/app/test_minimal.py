#!/usr/bin/env python3
"""
Minimal FastAPI test to identify issues
"""

import os
import sys
import logging
from datetime import datetime

# Basic FastAPI test
try:
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    import uvicorn
    print("SUCCESS: FastAPI imports successful")
except ImportError as e:
    print(f"ERROR: FastAPI import error: {e}")
    sys.exit(1)

# Test database imports
try:
    from databases import Database
    import asyncpg
    print("SUCCESS: Database imports successful")
    DATABASE_AVAILABLE = True
except ImportError as e:
    print(f"WARNING: Database import error: {e}")
    DATABASE_AVAILABLE = False

# Create minimal app
app = FastAPI(title="Minimal Test API", version="1.0.0")

@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "Minimal backend test",
        "timestamp": datetime.now().isoformat(),
        "database_available": DATABASE_AVAILABLE
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "python_version": sys.version,
        "database_modules": DATABASE_AVAILABLE
    }

if __name__ == "__main__":
    print("Starting minimal test server...")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")
    print(f"Database available: {DATABASE_AVAILABLE}")

    uvicorn.run(
        "test_minimal:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=False
    )