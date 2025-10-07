#!/usr/bin/env python3
"""
Simple FastAPI Backend with Authentication
For development and testing purposes
"""

import logging
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================
# FastAPI APP SETUP
# ====================================

app = FastAPI(
    title="E-commerce DSS Backend",
    description="Simple Backend with Authentication for E-commerce DSS Project",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====================================
# BASIC ENDPOINTS
# ====================================

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "E-commerce DSS Backend",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime": "running"
    }

@app.get("/api/status")
async def api_status():
    """API status endpoint"""
    return {
        "api_status": "active",
        "endpoints": {
            "authentication": "/api/v1/simple-auth/",
            "health": "/health",
            "docs": "/docs"
        },
        "timestamp": datetime.now().isoformat()
    }

# ====================================
# LOAD SIMPLE AUTHENTICATION
# ====================================

# Load Simple Authentication endpoints
try:
    from simple_auth_no_jwt import simple_auth_router
    app.include_router(simple_auth_router)
    logger.info("✅ Simple Authentication endpoints loaded successfully (Session-based)")
except Exception as e:
    logger.error(f"❌ Simple Auth endpoints not available: {e}")

    # Create fallback endpoint to show what's missing
    @app.get("/api/v1/simple-auth/status")
    async def auth_status():
        return {
            "status": "Simple Auth not available",
            "error": str(e),
            "note": "Check simple_auth_no_jwt.py file",
            "timestamp": datetime.now().isoformat()
        }

# ====================================
# ERROR HANDLERS
# ====================================

@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "message": f"The path {request.url.path} was not found",
            "available_endpoints": [
                "/",
                "/health",
                "/api/status",
                "/api/v1/simple-auth/",
                "/docs"
            ]
        }
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "Something went wrong on the server",
            "timestamp": datetime.now().isoformat()
        }
    )

# ====================================
# STARTUP EVENT
# ====================================

@app.on_event("startup")
async def startup_event():
    """Startup event"""
    logger.info("🚀 Starting E-commerce DSS Backend...")
    logger.info("📊 Simple Authentication System Loaded")
    logger.info("🌐 CORS enabled for development")
    logger.info("✅ Backend ready!")

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event"""
    logger.info("👋 Shutting down E-commerce DSS Backend...")

# ====================================
# MAIN
# ====================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "simple_main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )