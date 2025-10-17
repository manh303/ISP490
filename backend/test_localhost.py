#!/usr/bin/env python3
"""
Test server binding to localhost specifically
"""

import uvicorn
from fastapi import FastAPI

app = FastAPI(title="Localhost Test")

@app.get("/")
async def root():
    return {"message": "Localhost test working!", "host": "127.0.0.1"}

if __name__ == "__main__":
    print("Starting server on http://127.0.0.1:8003")
    print("This should be accessible at http://localhost:8003")

    uvicorn.run(
        app,
        host="127.0.0.1",  # Bind specifically to localhost
        port=8003,
        log_level="info"
    )