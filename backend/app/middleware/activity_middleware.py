from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from services.activity_logger import ActivityLogger
import json
import logging
import time

logger = logging.getLogger(__name__)

class ActivityLoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, db_manager):
        super().__init__(app)
        self.db_manager = db_manager

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Skip logging for certain paths
        skip_paths = ["/docs", "/openapi.json", "/health", "/favicon.ico", "/static"]
        if any(request.url.path.startswith(path) for path in skip_paths):
            return await call_next(request)

        # Get user info from token if available
        user_id = None
        email = None
        
        try:
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header.split(" ")[1]
                # Decode token to get user info
                from utils.jwt_utils import decode_access_token
                payload = decode_access_token(token)
                if payload:
                    user_id = payload.get("user_id")
                    email = payload.get("email")
        except Exception:
            pass  # Continue without user info

        # Process request
        response = await call_next(request)
        
        # Log the activity
        try:
            process_time = time.time() - start_time
            
            # Determine action based on method and path
            action = f"{request.method} {request.url.path}"
            
            # Create activity details
            details = {
                "status_code": response.status_code,
                "process_time": round(process_time, 3),
                "method": request.method,
                "path": request.url.path
            }
            
            # Add query params if present
            if request.query_params:
                details["query_params"] = dict(request.query_params)

            # Log the activity
            activity_logger = ActivityLogger(self.db_manager)
            await activity_logger.log_activity(
                user_id=user_id,
                email=email,
                action=action,
                resource=request.url.path,
                details=details,
                request=request,
                status="success" if response.status_code < 400 else "error"
            )
        except Exception as e:
            logger.error(f"Activity logging middleware error: {e}")

        return response