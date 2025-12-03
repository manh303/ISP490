from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from app.services.activity_logger import ActivityLogger
from app.core.config import settings
import json
import logging
import time

logger = logging.getLogger(__name__)

# Global flag: Check table existence only once at startup instead of every request
_ACTIVITY_TABLE_READY = None

class ActivityLoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, db_manager):
        super().__init__(app)
        self.db_manager = db_manager
        # Paths where activity logging should be skipped (to reduce latency)
        # Skip auth endpoints to avoid duplicate logging (done in handler) and reduce database queries
        self.skip_paths = [
            "/docs", "/openapi.json", "/health", "/favicon.ico", "/static",
            "/api/v1/auth/signin", "/api/v1/auth/signout", "/api/v1/auth/signup",
            "/api/v1/auth/login", "/api/v1/auth/register", "/api/v1/auth/logout"
        ]

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        # Skip logging for certain paths to reduce latency
        if any(request.url.path.startswith(path) for path in self.skip_paths):
            return await call_next(request)

        # Get user info from token if available
        user_id = None
        email = None
        role_at_time = None
        
        try:
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header.split(" ")[1]
                # Decode token to get user info
                try:
                    from app.utils.auth_helpers import decode_access_token
                except ImportError:
                    from app.utils.auth_helpers import decode_access_token
                payload = decode_access_token(token, settings.jwt_secret)
                if payload:
                    user_id = payload.get("user_id")
                    email = payload.get("email")
                    role_at_time = payload.get("role", payload.get("role_code"))
        except Exception:
            pass  # Continue without user info

        # Auto-detect module based on route
        module = self._detect_module(request.url.path)
        
        # Auto-detect action based on method and path
        action = self._detect_action(request.method, request.url.path)
        
        # Capture request payload (for POST/PUT/PATCH)
        request_payload = None
        if request.method in ["POST", "PUT", "PATCH"]:
            try:
                body = await request.body()
                if body:
                    request_payload = json.loads(body)
                    # Mask sensitive fields
                    request_payload = self._mask_sensitive_fields(request_payload)
            except Exception:
                pass

        # Process request
        response = await call_next(request)
        
        # Log the activity
        try:
            # Check if table exists before logging (using cached flag to avoid query per request)
            global _ACTIVITY_TABLE_READY
            if _ACTIVITY_TABLE_READY is None and self.db_manager.is_connected:
                # Check table existence only once at first request
                table_check = await self.db_manager.execute_query(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'iam' AND table_name = 'user_activity_logs')"
                )
                _ACTIVITY_TABLE_READY = bool(table_check and table_check[0].get('exists'))
                logger.info(f"Activity logging: table exists = {_ACTIVITY_TABLE_READY}")
            
            if _ACTIVITY_TABLE_READY:
                    process_time = time.time() - start_time
                    
                    details = {
                        "status_code": response.status_code,
                        "process_time": round(process_time, 3),
                    }
                    if request.query_params:
                        details["query_params"] = dict(request.query_params)
                    
                    status = "success" if response.status_code < 400 else "error"
                    message = None
                    if response.status_code >= 400:
                        message = f"Request failed with status {response.status_code}"
                    
                    activity_logger = ActivityLogger(self.db_manager)
                    await activity_logger.log_activity(
                        user_id=user_id,
                        email=email,
                        action=action,
                        module=module,
                        role_at_time=role_at_time,
                        request_method=request.method,
                        request_path=str(request.url.path),
                        request_payload=request_payload,
                        message=message,
                        details=details,
                        request=request,
                        status=status
                    )
        except Exception as e:
            # Silently skip logging if table doesn't exist
            logger.error(f"Middleware logging error: {e}")

        return response
    
    def _detect_module(self, path: str) -> str:
        """Detect module from request path"""
        if "/admin/users" in path or "/auth/" in path:
            return "IAM"
        elif "/analytics" in path:
            return "ANALYTICS"
        elif "/dss" in path:
            return "DSS"
        elif "/ml" in path:
            return "ML"
        elif "/data-engineer" in path or "/airflow" in path:
            return "DATA_PIPELINE"
        return "GENERAL"
    
    def _detect_action(self, method: str, path: str) -> str:
        """Detect action from method and path"""
        # Authentication actions
        if "/auth/login" in path:
            return "LOGIN"
        if "/auth/logout" in path:
            return "LOGOUT"
        if "/auth/register" in path:
            return "REGISTER"
        
        # User management
        if "/admin/users" in path:
            if method == "GET":
                return "VIEW_USERS"
            elif method == "POST":
                return "CREATE_USER"
            elif method == "PUT" and "/roles" in path:
                return "UPDATE_USER_ROLE"
            elif method == "PUT" and "/password" in path:
                return "CHANGE_PASSWORD"
            elif method == "PUT":
                return "UPDATE_USER"
            elif method == "DELETE":
                return "DELETE_USER"
        
        # DSS operations
        if "/dss" in path:
            if "/price" in path:
                return "RUN_PRICE_DSS"
            elif "/reco" in path:
                return "RUN_RECOMMENDATION_DSS"
            elif "/review" in path:
                return "RUN_REVIEW_DSS"
        
        # ML operations
        if "/ml" in path:
            return "RUN_ML_MODEL"
        
        # Analytics/Reports
        if "/analytics" in path or "/report" in path:
            if "export" in path:
                return "EXPORT_REPORT"
            return "VIEW_ANALYTICS"
        
        # Default action
        return f"{method}_{path.split('/')[-1].upper()}" if path.split('/')[-1] else f"{method}_REQUEST"
    
    def _mask_sensitive_fields(self, data: dict) -> dict:
        """Mask sensitive fields in request payload"""
        masked = data.copy()
        sensitive_fields = ['password', 'token', 'secret', 'api_key', 'access_token', 'refresh_token']
        
        for field in sensitive_fields:
            if field in masked:
                masked[field] = "***MASKED***"
        
        return masked