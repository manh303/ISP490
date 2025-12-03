#!/usr/bin/env python3
"""
Simplified Vietnam E-commerce DSS Backend - Main Application Entry Point
Only essential APIs for dashboard and authentication
"""

import logging
import os
import sys
import time
# Force reload trigger
from contextlib import asynccontextmanager
from datetime import datetime

# Ensure backend and project root are on sys.path (for `app` package)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
for path in (CURRENT_DIR, PROJECT_ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from app.core.database import db_manager
from app.core.settings import settings
from app.db_config import DATABASE_URL
from app.db_pool import close_pool, init_pool
from app.utils.auth_helpers import decode_access_token

# Configure logging early (force=True to show logs even when uvicorn sets handlers)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    load_dotenv()
    logger.info("Loaded .env file")
except ImportError:
    logger.warning("python-dotenv not installed, using system environment variables")

# Optional validators and role menus
try:
    from app.utils.validators import validate_email, validate_password, validate_phone
    from app.constants.roles import ROLE_MENUS, get_role_menu

    VALIDATORS_AVAILABLE = True
except ImportError:
    VALIDATORS_AVAILABLE = False

    def validate_password(value, **kwargs):
        return value

    def validate_email(email: str):
        return email

    validate_phone = lambda x: x  # noqa: E731
    ROLE_MENUS = {}
    get_role_menu = lambda x: {}  # noqa: E731
    logger.warning("Validator utilities not available; using fallbacks")

# Database client availability
try:
    from databases import Database  # noqa: F401
    import asyncpg  # noqa: F401

    DATABASE_AVAILABLE = True
    logger.info("✅ Database modules imported successfully")
except ImportError:
    DATABASE_AVAILABLE = False
    logger.warning("❌ Database modules not available")

# Activity logging middleware (optional)
try:
    from app.middleware.activity_middleware import ActivityLoggingMiddleware
    from app.services.activity_logger import ActivityLogger

    ACTIVITY_AVAILABLE = True
except ImportError:
    ACTIVITY_AVAILABLE = False
    ActivityLoggingMiddleware = None
    ActivityLogger = None
    logger.warning("Activity logging not available")

# Import DatabaseManager for type annotations (safe import)
try:
    from app.core.database import DatabaseManager
except ImportError:
    DatabaseManager = object  # type: ignore

# Import auth endpoints
try:
    from app.api.v1.auth_endpoints import router as auth_router

    AUTH_ROUTER_AVAILABLE = True
    logger.info("Auth endpoints router loaded")
except ImportError as exc:
    AUTH_ROUTER_AVAILABLE = False
    auth_router = None
    logger.warning("Auth endpoints not available: %s", exc)

# Import IAM system (legacy)
IAM_AVAILABLE = False
try:
    from app.api.v1.auth import router as iam_router, init_iam_service  # noqa: F401

    IAM_AVAILABLE = True
    logger.info("✅ IAM system imported successfully")
except ImportError as exc:
    logger.warning("❌ IAM system not available: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown hooks."""
    logger.info("=== LIFESPAN STARTUP BEGIN ===")
    logger.debug("DATABASE_URL: %s", DATABASE_URL[:50])

    try:
        logger.info("Initializing database connection pool...")
        ssl_mode = "require" if os.getenv("RENDER") or "render.com" in DATABASE_URL else None
        if ssl_mode:
            logger.info("SSL enabled for database connection (Render environment detected)")
        else:
            logger.info("SSL disabled for database connection (local/internal)")

        await init_pool(database_url=DATABASE_URL, min_size=5, max_size=20, ssl=ssl_mode)
        logger.info("Database connection pool initialized successfully")

        from app.db_pool import get_pool

        pool = await get_pool()
        if pool:
            logger.info("Database connection pool is accessible")
            app.state.start_time = time.time()
            app.state.pool_initialized = True
        else:
            raise RuntimeError("Pool initialization failed")

        logger.info("=== LIFESPAN STARTUP COMPLETE ===")
    except Exception as exc:
        logger.exception("Failed to initialize connection pool: %s", exc)
        raise

    yield

    logger.info("=== LIFESPAN SHUTDOWN BEGIN ===")
    try:
        await close_pool()
        logger.info("Database connection pool closed")
    except Exception as exc:
        logger.exception("Error closing connection pool: %s", exc)
    logger.info("=== LIFESPAN SHUTDOWN COMPLETE ===")


app = FastAPI(
    title="Vietnam E-commerce DSS API",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


def include_router_if_available(router, name: str, prefix: str = settings.API_V1_PREFIX, tags=None):
    """Safely include a router if present."""
    if not router:
        logger.warning("%s router not available", name)
        return

    kwargs = {"prefix": prefix}
    if tags:
        kwargs["tags"] = tags

    try:
        app.include_router(router, **kwargs)
        logger.info("✅ %s routes included", name)
    except Exception as exc:
        logger.error("❌ %s routes failed to include: %s", name, exc)


def include_router_from_module(module_path: str, name: str, prefix: str = settings.API_V1_PREFIX, tags=None):
    """Import a router lazily and include it if available."""
    kwargs = {"prefix": prefix}
    if tags:
        kwargs["tags"] = tags

    try:
        module = __import__(module_path, fromlist=["router"])
        app.include_router(module.router, **kwargs)
        logger.info("✅ %s routes included", name)
    except ImportError as exc:
        logger.warning("❌ %s routes not available: %s", name, exc)
    except Exception as exc:
        logger.error("❌ %s routes failed to include: %s", name, exc)


# Include Auth router (new modular endpoints)
include_router_if_available(auth_router, "Auth")

# Additional routers
include_router_from_module("app.api.v1.admin", "Admin")
include_router_from_module("app.api.v1.profile", "Profile")
include_router_from_module("app.api.v1.roles", "Role Management")
include_router_from_module("app.api.v1.analytics", "Analytics", tags=["Analytics / Analyst"])
include_router_from_module("app.api.v1.ml_router", "ML API", tags=["Machine Learning"])
include_router_from_module("app.api.v1.data_engineer", "Data Engineer", tags=["Data Engineer"])
include_router_from_module("app.api.v1.business_metadata", "Business Metadata", tags=["Business Metadata"])
include_router_from_module("app.api.v1.dss", "DSS", tags=["DSS - Decision Support System"])
include_router_from_module(
    "app.api.v1.reports", "Reports", prefix=f"{settings.API_V1_PREFIX}/reports", tags=["Reports"]
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS if not settings.DEBUG else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add activity logging middleware (only if available and enabled)
if False and ACTIVITY_AVAILABLE:  # Disabled for now
    app.add_middleware(ActivityLoggingMiddleware, db_manager=db_manager)
    logger.info("Activity logging middleware enabled")
else:
    logger.info("Activity logging middleware disabled (performance)")


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.update(
        {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Content-Security-Policy": "default-src 'self' 'unsafe-inline' 'unsafe-eval' https: data:; img-src 'self' data: https: blob:; style-src 'self' 'unsafe-inline' https:; script-src 'self' 'unsafe-inline' 'unsafe-eval' https:; font-src 'self' data: https:",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "SAMEORIGIN",
            "X-XSS-Protection": "1; mode=block",
        }
    )
    return response


# Dependency Injection
async def get_database():
    """Get database connection."""
    return db_manager


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Vietnam E-commerce DSS API",
        "version": settings.VERSION,
        "timestamp": datetime.now().isoformat(),
        "status": "running",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {},
        "uptime_seconds": time.time() - getattr(app.state, "start_time", 0),
    }

    try:
        from app.db_pool import get_pool

        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

        health_status["services"]["postgresql"] = "healthy"
    except RuntimeError as exc:
        health_status["services"]["postgresql"] = f"not connected: {exc}"
        health_status["status"] = "degraded"
    except Exception as exc:
        health_status["services"]["postgresql"] = f"unhealthy: {exc}"
        health_status["status"] = "degraded"

    return health_status


@app.get(f"{settings.API_V1_PREFIX}/status")
async def api_status():
    """API status endpoint."""
    database_connected = False
    try:
        from app.db_pool import get_pool

        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        database_connected = True
    except Exception:
        database_connected = False

    return {
        "api": "Vietnam E-commerce DSS - Simplified",
        "version": settings.VERSION,
        "status": "operational",
        "database_connected": database_connected,
        "features": {
            "database": DATABASE_AVAILABLE,
            "authentication": IAM_AVAILABLE,
        },
        "timestamp": datetime.now().isoformat(),
    }


@app.get(f"{settings.API_V1_PREFIX}/check-roles")
async def check_database_roles():
    """Check roles in database."""
    try:
        if not db_manager.is_connected:
            await db_manager.connect()

        query = "SELECT role_id, role_code, role_name, description FROM iam.iam_role ORDER BY role_code"
        roles = await db_manager.execute_query(query)

        return {
            "success": True,
            "total_roles": len(roles),
            "roles": roles,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "timestamp": datetime.now().isoformat(),
        }


@app.get(f"{settings.API_V1_PREFIX}/test-roles")
async def test_roles():
    """Test endpoint to check if roles router is working."""
    return {
        "message": "Roles router is working!",
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/debug/connection")
async def debug_connection():
    """Debug database connection."""
    import asyncio

    steps = []
    start_total = time.time()

    try:
        steps.append(f"Start: {datetime.now().isoformat()}")
        steps.append(f"Render Env: {os.getenv('RENDER')}")
        steps.append(f"DB URL: {DATABASE_URL[:20]}...")

        from app.db_pool import get_pool

        t0 = time.time()
        pool = await get_pool()
        steps.append(f"Get Pool: {time.time() - t0:.4f}s")

        t1 = time.time()
        try:
            async with asyncio.timeout(5.0):
                async with pool.acquire() as conn:
                    steps.append(f"Acquire Connection: {time.time() - t1:.4f}s")

                    t2 = time.time()
                    version = await conn.fetchval("SELECT version()")
                    steps.append(f"Execute Query: {time.time() - t2:.4f}s")
                    steps.append(f"DB Version: {version}")
        except asyncio.TimeoutError:
            steps.append("Acquire Connection: TIMED OUT (5s)")
            raise Exception("Connection acquisition timed out")

        steps.append(f"Total Time: {time.time() - start_total:.4f}s")

        return {
            "status": "success",
            "steps": steps,
        }
    except Exception as exc:
        steps.append(f"Error: {exc}")
        return {
            "status": "error",
            "steps": steps,
            "error": str(exc),
        }


@app.get(f"{settings.API_V1_PREFIX}/dss/dashboard")
async def get_dss_dashboard(request: Request, db: DatabaseManager = Depends(get_database)):
    """Get DSS dashboard data with optional role-based menu/actions."""
    try:
        default_dashboard = {
            "status": "success",
            "dashboard": {
                "summary_metrics": {
                    "total_users": 1247,
                    "total_orders": 15420,
                    "total_revenue": 45670000,
                    "conversion_rate": 3.2,
                    "system_health": "Excellent",
                },
                "recommendations": [],
                "action_plans": [],
            },
            "timestamp": datetime.now().isoformat(),
            "menu": {
                "modules": ["Dashboard"],
                "actions": ["view"],
                "admin_features": {
                    "user_management": False,
                    "activity_logs": False,
                    "system_settings": False,
                    "user_creation": False,
                    "user_deletion": False,
                },
            },
        }

        auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
        if not auth_header:
            return default_dashboard

        try:
            token = auth_header.split()[1]
        except Exception:
            return default_dashboard

        payload = decode_access_token(token, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)
        if not payload or "role" not in payload:
            return default_dashboard

        role = payload.get("role")
        role_cfg = get_role_menu(role)

        role_dashboard = {
            "status": "success",
            "dashboard": {
                "summary_metrics": {
                    "role": role,
                    "total_users": 1247 if role != "CUSTOMER" else 1,
                    "total_orders": 15420 if role in ("ADMIN", "ANALYST") else 12,
                    "total_revenue": 45670000 if role != "CUSTOMER" else 120000,
                    "conversion_rate": 3.2,
                },
                "recommendations": [],
                "action_plans": [],
            },
            "timestamp": datetime.now().isoformat(),
            "menu": role_cfg,
        }

        return role_dashboard

    except Exception as exc:
        logger.error("DSS dashboard error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/setup-activity-logs")
async def setup_activity_logs_direct(db: DatabaseManager = Depends(get_database)):
    """Create activity logs table and insert test data - Direct endpoint."""
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_activity_logs (
            log_id SERIAL PRIMARY KEY,
            user_id INTEGER,
            email VARCHAR(255),
            action VARCHAR(100) NOT NULL,
            resource VARCHAR(100),
            details JSONB,
            ip_address INET,
            user_agent TEXT,
            status VARCHAR(20) DEFAULT 'success',
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
        await db.execute_query(create_table_query)

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_activity_logs_user_id ON user_activity_logs(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON user_activity_logs(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_activity_logs_action ON user_activity_logs(action)",
        ]

        for index_query in indexes:
            await db.execute_query(index_query)

        test_data_query = """
        INSERT INTO user_activity_logs (user_id, email, action, resource, details, ip_address, status)
        VALUES 
            (1, 'admin@dss.com', 'USER_SIGNIN', '/api/v1/auth/signin', '{"role": "ADMIN"}', '127.0.0.1', 'success'),
            (2, 'analyst@dss.com', 'USER_SIGNIN', '/api/v1/auth/signin', '{"role": "ANALYST"}', '127.0.0.1', 'success'),
            (3, 'customer@dss.com', 'GET /api/v1/dss/dashboard', '/api/v1/dss/dashboard', '{"status_code": 200}', '127.0.0.1', 'success'),
            (1, 'admin@dss.com', 'GET /api/v1/admin/users', '/api/v1/admin/users', '{"status_code": 200}', '127.0.0.1', 'success'),
            (2, 'analyst@dss.com', 'USER_SIGNOUT', '/api/v1/auth/signout', '{"method": "manual"}', '127.0.0.1', 'success')
        """
        await db.execute_query(test_data_query)

        return {
            "success": True,
            "message": "Activity logs table created and test data inserted successfully!",
            "table_created": True,
            "test_data_inserted": 5,
        }

    except Exception as exc:
        logger.error("Setup activity logs error: %s", exc)
        return {
            "success": False,
            "message": f"Setup failed: {exc}",
            "error": str(exc),
        }


@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "message": "The requested endpoint was not found",
            "path": str(request.url.path),
            "timestamp": datetime.now().isoformat(),
            "available_endpoints": [
                "/",
                "/health",
                "/api/v1/status",
                "/api/v1/auth/signin",
                "/api/v1/auth/signup",
                "/api/v1/auth/signout",
                "/api/v1/auth/verify-email",
                "/api/v1/dss/dashboard",
                "/api/v1/admin/users",
                "/api/v1/profile",
                "/api/v1/admin/activity-logs",
                "/api/v1/admin/activity-stats",
                "/api/v1/admin/user-activity/{user_id}",
                "/setup-activity-logs",
                "/api/v1/test-admin/users",
                "/api/v1/test-admin/profile/{user_id}",
                "/api/v1/test-admin/get-token",
                "/api/v1/roles",
                "/api/v1/roles/{role_id}",
                "/docs",
            ],
        },
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc: Exception):
    logger.error("Internal server error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred",
            "timestamp": datetime.now().isoformat(),
        },
    )


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=False,
        workers=1,
        log_level="info",
    )
