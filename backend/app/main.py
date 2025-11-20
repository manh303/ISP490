#!/usr/bin/env python3
"""
Simplified Vietnam E-commerce DSS Backend
Only essential APIs for dashboard and authentication
"""

import os
import sys
import time
import logging
import secrets
import hashlib
import bcrypt
import requests
from datetime import datetime, timedelta
from typing import Dict, Optional

# Setup logging FIRST (before other imports)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("✅ Loaded .env file")
except ImportError:
    logger.warning("python-dotenv not installed, using system environment variables")

# Ensure parent directory of `app` is on sys.path
parent_dir = os.path.dirname(os.path.dirname(__file__))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Now import after path setup
try:
        from .middleware.activity_middleware import ActivityLoggingMiddleware
        from .services.activity_logger import ActivityLogger
        ACTIVITY_AVAILABLE = True
except ImportError:
        ACTIVITY_AVAILABLE = False
        logger.warning("Activity logging not available")
        
try:
    from pydantic import field_validator
    from app.utils.validators import validate_phone, validate_password, validate_email
    from app.constants.roles import ROLE_MENUS, get_role_menu
    VALIDATORS_AVAILABLE = True
except ImportError:
    VALIDATORS_AVAILABLE = False
    field_validator = lambda x: lambda f: f
    validate_phone = lambda x: x
    def validate_password(x, **kwargs):
        return x
    def validate_email(email: str):
        return email
    ROLE_MENUS = {}
    get_role_menu = lambda x: {}

# FastAPI and async
from fastapi import FastAPI, HTTPException, Depends, Request
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import uvicorn

# Database connections
try:
    from databases import Database
    import asyncpg
    DATABASE_AVAILABLE = True
    print("Database modules imported successfully")
except ImportError:
    DATABASE_AVAILABLE = False
    print("WARNING: Database modules not available")

# Import IAM system
try:
    from .api.v1.auth import router as auth_router, init_iam_service
    IAM_AVAILABLE = True
    print("IAM system imported successfully")
except ImportError:
    try:
        # Try alternative import path
        import sys
        import os
        sys.path.append(os.path.dirname(__file__))
        from api.v1.auth import router as auth_router, init_iam_service
        IAM_AVAILABLE = True
        print("IAM system imported successfully (alternative path)")
    except ImportError as e2:
        IAM_AVAILABLE = False
        print(f"WARNING: IAM system not available: {e2}")

# Import email service
try:
    from .email_service import EmailService, send_otp_email, verify_otp
    email_service_module = True
    print("Email service imported successfully")
except ImportError:
    try:
        import sys
        import os
        sys.path.append(os.path.dirname(__file__))
        from email_service import EmailService, send_otp_email, verify_otp
        email_service_module = True
        print("Email service imported successfully (alternative path)")
    except ImportError as e2:
        email_service_module = False
        print(f"WARNING: Email service not available: {e2}")

# ====================================
# CONFIGURATION
# ====================================
class Settings:
    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")  # Use production to connect to Render
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    # Database URLs - Always use Render database (has data)
    POSTGRES_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"
    )

    # API Configuration
    API_V1_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "Vietnam E-commerce DSS API"
    VERSION: str = "2.0.0"
    HOST: str = os.getenv("API_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", 8000))

    # Security
    SECRET_KEY: str = os.getenv("SECRET_KEY", "sY-A335Mj9qloyUE94maevhmrg25MZ3RxbVhBYAhmu5QnIS1qsCKIiiGjRshkZA4OSwZN2k2O5VSzDn3XdZo5A")
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "KvyFNJHBkDzgAwCsx659EvNCa9tWUsOlIKpoQZztIyg")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    JWT_EXPIRATION_HOURS: int = int(os.getenv("JWT_EXPIRATION_HOURS", 24))

    # CORS
    CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "*").split(",")

settings = Settings()

# ====================================
# PYDANTIC MODELS
# ====================================
class HealthCheck(BaseModel):
    status: str
    timestamp: str
    services: Dict[str, str]

# Signup models
class SignupRequest(BaseModel):
    name: str = Field(..., description="Full name")
    email: str = Field(..., description="Email address")
    password: str = Field(..., min_length=8, description="Password (min 8 chars)")
    confirm_password: str = Field(..., description="Confirm password")
    phone: Optional[str] = Field(None, description="Phone number")
    
    @field_validator('password')
    @classmethod
    def validate_password_field(cls, v):
        return validate_password(v, min_length=8)
    
    @field_validator('phone')
    @classmethod
    def validate_phone_field(cls, v):
        return validate_phone(v)
    
    @field_validator('email')
    @classmethod
    def validate_email_field(cls, v):
        return validate_email(v)

class SignupResponse(BaseModel):
    success: bool
    message: str
    verification_sent: bool
    email: str

class VerifyEmailRequest(BaseModel):
    email: str = Field(..., description="Email address")
    verification_code: str = Field(..., description="6-digit verification code")

class VerifyEmailResponse(BaseModel):
    success: bool
    message: str
    user_created: bool
    user_id: Optional[int]

# ====================================
# DATABASE CONNECTION
# ====================================
class DatabaseManager:
    def __init__(self):
        self.database = None
        self.is_connected = False

    async def connect(self):
        """Connect to PostgreSQL database"""
        if not DATABASE_AVAILABLE:
            logger.warning("Database modules not available, running in mock mode")
            return

        try:
            self.database = Database(settings.POSTGRES_URL)
            await self.database.connect()
            self.is_connected = True
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            self.is_connected = False

    async def disconnect(self):
        """Disconnect from database"""
        if self.database and self.is_connected:
            await self.database.disconnect()
            self.is_connected = False
            logger.info("Disconnected from database")

    async def execute_query(self, query: str, values = None):
        """Execute a query safely - supports both dict and tuple parameters"""
        if not self.is_connected:
            return []

        try:
            if values:
                if isinstance(values, (tuple, list)):
                    # Convert positional parameters to named parameters for databases library
                    # Find $1, $2, etc. and replace with :param1, :param2, etc.
                    import re
                    converted_query = query
                    converted_values = {}

                    # Find all $n parameters
                    param_matches = re.findall(r'\$(\d+)', query)
                    if param_matches:
                        for param_num in param_matches:
                            param_name = f"param{param_num}"
                            converted_query = converted_query.replace(f"${param_num}", f":{param_name}")
                            param_index = int(param_num) - 1  # Convert to 0-based index
                            if param_index < len(values):
                                converted_values[param_name] = values[param_index]

                        result = await self.database.fetch_all(converted_query, converted_values)
                    else:
                        # No positional parameters, use as-is
                        result = await self.database.fetch_all(query, dict(zip(range(len(values)), values)))
                else:
                    # Dict parameters (original behavior)
                    result = await self.database.fetch_all(query, values)
            else:
                result = await self.database.fetch_all(query)
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Values: {values}")
            return []

    def transaction(self):
        """Return transaction context manager for atomic operations"""
        @asynccontextmanager
        async def _transaction():
            # Access raw asyncpg pool from databases library
            pool = self.database._backend._pool
            async with pool.acquire() as conn:
                async with conn.transaction():
                    yield conn
        return _transaction()

# ====================================
# INITIALIZE COMPONENTS
# ====================================
db_manager = DatabaseManager()

# ====================================
# LIFESPAN EVENT HANDLER
# ====================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Vietnam E-commerce DSS API...")
    app.state.start_time = time.time()
    
    if DATABASE_AVAILABLE:
        try:
            await db_manager.connect()
            if db_manager.is_connected:
                logger.info("PostgreSQL database connected successfully!")
                if IAM_AVAILABLE:
                    try:
                        init_iam_service(db_manager, settings.JWT_SECRET_KEY)
                        logger.info("IAM service initialized successfully!")
                    except Exception as e:
                        logger.error(f"IAM service initialization failed: {e}")
        except Exception as e:
            logger.warning(f"PostgreSQL database connection failed: {e}")
    
    logger.info("API startup completed successfully!")
    
    yield
    
    # Shutdown
    logger.info("Shutting down API...")
    try:
        await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    logger.info("API shutdown completed")

# ====================================
# FASTAPI APPLICATION
# ====================================
from fastapi.openapi.utils import get_openapi

app = FastAPI(
    title="Vietnam E-commerce DSS API",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Remove custom OpenAPI to avoid duplicate security schemes
# FastAPI will auto-generate based on dependencies

# Include IAM router (temporarily disabled due to database parameter binding issues)
# if IAM_AVAILABLE:
#     app.include_router(auth_router, prefix=f"{settings.API_V1_PREFIX}")
#     logger.info("IAM routes included")

# Include Admin router
try:
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from api.v1.admin import router as admin_router
    app.include_router(admin_router, prefix=f"{settings.API_V1_PREFIX}")
    logger.info("Admin routes included")
except ImportError as e:
    logger.warning(f"Admin routes not available: {e}")

# Include Profile router
try:
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from api.v1.profile import router as profile_router
    app.include_router(profile_router, prefix=f"{settings.API_V1_PREFIX}")
    logger.info("✅ Profile routes included successfully")
except Exception as e:
    logger.error(f"❌ Profile routes failed: {e}")

# Include Role Management router
try:
    from api.v1.roles import router as roles_router
    app.include_router(roles_router, prefix=f"{settings.API_V1_PREFIX}")
    logger.info("Role Management routes included")
except ImportError as e:
    logger.warning(f"Role Management routes not available: {e}")

# Include Unified ML Insights & Predictions router
try:
    from api.v1.ml_insights import router as ml_unified_router
    app.include_router(ml_unified_router, prefix=f"{settings.API_V1_PREFIX}")
    logger.info("✅ ML Unified (Insights & Predictions) routes included")
except ImportError as e:
    logger.warning(f"ML Unified routes not available: {e}")

# Include Analytics router
try:
    from api.v1.analytics import router as analytics_router
    app.include_router(analytics_router, prefix=f"{settings.API_V1_PREFIX}")
    logger.info("Analytics routes included")
except ImportError as e:
    logger.warning(f"Analytics routes not available: {e}")

# Include Dashboard API (v1)
try:
    from api.v1.dashboard import router as dashboard_router
    app.include_router(dashboard_router, prefix=f"{settings.API_V1_PREFIX}/dashboard", tags=["Dashboard"])
    logger.info("Dashboard API routes included")
except ImportError as e:
    logger.warning(f"Dashboard API routes not available: {e}")

# Include ML Serving API (v1)
try:
    from api.v1.ml_serving import router as ml_serving_router
    app.include_router(ml_serving_router, prefix=f"{settings.API_V1_PREFIX}/ml", tags=["ML Serving"])
    logger.info("ML Serving API routes included")
except ImportError as e:
    logger.warning(f"ML Serving API routes not available: {e}")

# Include Reports API (v1)
try:
    from api.v1.reports import router as reports_router
    app.include_router(reports_router, prefix=f"{settings.API_V1_PREFIX}/reports", tags=["Reports"])
    logger.info("Reports API routes included")
except ImportError as e:
    logger.warning(f"Reports API routes not available: {e}")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS if not settings.DEBUG else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add activity logging middleware (only if available)
if ACTIVITY_AVAILABLE:
    app.add_middleware(ActivityLoggingMiddleware, db_manager=db_manager)
    logger.info("Activity logging middleware enabled")
else:
    logger.warning("Activity logging middleware disabled - module not available")

# Update security headers middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.update({
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Content-Security-Policy": "default-src 'self' 'unsafe-inline' 'unsafe-eval' https: data:; img-src 'self' data: https: blob:; style-src 'self' 'unsafe-inline' https:; script-src 'self' 'unsafe-inline' 'unsafe-eval' https:; font-src 'self' data: https:;",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "SAMEORIGIN",
        "X-XSS-Protection": "1; mode=block"
    })
    return response

# Startup/shutdown events now handled by lifespan context manager above

# ====================================
# DEPENDENCY INJECTION
# ====================================
async def get_database():
    """Get database connection"""
    return db_manager

# ====================================
# ESSENTIAL ENDPOINTS
# ====================================
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Vietnam E-commerce DSS API",
        "version": settings.VERSION,
        "timestamp": datetime.now().isoformat(),
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {},
        "uptime_seconds": time.time() - getattr(app.state, 'start_time', 0)
    }

    # Check database
    if db_manager.is_connected:
        try:
            result = await db_manager.execute_query("SELECT 1 as test")
            health_status["services"]["postgresql"] = "healthy"
        except Exception as e:
            health_status["services"]["postgresql"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    else:
        health_status["services"]["postgresql"] = "not connected"
        health_status["status"] = "degraded"

    return health_status

@app.get(f"{settings.API_V1_PREFIX}/status")
async def api_status():
    """API status endpoint"""
    return {
        "api": "Vietnam E-commerce DSS - Simplified",
        "version": settings.VERSION,
        "status": "operational",
        "database_connected": db_manager.is_connected,
        "features": {
            "database": DATABASE_AVAILABLE,
            "authentication": IAM_AVAILABLE
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get(f"{settings.API_V1_PREFIX}/check-roles")
async def check_database_roles():
    """Check roles in database"""
    try:
        if not db_manager.is_connected:
            await db_manager.connect()
        
        # Get all roles from database
        query = "SELECT role_id, role_code, role_name, description FROM iam_role ORDER BY role_code"
        roles = await db_manager.execute_query(query)
        
        return {
            "success": True,
            "total_roles": len(roles),
            "roles": roles,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }



@app.get(f"{settings.API_V1_PREFIX}/test-roles")
async def test_roles():
    """Test endpoint to check if roles router is working"""
    return {
        "message": "Roles router is working!",
        "timestamp": datetime.now().isoformat()
    }

# ====================================
# DSS ENDPOINTS (Essential for Dashboard)
# ====================================

@app.get(f"{settings.API_V1_PREFIX}/dss/ecommerce/overview")
async def get_ecommerce_overview(db: DatabaseManager = Depends(get_database)):
    """Get ecommerce platform overview metrics"""
    try:
        if not db.is_connected:
            # Return mock data if database not connected
            return {
                "success": True,
                "data": {
                    "total_products": 1250,
                    "platforms": 2,
                    "avg_price": 15000000,
                    "rated_products": 980
                },
                "timestamp": datetime.now().isoformat()
            }

        query = """
        SELECT
            source_platform,
            COUNT(*) as total_products,
            AVG(CAST(raw_data->>'price' AS DECIMAL)) as avg_price,
            COUNT(CASE WHEN raw_data->>'rating' IS NOT NULL THEN 1 END) as rated_products
        FROM stg_raw_products
        WHERE source_platform IN ('tiki', 'lazada')
        GROUP BY source_platform
        """

        result = await db.execute_query(query)

        # Aggregate metrics
        total_products = sum(row['total_products'] for row in result)
        platforms = len(result)
        avg_price = sum(row['avg_price'] or 0 for row in result) / len(result) if result else 0
        rated_products = sum(row['rated_products'] for row in result)

        return {
            "success": True,
            "data": {
                "total_products": total_products,
                "platforms": platforms,
                "avg_price": avg_price,
                "rated_products": rated_products,
                "platform_breakdown": result
            },
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Ecommerce overview error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get(f"{settings.API_V1_PREFIX}/dss/ecommerce/price-comparison")
async def get_price_comparison(db: DatabaseManager = Depends(get_database)):
    """Get price comparison data between platforms"""
    try:
        if not db.is_connected:
            # Return mock data
            return {
                "success": True,
                "data": [
                    {"source_platform": "tiki", "product_name": "iPhone 15", "brand": "Apple", "price": 25000000, "discount": 10},
                    {"source_platform": "lazada", "product_name": "Samsung Galaxy", "brand": "Samsung", "price": 22000000, "discount": 12}
                ],
                "timestamp": datetime.now().isoformat()
            }

        query = """
        SELECT
            source_platform,
            raw_data->>'product_name' as product_name,
            raw_data->>'brand' as brand,
            CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL) as price,
            CAST(raw_data->>'discount_percent' AS DECIMAL) as discount
        FROM stg_raw_products
        WHERE source_platform IN ('tiki', 'lazada')
            AND (raw_data->>'price' IS NOT NULL OR raw_data->>'price_current' IS NOT NULL)
            AND CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL) > 0
        LIMIT 100
        """

        result = await db.execute_query(query)

        return {
            "success": True,
            "data": result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Price comparison error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get(f"{settings.API_V1_PREFIX}/dss/ecommerce/brand-analysis")
async def get_brand_analysis(db: DatabaseManager = Depends(get_database)):
    """Get brand performance analysis"""
    try:
        if not db.is_connected:
            # Return mock data
            return {
                "success": True,
                "data": [
                    {"brand": "Apple", "source_platform": "tiki", "product_count": 50, "avg_price": 25000000, "avg_rating": 4.5},
                    {"brand": "Samsung", "source_platform": "lazada", "product_count": 45, "avg_price": 18000000, "avg_rating": 4.3}
                ],
                "timestamp": datetime.now().isoformat()
            }

        query = """
        SELECT
            COALESCE(raw_data->>'brand', raw_data->>'brand_name', 'Unknown') as brand,
            source_platform,
            COUNT(*) as product_count,
            AVG(CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL)) as avg_price,
            AVG(CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL)) as avg_rating
        FROM stg_raw_products
        WHERE source_platform IN ('tiki', 'lazada')
        GROUP BY brand, source_platform
        HAVING COUNT(*) > 1 AND brand != 'Unknown' AND brand IS NOT NULL
        ORDER BY product_count DESC
        LIMIT 20
        """

        result = await db.execute_query(query)

        return {
            "success": True,
            "data": result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Brand analysis error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get(f"{settings.API_V1_PREFIX}/dss/ecommerce/trending-products")
async def get_trending_products(db: DatabaseManager = Depends(get_database)):
    """Get trending products data"""
    try:
        if not db.is_connected:
            # Return mock data
            return {
                "success": True,
                "data": [
                    {"product_name": "iPhone 15 Pro", "brand": "Apple", "source_platform": "tiki", "price": 25000000, "rating": 4.5, "review_count": 150, "sold_count": 50},
                    {"product_name": "Samsung Galaxy S24", "brand": "Samsung", "source_platform": "lazada", "price": 22000000, "rating": 4.3, "review_count": 200, "sold_count": 75}
                ],
                "timestamp": datetime.now().isoformat()
            }

        query = """
        SELECT
            raw_data->>'product_name' as product_name,
            COALESCE(raw_data->>'brand', raw_data->>'brand_name') as brand,
            source_platform,
            CAST(COALESCE(raw_data->>'price', raw_data->>'price_current', '0') AS DECIMAL) as price,
            CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL) as rating,
            CAST(COALESCE(raw_data->>'review_count', '0') AS INTEGER) as review_count,
            CAST(COALESCE(raw_data->>'sold_count', '0') AS INTEGER) as sold_count,
            created_at
        FROM stg_raw_products
        WHERE source_platform IN ('tiki', 'lazada')
            AND raw_data->>'product_name' IS NOT NULL
            AND LENGTH(raw_data->>'product_name') > 5
        ORDER BY
            CAST(COALESCE(raw_data->>'review_count', '0') AS INTEGER) DESC,
            CAST(COALESCE(raw_data->>'rating', raw_data->>'rating_avg', '0') AS DECIMAL) DESC
        LIMIT 20
        """

        result = await db.execute_query(query)

        return {
            "success": True,
            "data": result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Trending products error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get(f"{settings.API_V1_PREFIX}/dss/dashboard")
async def get_dss_dashboard(request: Request, db: DatabaseManager = Depends(get_database)):
    """Get DSS dashboard data. If Authorization header with valid JWT is provided,
    return a role-specific dashboard and allowed actions/menus for that role.
    """
    try:
        # Use shared role definitions
        ROLE_DASHBOARDS = ROLE_MENUS

        # Default (unauthenticated) dashboard
        default_dashboard = {
            "status": "success",
            "dashboard": {
                "summary_metrics": {
                    "total_users": 1247,
                    "total_orders": 15420,
                    "total_revenue": 45670000,
                    "conversion_rate": 3.2,
                    "system_health": "Excellent"
                },
                "recommendations": [],
                "action_plans": []
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
                    "user_deletion": False
                }
            }
        }

        # Check Authorization header
        auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
        if not auth_header:
            return default_dashboard

        # Expect "Bearer <token>"
        try:
            token = auth_header.split()[1]
        except Exception:
            return default_dashboard

        payload = decode_access_token(token, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)
        if not payload or "role" not in payload:
            return default_dashboard

        role = payload.get("role")
        role_cfg = get_role_menu(role)

        # Role-specific dashboard data
        role_dashboard = {
            "status": "success",
            "dashboard": {
                "summary_metrics": {
                    "role": role,
                    "total_users": 1247 if role != "CUSTOMER" else 1,
                    "total_orders": 15420 if role in ("ADMIN","ANALYST") else 12,
                    "total_revenue": 45670000 if role != "CUSTOMER" else 120000,
                    "conversion_rate": 3.2
                },
                "recommendations": [],
                "action_plans": []
            },
            "timestamp": datetime.now().isoformat(),
            "menu": role_cfg
        }

        return role_dashboard

    except Exception as e:
        logger.error(f"DSS dashboard error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Removed duplicate setup endpoint - use /setup-activity-logs instead

@app.get(f"{settings.API_V1_PREFIX}/dss/overview")
async def dss_overview():
    """DSS system overview"""
    return {
        "system": "Vietnam E-commerce Decision Support System",
        "modules": [
            "Customer Analytics",
            "Sales Analytics",
            "Inventory Management",
            "Recommendation Engine"
        ],
        "status": "operational",
        "database_connected": db_manager.is_connected,
        "timestamp": datetime.now().isoformat()
    }

@app.get(f"{settings.API_V1_PREFIX}/dss/modules")
async def get_dss_modules():
    """Get available DSS modules"""
    modules = [
        {
            "name": "Dashboard",
            "description": "Main dashboard with KPIs and metrics",
            "status": "active",
            "endpoint": "/api/v1/dss/dashboard"
        },
        {
            "name": "Customer Analytics",
            "description": "Customer segmentation and insights",
            "status": "active",
            "endpoint": "/api/v1/dss/customers/analytics"
        },
        {
            "name": "Sales Analytics",
            "description": "Sales performance and forecasting",
            "status": "active",
            "endpoint": "/api/v1/dss/sales/analytics"
        }
    ]

    return {
        "modules": modules,
        "total_modules": len(modules),
        "timestamp": datetime.now().isoformat()
    }

@app.post("/setup-activity-logs")
async def setup_activity_logs_direct(db: DatabaseManager = Depends(get_database)):
    """Create activity logs table and insert test data - Direct endpoint"""
    try:
        # Create table
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
        
        # Create indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_activity_logs_user_id ON user_activity_logs(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON user_activity_logs(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_activity_logs_action ON user_activity_logs(action)"
        ]
        
        for index_query in indexes:
            await db.execute_query(index_query)
        
        # Insert test data
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
            "test_data_inserted": 5
        }
        
    except Exception as e:
        logger.error(f"Setup activity logs error: {e}")
        return {
            "success": False,
            "message": f"Setup failed: {str(e)}",
            "error": str(e)
        }

# ====================================
# SIMPLE AUTHENTICATION (Temporary Fix)
# ====================================
import bcrypt
import jwt

class SignInRequest(BaseModel):
    email: str = Field(..., description="User email")
    password: str = Field(..., description="User password")

class SignInResponse(BaseModel):
    success: bool
    message: str
    access_token: str
    user: dict

# Forgot password models
class ForgotPasswordRequest(BaseModel):
    email: str

class ForgotPasswordResponse(BaseModel):
    success: bool
    message: str

class ResetPasswordRequest(BaseModel):
    email: str
    otp: str
    new_password: str
    
    @field_validator('new_password')
    @classmethod
    def validate_password_field(cls, v):
        return validate_password(v, min_length=8)

class ResetPasswordResponse(BaseModel):
    success: bool
    message: str

class SignOutResponse(BaseModel):
    success: bool
    message: str

# Define valid users for authentication - 3 main roles
# Note: user_ids must match database values
VALID_USERS = {
    "admin@dss.com": {
        "user_id": 1,
        "password": "admin123",
        "full_name": "System Administrator",
        "role": "ADMIN"
    },
    "analyst@dss.com": {
        "user_id": 3,
        "password": "analyst123",
        "full_name": "Data Analyst",
        "role": "ANALYST"
    },
    "ml@dss.com": {
        "user_id": 4,
        "password": "mleng123",
        "full_name": "ML Engineer",
        "role": "ML"
    },
    "dataeng@dss.com": {
        "user_id": 2,
        "password": "dataeng123",
        "full_name": "Data Engineer",
        "role": "DATA_ENGINEER"
    }
}

async def authenticate_user_db(email: str, password: str, db: DatabaseManager):
    """Authenticate user from database"""
    try:
        query = """
        SELECT user_id, email, password_hash, full_name, status
        FROM iam.iam_user
        WHERE email = $1 AND status = 'active'
        """
        result = await db.execute_query(query, (email.lower(),))
        
        if not result or len(result) == 0:
            return None
        
        user = result[0]
        password_hash = user['password_hash']
        
        # Verify password using bcrypt
        if not bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
            return None
        
        # Get user roles
        role_query = """
        SELECT r.role_code, r.role_name
        FROM iam.iam_user_role ur
        JOIN iam.iam_role r ON ur.role_id = r.role_id
        WHERE ur.user_id = $1
        """
        roles_result = await db.execute_query(role_query, (user['user_id'],))
        role_codes = [r['role_code'] for r in roles_result] if roles_result else []
        
        # Determine main role (first role or CUSTOMER by default)
        main_role = role_codes[0] if role_codes else 'CUSTOMER'
        
        return {
            'user_id': user['user_id'],
            'email': user['email'],
            'full_name': user['full_name'],
            'role': main_role,
            'roles': role_codes
        }
    except Exception as e:
        logger.error(f"Database authentication error: {e}")
        return None

def authenticate_user(email: str, password: str):
    """Fallback: authenticate user with hardcoded users"""
    user_data = VALID_USERS.get(email.lower())
    if not user_data or user_data["password"] != password:
        return None
    return user_data

# Use shared auth helpers
try:
    from app.utils.auth_helpers import create_access_token as create_jwt_token, decode_access_token
except ImportError:
    from utils.auth_helpers import create_access_token as create_jwt_token, decode_access_token

def create_access_token(user_data: dict, email: str):
    """Create JWT access token using shared helper"""
    jwt_exp_hours = getattr(settings, 'JWT_EXPIRATION_HOURS', 24)
    jwt_exp_minutes_env = os.getenv("JWT_EXPIRE_MINUTES")
    
    if jwt_exp_minutes_env:
        try:
            expire_hours = int(jwt_exp_minutes_env) / 60
        except Exception:
            expire_hours = jwt_exp_hours
    else:
        expire_hours = jwt_exp_hours
    
    return create_jwt_token(user_data, email, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM, expire_hours)

# ====================================
# EMAIL SERVICE (Mailjet)
# ====================================
try:
    import requests
    EMAIL_AVAILABLE = True
    logger.info("Email service available (Mailjet)")
except ImportError:
    EMAIL_AVAILABLE = False
    logger.warning("Email service not available: No module named 'requests'")

class EmailService:
    """Email service using Mailjet HTTP API"""

    def __init__(self):
        self.api_key = os.getenv("MAILJET_API_KEY")
        self.api_secret = os.getenv("MAILJET_API_SECRET")
        self.from_email = os.getenv("EMAIL_FROM")
        self.from_name = os.getenv("EMAIL_FROM_NAME", "E-commerce DSS")
        self.mailjet_url = "https://api.mailjet.com/v3.1/send"

    def send_verification_email(self, to_email: str, verification_code: str, user_name: str) -> bool:
        """Send email verification code using Mailjet API"""
        try:
            # Nếu thiếu config thì bật dev mode, không gửi thật
            if not self.api_key or not self.api_secret or not self.from_email:
                logger.warning(
                    f"DEV MODE (Mailjet): Missing MAILJET_API_KEY / MAILJET_API_SECRET / EMAIL_FROM. "
                    f"Verification code for {to_email}: {verification_code}"
                )
                return True

            html_content = f"""
            <html>
            <body>
                <h2>Welcome to E-commerce DSS, {user_name}!</h2>
                <p>Please verify your email address by entering the following code:</p>
                <h1 style="color: #4CAF50; font-size: 32px; letter-spacing: 5px;">{verification_code}</h1>
                <p>This code will expire in 10 minutes.</p>
                <p>If you didn't request this verification, please ignore this email.</p>
                <br>
                <p>Best regards,<br>E-commerce DSS Team</p>
            </body>
            </html>
            """

            payload = {
                "Messages": [
                    {
                        "From": {
                            "Email": self.from_email,
                            "Name": self.from_name
                        },
                        "To": [
                            {
                                "Email": to_email,
                                "Name": user_name or to_email
                            }
                        ],
                        "Subject": "Verify Your Email - E-commerce DSS",
                        "HTMLPart": html_content
                    }
                ]
            }

            # Mailjet dùng Basic Auth
            auth = (self.api_key, self.api_secret)

            response = requests.post(
                self.mailjet_url,
                json=payload,
                auth=auth,
                timeout=10
            )

            if 200 <= response.status_code < 300:
                logger.info(f"[Mailjet] Verification email sent to {to_email}")
                return True
            else:
                logger.error(
                    f"[Mailjet] Failed to send email to {to_email}. "
                    f"Status: {response.status_code}, Body: {response.text}"
                )
                return False

        except Exception as e:
            logger.error(f"[Mailjet] Exception while sending verification email: {e}")
            return False

# Initialize email service
email_service = EmailService()


# ====================================
# DATABASE HELPERS
# ====================================
async def create_user_in_db(db: DatabaseManager, name: str, email: str, password_hash: str) -> int:
    """Create new user in database"""
    if not db.is_connected:
        raise HTTPException(status_code=500, detail="Database not connected")

    # Insert user
    query = """
    INSERT INTO iam.iam_user (email, password_hash, full_name, status, created_at, updated_at)
    VALUES ($1, $2, $3, 'active', NOW(), NOW())
    RETURNING user_id
    """
    result = await db.execute_query(query, (email, password_hash, name))

    if not result:
        raise HTTPException(status_code=500, detail="Failed to create user")

    user_id = result[0]['user_id']

    # Assign default CUSTOMER role
    role_query = """
    INSERT INTO iam.iam_user_role (user_id, role_id, assigned_at)
    SELECT $1, role_id, NOW() FROM iam.iam_role WHERE role_code = 'CUSTOMER'
    """
    await db.execute_query(role_query, (user_id,))

    return user_id

async def store_verification_token(db: DatabaseManager, email: str, token_hash: str) -> None:
    # Nếu đã có token trước đó, upsert hoặc ghi đè
    await db.execute_query(
        """
        INSERT INTO iam_email_verification (email, token_hash, created_at, expires_at, consumed)
        VALUES ($1, $2, NOW(), NOW() + INTERVAL '15 minutes', false)
        ON CONFLICT (email) DO UPDATE
        SET token_hash = EXCLUDED.token_hash,
            created_at = NOW(),
            expires_at = NOW() + INTERVAL '15 minutes',
            consumed = false
        """,
        (email, token_hash)
    )
async def verify_email_token(db: DatabaseManager, email: str, token_hash: str) -> bool:
    """Verify email verification token"""
    if not db.is_connected:
        return False

    query = """
    SELECT token_id FROM iam_email_verification_token
    WHERE email = $1 AND token_hash = $2 AND expires_at > NOW() AND used_at IS NULL
    """
    result = await db.execute_query(query, (email, token_hash))

    if result:
        # Mark token as used
        update_query = """
        UPDATE iam_email_verification_token
        SET used_at = NOW()
        WHERE token_id = $1
        """
        await db.execute_query(update_query, (result[0]['token_id'],))
        return True

    return False

async def activate_user(db: DatabaseManager, email: str) -> int:
    """Activate user account"""
    if not db.is_connected:
        raise HTTPException(status_code=500, detail="Database not connected")

    query = """
    UPDATE iam.iam_user
    SET status = 'active', updated_at = NOW()
    WHERE email = $1 AND status = 'pending'
    RETURNING user_id
    """
    result = await db.execute_query(query, (email,))

    if not result:
        raise HTTPException(status_code=400, detail="User not found or already activated")

    return result[0]['user_id']

async def check_email_exists(db: DatabaseManager, email: str) -> bool:
    """Check if email already exists"""
    if not db.is_connected:
        return False

    query = "SELECT user_id FROM iam.iam_user WHERE email = $1"
    result = await db.execute_query(query, (email,))
    return len(result) > 0

@app.post(f"{settings.API_V1_PREFIX}/auth/signin", response_model=SignInResponse)
async def simple_signin(request: SignInRequest, db: DatabaseManager = Depends(get_database)):
    """Simple database signin endpoint - matches frontend expectations"""
    try:
        logger.info(f"Simple signin attempt for: {request.email}")

        # Try database authentication first
        user_data = await authenticate_user_db(request.email, request.password, db)
        
        # Fallback to hardcoded users if DB auth fails
        if not user_data:
            user_data = authenticate_user(request.email, request.password)
        
        if not user_data:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # Update last login time
        try:
            try:
                from app.services.admin_service import AdminService
            except ImportError:
                from services.admin_service import AdminService
            admin_service = AdminService(db)
            await admin_service.update_last_login(user_data["user_id"])
        except Exception as e:
            logger.error(f"Failed to update last login: {e}")

        access_token = create_access_token(user_data, request.email)

        # Log signin activity
        try:
            activity_logger = ActivityLogger(db)
            await activity_logger.log_activity(
                user_id=user_data["user_id"],
                email=user_data["email"],
                action="USER_SIGNIN",
                resource="/api/v1/auth/signin",
                details={"role": user_data["role"], "method": "password"},
                status="success"
            )
        except Exception as e:
            logger.error(f"Failed to log signin activity: {e}")

        logger.info(f"Successful signin for: {request.email} with role: {user_data['role']}")

        # Use shared role menu definitions

        role_cfg = get_role_menu(user_data['role'])

        return SignInResponse(
            success=True,
            message=f"Welcome back, {user_data['full_name']}!",
            access_token=access_token,
            user={
                "user_id": user_data['user_id'],
                "email": request.email,
                "full_name": user_data['full_name'],
                "role": user_data['role'],
                "menu": role_cfg
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Signin error: {e}")
        raise HTTPException(status_code=500, detail="Signin failed")

# Forgot Password - with OTP generation and email
@app.post(f"{settings.API_V1_PREFIX}/auth/forgot-password", response_model=ForgotPasswordResponse)
async def forgot_password(request: ForgotPasswordRequest, db: DatabaseManager = Depends(get_database)):
    """Request password reset OTP code"""
    try:
        email = request.email.strip().lower()
        if not email:
            raise HTTPException(status_code=400, detail="Email is required")

        # Check if email exists and get user info
        user_query = "SELECT full_name FROM iam.iam_user WHERE email = $1"
        user_result = await db.execute_query(user_query, (email,))
        
        # Always respond success (avoid user enumeration)
        if not user_result:
            logger.info(f"Forgot password requested for non-existent email: {email}")
            return ForgotPasswordResponse(
                success=True,
                message="If this email exists, we've sent a reset code."
            )
        
        # Get user's full name
        user_name = user_result[0].get('full_name', 'User') if user_result else 'User'

        # Use the new EmailService to send OTP
        if email_service_module:
            try:
                result = await send_otp_email(email, name=user_name)
                if result['success']:
                    logger.info(f"✅ OTP sent to {email}")
                    return ForgotPasswordResponse(
                        success=True,
                        message="A verification code has been sent to your email."
                    )
                else:
                    logger.error(f"Failed to send OTP: {result.get('error')}")
                    return ForgotPasswordResponse(
                        success=False,
                        message="Failed to send email. Please try again later."
                    )
            except Exception as e:
                logger.error(f"Email service error: {e}")
                return ForgotPasswordResponse(
                    success=False,
                    message="Failed to send email. Please check server logs."
                )
        else:
            # Fallback if email service not available
            logger.warning("Email service not available - check logs for OTP")
            return ForgotPasswordResponse(
                success=True,
                message="Reset code would be sent (check server logs in dev mode)"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Forgot-password error: {e}")
        raise HTTPException(status_code=500, detail="Forgot password failed")





@app.post(f"{settings.API_V1_PREFIX}/auth/signout", response_model=SignOutResponse)
async def signout(request: Request):
    """Sign out user - invalidate token on client side"""
    try:
        # Check Authorization header
        auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
        if not auth_header:
            return SignOutResponse(
                success=True,
                message="Signed out successfully"
            )

        # Expect "Bearer <token>"
        try:
            token = auth_header.split()[1]
            payload = decode_access_token(token, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)
            if payload:
                user_email = payload.get("email", "unknown")
                user_id = payload.get("user_id")
                
                # Log signout activity
                try:
                    activity_logger = ActivityLogger(db_manager)
                    await activity_logger.log_activity(
                        user_id=user_id,
                        email=user_email,
                        action="USER_SIGNOUT",
                        resource="/api/v1/auth/signout",
                        details={"method": "manual"},
                        status="success"
                    )
                except Exception as e:
                    logger.error(f"Failed to log signout activity: {e}")
                
                logger.info(f"User signed out: {user_email}")
        except Exception:
            pass  # Token invalid, but still return success

        return SignOutResponse(
            success=True,
            message="Signed out successfully"
        )

    except Exception as e:
        logger.error(f"Signout error: {e}")
        return SignOutResponse(
            success=True,
            message="Signed out successfully"
        )

@app.get("/api/auth/profile")
async def get_auth_profile(request: Request):
    """Get current user profile from token"""
    try:
        # Check Authorization header
        auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
        if not auth_header:
            raise HTTPException(status_code=401, detail="Authorization header missing")

        # Expect "Bearer <token>"
        try:
            token = auth_header.split()[1]
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid authorization format")

        payload = decode_access_token(token, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)
        if not payload:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Return user profile from token
        return {
            "success": True,
            "user": {
                "user_id": payload.get("user_id"),
                "email": payload.get("email"),
                "full_name": payload.get("full_name", "User"),  # Add full_name
                "role": payload.get("role"),
                "roles": payload.get("roles", []),
                "permissions": payload.get("permissions", [])
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Profile error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get profile")

# ====================================
# SIGNUP ENDPOINTS
# ====================================
@app.post(f"{settings.API_V1_PREFIX}/auth/signup", response_model=SignupResponse)
async def signup(request: SignupRequest, db: DatabaseManager = Depends(get_database)):
    """User registration endpoint with email verification"""
    try:
        # Validate input
        if request.password != request.confirm_password:
            raise HTTPException(status_code=400, detail="Passwords do not match")

        if len(request.password) < 6:
            raise HTTPException(status_code=400, detail="Mật khẩu phải có tối thiểu 6 ký tự")

        # Check if email already exists
        email_exists = await check_email_exists(db, request.email.lower())
        if email_exists:
            raise HTTPException(status_code=400, detail="Email already registered")

        # Generate verification code
        verification_code = f"{secrets.randbelow(900000) + 100000:06d}"  # 6-digit code
        token_hash = hashlib.sha256(verification_code.encode()).hexdigest()

        # Hash password
        password_hash = bcrypt.hashpw(request.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        # Create user in database (pending status)
        user_id = await create_user_in_db(db, request.name, request.email.lower(), password_hash)

        # Log signup activity
        try:
            activity_logger = ActivityLogger(db)
            await activity_logger.log_activity(
                user_id=user_id,
                email=request.email.lower(),
                action="USER_SIGNUP",
                resource="/api/v1/auth/signup",
                details={"full_name": request.name, "method": "email"},
                status="success"
            )
        except Exception as e:
            logger.error(f"Failed to log signup activity: {e}")

        # Store verification token
        # Temporarily disabled: email verification storage requires table iam_email_verification
        # await store_verification_token(db, request.email.lower(), token_hash)

        # Send verification email
        # Temporarily disabled email sending to avoid dependency during active-by-default signup
        email_sent = False

        logger.info(f"User signup: {request.email} (ID: {user_id}) - Email sent: {email_sent}")

        return SignupResponse(
            success=True,
            message="Account created successfully. You can now sign in.",
            verification_sent=email_sent,
            email=request.email
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Signup error: {e}")
        raise HTTPException(status_code=500, detail="Registration failed")

@app.post(f"{settings.API_V1_PREFIX}/auth/verify-email", response_model=VerifyEmailResponse)
async def verify_email(request: VerifyEmailRequest, db: DatabaseManager = Depends(get_database)):
    """Verify email with verification code"""
    try:
        # Hash the provided code
        token_hash = hashlib.sha256(request.verification_code.encode()).hexdigest()

        # Verify token
        token_valid = await verify_email_token(db, request.email.lower(), token_hash)

        if not token_valid:
            raise HTTPException(status_code=400, detail="Invalid or expired verification code")

        # Activate user account
        user_id = await activate_user(db, request.email.lower())

        logger.info(f"Email verified and account activated: {request.email} (ID: {user_id})")

        return VerifyEmailResponse(
            success=True,
            message="Email verified successfully! Your account is now active.",
            user_created=True,
            user_id=user_id
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Email verification error: {e}")
        raise HTTPException(status_code=500, detail="Email verification failed")

# ====================================
# ERROR HANDLERS
# ====================================
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
            "/", "/health", "/api/v1/status",
            "/api/v1/auth/signin", "/api/v1/auth/signup","/api/v1/auth/signout", "/api/v1/auth/verify-email",
            "/api/v1/dss/dashboard", "/api/v1/admin/users", "/api/v1/profile",
            "/api/v1/admin/activity-logs", "/api/v1/admin/activity-stats", "/api/v1/admin/user-activity/{user_id}",
            "/setup-activity-logs", "/api/v1/test-admin/users", "/api/v1/test-admin/profile/{user_id}", "/api/v1/test-admin/get-token",
            "/api/v1/roles", "/api/v1/roles/{role_id}", "/docs"

            ]
        }
    )

@app.exception_handler(500)
async def internal_error_handler(request: Request, exc: Exception):
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred",
            "timestamp": datetime.now().isoformat()
        }
    )

# ====================================
# DEVELOPMENT SERVER
# ====================================
if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=False,
        workers=1
    )