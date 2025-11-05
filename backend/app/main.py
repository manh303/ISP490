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
import smtplib
import bcrypt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# FastAPI and async
from fastapi import FastAPI, HTTPException, Depends, Request
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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================
# CONFIGURATION
# ====================================
class Settings:
    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    # Database URLs
    POSTGRES_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:69420@localhost:5432/ecommerce_dss")

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

# ====================================
# INITIALIZE COMPONENTS
# ====================================
db_manager = DatabaseManager()

# ====================================
# FASTAPI APPLICATION
# ====================================
from fastapi.openapi.utils import get_openapi
from fastapi.security import HTTPBearer

app = FastAPI(
    title="Vietnam E-commerce DSS API",
    description="""
    ## Vietnam E-commerce Decision Support System API
    
    ### 🔐 Authentication Required
    **IMPORTANT**: Click the **🔒 Authorize** button below to authenticate!
    
    **Quick Start**:
    1. **Easy Testing**: Use `/api/v1/test-admin/users` (no auth needed)
    2. **With Auth**: Get token from `/api/v1/test-admin/get-token`
    3. **Click 🔒 Authorize**: Enter `Bearer <your_token>`
    
    **Default Admin**: `admin@dss.com` / `admin123`
    
    ### 📋 User Management Features
    - ✅ Create, Read, Update users
    - ✅ Soft delete (move to deleted list)
    - ✅ Restore from deleted list  
    - ✅ Permanent delete (with confirmation)
    - ✅ Role-based access control
    """,
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "DSS API Support",
        "email": "admin@dss.com"
    }
)

# Add security scheme for Swagger UI
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Vietnam E-commerce DSS API",
        version="2.0.0",
        description=app.description,
        routes=app.routes,
    )
    openapi_schema["components"]["securitySchemes"] = {
        "HTTPBearer": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "Enter: Bearer <your_token>"
        }
    }
    # Add security to all admin endpoints
    for path, path_item in openapi_schema["paths"].items():
        if "/admin/" in path and path != "/api/v1/admin/test/admin-token":
            for method, operation in path_item.items():
                if method.lower() in ["get", "post", "put", "delete"]:
                    operation["security"] = [{"HTTPBearer": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

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
    from api.v1.profile import router as profile_router
    app.include_router(profile_router, prefix=f"{settings.API_V1_PREFIX}")
    logger.info("Profile routes included")
except ImportError as e:
    logger.warning(f"Profile routes not available: {e}")

# Include Test Admin router
try:
    from api.v1.test_admin import router as test_admin_router
    app.include_router(test_admin_router, prefix=f"{settings.API_V1_PREFIX}")
    logger.info("Test Admin routes included")
except ImportError as e:
    logger.warning(f"Test Admin routes not available: {e}")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS if not settings.DEBUG else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

# ====================================
# STARTUP/SHUTDOWN EVENTS
# ====================================
@app.on_event("startup")
async def startup_event():
    """Initialize connections"""
    logger.info("Starting Vietnam E-commerce DSS API...")

    # Set startup time
    app.state.start_time = time.time()

    # Try PostgreSQL connection
    if DATABASE_AVAILABLE:
        try:
            await db_manager.connect()
            if db_manager.is_connected:
                logger.info("PostgreSQL database connected successfully!")

                # Initialize IAM service if available
                if IAM_AVAILABLE:
                    try:
                        init_iam_service(db_manager, settings.JWT_SECRET_KEY)
                        logger.info("IAM service initialized successfully!")
                    except Exception as e:
                        logger.error(f"IAM service initialization failed: {e}")
        except Exception as e:
            logger.warning(f"PostgreSQL database connection failed: {e}")

    logger.info("API startup completed successfully!")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup connections"""
    logger.info("Shutting down API...")
    try:
        await db_manager.disconnect()
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    logger.info("API shutdown completed")

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

# ====================================
# DSS ENDPOINTS (Essential for Dashboard)
# ====================================
@app.get(f"{settings.API_V1_PREFIX}/dss/dashboard")
async def get_dss_dashboard(request: Request, db: DatabaseManager = Depends(get_database)):
    """Get DSS dashboard data. If Authorization header with valid JWT is provided,
    return a role-specific dashboard and allowed actions/menus for that role.
    """
    try:
        # Role-based menu definitions
        ROLE_DASHBOARDS = {
            "ADMIN": {
                "modules": ["Dashboard", "User Management", "System Settings", "Audit Logs", "Data Management"],
                "actions": ["view", "create", "update", "delete"],
                "permissions": ["system.admin", "user.manage", "data.write", "analytics.view", "dss.dashboard"]
            },
            "ANALYST": {
                "modules": ["Dashboard", "Customer Analytics", "Sales Analytics", "Reports", "Data Visualization"],
                "actions": ["view", "export", "analyze"],
                "permissions": ["data.read", "analytics.view", "reports.generate", "dss.dashboard"]
            },
            "CUSTOMER": {
                "modules": ["Dashboard", "Orders", "Profile", "Purchase History"],
                "actions": ["view", "create_order", "update_profile"],
                "permissions": ["profile.view", "orders.create", "data.read_own"]
            }
        }

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
                "actions": ["view"]
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

        payload = decode_access_token(token)
        if not payload or "role" not in payload:
            return default_dashboard

        role = payload.get("role")
        role_cfg = ROLE_DASHBOARDS.get(role, ROLE_DASHBOARDS.get("CUSTOMER"))

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

class ResetPasswordResponse(BaseModel):
    success: bool
    message: str

# Define valid users for authentication - 3 main roles
VALID_USERS = {
    "admin@dss.com": {
        "user_id": 1,
        "password": "admin123",
        "full_name": "System Administrator",
        "role": "ADMIN"
    },
    "analyst@dss.com": {
        "user_id": 2,
        "password": "analyst123",
        "full_name": "Data Analyst",
        "role": "ANALYST"
    },
    "customer@dss.com": {
        "user_id": 3,
        "password": "customer123",
        "full_name": "Customer User",
        "role": "CUSTOMER"
    }
}

async def authenticate_user_db(email: str, password: str, db: DatabaseManager):
    """Authenticate user from database"""
    try:
        query = """
        SELECT user_id, email, password_hash, full_name, status
        FROM iam_user
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
        FROM iam_user_role ur
        JOIN iam_role r ON ur.role_id = r.role_id
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

# ---------- REPLACE this function in main.py ----------
def create_access_token(user_data: dict, email: str):
    """Create JWT access token with role, roles[], permissions for cross-service guards"""
    jwt_exp_hours = getattr(settings, 'JWT_EXPIRATION_HOURS', None)
    jwt_exp_minutes_env = os.getenv("JWT_EXPIRE_MINUTES")

    if jwt_exp_minutes_env:
        try:
            minutes = int(jwt_exp_minutes_env)
            exp_time = datetime.utcnow() + timedelta(minutes=minutes)
        except Exception:
            exp_time = datetime.utcnow() + timedelta(hours=(jwt_exp_hours or 24))
    else:
        exp_time = datetime.utcnow() + timedelta(hours=(jwt_exp_hours or 24))

    # Default permissions per role (extend as needed)
    DEFAULT_PERMS = {
        "ADMIN": [
            "system.admin", "user.manage", "data.read", "analytics.view", "dss.dashboard"
        ],
        "ANALYST": ["data.read", "analytics.view"],
        "CUSTOMER": ["data.read"]
    }

    role = user_data['role']
    token_payload = {
        "user_id": user_data['user_id'],
        "email": email.lower(),
        "full_name": user_data.get('full_name', 'User'),  # Add full_name to token
        "role": role,                          # keep string for backward compat
        "roles": [role],                       # <-- NEW: array for Node middleware
        "permissions": DEFAULT_PERMS.get(role, []),  # <-- NEW: basic perms
        "exp": exp_time,
        "iat": datetime.utcnow()
    }
    return jwt.encode(token_payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
# ---------- END REPLACE ----------

def decode_access_token(token: str) -> Optional[dict]:
    """Decode JWT token and return payload or None"""
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        return payload
    except Exception as e:
        logger.debug(f"Token decode failed: {e}")
        return None

# ====================================
# EMAIL SERVICE
# ====================================
class EmailService:
    """Simple email service for verification codes"""

    def __init__(self):
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_username = os.getenv("manhndhe173383@fpt.edu.vn")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.from_email = os.getenv("FROM_EMAIL", self.smtp_username)

    def send_verification_email(self, to_email: str, verification_code: str, user_name: str) -> bool:
        """Send email verification code"""
        try:
            # For development, just log the code
            if not self.smtp_username or not self.smtp_password:
                logger.info(f"DEV MODE: Verification code for {to_email}: {verification_code}")
                return True

            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = to_email
            msg['Subject'] = "Verify Your Email - E-commerce DSS"

            html = f"""
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

            msg.attach(MIMEText(html, 'html'))

            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.smtp_username, self.smtp_password)
            text = msg.as_string()
            server.sendmail(self.from_email, to_email, text)
            server.quit()

            return True
        except Exception as e:
            logger.error(f"Failed to send verification email: {e}")
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
    INSERT INTO iam_user (email, password_hash, full_name, status, created_at, updated_at)
    VALUES ($1, $2, $3, 'active', NOW(), NOW())
    RETURNING user_id
    """
    result = await db.execute_query(query, (email, password_hash, name))

    if not result:
        raise HTTPException(status_code=500, detail="Failed to create user")

    user_id = result[0]['user_id']

    # Assign default CUSTOMER role
    role_query = """
    INSERT INTO iam_user_role (user_id, role_id, assigned_at)
    SELECT $1, role_id, NOW() FROM iam_role WHERE role_code = 'CUSTOMER'
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
    UPDATE iam_user
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

    query = "SELECT user_id FROM iam_user WHERE email = $1"
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

        access_token = create_access_token(user_data, request.email)

        logger.info(f"Successful signin for: {request.email} with role: {user_data['role']}")

        # Role -> menu mapping to return allowed screens/actions for frontend
        ROLE_MENU = {
            "ADMIN": {"modules": ["Dashboard", "User Management", "System Settings", "Audit Logs", "Data Management"], "actions": ["view","create","update","delete"]},
            "ANALYST": {"modules": ["Dashboard", "Customer Analytics", "Sales Analytics", "Reports", "Data Visualization"], "actions": ["view","export","analyze"]},
            "CUSTOMER": {"modules": ["Dashboard", "Orders", "Profile", "Purchase History"], "actions": ["view","create_order","update_profile"]}
        }

        role_cfg = ROLE_MENU.get(user_data['role'], ROLE_MENU['CUSTOMER'])

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

        # Check if email exists
        email_exists = await check_email_exists(db, email)
        
        # Always respond success (avoid user enumeration)
        if not email_exists:
            logger.info(f"Forgot password requested for non-existent email: {email}")
            return ForgotPasswordResponse(
                success=True,
                message="If this email exists, we've sent a reset code."
            )

        # Use the new EmailService to send OTP
        if email_service_module:
            try:
                result = await send_otp_email(email, name="User")
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

@app.post(f"{settings.API_V1_PREFIX}/auth/reset-password", response_model=ResetPasswordResponse)
async def reset_password(request: ResetPasswordRequest, db: DatabaseManager = Depends(get_database)):
    """Reset password using OTP"""
    try:
        email = request.email.strip().lower()
        otp = request.otp.strip()
        new_password = request.new_password

        # Validate input
        if not email or not otp or not new_password:
            raise HTTPException(status_code=400, detail="Email, OTP, and new password are required")

        if len(new_password) < 8:
            raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

        # Verify OTP
        if email_service_module:
            otp_result = await verify_otp(email, otp)
            
            if not otp_result.get('valid'):
                error = otp_result.get('error', 'Invalid OTP')
                attempts = otp_result.get('attempts_remaining', 0)
                raise HTTPException(
                    status_code=400,
                    detail=f"{error}. Attempts remaining: {attempts}"
                )

            # OTP verified - reset password
            password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            query = """
            UPDATE iam_user
            SET password_hash = $1, updated_at = NOW()
            WHERE email = $2
            RETURNING user_id
            """
            result = await db.execute_query(query, (password_hash, email))
            
            if not result:
                raise HTTPException(status_code=404, detail="User not found")
            
            logger.info(f"Password reset successful for {email}")
            return ResetPasswordResponse(
                success=True,
                message="Password reset successfully. You can now sign in."
            )
        else:
            raise HTTPException(status_code=500, detail="OTP verification service not available")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reset password error: {e}")
        raise HTTPException(status_code=500, detail=f"Password reset failed: {str(e)}")



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

        payload = decode_access_token(token)
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

        if len(request.password) < 8:
            raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

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
                "/api/v1/auth/signin", "/api/v1/auth/signup", "/api/v1/auth/verify-email",
                "/api/v1/dss/dashboard", "/api/v1/admin/users", "/api/v1/profile",
                "/api/v1/test-admin/users", "/api/v1/test-admin/profile/{user_id}", "/api/v1/test-admin/get-token", "/docs"
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