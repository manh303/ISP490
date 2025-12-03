"""
Complete IAM Authentication API endpoints
Full RBAC implementation with JWT tokens
"""

from fastapi import APIRouter, HTTPException, Depends, Request, status
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime
import os
import sys

# Import IAM service
try:
    from ...services.iam_service import IAMService
except ImportError:
    # Fallback import path
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from app.services.iam_service import IAMService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Authentication"])

# Pydantic Models
class LoginRequest(BaseModel):
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., min_length=1, description="User password")

class RegisterRequest(BaseModel):
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., min_length=8, description="User password (minimum 8 characters)")
    confirm_password: str = Field(..., min_length=8, description="Confirm password")
    full_name: Optional[str] = Field(None, max_length=100, description="Full name")
    phone: Optional[str] = Field(None, max_length=32, description="Phone number")

class UpdateProfileRequest(BaseModel):
    full_name: Optional[str] = Field(None, max_length=100)
    phone: Optional[str] = Field(None, max_length=32)

class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8)

class RefreshTokenRequest(BaseModel):
    refresh_token: str = Field(..., description="Refresh token")

class ForgotPasswordRequest(BaseModel):
    email: EmailStr = Field(..., description="User email address")

class ResetPasswordRequest(BaseModel):
    token: str = Field(..., description="Password reset token")
    new_password: str = Field(..., min_length=8, description="New password (minimum 8 characters)")
    confirm_password: str = Field(..., min_length=8, description="Confirm new password")

class ForgotPasswordOTPRequest(BaseModel):
    email: EmailStr = Field(..., description="User email address")

class VerifyOTPResetPasswordRequest(BaseModel):
    email: EmailStr = Field(..., description="User email address")
    otp: str = Field(..., min_length=6, max_length=6, description="6-digit OTP code")
    new_password: str = Field(..., min_length=8, description="New password (minimum 8 characters)")
    confirm_password: str = Field(..., min_length=8, description="Confirm new password")

class SignupRequest(BaseModel):
    name: str = Field(..., max_length=100, description="Full name")
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., min_length=8, description="User password (minimum 8 characters)")
    confirm_password: str = Field(..., min_length=8, description="Confirm password")

class UserResponse(BaseModel):
    user_id: int
    email: str
    full_name: Optional[str]
    phone: Optional[str]
    status: str
    roles: List[Dict[str, Any]]
    permissions: List[Dict[str, Any]]
    last_login_at: Optional[datetime]
    created_at: datetime

class LoginResponse(BaseModel):
    success: bool
    message: str
    data: Dict[str, Any]

# Global IAM service (will be initialized with database connection)
iam_service: Optional[IAMService] = None

def get_iam_service() -> IAMService:
    """Dependency to get IAM service"""
    if iam_service is None:
        raise HTTPException(
            status_code=503,
            detail="IAM service not available"
        )
    return iam_service

def init_iam_service(db_manager, secret_key: str = None):
    """Initialize IAM service with database connection"""
    global iam_service
    if secret_key is None:
        secret_key = os.getenv("JWT_SECRET_KEY", "your-default-secret-key-change-in-production")
    iam_service = IAMService(db_manager, secret_key)
    logger.info("IAM service initialized")

def get_current_user_from_token(request: Request, iam: IAMService = Depends(get_iam_service)) -> Dict[str, Any]:
    """Extract current user from JWT token"""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing or invalid"
        )

    token = auth_header[7:]  # Remove "Bearer " prefix
    user = iam.verify_access_token(token)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token"
        )

    return user

# Authentication Endpoints

@router.post("/register", response_model=Dict[str, Any])
async def register(request: RegisterRequest, iam: IAMService = Depends(get_iam_service)):
    """Register new user"""
    try:
        # Validate password confirmation
        if request.password != request.confirm_password:
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "message": "Passwords do not match",
                    "field": "confirm_password"
                }
            )

        # Create user
        user = await iam.create_user(
            email=request.email,
            password=request.password,
            full_name=request.full_name,
            phone=request.phone
        )

        # Log registration
        await iam.log_user_action(
            user_id=user['user_id'],
            action="USER_REGISTER",
            details=f"User registered with email: {request.email}"
        )

        return {
            "success": True,
            "message": "User registered successfully",
            "data": {
                "user": {
                    "user_id": user['user_id'],
                    "email": user['email'],
                    "full_name": user['full_name'],
                    "phone": user['phone'],
                    "status": user['status']
                }
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Registration error: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Registration failed",
                "error": str(e)
            }
        )

@router.post("/signin", response_model=LoginResponse)
async def signin(request: LoginRequest, iam: IAMService = Depends(get_iam_service)):
    """Authenticate user and return tokens"""
    import time
    t0 = time.time()
    logger.info(f"[SIGNIN] START for email: {request.email}")
    
    try:
        # Authenticate user
        t_auth_start = time.time()
        user = await iam.authenticate_user(request.email, request.password)
        logger.info(f"[SIGNIN] Auth done in {time.time() - t_auth_start:.3f}s")

        if not user:
            raise HTTPException(
                status_code=401,
                detail={
                    "success": False,
                    "message": "Invalid email or password",
                    "field": "credentials"
                }
            )

        # Create tokens
        t_token_start = time.time()
        access_token = await iam.create_access_token(user)
        refresh_token = await iam.create_refresh_token(user['user_id'])
        logger.info(f"[SIGNIN] Token creation done in {time.time() - t_token_start:.3f}s")

        # Log successful login
        t_log_start = time.time()
        await iam.log_user_action(
            user_id=user['user_id'],
            action="LOGIN_SUCCESS",
            details=f"Successful login from email: {request.email}"
        )
        logger.info(f"[SIGNIN] Activity logging done in {time.time() - t_log_start:.3f}s")

        logger.info(f"[SIGNIN] COMPLETE in {time.time() - t0:.3f}s")
        return {
            "success": True,
            "message": "Login successful",
            "data": {
                "user": {
                    "user_id": user['user_id'],
                    "email": user['email'],
                    "full_name": user['full_name'],
                    "phone": user['phone'],
                    "status": user['status'],
                    "roles": user.get('roles', []),
                    "permissions": user.get('permissions', []),
                    "last_login_at": user.get('last_login_at')
                },
                "tokens": {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "token_type": "bearer",
                    "expires_in": 24 * 60 * 60  # 24 hours
                }
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[SIGNIN] ERROR after {time.time() - t0:.3f}s: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Login failed",
                "error": str(e)
            }
        )

@router.post("/logout")
async def logout(request: RefreshTokenRequest, iam: IAMService = Depends(get_iam_service)):
    """Logout user and revoke refresh token"""
    try:
        # Revoke refresh token
        await iam.revoke_refresh_token(request.refresh_token)

        return {
            "success": True,
            "message": "Logout successful"
        }

    except Exception as e:
        logger.error(f"Logout error: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Logout failed",
                "error": str(e)
            }
        )

@router.post("/refresh")
async def refresh_access_token(request: RefreshTokenRequest, iam: IAMService = Depends(get_iam_service)):
    """Refresh access token using refresh token"""
    try:
        # Verify refresh token
        user_id = await iam.verify_refresh_token(request.refresh_token)

        if not user_id:
            raise HTTPException(
                status_code=401,
                detail={
                    "success": False,
                    "message": "Invalid or expired refresh token"
                }
            )

        # Get user data
        user = await iam.get_user_by_id(user_id)
        if not user:
            raise HTTPException(
                status_code=404,
                detail={
                    "success": False,
                    "message": "User not found"
                }
            )

        # Create new access token
        access_token = await iam.create_access_token(user)

        return {
            "success": True,
            "message": "Token refreshed successfully",
            "data": {
                "access_token": access_token,
                "token_type": "bearer",
                "expires_in": 24 * 60 * 60  # 24 hours
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token refresh error: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Token refresh failed",
                "error": str(e)
            }
        )



@router.post("/change-password")
async def change_password(
    request: ChangePasswordRequest,
    current_user: Dict[str, Any] = Depends(get_current_user_from_token),
    iam: IAMService = Depends(get_iam_service)
):
    """Change user password"""
    try:
        # Change password
        await iam.change_password(
            user_id=current_user['user_id'],
            current_password=request.current_password,
            new_password=request.new_password
        )

        # Log password change
        await iam.log_user_action(
            user_id=current_user['user_id'],
            action="PASSWORD_CHANGE",
            details="User password changed"
        )

        return {
            "success": True,
            "message": "Password changed successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Password change error: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Password change failed",
                "error": str(e)
            }
        )

@router.get("/validate")
async def validate_token(current_user: Dict[str, Any] = Depends(get_current_user_from_token)):
    """Validate JWT token"""
    return {
        "success": True,
        "message": "Token is valid",
        "data": {
            "user": {
                "user_id": current_user['user_id'],
                "email": current_user['email'],
                "full_name": current_user.get('full_name'),
                "roles": [role['role_code'] for role in current_user.get('roles', [])],
                "permissions": [perm['perm_code'] for perm in current_user.get('permissions', [])]
            },
            "token_valid": True
        }
    }

@router.get("/permissions")
async def get_user_permissions(current_user: Dict[str, Any] = Depends(get_current_user_from_token)):
    """Get current user permissions"""
    return {
        "success": True,
        "data": {
            "user_id": current_user['user_id'],
            "roles": current_user.get('roles', []),
            "permissions": current_user.get('permissions', []),
            "permission_codes": [perm['perm_code'] for perm in current_user.get('permissions', [])]
        }
    }

# Frontend compatible signup endpoint
@router.post("/signup", response_model=Dict[str, Any])
async def signup(request: SignupRequest, iam: IAMService = Depends(get_iam_service)):
    """Sign up new user (frontend compatible endpoint)"""
    try:
        # Validate password confirmation
        if request.password != request.confirm_password:
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "message": "Passwords do not match",
                    "field": "confirm_password"
                }
            )

        # Create user
        user = await iam.create_user(
            email=request.email,
            password=request.password,
            full_name=request.name
        )

        # Log registration
        await iam.log_user_action(
            user_id=user['user_id'],
            action="USER_SIGNUP",
            details=f"User signed up with email: {request.email}"
        )

        return {
            "success": True,
            "message": "Account created successfully! You can now sign in.",
            "data": {
                "user": {
                    "user_id": user['user_id'],
                    "email": user['email'],
                    "full_name": user['full_name'],
                    "status": user['status']
                }
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Signup error: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Account creation failed",
                "error": str(e)
            }
        )


# Forgot Password with OTP endpoints
@router.post("/forgot-password-otp")
async def forgot_password_otp(request: ForgotPasswordOTPRequest, iam: IAMService = Depends(get_iam_service)):
    """Request OTP for password reset via email"""
    try:
        # Check if user exists
        user = await iam.get_user_by_email(request.email)
        if not user:
            # Don't reveal if email exists for security
            return {
                "success": True,
                "message": "If your email is registered, you will receive an OTP code to reset your password."
            }

        # Generate and send OTP via email
        from app.services.email_service import send_otp_email
        
        result = await send_otp_email(
            email=request.email,
            name=user.get('full_name', 'User')
        )

        if not result.get('success'):
            raise HTTPException(
                status_code=500,
                detail={
                    "success": False,
                    "message": "Failed to send OTP email"
                }
            )

        # Log password reset request
        await iam.log_user_action(
            user_id=user['user_id'],
            action="PASSWORD_RESET_OTP_REQUEST",
            details=f"Password reset OTP sent to email: {request.email}"
        )

        return {
            "success": True,
            "message": "If your email is registered, you will receive an OTP code to reset your password.",
            "data": {
                "email": request.email,
                "expires_in_minutes": 10,
                "note": "Please check your email for the 6-digit OTP code"
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Forgot password OTP error: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Failed to process password reset request",
                "error": str(e)
            }
        )

@router.post("/verify-otp-reset-password")
async def verify_otp_reset_password(request: VerifyOTPResetPasswordRequest, iam: IAMService = Depends(get_iam_service)):
    """Verify OTP and reset password"""
    try:
        # Validate password confirmation
        if request.new_password != request.confirm_password:
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "message": "Passwords do not match",
                    "field": "confirm_password"
                }
            )

        # Check if user exists
        user = await iam.get_user_by_email(request.email)
        if not user:
            raise HTTPException(
                status_code=404,
                detail={
                    "success": False,
                    "message": "User not found"
                }
            )

        # Verify OTP
        from app.services.email_service import verify_otp
        
        otp_result = await verify_otp(request.email, request.otp)
        
        if not otp_result.get('valid'):
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "message": otp_result.get('message', 'Invalid or expired OTP'),
                    "field": "otp"
                }
            )

        # Hash new password
        hashed_password = await iam.hash_password(request.new_password)

        # Update password in database
        update_query = """
            UPDATE iam.iam_user 
            SET password_hash = $1, updated_at = NOW()
            WHERE email = $2
            RETURNING user_id, email
        """
        
        result = await iam.db.execute_query(update_query, (hashed_password, request.email))
        
        if not result:
            raise HTTPException(
                status_code=500,
                detail={
                    "success": False,
                    "message": "Failed to reset password"
                }
            )

        # Log password reset success
        await iam.log_user_action(
            user_id=user['user_id'],
            action="PASSWORD_RESET_SUCCESS",
            details=f"Password reset completed via OTP for user: {request.email}"
        )

        return {
            "success": True,
            "message": "Password reset successfully. You can now sign in with your new password.",
            "data": {
                "email": request.email
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Verify OTP reset password error: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "message": "Password reset failed",
                "error": str(e)
            }
        )

# Test endpoints for development
@router.get("/test-credentials")
async def get_test_credentials():
    """Get test credentials for development"""
    return {
        "success": True,
        "data": {
            "test_users": [
                {
                    "email": "admin@dss.com",
                    "password": "admin123",
                    "role": "SUPER_ADMIN",
                    "description": "System Administrator"
                },
                {
                    "email": "manager@dss.com",
                    "password": "manager123",
                    "role": "MANAGER",
                    "description": "Business Manager"
                },
                {
                    "email": "analyst@dss.com",
                    "password": "analyst123",
                    "role": "ANALYST",
                    "description": "Data Analyst"
                },
                {
                    "email": "customer@dss.com",
                    "password": "customer123",
                    "role": "CUSTOMER",
                    "description": "Customer User"
                }
            ],
            "note": "These are test accounts for development. Use /auth/signup to create new accounts."
        }
    }