"""
Authentication API endpoints
"""

import os
import time
import logging
import secrets
import hashlib
import bcrypt
from typing import Any

from fastapi import APIRouter, HTTPException, Depends, Request

from app.schemas.models import (
    SignInRequest, SignInResponse,
    SignupRequest, SignupResponse,
    VerifyEmailRequest, VerifyEmailResponse,
    ForgotPasswordOTPRequest, ForgotPasswordOTPResponse,
    VerifyOTPResetPasswordRequest, VerifyOTPResetPasswordResponse,
    VerifyOTPOnlyRequest, VerifyOTPOnlyResponse,
    SignOutResponse
)

from app.core.settings import settings
from app.core.database import db_manager
from app.helpers.auth_helpers import (
    authenticate_user_db, authenticate_user,
    create_user_in_db, verify_email_token,
    activate_user, check_email_exists,
    store_verification_token
)
from app.services.email_service import send_otp_email
from app.utils.auth_helpers import create_access_token as create_jwt_token, decode_access_token

logger = logging.getLogger(__name__)

# Check if activity logging is available
try:
    from app.middleware.activity_middleware import ActivityLoggingMiddleware
    from app.services.activity_logger import ActivityLogger
    ACTIVITY_AVAILABLE = True
except ImportError:
    ACTIVITY_AVAILABLE = False
    ActivityLogger = None

router = APIRouter(prefix="/auth", tags=["Authentication"])


async def get_database():
    """Get database connection"""
    return db_manager


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
# SIGNIN ENDPOINT
# ====================================
@router.post("/signin", response_model=SignInResponse)
async def simple_signin(request: SignInRequest, db = Depends(get_database)):
    """Simple database signin endpoint - matches frontend expectations"""
    import time
    t0 = time.time()
    logger.info(f"[SIGNIN] START for email: {request.email}")
    
    try:
        # Try database authentication first
        t_auth_start = time.time()
        user_data = await authenticate_user_db(request.email, request.password, db)
        logger.info(f"[SIGNIN] Auth done in {time.time() - t_auth_start:.3f}s")
        
        # Fallback to hardcoded users if DB auth fails
        if not user_data:
            user_data = authenticate_user(request.email, request.password)
        
        if not user_data:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # Update last login time
        try:
            from app.services.iam_service import IAMService
            iam_service = IAMService(db)
            await iam_service.update_last_login(user_data["user_id"])
        except Exception as e:
            logger.error(f"Failed to update last login: {e}")

        # Create tokens
        t_token_start = time.time()
        access_token = create_access_token(user_data, request.email)
        logger.info(f"[SIGNIN] Token creation done in {time.time() - t_token_start:.3f}s")

        # Log signin activity
        t_log_start = time.time()
        try:
            if ACTIVITY_AVAILABLE and ActivityLogger:
                activity_logger = ActivityLogger(db)
                await activity_logger.log_activity(
                    user_id=user_data["user_id"],
                    email=user_data.get("email", request.email),
                    action="USER_SIGNIN",
                    request_path="/api/v1/auth/signin",
                    details={"role": user_data.get("role", "unknown"), "method": "password"},
                    status="success"
                )
        except Exception as e:
            logger.error(f"Failed to log signin activity: {e}")
        logger.info(f"[SIGNIN] Activity logging done in {time.time() - t_log_start:.3f}s")

        logger.info(f"[SIGNIN] COMPLETE in {time.time() - t0:.3f}s")
        logger.info(f"Successful signin for: {request.email} with role: {user_data['role']}")

        # Use shared role menu definitions
        from app.constants.roles import get_role_menu
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
        logger.error(f"[SIGNIN] ERROR after {time.time() - t0:.3f}s: {e}")
        raise HTTPException(status_code=500, detail="Signin failed")


# ====================================
# SIGNUP ENDPOINT
# ====================================
@router.post("/signup", response_model=SignupResponse)
async def signup(request: SignupRequest, db = Depends(get_database)):
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
            if ACTIVITY_AVAILABLE and ActivityLogger:
                activity_logger = ActivityLogger(db)
                await activity_logger.log_activity(
                    user_id=user_id,
                    email=request.email.lower(),
                    action="USER_SIGNUP",
                    request_path="/api/v1/auth/signup",
                    details={"full_name": request.name, "method": "email"},
                    status="success"
                )
        except Exception as e:
            logger.error(f"Failed to log signup activity: {e}")

        # Store verification token
        # await store_verification_token(db, request.email.lower(), token_hash)

        # Send verification email
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


# ====================================
# VERIFY EMAIL ENDPOINT
# ====================================
@router.post("/verify-email", response_model=VerifyEmailResponse)
async def verify_email(request: VerifyEmailRequest, db = Depends(get_database)):
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
# SIGNOUT ENDPOINT
# ====================================
@router.post("/signout", response_model=SignOutResponse)
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
                    if ACTIVITY_AVAILABLE and ActivityLogger:
                        activity_logger = ActivityLogger(db_manager)
                        await activity_logger.log_activity(
                            user_id=user_id,
                            email=user_email,
                            action="USER_SIGNOUT",
                            request_path="/api/v1/auth/signout",
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


# ====================================
# GET PROFILE ENDPOINT
# ====================================
@router.get("/profile")
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
                "full_name": payload.get("full_name", "User"),
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
# FORGOT PASSWORD WITH OTP
# ====================================
@router.post("/forgot-password-otp", response_model=ForgotPasswordOTPResponse)
async def forgot_password_otp(request: ForgotPasswordOTPRequest, db = Depends(get_database)):
    """Request OTP for password reset via email"""
    try:
        # Check if user exists (any status)
        user_query = "SELECT user_id, email, full_name, status FROM iam.iam_user WHERE email = $1"
        user_result = await db.execute_query(user_query, (request.email,))
        
        if not user_result:
            # Return error when email not found
            raise HTTPException(
                status_code=404,
                detail="No account found with this email address."
            )
        
        user = user_result[0]
        
        # Check if user is active
        if user.get('status') != 'active':
            raise HTTPException(
                status_code=403,
                detail="Your account has been disabled. Please contact support."
            )

        # Generate and send OTP via email
        from app.services.email_service import send_otp_email
        
        result = await send_otp_email(
            email=request.email,
            name=user.get('full_name', 'User')
        )

        if not result.get('success'):
            raise HTTPException(
                status_code=500,
                detail="Failed to send OTP email"
            )

        # Log password reset request
        try:
            if ACTIVITY_AVAILABLE and ActivityLogger:
                activity_logger = ActivityLogger(db)
                await activity_logger.log_activity(
                    user_id=user['user_id'],
                    email=request.email,
                    action="PASSWORD_RESET_OTP_REQUEST",
                    details={"email": request.email},
                    status="success"
                )
        except Exception as e:
            logger.error(f"Failed to log activity: {e}")

        return ForgotPasswordOTPResponse(
            success=True,
            message="If your email is registered, you will receive an OTP code to reset your password.",
            data={
                "email": request.email,
                "expires_in_minutes": 10,
                "note": "Please check your email for the 6-digit OTP code"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Forgot password OTP error: {e}")
        raise HTTPException(status_code=500, detail="Failed to process password reset request")


# ====================================
# VERIFY OTP AND RESET PASSWORD
# ====================================
@router.post("/verify-otp-reset-password", response_model=VerifyOTPResetPasswordResponse)
async def verify_otp_reset_password(request: VerifyOTPResetPasswordRequest, db = Depends(get_database)):
    """Verify OTP and reset password"""
    try:
        # Validate password confirmation
        if request.new_password != request.confirm_password:
            raise HTTPException(
                status_code=400,
                detail="Passwords do not match"
            )

        # Check if user exists
        user_query = "SELECT user_id, email FROM iam.iam_user WHERE email = $1 AND status = 'active'"
        user_result = await db.execute_query(user_query, (request.email,))
        
        if not user_result:
            raise HTTPException(
                status_code=404,
                detail="User not found"
            )
        
        user = user_result[0]

        # Verify OTP
        from app.services.email_service import verify_otp
        
        otp_result = await verify_otp(request.email, request.otp)
        
        if not otp_result.get('valid'):
            raise HTTPException(
                status_code=400,
                detail=otp_result.get('message', 'Invalid or expired OTP')
            )

        # Hash new password
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(request.new_password.encode('utf-8'), salt).decode('utf-8')

        # Update password in database
        update_query = """
            UPDATE iam.iam_user 
            SET password_hash = $1, updated_at = NOW()
            WHERE email = $2
            RETURNING user_id, email
        """
        
        result = await db.execute_query(update_query, (hashed_password, request.email))
        
        if not result:
            raise HTTPException(
                status_code=500,
                detail="Failed to reset password"
            )

        # Log password reset success
        try:
            if ACTIVITY_AVAILABLE and ActivityLogger:
                activity_logger = ActivityLogger(db)
                await activity_logger.log_activity(
                    user_id=user['user_id'],
                    email=request.email,
                    action="PASSWORD_RESET_SUCCESS",
                    details={"email": request.email},
                    status="success"
                )
        except Exception as e:
            logger.error(f"Failed to log activity: {e}")

        return VerifyOTPResetPasswordResponse(
            success=True,
            message="Password reset successfully. You can now sign in with your new password.",
            data={
                "email": request.email
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Verify OTP reset password error: {e}")
        raise HTTPException(status_code=500, detail="Password reset failed")


# ====================================
# VERIFY OTP ONLY (without password reset)
# ====================================
@router.post("/verify-otp-only", response_model=VerifyOTPOnlyResponse)
async def verify_otp_only(request: VerifyOTPOnlyRequest, db = Depends(get_database)):
    """Verify OTP only without resetting password - used to validate OTP before showing reset password form"""
    try:
        # Check if user exists
        user_query = "SELECT user_id, email FROM iam.iam_user WHERE email = $1 AND status = 'active'"
        user_result = await db.execute_query(user_query, (request.email,))
        
        if not user_result:
            return VerifyOTPOnlyResponse(
                success=False,
                message="User not found",
                valid=False
            )

        # Verify OTP WITHOUT consuming it (so it can be used for reset password)
        from app.services.email_service import verify_otp_no_consume
        
        otp_result = await verify_otp_no_consume(request.email, request.otp)
        
        if not otp_result.get('valid'):
            return VerifyOTPOnlyResponse(
                success=False,
                message=otp_result.get('message', 'Invalid or expired OTP'),
                valid=False
            )

        # OTP is valid
        return VerifyOTPOnlyResponse(
            success=True,
            message="OTP verified successfully",
            valid=True
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Verify OTP only error: {e}")
        return VerifyOTPOnlyResponse(
            success=False,
            message="OTP verification failed",
            valid=False
        )
