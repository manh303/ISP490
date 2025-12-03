"""
Pydantic models for authentication and signup
"""

from pydantic import BaseModel, Field, EmailStr
from typing import Optional, Dict, List
from datetime import datetime


# ====================================
# HEALTH & SYSTEM MODELS
# ====================================
class HealthCheck(BaseModel):
    """Health check response model"""
    status: str
    timestamp: str
    services: Dict[str, str]


# ====================================
# AUTHENTICATION MODELS
# ====================================
class SignInRequest(BaseModel):
    """Sign in request model"""
    email: str = Field(..., description="User email")
    password: str = Field(..., description="User password")


class SignInResponse(BaseModel):
    """Sign in response model"""
    success: bool
    message: str
    access_token: str
    user: dict


class SignOutResponse(BaseModel):
    """Sign out response model"""
    success: bool
    message: str


class ForgotPasswordOTPRequest(BaseModel):
    """Forgot password OTP request model"""
    email: EmailStr = Field(..., description="User email address")


class ForgotPasswordOTPResponse(BaseModel):
    """Forgot password OTP response model"""
    success: bool
    message: str
    data: Optional[dict] = None


class VerifyOTPResetPasswordRequest(BaseModel):
    """Verify OTP and reset password request model"""
    email: EmailStr = Field(..., description="User email address")
    otp: str = Field(..., min_length=6, max_length=6, description="6-digit OTP code")
    new_password: str = Field(..., min_length=8, description="New password (minimum 8 characters)")
    confirm_password: str = Field(..., min_length=8, description="Confirm new password")


class VerifyOTPResetPasswordResponse(BaseModel):
    """Verify OTP and reset password response model"""
    success: bool
    message: str
    data: Optional[dict] = None


# ====================================
# SIGNUP & EMAIL VERIFICATION MODELS
# ====================================
class SignupRequest(BaseModel):
    """User signup request model"""
    name: str = Field(..., description="Full name")
    email: str = Field(..., description="Email address")
    password: str = Field(..., min_length=8, description="Password (min 8 chars)")
    confirm_password: str = Field(..., description="Confirm password")
    phone: Optional[str] = Field(None, description="Phone number")


class SignupResponse(BaseModel):
    """User signup response model"""
    success: bool
    message: str
    verification_sent: bool
    email: str


class VerifyEmailRequest(BaseModel):
    """Email verification request model"""
    email: str = Field(..., description="Email address")
    verification_code: str = Field(..., description="6-digit verification code")


class VerifyEmailResponse(BaseModel):
    """Email verification response model"""
    success: bool
    message: str
    user_created: bool
    user_id: Optional[int]


# ====================================
# USER PROFILE MODELS
# ====================================
class UserProfile(BaseModel):
    """User profile model"""
    user_id: int
    email: str
    full_name: str
    role: str
    roles: List[str]
    permissions: List[str]


class UserProfileResponse(BaseModel):
    """User profile response model"""
    success: bool
    user: UserProfile
