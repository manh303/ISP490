"""
Shared Pydantic models used across multiple modules
"""
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional
from datetime import datetime
from app.utils.validators import validate_phone, validate_password, validate_full_name

class UserResponse(BaseModel):
    """Standard user response model"""
    user_id: int
    email: str
    full_name: Optional[str]
    phone: Optional[str]
    status: str
    role_code: Optional[str]
    role_name: Optional[str]
    last_login_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

class BaseUserRequest(BaseModel):
    """Base user request with common validation"""
    full_name: Optional[str] = Field(None, max_length=100, description="Full name")
    phone: Optional[str] = Field(None, max_length=32, description="Phone number")
    
    @field_validator('full_name')
    @classmethod
    def validate_full_name_field(cls, v):
        return validate_full_name(v)
    
    @field_validator('phone')
    @classmethod
    def validate_phone_field(cls, v):
        return validate_phone(v)

class PasswordRequest(BaseModel):
    """Base password request with validation"""
    password: str = Field(..., min_length=8, description="Password (min 8 chars)")
    
    @field_validator('password')
    @classmethod
    def validate_password_field(cls, v):
        return validate_password(v, min_length=8)

class ActionResponse(BaseModel):
    """Standard action response"""
    success: bool
    message: str
    data: Optional[dict] = None

class RoleInfo(BaseModel):
    """Role information model"""
    role_id: int
    role_code: str
    role_name: str
    description: Optional[str]
    is_active: bool
    user_count: Optional[int] = 0
