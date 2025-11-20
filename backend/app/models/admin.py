"""
Admin Management Pydantic Models
"""
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import List, Optional
from datetime import datetime
from utils.validators import validate_phone, validate_password, validate_full_name,validate_email
from models.shared import UserResponse  # Import from shared models

class UserCreateRequest(BaseModel):
    """Create user request"""
    email: EmailStr = Field(..., description="Email address")
    full_name: str = Field(..., min_length=2, max_length=100, description="Full name")
    password: str = Field(..., min_length=8, description="Password")
    phone: Optional[str] = Field(None, description="Phone number")
    role: str = Field(..., description="User role")
    

    @field_validator('full_name')
    @classmethod
    def validate_full_name_field(cls, v):
        return validate_full_name(v)
    
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

class UserUpdateRequest(BaseModel):
    """Update user request"""
    full_name: Optional[str] = Field(None, min_length=2, max_length=100, description="Full name")
    phone: Optional[str] = Field(None, description="Phone number")
    role: Optional[str] = Field(None, description="User role")
    status: Optional[str] = Field(None, description="User status")
    
    @field_validator('full_name')
    @classmethod
    def validate_full_name_field(cls, v):
        return validate_full_name(v)
    
    @field_validator('phone')
    @classmethod
    def validate_phone_field(cls, v):
        return validate_phone(v)

class PasswordChangeRequest(BaseModel):
    """Change password request"""
    new_password: str = Field(..., min_length=8, description="New password")
    
    @field_validator('new_password')
    @classmethod
    def validate_password_field(cls, v):
        return validate_password(v, min_length=8)

class UserPasswordUpdateRequest(BaseModel):
    """Update user password request"""
    new_password: str = Field(..., min_length=8, description="New password")
    
    @field_validator('new_password')
    @classmethod
    def validate_password_field(cls, v):
        return validate_password(v, min_length=8)

class UserListResponse(BaseModel):
    """User list response"""
    success: bool
    data: List[UserResponse]
    total: int
    page: int
    limit: int

class UserActionResponse(BaseModel):
    """User action response"""
    success: bool
    message: str
    user_id: Optional[int] = None

class ActivityLogResponse(BaseModel):
    """Activity log response"""
    log_id: int
    user_id: Optional[int]
    email: Optional[str]
    action: str
    resource: Optional[str]
    details: Optional[dict]
    ip_address: Optional[str]
    status: str
    created_at: datetime

class ActivityStatsResponse(BaseModel):
    """Activity statistics response"""
    total_activities: int
    successful_activities: int
    failed_activities: int
    unique_users: int
    top_actions: List[dict]
    recent_activities: List[ActivityLogResponse]