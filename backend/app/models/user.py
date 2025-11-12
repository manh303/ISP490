"""
User Profile Management Pydantic Models
"""
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional
from utils.validators import validate_phone

class ProfileResponse(BaseModel):
    """Profile response model"""
    user_id: int
    email: str
    full_name: str
    phone: Optional[str] = None
    role: str
    last_login_at: Optional[str] = None

class ProfileUpdateRequest(BaseModel):
    """Profile update request"""
    full_name: Optional[str] = Field(None, min_length=2, max_length=100, description="Full name")
    phone: Optional[str] = Field(None, description="Phone number")
    email: Optional[EmailStr] = Field(None, description="Email address")
    
    @field_validator('phone')
    @classmethod
    def validate_phone_field(cls, v):
        return validate_phone(v)

class ProfileActionResponse(BaseModel):
    """Profile action response"""
    success: bool
    message: str
    user_id: Optional[int] = None