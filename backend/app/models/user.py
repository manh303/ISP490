"""
User Profile Management Pydantic Models
"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime

class ProfileResponse(BaseModel):
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

class ProfileUpdateRequest(BaseModel):
    full_name: Optional[str] = Field(None, max_length=100, description="Full name")
    phone: Optional[str] = Field(None, max_length=32, description="Phone number")
    email: Optional[EmailStr] = Field(None, description="Email address")