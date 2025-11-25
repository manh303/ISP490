"""
User Profile Management Pydantic Models
"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from models.shared import BaseUserRequest

# Use shared UserResponse instead of duplicate ProfileResponse


class ProfileUpdateRequest(BaseUserRequest):
    """Profile update request"""
    email: Optional[EmailStr] = Field(None, description="Email address")

class ProfileActionResponse(BaseModel):
    """Profile action response"""
    success: bool
    message: str
    user_id: Optional[int] = None

class EmailChangeRequestIn(BaseModel):
    """Email change request"""
    new_email: EmailStr

class EmailChangeConfirmIn(BaseModel):
    """Email change confirmation"""
    request_id: str
    otp: str

class UserProfileResponse(BaseModel):
    """User profile response"""
    user_id: int
    email: str
    full_name: Optional[str]
    phone: Optional[str]
    role_name: Optional[str]
    
   