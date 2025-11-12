"""
User Profile Management Pydantic Models
"""
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional
from models.shared import UserResponse, BaseUserRequest

# Use shared UserResponse instead of duplicate ProfileResponse
ProfileResponse = UserResponse

class ProfileUpdateRequest(BaseUserRequest):
    email: Optional[EmailStr] = Field(None, description="Email address")
