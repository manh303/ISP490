"""
Admin User Management Pydantic Models
"""
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from utils.validators import validate_phone, validate_password
from models.shared import UserResponse, BaseUserRequest, PasswordRequest, ActionResponse
from constants.roles import validate_role_code

class UserCreateRequest(BaseModel):
    email: EmailStr = Field(..., description="User email")
    password: str = Field(..., min_length=8, description="Password (min 8 chars)")
    full_name: str = Field(..., max_length=100, description="Full name")
    phone: Optional[str] = Field(None, max_length=32, description="Phone number")
    role_code: str = Field(..., description="Role code (ADMIN, ANALYST, CUSTOMER)")
    
    @field_validator('phone')
    @classmethod
    def validate_phone_field(cls, v):
        return validate_phone(v)
    
    @field_validator('password')
    @classmethod
    def validate_password_field(cls, v):
        return validate_password(v, min_length=8)

class UserUpdateRequest(BaseUserRequest):
    role_code: Optional[str] = Field(None, description="Role code")
    
    @field_validator('role_code')
    @classmethod
    def validate_role_field(cls, v):
        if v and not validate_role_code(v):
            raise ValueError('Invalid role code')
        return v

class UserPasswordUpdateRequest(PasswordRequest):
    new_password: str = Field(..., min_length=8, description="New password")
    
    @field_validator('new_password')
    @classmethod
    def validate_password_field(cls, v):
        return validate_password(v, min_length=8)

# Use shared UserResponse from models.shared

class UserListResponse(BaseModel):
    success: bool
    data: List[UserResponse]
    total: int
    page: int
    limit: int

class UserActionResponse(ActionResponse):
    user_id: Optional[int] = None