"""
Admin User Management Pydantic Models
"""
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class UserCreateRequest(BaseModel):
    email: EmailStr = Field(..., description="User email")
    password: str = Field(..., min_length=8, description="Password (min 8 chars)")
    full_name: str = Field(..., max_length=100, description="Full name")
    phone: Optional[str] = Field(None, max_length=32, description="Phone number")
    role_code: str = Field(..., description="Role code (ADMIN, ANALYST, CUSTOMER)")

class UserUpdateRequest(BaseModel):
    full_name: Optional[str] = Field(None, max_length=100)
    phone: Optional[str] = Field(None, max_length=32)
    role_code: Optional[str] = Field(None, description="Role code")

class UserPasswordUpdateRequest(BaseModel):
    new_password: str = Field(..., min_length=8, description="New password")

class UserResponse(BaseModel):
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

class UserListResponse(BaseModel):
    success: bool
    data: List[UserResponse]
    total: int
    page: int
    limit: int

class UserDetailResponse(BaseModel):
    user_id: int
    email: str
    full_name: Optional[str]
    phone: Optional[str]
    status: str
    role_code: Optional[str]
    role_name: Optional[str]
    role_description: Optional[str]
    last_login_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    is_deleted: bool = Field(default=False, description="Whether user is soft deleted")

class UserActionResponse(BaseModel):
    success: bool
    message: str
    user_id: Optional[int] = None