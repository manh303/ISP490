"""
Role Pydantic Models
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class RoleResponse(BaseModel):
    """Role response model"""
    role_id: int
    role_code: str
    role_name: str
    description: Optional[str]
    is_active: bool = True

class RoleDetailResponse(BaseModel):
    """Detailed role response with permissions"""
    role_id: int
    role_code: str
    role_name: str
    description: Optional[str]
    is_active: bool = True
    permissions: List[str]
    modules: List[str]
    actions: List[str]
    admin_features: Dict[str, bool]
    user_count: int

class RoleCreateRequest(BaseModel):
    """Create role request"""
    role_code: str = Field(..., max_length=50, description="Role code (e.g., MANAGER)")
    role_name: str = Field(..., max_length=100, description="Role display name")
    description: Optional[str] = Field(None, max_length=255, description="Role description")

class RoleUpdateRequest(BaseModel):
    """Update role request"""
    role_name: Optional[str] = Field(None, max_length=100, description="Role display name")
    description: Optional[str] = Field(None, max_length=255, description="Role description")

class RoleListResponse(BaseModel):
    """Role list response"""
    success: bool
    data: List[RoleResponse]
    total: int
    page: int
    limit: int

class RoleActionResponse(BaseModel):
    """Role action response"""
    success: bool
    message: str
    role_id: Optional[int] = None