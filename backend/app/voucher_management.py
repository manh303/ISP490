#!/usr/bin/env python3
"""
Voucher Management System
========================
Comprehensive voucher/promotion management for e-commerce DSS
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json
import uuid
import random
import string

# Database
from databases import Database
from sqlalchemy import text

# Auth
try:
    from simple_auth import get_current_active_user
except:
    get_current_active_user = lambda: None

# ====================================
# ENUMS AND MODELS
# ====================================

class VoucherType(str, Enum):
    PERCENTAGE = "percentage"
    FIXED_AMOUNT = "fixed_amount"
    FREE_SHIPPING = "free_shipping"
    BUY_X_GET_Y = "buy_x_get_y"

class VoucherStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    EXPIRED = "expired"
    USED_UP = "used_up"

class VoucherTargetType(str, Enum):
    ALL_CUSTOMERS = "all_customers"
    CUSTOMER_SEGMENT = "customer_segment"
    SPECIFIC_CUSTOMERS = "specific_customers"
    NEW_CUSTOMERS = "new_customers"

class VoucherCreate(BaseModel):
    code: Optional[str] = None  # Auto-generate if None
    name: str = Field(..., min_length=3, max_length=100)
    description: str = Field(..., max_length=500)
    voucher_type: VoucherType
    discount_value: float = Field(..., gt=0)
    max_discount_amount: Optional[float] = None
    min_order_amount: Optional[float] = 0
    usage_limit: Optional[int] = None
    usage_limit_per_customer: Optional[int] = 1
    target_type: VoucherTargetType = VoucherTargetType.ALL_CUSTOMERS
    target_segments: Optional[List[str]] = []
    target_customer_ids: Optional[List[str]] = []
    start_date: datetime
    end_date: datetime
    applicable_categories: Optional[List[str]] = []
    applicable_products: Optional[List[str]] = []

    @validator('end_date')
    def end_date_must_be_after_start_date(cls, v, values):
        if 'start_date' in values and v <= values['start_date']:
            raise ValueError('End date must be after start date')
        return v

class VoucherUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    discount_value: Optional[float] = None
    max_discount_amount: Optional[float] = None
    min_order_amount: Optional[float] = None
    usage_limit: Optional[int] = None
    usage_limit_per_customer: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: Optional[VoucherStatus] = None

class VoucherUsage(BaseModel):
    voucher_code: str
    customer_id: str
    order_id: str
    order_amount: float
    discount_applied: float

class VoucherResponse(BaseModel):
    voucher_id: str
    code: str
    name: str
    description: str
    voucher_type: VoucherType
    discount_value: float
    max_discount_amount: Optional[float]
    min_order_amount: float
    usage_limit: Optional[int]
    usage_limit_per_customer: int
    usage_count: int
    target_type: VoucherTargetType
    start_date: datetime
    end_date: datetime
    status: VoucherStatus
    created_at: datetime

# ====================================
# ROUTER SETUP
# ====================================

voucher_router = APIRouter(prefix="/api/v1/vouchers", tags=["Voucher Management"])

# ====================================
# VOUCHER SERVICE CLASS
# ====================================

class VoucherService:
    def __init__(self):
        self.db = None  # Will be injected

    def generate_voucher_code(self, prefix: str = "VN") -> str:
        """Generate unique voucher code"""
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        return f"{prefix}{random_part}"

    async def create_voucher(self, voucher_data: VoucherCreate) -> Dict[str, Any]:
        """Create new voucher"""
        voucher_id = str(uuid.uuid4())
        code = voucher_data.code or self.generate_voucher_code()

        # Check if code already exists
        existing = await self.db.fetch_one(
            "SELECT voucher_id FROM vouchers WHERE code = :code",
            {"code": code}
        )
        if existing:
            raise HTTPException(status_code=400, detail="Voucher code already exists")

        query = """
        INSERT INTO vouchers (
            voucher_id, code, name, description, voucher_type, discount_value,
            max_discount_amount, min_order_amount, usage_limit, usage_limit_per_customer,
            target_type, target_segments, target_customer_ids, start_date, end_date,
            applicable_categories, applicable_products, status, created_at
        ) VALUES (
            :voucher_id, :code, :name, :description, :voucher_type, :discount_value,
            :max_discount_amount, :min_order_amount, :usage_limit, :usage_limit_per_customer,
            :target_type, :target_segments, :target_customer_ids, :start_date, :end_date,
            :applicable_categories, :applicable_products, 'active', NOW()
        ) RETURNING voucher_id
        """

        values = {
            "voucher_id": voucher_id,
            "code": code,
            "name": voucher_data.name,
            "description": voucher_data.description,
            "voucher_type": voucher_data.voucher_type.value,
            "discount_value": voucher_data.discount_value,
            "max_discount_amount": voucher_data.max_discount_amount,
            "min_order_amount": voucher_data.min_order_amount or 0,
            "usage_limit": voucher_data.usage_limit,
            "usage_limit_per_customer": voucher_data.usage_limit_per_customer,
            "target_type": voucher_data.target_type.value,
            "target_segments": json.dumps(voucher_data.target_segments or []),
            "target_customer_ids": json.dumps(voucher_data.target_customer_ids or []),
            "start_date": voucher_data.start_date,
            "end_date": voucher_data.end_date,
            "applicable_categories": json.dumps(voucher_data.applicable_categories or []),
            "applicable_products": json.dumps(voucher_data.applicable_products or [])
        }

        try:
            result = await self.db.fetch_one(query, values)
            return {"voucher_id": result["voucher_id"], "code": code, "status": "created"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to create voucher: {str(e)}")

    async def validate_voucher(self, code: str, customer_id: str, order_amount: float,
                              categories: List[str] = None, products: List[str] = None) -> Dict[str, Any]:
        """Validate if voucher can be used"""

        # Get voucher details
        voucher = await self.db.fetch_one("""
            SELECT * FROM vouchers
            WHERE code = :code AND status = 'active'
            AND start_date <= NOW() AND end_date >= NOW()
        """, {"code": code})

        if not voucher:
            return {"valid": False, "error": "Voucher không tồn tại hoặc đã hết hạn"}

        # Check minimum order amount
        if order_amount < voucher["min_order_amount"]:
            return {
                "valid": False,
                "error": f"Đơn hàng tối thiểu {voucher['min_order_amount']:,.0f} VNĐ"
            }

        # Check usage limit
        if voucher["usage_limit"]:
            usage_count = await self.db.fetch_val(
                "SELECT COUNT(*) FROM voucher_usage WHERE voucher_id = :voucher_id",
                {"voucher_id": voucher["voucher_id"]}
            )
            if usage_count >= voucher["usage_limit"]:
                return {"valid": False, "error": "Voucher đã hết lượt sử dụng"}

        # Check usage limit per customer
        customer_usage = await self.db.fetch_val("""
            SELECT COUNT(*) FROM voucher_usage
            WHERE voucher_id = :voucher_id AND customer_id = :customer_id
        """, {"voucher_id": voucher["voucher_id"], "customer_id": customer_id})

        if customer_usage >= voucher["usage_limit_per_customer"]:
            return {"valid": False, "error": "Bạn đã sử dụng hết lượt voucher này"}

        # Check target customer
        if voucher["target_type"] == "customer_segment":
            customer_segment = await self.db.fetch_val("""
                SELECT customer_segment FROM customers WHERE customer_id = :customer_id
            """, {"customer_id": customer_id})

            target_segments = json.loads(voucher["target_segments"] or "[]")
            if customer_segment not in target_segments:
                return {"valid": False, "error": "Voucher không áp dụng cho phân khúc khách hàng của bạn"}

        elif voucher["target_type"] == "specific_customers":
            target_customers = json.loads(voucher["target_customer_ids"] or "[]")
            if customer_id not in target_customers:
                return {"valid": False, "error": "Voucher không áp dụng cho tài khoản này"}

        # Calculate discount
        discount_amount = await self.calculate_discount(voucher, order_amount)

        return {
            "valid": True,
            "voucher_id": voucher["voucher_id"],
            "code": voucher["code"],
            "name": voucher["name"],
            "discount_type": voucher["voucher_type"],
            "discount_amount": discount_amount,
            "description": voucher["description"]
        }

    async def calculate_discount(self, voucher: Dict, order_amount: float) -> float:
        """Calculate discount amount"""
        voucher_type = voucher["voucher_type"]
        discount_value = voucher["discount_value"]
        max_discount = voucher["max_discount_amount"]

        if voucher_type == "percentage":
            discount = order_amount * (discount_value / 100)
            if max_discount:
                discount = min(discount, max_discount)
        elif voucher_type == "fixed_amount":
            discount = discount_value
        elif voucher_type == "free_shipping":
            discount = 30000  # Assume 30k VND shipping cost
        else:
            discount = 0

        return min(discount, order_amount)

    async def use_voucher(self, usage_data: VoucherUsage) -> Dict[str, Any]:
        """Record voucher usage"""
        usage_id = str(uuid.uuid4())

        query = """
        INSERT INTO voucher_usage (
            usage_id, voucher_id, customer_id, order_id,
            order_amount, discount_applied, used_at
        )
        SELECT
            :usage_id, voucher_id, :customer_id, :order_id,
            :order_amount, :discount_applied, NOW()
        FROM vouchers
        WHERE code = :voucher_code
        RETURNING usage_id
        """

        values = {
            "usage_id": usage_id,
            "customer_id": usage_data.customer_id,
            "order_id": usage_data.order_id,
            "order_amount": usage_data.order_amount,
            "discount_applied": usage_data.discount_applied,
            "voucher_code": usage_data.voucher_code
        }

        try:
            result = await self.db.fetch_one(query, values)
            return {"usage_id": result["usage_id"], "status": "recorded"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to record usage: {str(e)}")

    async def get_voucher_analytics(self) -> Dict[str, Any]:
        """Get voucher analytics"""

        # Active vouchers
        active_vouchers = await self.db.fetch_val("""
            SELECT COUNT(*) FROM vouchers
            WHERE status = 'active' AND start_date <= NOW() AND end_date >= NOW()
        """)

        # Total usage this month
        monthly_usage = await self.db.fetch_val("""
            SELECT COUNT(*) FROM voucher_usage
            WHERE used_at >= DATE_TRUNC('month', NOW())
        """)

        # Total discount given this month
        monthly_discount = await self.db.fetch_val("""
            SELECT COALESCE(SUM(discount_applied), 0) FROM voucher_usage
            WHERE used_at >= DATE_TRUNC('month', NOW())
        """) or 0

        # Top performing vouchers
        top_vouchers = await self.db.fetch_all("""
            SELECT v.code, v.name, v.voucher_type,
                   COUNT(vu.usage_id) as usage_count,
                   SUM(vu.discount_applied) as total_discount
            FROM vouchers v
            LEFT JOIN voucher_usage vu ON v.voucher_id = vu.voucher_id
            WHERE vu.used_at >= NOW() - INTERVAL '30 days'
            GROUP BY v.voucher_id, v.code, v.name, v.voucher_type
            ORDER BY usage_count DESC
            LIMIT 10
        """)

        return {
            "active_vouchers": active_vouchers,
            "monthly_usage": monthly_usage,
            "monthly_discount_vnd": monthly_discount,
            "top_performing_vouchers": [dict(row) for row in top_vouchers],
            "analytics_date": datetime.now().isoformat()
        }

# Global service instance
voucher_service = VoucherService()

# ====================================
# ENDPOINTS
# ====================================

@voucher_router.post("/create", response_model=Dict[str, Any])
async def create_voucher(
    voucher_data: VoucherCreate,
    current_user: Any = Depends(get_current_active_user)
):
    """Create new voucher"""
    return await voucher_service.create_voucher(voucher_data)

@voucher_router.post("/validate")
async def validate_voucher(
    code: str,
    customer_id: str,
    order_amount: float,
    categories: List[str] = None,
    products: List[str] = None
):
    """Validate voucher for use"""
    return await voucher_service.validate_voucher(
        code, customer_id, order_amount, categories, products
    )

@voucher_router.post("/use")
async def use_voucher(usage_data: VoucherUsage):
    """Record voucher usage"""
    return await voucher_service.use_voucher(usage_data)

@voucher_router.get("/analytics")
async def get_voucher_analytics(
    current_user: Any = Depends(get_current_active_user)
):
    """Get voucher analytics"""
    return await voucher_service.get_voucher_analytics()

@voucher_router.get("/list")
async def list_vouchers(
    status: Optional[VoucherStatus] = None,
    limit: int = 50,
    offset: int = 0,
    current_user: Any = Depends(get_current_active_user)
):
    """List all vouchers with pagination"""
    where_clause = ""
    params = {"limit": limit, "offset": offset}

    if status:
        where_clause = "WHERE status = :status"
        params["status"] = status.value

    vouchers = await voucher_service.db.fetch_all(f"""
        SELECT voucher_id, code, name, description, voucher_type,
               discount_value, min_order_amount, usage_limit,
               start_date, end_date, status, created_at,
               (SELECT COUNT(*) FROM voucher_usage vu WHERE vu.voucher_id = v.voucher_id) as usage_count
        FROM vouchers v
        {where_clause}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """, params)

    return {"vouchers": [dict(row) for row in vouchers]}

@voucher_router.put("/{voucher_id}")
async def update_voucher(
    voucher_id: str,
    update_data: VoucherUpdate,
    current_user: Any = Depends(get_current_active_user)
):
    """Update voucher"""
    # Build dynamic update query
    update_fields = []
    params = {"voucher_id": voucher_id}

    for field, value in update_data.dict(exclude_unset=True).items():
        if value is not None:
            update_fields.append(f"{field} = :{field}")
            params[field] = value

    if not update_fields:
        return {"message": "No fields to update"}

    query = f"""
        UPDATE vouchers
        SET {', '.join(update_fields)}, updated_at = NOW()
        WHERE voucher_id = :voucher_id
        RETURNING voucher_id
    """

    result = await voucher_service.db.fetch_one(query, params)
    if not result:
        raise HTTPException(status_code=404, detail="Voucher not found")

    return {"voucher_id": result["voucher_id"], "status": "updated"}

@voucher_router.delete("/{voucher_id}")
async def delete_voucher(
    voucher_id: str,
    current_user: Any = Depends(get_current_active_user)
):
    """Soft delete voucher"""
    result = await voucher_service.db.fetch_one("""
        UPDATE vouchers
        SET status = 'inactive', updated_at = NOW()
        WHERE voucher_id = :voucher_id
        RETURNING voucher_id
    """, {"voucher_id": voucher_id})

    if not result:
        raise HTTPException(status_code=404, detail="Voucher not found")

    return {"voucher_id": result["voucher_id"], "status": "deactivated"}