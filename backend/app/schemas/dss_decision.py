"""
DSS Decision & Action Schemas
Models for saving and managing analyst decisions based on DSS analysis
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date


# ============================================
# ACTION ITEM MODELS
# ============================================

class DSSActionItem(BaseModel):
    """Represents a single action item in a decision plan"""
    action_type: str = Field(..., description="Type: change_price, marketing_campaign, fix_quality, etc.")
    target_level: str = Field(..., description="Target level: product, category, or platform")
    
    # Target references (nullable based on target_level)
    # NEW: Support product_key for easier frontend integration
    product_key: Optional[str] = Field(None, description="Product key (e.g., 'tiki_123'). Backend will lookup product_sk.")
    product_sk: Optional[int] = Field(None, description="Reference to dim_product.product_sk")
    platform_sk: Optional[int] = Field(None, description="Reference to dim_platform.platform_sk")
    category_sk: Optional[int] = Field(None, description="Reference to dim_category.category_sk")
    
    # Values
    current_value: Optional[float] = Field(None, description="Current value (e.g., current price)")
    recommended_value: Optional[float] = Field(None, description="ML/AI recommended value")
    chosen_value: Optional[float] = Field(None, description="Value chosen by analyst")
    unit: Optional[str] = Field(None, description="Unit: VND, %, score, etc.")
    
    # Planning
    planned_start_date: Optional[date] = Field(None, description="Planned start date for action")
    planned_end_date: Optional[date] = Field(None, description="Planned end date for action")
    status: str = Field("PLANNED", description="Status: PLANNED, IN_PROGRESS, DONE, CANCELLED")
    
    # Notes
    note: Optional[str] = Field(None, description="Additional notes about this action")

    class Config:
        json_schema_extra = {
            "example": {
                "action_type": "change_price",
                "target_level": "product",
                "product_key": "tiki_278890829",  # ← Frontend can send this
                "current_value": 100000,
                "recommended_value": 102000,
                "chosen_value": 102000,
                "unit": "VND",
                "planned_start_date": "2025-12-01",
                "planned_end_date": "2025-12-31",
                "status": "PLANNED",
                "note": "Increase price by 2% based on ML recommendation"
            }
        }


class DSSActionItemResponse(DSSActionItem):
    """Action item response with enriched data"""
    action_id: int = Field(..., description="Action item ID")
    
    # Enriched names from dimension tables
    product_name: Optional[str] = Field(None, description="Product name")
    category_name: Optional[str] = Field(None, description="Category name")
    platform_name: Optional[str] = Field(None, description="Platform name")


# ============================================
# DECISION REQUEST/RESPONSE MODELS
# ============================================

class SaveDSSDecisionRequest(BaseModel):
    """Request to save a new DSS decision with action plan"""
    scenario_key: str = Field(
        ...,
        description="Scenario type: price_prediction, product_recommendation, or review_sentiment"
    )
    session_id: Optional[int] = Field(
        None,
        description="Existing session ID if analyst already ran analysis. If null, will create new session from snapshot data."
    )
    
    # Snapshot from analysis (required if session_id is null)
    filters: Optional[Dict[str, Any]] = Field(None, description="Filters used in analysis")
    kpi_summary: Optional[Dict[str, Any]] = Field(None, description="KPI summary snapshot")
    ai_summary_insights: Optional[List[str]] = Field(None, description="AI-generated insights")
    ai_recommended_actions: Optional[List[str]] = Field(None, description="AI-recommended actions")
    date_adjustment_info: Optional[Dict[str, Any]] = Field(None, description="Date adjustment info (for price/review scenarios)")
    
    # Decision information
    title: str = Field(..., description="Decision title", min_length=1, max_length=500)
    description: Optional[str] = Field(None, description="Detailed description or context")
    status: str = Field("DRAFT", description="Status: DRAFT, APPROVED, REJECTED, IMPLEMENTED")
    
    # Action plan
    actions: List[DSSActionItem] = Field(..., description="List of action items", min_length=1)

    class Config:
        json_schema_extra = {
            "example": {
                "scenario_key": "price_prediction",
                "session_id": None,
                "filters": {
                    "from_date": "2025-11-23",
                    "to_date": "2025-11-24",
                    "platforms": ["tiki"],
                    "categories": ["1"]
                },
                "kpi_summary": {
                    "num_products": 150,
                    "current_total_revenue": 50000000,
                    "projected_total_revenue": 51000000
                },
                "ai_summary_insights": [
                    "2% price increase recommended for printer category",
                    "High confidence predictions for top-selling products"
                ],
                "ai_recommended_actions": [
                    "Implement price changes during low-traffic hours",
                    "Monitor competitor pricing for 7 days"
                ],
                "title": "Tăng giá 2% cho nhóm máy in Tiki",
                "description": "Dựa trên phân tích ML, tăng giá nhẹ để tối ưu revenue",
                "status": "DRAFT",
                "actions": [
                    {
                        "action_type": "change_price",
                        "target_level": "product",
                        "product_sk": 12345,
                        "current_value": 100000,
                        "recommended_value": 102000,
                        "chosen_value": 102000,
                        "unit": "VND",
                        "status": "PLANNED"
                    }
                ]
            }
        }


class DSSDecisionSummaryResponse(BaseModel):
    """Summary response for decision list"""
    decision_id: int
    scenario_key: str
    title: str
    status: str
    created_by: int
    created_by_email: Optional[str] = None
    created_at: str
    num_actions: int

    class Config:
        json_schema_extra = {
            "example": {
                "decision_id": 1,
                "scenario_key": "price_prediction",
                "title": "Tăng giá 2% cho nhóm máy in Tiki",
                "status": "DRAFT",
                "created_by": 3,
                "created_by_email": "analyst@example.com",
                "created_at": "2025-11-30T14:30:00",
                "num_actions": 5
            }
        }


class DSSDecisionDetailResponse(BaseModel):
    """Detailed response for a single decision"""
    decision_id: int
    session_id: int
    scenario_key: str
    title: str
    description: Optional[str] = None
    status: str
    
    # Creator info
    created_by: int
    created_by_email: Optional[str] = None
    created_at: str
    updated_at: str
    
    # Approval info (if applicable)
    approved_by: Optional[int] = None
    approved_by_email: Optional[str] = None
    approved_at: Optional[str] = None
    
    # Session snapshot
    filters: Dict[str, Any]
    kpi_summary: Dict[str, Any]
    ai_summary_insights: List[str]
    ai_recommended_actions: List[str]
    date_adjustment_info: Optional[Dict[str, Any]] = None
    
    # Actions
    actions: List[DSSActionItemResponse]

    class Config:
        json_schema_extra = {
            "example": {
                "decision_id": 1,
                "session_id": 10,
                "scenario_key": "price_prediction",
                "title": "Tăng giá 2% cho nhóm máy in Tiki",
                "description": "Dựa trên phân tích ML từ ngày 23-24/11",
                "status": "DRAFT",
                "created_by": 3,
                "created_by_email": "analyst@example.com",
                "created_at": "2025-11-30T14:30:00",
                "updated_at": "2025-11-30T14:30:00",
                "approved_by": None,
                "approved_by_email": None,
                "approved_at": None,
                "filters": {
                    "from_date": "2025-11-23",
                    "to_date": "2025-11-24",
                    "platforms": ["tiki"]
                },
                "kpi_summary": {
                    "num_products": 150,
                    "current_total_revenue": 50000000
                },
                "ai_summary_insights": ["Insight 1", "Insight 2"],
                "ai_recommended_actions": ["Action 1", "Action 2"],
                "date_adjustment_info": None,
                "actions": [
                    {
                        "action_id": 1,
                        "action_type": "change_price",
                        "target_level": "product",
                        "product_sk": 12345,
                        "product_name": "Máy in HP LaserJet",
                        "current_value": 100000,
                        "recommended_value": 102000,
                        "chosen_value": 102000,
                        "unit": "VND",
                        "status": "PLANNED",
                        "planned_start_date": "2025-12-01",
                        "planned_end_date": None,
                        "note": None,
                        "category_name": "Máy in",
                        "platform_name": "Tiki"
                    }
                ]
            }
        }


class DSSDecisionListResponse(BaseModel):
    """Paginated list response for decisions"""
    total: int
    page: int
    page_size: int
    items: List[DSSDecisionSummaryResponse]

    class Config:
        json_schema_extra = {
            "example": {
                "total": 25,
                "page": 1,
                "page_size": 10,
                "items": [
                    {
                        "decision_id": 1,
                        "scenario_key": "price_prediction",
                        "title": "Tăng giá 2% cho nhóm máy in",
                        "status": "DRAFT",
                        "created_by": 3,
                        "created_by_email": "analyst@example.com",
                        "created_at": "2025-11-30T14:30:00",
                        "num_actions": 5
                    }
                ]
            }
        }
