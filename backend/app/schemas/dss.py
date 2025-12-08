"""
DSS (Decision Support System) Schemas
Models for AI-powered decision support endpoints
"""

from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Dict, Any, Literal
from datetime import date
from enum import Enum


# ============================================
# ENUMS
# ============================================

class PriceScopeMode(str, Enum):
    """Scope mode for Price Prediction DSS"""
    BY_CATEGORY = "by_category"
    BY_PRODUCT = "by_product"


# ============================================
# COMMON REQUEST/RESPONSE MODELS
# ============================================

class DSSFilters(BaseModel):
    """Common filters for all DSS scenarios"""
    from_date: date = Field(..., description="Start date for analysis")
    to_date: date = Field(..., description="End date for analysis")
    platforms: Optional[List[str]] = Field(None, description="List of platform codes (e.g., ['tiki', 'lazada'])")
    categories: Optional[List[str]] = Field(None, description="List of category keys")


class DSSBaseResponse(BaseModel):
    """Base response for all DSS scenarios"""
    scenario: str = Field(..., description="Scenario type: price_prediction, product_recommendation, review_sentiment")
    filters: Dict[str, Any] = Field(..., description="Filters used for analysis")
    kpi_summary: Dict[str, Any] = Field(..., description="Summary KPIs")
    table_data: List[Dict[str, Any]] = Field(..., description="Detailed data table")
    
    # AI-generated insights
    ai_summary_insights: List[str] = Field(..., description="AI-generated insights (3-5 items)")
    ai_recommended_actions: List[str] = Field(..., description="AI-generated action recommendations (3-7 items)")
    
    # Metadata
    generated_at: str = Field(..., description="Timestamp of generation")
    ai_model_used: Optional[str] = Field(None, description="AI model used for insights")


# ============================================
# PRICE PREDICTION DSS
# ============================================

class PricePredictionRequest(BaseModel):
    """
    Request for Price Prediction DSS
    
    Supports two modes:
    1. by_category: Filter by platforms and categories, get top N products
    2. by_product: Get predictions for specific product keys
    
    Example 1 - by_category:
    {
        "scope_mode": "by_category",
        "from_date": "2025-12-01",
        "to_date": "2025-12-03",
        "platforms": ["tiki", "lazada"],
        "categories": ["1", "2"],
        "page": 1,
        "page_size": 50,
        "top_n": 50,
        "min_confidence": 0.7
    }
    
    Example 2 - by_product:
    {
        "scope_mode": "by_product",
        "from_date": "2025-12-01",
        "to_date": "2025-12-03",
        "product_keys": ["tiki_123456", "lazada_987654"],
        "min_confidence": 0.7
    }
    """
    from_date: date
    to_date: date
    
    # Filters for BY_CATEGORY mode
    platforms: Optional[List[str]] = Field(default=None, example=["tiki", "lazada"])
    categories: Optional[List[str]] = Field(default=None, example=["1", "2"])
    
    # Filters for BY_PRODUCT mode
    product_keys: Optional[List[str]] = Field(default=None, description="List of product keys for by_product mode")
    
    # Pagination
    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(50, ge=1, le=500, description="Items per page")
    
    # Scope mode (NEW)
    scope_mode: PriceScopeMode = Field(PriceScopeMode.BY_CATEGORY, description="'by_category' or 'by_product' (alias: 'specific_products' -> 'by_product')")
    top_n: int = Field(50, ge=1, le=500, description="Number of top products (only meaningful for by_category mode)")
    
    # Optimization constraints
    max_discount_pct: Optional[float] = Field(0.15, ge=0, le=0.5, description="Max discount allowed (0.15 = 15%)")
    min_margin_pct: Optional[float] = Field(0.10, ge=0, le=1.0, description="Min margin required (0.10 = 10%)")
    min_confidence: Optional[float] = Field(0.70, ge=0, le=1.0, description="Min confidence for recommendations")
    min_price_change_pct: Optional[float] = Field(0.02, ge=0, le=0.5, description="Min price change to include (0.02 = 2%)")
    ai_mode: Literal["full", "fast"] = Field("full", description="'full' uses LLM, 'fast' uses rule-based")
    
    @model_validator(mode='before')
    def normalize_scope_mode(cls, values):
        """Allow legacy alias 'specific_products' by mapping to by_product before validation."""
        if isinstance(values, dict):
            scope = values.get("scope_mode")
            if scope == "specific_products":
                values["scope_mode"] = PriceScopeMode.BY_PRODUCT
        return values

    @model_validator(mode='after')
    def validate_scope_requirements(self):
        """Validate that required fields are present based on scope_mode"""
        if self.scope_mode == PriceScopeMode.BY_CATEGORY:
            if not self.categories or len(self.categories) == 0:
                raise ValueError("categories must not be empty when scope_mode='by_category'")
        elif self.scope_mode == PriceScopeMode.BY_PRODUCT:
            if not self.product_keys or len(self.product_keys) == 0:
                raise ValueError("product_keys must not be empty when scope_mode='by_product'")
        return self


class PriceProductDetail(BaseModel):
    """Detail for one product in price prediction"""
    product_key: str
    product_name: str
    platform: str
    category_name: Optional[str] = None
    
    current_price: float
    recommended_price: float
    price_change_pct: float  # Negative = discount
    
    current_revenue: float
    projected_revenue: float
    expected_revenue_change_pct: float
    
    margin_pct: Optional[float] = None
    confidence: float
    
    # Additional metrics
    current_orders: Optional[int] = None
    projected_orders: Optional[int] = None
    avg_rating: Optional[float] = None
    total_reviews: Optional[int] = None


class PricePredictionResponse(DSSBaseResponse):
    """Response for Price Prediction DSS"""
    scenario: str = "price_prediction"
    
    # Override with specific types
    kpi_summary: Dict[str, Any] = Field(
        ...,
        description="KPIs: num_products, num_with_recommendation, current_total_revenue, projected_total_revenue"
    )
    table_data: List[Dict[str, Any]] = Field(..., description="List of PriceProductDetail dicts")


# ============================================
# PRODUCT RECOMMENDATION DSS
# ============================================

class ProductRecommendationRequest(BaseModel):
    """
    Request for Product Recommendation DSS
    
    Example (by_product):
    {
        "from_date": "2025-11-23",
        "to_date": "2025-11-24",
        "platforms": ["tiki"],
        "scope_mode": "by_product",
        "source_product_key": "tiki_9975869",
        "top_k": 10,
        "min_similarity": 0.5
    }
    
    Example (by_category):
    {
        "from_date": "2025-11-23",
        "to_date": "2025-11-24",
        "platforms": ["tiki", "lazada"],
        "categories": ["1", "2"],
        "scope_mode": "by_category",
        "top_k": 20,
        "min_similarity": 0.6
    }
    """
    from_date: date
    to_date: date
    platforms: Optional[List[str]] = Field(None, example=["tiki"])
    categories: Optional[List[str]] = Field(None, example=["1"])
    
    # Scope configuration (accept alias 'specific_products' -> 'by_product')
    scope_mode: str = Field("by_category", description="'by_product' or 'by_category' (alias: 'specific_products' -> 'by_product')")
    
    # If by_product
    source_product_key: Optional[str] = Field(None, description="Source product key (if scope_mode=by_product)")
    
    # Common
    top_k: int = Field(10, ge=1, le=50, description="Number of recommendations per product")
    min_similarity: Optional[float] = Field(0.5, ge=0, le=1.0, description="Min similarity score")
    min_co_purchase_rate: Optional[float] = Field(0.05, ge=0, le=1.0, description="Min co-purchase rate")
    ai_mode: Literal["full", "fast"] = Field("full", description="'full' uses LLM, 'fast' uses rule-based")

    @model_validator(mode='before')
    def normalize_scope_mode(cls, values):
        """Map legacy alias 'specific_products' -> 'by_product' before validation."""
        if isinstance(values, dict):
            scope = values.get("scope_mode")
            if scope == "specific_products":
                values["scope_mode"] = "by_product"
        return values

    @model_validator(mode='after')
    def validate_scope_mode(self):
        allowed = {"by_category", "by_product"}
        if self.scope_mode not in allowed:
            raise ValueError("scope_mode must be 'by_category' or 'by_product'")
        if self.scope_mode == "by_product" and not self.source_product_key:
            raise ValueError("source_product_key is required when scope_mode='by_product'")
        if self.scope_mode == "by_category" and (not self.categories or len(self.categories) == 0):
            raise ValueError("categories must not be empty when scope_mode='by_category'")
        return self


class RecommendationPairDetail(BaseModel):
    """Detail for one recommendation pair"""
    source_product_key: str
    source_product_name: str
    
    recommended_product_key: str
    recommended_product_name: str
    
    platform: str
    category_name: Optional[str] = None
    
    similarity_score: float
    co_purchase_rate: float  # Percentage of orders with both products
    co_purchase_count: Optional[int] = None  # NEW: Number of co-purchases
    
    avg_bundle_revenue: float  # Average revenue when bought together
    total_bundle_orders: Optional[int] = None
    window_days: Optional[int] = None  # NEW: Data window for analysis
    
    recommendation_type: str = Field("cross_sell", description="cross_sell, upsell, similar")


class ProductRecommendationResponse(DSSBaseResponse):
    """Response for Product Recommendation DSS"""
    scenario: str = "product_recommendation"
    
    kpi_summary: Dict[str, Any] = Field(
        ...,
        description="KPIs: source_product, num_recommendations, avg_similarity, total_bundle_opportunity"
    )
    table_data: List[Dict[str, Any]] = Field(..., description="List of RecommendationPairDetail dicts")


# ============================================
# REVIEW SENTIMENT DSS
# ============================================

class ReviewSentimentRequest(BaseModel):
    """
    Request for Review Sentiment Analysis DSS
    
    Example:
    {
        "from_date": "2025-11-23",
        "to_date": "2025-11-24",
        "platforms": ["tiki", "lazada"],
        "categories": ["1", "2"],
        "min_reviews_per_product": 10,
        "sentiment_focus": "all",
        "negative_threshold": 0.25
    }
    """
    from_date: date
    to_date: date
    platforms: Optional[List[str]] = Field(None, example=["tiki", "lazada"])
    categories: Optional[List[str]] = Field(None, example=["1"])
    
    # Filters
    min_reviews_per_product: int = Field(10, ge=1, description="Min number of reviews to include product")
    sentiment_focus: str = Field("all", description="'all', 'only_negative', 'only_positive'")
    
    # Thresholds
    negative_threshold: float = Field(0.25, ge=0, le=1.0, description="Products with negative_pct > threshold are flagged")
    positive_threshold: float = Field(0.7, ge=0, le=1.0, description="Threshold for positive_pct in only_positive filter")
    ai_mode: Literal["full", "fast"] = Field("full", description="'full' uses LLM, 'fast' uses rule-based")


class ProductSentimentDetail(BaseModel):
    """Sentiment detail for one product"""
    product_key: str
    product_name: str
    platform: str
    category_name: Optional[str] = None
    
    total_reviews: int
    
    positive_count: int
    neutral_count: int
    negative_count: int
    
    positive_pct: float
    neutral_pct: float
    negative_pct: float
    
    avg_rating: Optional[float] = None
    
    # Top reasons (keywords/phrases)
    top_positive_reasons: List[str] = Field(default_factory=list, description="Top positive keywords/phrases")
    top_negative_reasons: List[str] = Field(default_factory=list, description="Top negative keywords/phrases")
    
    # Flags
    is_critical: bool = Field(False, description="True if negative_pct > threshold")
    sentiment_trend: Optional[str] = Field(None, description="'improving', 'declining', 'stable'")


class ReviewSentimentResponse(DSSBaseResponse):
    """Response for Review Sentiment DSS"""
    scenario: str = "review_sentiment"
    
    kpi_summary: Dict[str, Any] = Field(
        ...,
        description="KPIs: num_products, avg_positive_pct, num_products_with_critical_negative, total_reviews"
    )
    table_data: List[Dict[str, Any]] = Field(..., description="List of ProductSentimentDetail dicts")

