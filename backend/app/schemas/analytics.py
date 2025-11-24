# app/schemas/analytics.py
from datetime import date
from typing import List, Optional, Dict
from pydantic import BaseModel, Field


# ====== FILTER / METADATA ======

class PlatformFilterItem(BaseModel):
    platform_code: str = Field(..., description="tiki / lazada / ...")
    platform_name: Optional[str] = Field(None, description="Tên hiển thị")


class CategoryFilterItem(BaseModel):
    category_key: str
    category_name: str
    level: Optional[int] = None
    parent_key: Optional[str] = None
    platform_code: Optional[str] = None


class ProductFilterItem(BaseModel):
    product_key: str
    product_name: str
    platform_code: str
    category_key: Optional[str] = None


# ====== OVERVIEW KPI & TRENDS ======

class OverviewKPIResponse(BaseModel):
    from_date: date
    to_date: date
    platform_code: Optional[str] = None
    category_key: Optional[str] = None
    category_name: Optional[str] = None

    total_revenue: float
    total_products: int
    total_reviews: int
    avg_price: Optional[float] = None
    avg_rating: Optional[float] = None


class OverviewTrendPoint(BaseModel):
    date: date
    revenue: float
    total_orders: int
    avg_price: Optional[float] = None
    avg_rating: Optional[float] = None
    total_reviews: int


class OverviewTrendResponse(BaseModel):
    from_date: date
    to_date: date
    platform_code: Optional[str] = None
    category_key: Optional[str] = None
    points: List[OverviewTrendPoint]


# ====== PLATFORM COMPARISON ======

class PlatformComparisonItem(BaseModel):
    platform_code: str
    platform_name: Optional[str] = None

    total_revenue: float
    total_products: int
    avg_price: Optional[float] = None
    avg_rating: Optional[float] = None
    total_reviews: int


class CategoryShareItem(BaseModel):
    category_key: str
    category_name: Optional[str] = None
    platform_code: str
    revenue: float
    revenue_share: float


# ====== PRODUCT PERFORMANCE ======

class TopProductItem(BaseModel):
    product_key: str
    product_name: str
    platform_code: str
    category_key: Optional[str] = None
    category_name: Optional[str] = None  # Add category name

    total_revenue: float
    total_reviews: int
    avg_rating: Optional[float] = None
    avg_price: Optional[float] = None


class ProductTimeseriesPoint(BaseModel):
    date: date
    avg_price: Optional[float] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    total_reviews: int
    avg_rating: Optional[float] = None
    revenue: float


class ProductTimeseriesResponse(BaseModel):
    product_key: str
    platform_code: str
    from_date: date
    to_date: date
    points: List[ProductTimeseriesPoint]


# ====== REVIEW SUMMARY ======

class ReviewRatingBreakdown(BaseModel):
    by_rating: Dict[int, int]


class ReviewSummaryResponse(BaseModel):
    product_key: str
    platform_code: str
    from_date: date
    to_date: date

    total_reviews: int
    avg_rating: Optional[float] = None
    rating_breakdown: ReviewRatingBreakdown
    top_helpful_reviews: List[Dict]


# ====== PRICING ANALYTICS ======

class PriceDistributionResponse(BaseModel):
    platform_code: str
    category_key: Optional[str] = None
    category_name: Optional[str] = None
    from_date: date
    to_date: date

    min_price: Optional[float]
    p25_price: Optional[float]  # 25% products are cheaper than this price (Q1)
    median_price: Optional[float]  # 50% products are cheaper (median)
    p75_price: Optional[float]  # 75% products are cheaper than this price (Q3)
    max_price: Optional[float]


class PriceVsRevenueItem(BaseModel):
    product_key: str
    product_name: str
    platform_code: str
    category_key: Optional[str] = None

    avg_price: Optional[float]
    total_revenue: float
    avg_rating: Optional[float]
    total_reviews: int

class OverviewReportResponse(BaseModel):
    """Report tổng quan cho dashboard chính của Analyst."""
    from_date: date
    to_date: date
    platform_code: Optional[str] = None
    category_key: Optional[str] = None

    kpis: OverviewKPIResponse
    trends: OverviewTrendResponse
    platform_comparison: List[PlatformComparisonItem]
    category_share: List[CategoryShareItem]


class ProductReportResponse(BaseModel):
    """Report chi tiết cho 1 sản phẩm (price + review)."""
    product_key: str
    platform_code: str
    from_date: date
    to_date: date

    timeseries: ProductTimeseriesResponse
    review_summary: ReviewSummaryResponse