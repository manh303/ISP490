#!/usr/bin/env python3
"""
DSS Drill-Down Analytics API
=============================
Backend cho Interactive Drill-Down Analysis:
- Overall Dashboard (tổng quát)
- Platform Dashboard (drill down theo platform)
- Campaign Dashboard (drill down theo campaign)
- Category Dashboard (drill down theo category)
- Product Level Analysis (mở rộng top products)
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from enum import Enum
import logging

router = APIRouter(prefix="/api/v1/dss/drilldown", tags=["DSS Drill-Down Analytics"])
logger = logging.getLogger(__name__)


# ====================================
# PYDANTIC MODELS
# ====================================

class DateRange(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD


class FilterParams(BaseModel):
    date_range: Optional[DateRange] = None
    platforms: Optional[List[str]] = None  # ['lazada', 'tiki', 'fptshop']
    categories: Optional[List[str]] = None
    campaigns: Optional[List[str]] = None
    brands: Optional[List[str]] = None
    price_range: Optional[Dict[str, float]] = None  # {min, max}


class RevenueMetrics(BaseModel):
    total_revenue: float
    previous_period_revenue: float
    revenue_change_percent: float
    revenue_trend: str  # 'increasing', 'decreasing', 'stable'
    orders_count: int
    avg_order_value: float


class CategoryMetrics(BaseModel):
    category_name: str
    category_code: str
    revenue: float
    revenue_percent: float  # % of total
    previous_period_revenue: float
    revenue_change_percent: float
    orders_count: int
    avg_rating: float
    products_count: int
    out_of_stock_count: int


class PlatformMetrics(BaseModel):
    platform_name: str
    platform_code: str
    revenue: float
    revenue_percent: float
    previous_period_revenue: float
    revenue_change_percent: float
    orders_count: int
    avg_rating: float
    products_count: int


class ProductMetrics(BaseModel):
    global_product_id: str
    product_name: str
    brand_name: str
    category_name: str
    platform_name: str
    current_price: float
    previous_price: float
    price_change_percent: float
    revenue: float
    orders_count: int
    avg_rating: float
    sold_count: int
    is_available: bool
    out_of_stock_reason: Optional[str] = None  # 'price_increased', 'out_of_stock', 'demand_low'
    review_sentiment: str  # 'positive', 'neutral', 'negative'


class OverallDashboard(BaseModel):
    period_label: str
    revenue_metrics: RevenueMetrics
    top_categories: List[CategoryMetrics]
    top_platforms: List[PlatformMetrics]
    key_alerts: List[Dict[str, Any]]
    timestamp: str


class PlatformDrilldown(BaseModel):
    platform_name: str
    platform_revenue_metrics: RevenueMetrics
    top_categories: List[CategoryMetrics]
    category_performance: Dict[str, Any]  # detailed breakdown
    problematic_categories: List[Dict[str, Any]]  # categories with decline
    timestamp: str


class CategoryDrilldown(BaseModel):
    category_name: str
    category_revenue_metrics: RevenueMetrics
    top_brands: List[Dict[str, Any]]
    top_products: List[ProductMetrics]
    price_changes: List[Dict[str, Any]]
    out_of_stock_products: List[ProductMetrics]
    stock_changes: List[Dict[str, Any]]
    timestamp: str


class ProductDrilldown(BaseModel):
    product_info: ProductMetrics
    price_history: List[Dict[str, Any]]  # date, price, discount
    availability_history: List[Dict[str, Any]]  # date, available, reason
    reviews_summary: Dict[str, Any]  # rating distribution, sentiment
    competitor_prices: List[Dict[str, Any]]  # platform, brand, price
    sales_trend: List[Dict[str, Any]]  # date, sales, orders
    timestamp: str


# ====================================
# HELPER FUNCTIONS FOR DATABASE QUERIES
# ====================================

async def get_db():
    """Get database connection - inject from main.py"""
    try:
        from app.main import db_manager
    except ImportError:
        raise HTTPException(status_code=500, detail="Failed to get database connection")
    
    if not db_manager.is_connected:
        await db_manager.connect()
    
    return db_manager


def build_date_filter(date_range: Optional[DateRange]) -> tuple:
    """
    Build date filter SQL and parameters
    Returns: (where_clause, params_dict)
    """
    if not date_range:
        # Default: last 30 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        return (
            "AND date_sk >= :start_date_sk AND date_sk <= :end_date_sk",
            {"start_date": start_date, "end_date": end_date}
        )
    
    return (
        "AND date_sk >= :start_date_sk AND date_sk <= :end_date_sk",
        {"start_date": date_range.start_date, "end_date": date_range.end_date}
    )


def build_platform_filter(platforms: Optional[List[str]]) -> tuple:
    """Build platform filter SQL and parameters"""
    if not platforms:
        return "", {}
    
    placeholders = ",".join([f"'{p}'" for p in platforms])
    return f"AND pl.platform_code IN ({placeholders})", {}


def build_category_filter(categories: Optional[List[str]]) -> tuple:
    """Build category filter SQL and parameters"""
    if not categories:
        return "", {}
    
    placeholders = ",".join([f"'{c}'" for c in categories])
    return f"AND cat.category_code IN ({placeholders})", {}


# ====================================
# ENDPOINT 1: OVERALL DASHBOARD
# ====================================

@router.get("/overall", response_model=OverallDashboard)
async def get_overall_dashboard(
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    db = Depends(get_db)
):
    """
    Get overall dashboard with revenue metrics, top categories & platforms, and key alerts
    
    Flow:
    1. Get revenue metrics (current vs previous period)
    2. Get top 5 categories by revenue
    3. Get top 3 platforms by revenue
    4. Identify problematic areas (declining revenue)
    """
    try:
        date_range = DateRange(
            start_date=start_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            end_date=end_date or datetime.now().strftime("%Y-%m-%d")
        )
        
        # 1. Get current period revenue metrics
        query_current = """
        SELECT 
            SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as total_revenue,
            COUNT(DISTINCT f.product_sk) as products_count,
            COUNT(DISTINCT CASE WHEN f.is_available THEN f.product_sk END) as available_products,
            AVG(f.price_current) as avg_price
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE d.date_value >= :start_date AND d.date_value <= :end_date
        """
        
        current_result = await db.execute_query(query_current, {
            "start_date": date_range.start_date,
            "end_date": date_range.end_date
        })
        
        current_data = current_result[0] if current_result else {}
        total_revenue = float(current_data.get('total_revenue') or 0)
        
        # 2. Get previous period revenue for comparison (same duration)
        start_dt = datetime.strptime(date_range.start_date, "%Y-%m-%d")
        period_days = (datetime.strptime(date_range.end_date, "%Y-%m-%d") - start_dt).days
        prev_start = (start_dt - timedelta(days=period_days)).strftime("%Y-%m-%d")
        prev_end = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        
        query_previous = """
        SELECT SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as prev_revenue
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE d.date_value >= :prev_start AND d.date_value <= :prev_end
        """
        
        prev_result = await db.execute_query(query_previous, {
            "prev_start": prev_start,
            "prev_end": prev_end
        })
        
        prev_data = prev_result[0] if prev_result else {}
        prev_revenue = float(prev_data.get('prev_revenue') or 0)
        
        # Calculate change
        revenue_change = 0 if prev_revenue == 0 else ((total_revenue - prev_revenue) / prev_revenue) * 100
        
        # 3. Get top 5 categories by revenue
        query_categories = """
        SELECT 
            cat.category_name,
            cat.category_code,
            SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as category_revenue,
            COUNT(DISTINCT f.product_sk) as product_count,
            AVG(f.rating_avg) as avg_rating
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE d.date_value >= :start_date AND d.date_value <= :end_date
        GROUP BY cat.category_name, cat.category_code
        ORDER BY category_revenue DESC
        LIMIT 5
        """
        
        categories_result = await db.execute_query(query_categories, {
            "start_date": date_range.start_date,
            "end_date": date_range.end_date
        })
        
        top_categories = []
        for row in categories_result:
            cat_revenue = float(row.get('category_revenue') or 0)
            top_categories.append(CategoryMetrics(
                category_name=row['category_name'],
                category_code=row['category_code'],
                revenue=cat_revenue,
                revenue_percent=round((cat_revenue / total_revenue * 100) if total_revenue > 0 else 0, 2),
                previous_period_revenue=0,  # TODO: implement
                revenue_change_percent=0,   # TODO: implement
                orders_count=0,             # TODO: query orders
                avg_rating=float(row.get('avg_rating') or 0),
                products_count=row.get('product_count', 0),
                out_of_stock_count=0        # TODO: query
            ))
        
        # 4. Get top 3 platforms by revenue
        query_platforms = """
        SELECT 
            pl.platform_name,
            pl.platform_code,
            SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as platform_revenue,
            COUNT(DISTINCT f.product_sk) as product_count,
            AVG(f.rating_avg) as avg_rating
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE d.date_value >= :start_date AND d.date_value <= :end_date
        GROUP BY pl.platform_name, pl.platform_code
        ORDER BY platform_revenue DESC
        LIMIT 3
        """
        
        platforms_result = await db.execute_query(query_platforms, {
            "start_date": date_range.start_date,
            "end_date": date_range.end_date
        })
        
        top_platforms = []
        for row in platforms_result:
            plat_revenue = float(row.get('platform_revenue') or 0)
            top_platforms.append(PlatformMetrics(
                platform_name=row['platform_name'],
                platform_code=row['platform_code'],
                revenue=plat_revenue,
                revenue_percent=round((plat_revenue / total_revenue * 100) if total_revenue > 0 else 0, 2),
                previous_period_revenue=0,
                revenue_change_percent=0,
                orders_count=0,
                avg_rating=float(row.get('avg_rating') or 0),
                products_count=row.get('product_count', 0)
            ))
        
        # 5. Identify alerts (categories/platforms with >10% decline)
        alerts = []
        if revenue_change < -10:
            alerts.append({
                "type": "warning",
                "severity": "high",
                "title": f"Overall Revenue Declined {abs(revenue_change):.1f}%",
                "message": f"Revenue decreased from {prev_revenue:,.0f} to {total_revenue:,.0f} VND",
                "action": "Drill down to platform/category level to identify root cause"
            })
        
        return OverallDashboard(
            period_label=f"{date_range.start_date} to {date_range.end_date}",
            revenue_metrics=RevenueMetrics(
                total_revenue=total_revenue,
                previous_period_revenue=prev_revenue,
                revenue_change_percent=round(revenue_change, 2),
                revenue_trend="decreasing" if revenue_change < -10 else ("increasing" if revenue_change > 10 else "stable"),
                orders_count=0,  # TODO
                avg_order_value=0  # TODO
            ),
            top_categories=top_categories,
            top_platforms=top_platforms,
            key_alerts=alerts,
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error in overall dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ====================================
# ENDPOINT 2: PLATFORM DRILL-DOWN
# ====================================

@router.get("/platform/{platform_code}", response_model=PlatformDrilldown)
async def get_platform_drilldown(
    platform_code: str,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db = Depends(get_db)
):
    """
    Drill down into a specific platform
    
    Flow:
    1. Get platform revenue metrics (current vs previous)
    2. Get top categories within this platform
    3. Identify categories with revenue decline
    
    Example: Analyst thấy "Lazada revenue giảm 20%"
    - Drill down to see: Electronics -20%, Home -5%, Fashion +10%
    """
    try:
        date_range = DateRange(
            start_date=start_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            end_date=end_date or datetime.now().strftime("%Y-%m-%d")
        )
        
        # 1. Get platform revenue metrics
        query_platform = """
        SELECT 
            pl.platform_name,
            pl.platform_code,
            SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as platform_revenue
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE pl.platform_code = :platform_code 
          AND d.date_value >= :start_date 
          AND d.date_value <= :end_date
        GROUP BY pl.platform_name, pl.platform_code
        """
        
        platform_result = await db.execute_query(query_platform, {
            "platform_code": platform_code,
            "start_date": date_range.start_date,
            "end_date": date_range.end_date
        })
        
        if not platform_result:
            raise HTTPException(status_code=404, detail=f"Platform {platform_code} not found")
        
        platform_data = platform_result[0]
        platform_name = platform_data.get('platform_name')
        platform_revenue = float(platform_data.get('platform_revenue') or 0)
        
        # 2. Get top categories for this platform
        query_categories = """
        SELECT 
            cat.category_name,
            cat.category_code,
            SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as category_revenue,
            COUNT(DISTINCT f.product_sk) as product_count,
            AVG(f.rating_avg) as avg_rating,
            SUM(CASE WHEN NOT f.is_available THEN 1 ELSE 0 END) as out_of_stock
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE pl.platform_code = :platform_code 
          AND d.date_value >= :start_date 
          AND d.date_value <= :end_date
        GROUP BY cat.category_name, cat.category_code
        ORDER BY category_revenue DESC
        """
        
        categories_result = await db.execute_query(query_categories, {
            "platform_code": platform_code,
            "start_date": date_range.start_date,
            "end_date": date_range.end_date
        })
        
        top_categories = []
        for row in categories_result:
            cat_revenue = float(row.get('category_revenue') or 0)
            top_categories.append(CategoryMetrics(
                category_name=row['category_name'],
                category_code=row['category_code'],
                revenue=cat_revenue,
                revenue_percent=round((cat_revenue / platform_revenue * 100) if platform_revenue > 0 else 0, 2),
                previous_period_revenue=0,
                revenue_change_percent=0,
                orders_count=0,
                avg_rating=float(row.get('avg_rating') or 0),
                products_count=row.get('product_count', 0),
                out_of_stock_count=row.get('out_of_stock', 0) or 0
            ))
        
        # 3. Identify problematic categories (>10% decline)
        problematic_categories = []
        for cat in top_categories:
            if cat.revenue_change_percent < -10:
                problematic_categories.append({
                    "category_name": cat.category_name,
                    "revenue": cat.revenue,
                    "change_percent": cat.revenue_change_percent,
                    "out_of_stock_count": cat.out_of_stock_count,
                    "action": "Check category for price changes and stock issues"
                })
        
        return PlatformDrilldown(
            platform_name=platform_name,
            platform_revenue_metrics=RevenueMetrics(
                total_revenue=platform_revenue,
                previous_period_revenue=0,
                revenue_change_percent=0,
                revenue_trend="stable",
                orders_count=0,
                avg_order_value=0
            ),
            top_categories=top_categories,
            category_performance={},  # detailed breakdown
            problematic_categories=problematic_categories,
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error in platform drilldown: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ====================================
# ENDPOINT 3: CATEGORY DRILL-DOWN
# ====================================

@router.get("/category/{category_code}", response_model=CategoryDrilldown)
async def get_category_drilldown(
    category_code: str,
    platform_code: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db = Depends(get_db)
):
    """
    Drill down into a specific category
    
    Flow:
    1. Get category revenue metrics
    2. Get top brands within category
    3. Get top products (show price changes and stock status)
    4. Identify problematic products
    
    Example: Analyst drill down "Lazada - Electronics"
    - See revenue breakdown by brand
    - See top products with price changes and stock status
    - Identify: Brand X prices up 15%, 3 products out of stock
    """
    try:
        date_range = DateRange(
            start_date=start_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            end_date=end_date or datetime.now().strftime("%Y-%m-%d")
        )
        
        # Build platform filter if provided
        platform_filter = ""
        platform_params = {}
        if platform_code:
            platform_filter = "AND pl.platform_code = :platform_code"
            platform_params = {"platform_code": platform_code}
        
        # 1. Get category revenue metrics
        query_category = f"""
        SELECT 
            cat.category_name,
            cat.category_code,
            SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as category_revenue
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
        {"JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk" if platform_filter else ""}
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE cat.category_code = :category_code 
          AND d.date_value >= :start_date 
          AND d.date_value <= :end_date
          {platform_filter}
        GROUP BY cat.category_name, cat.category_code
        """
        
        query_params = {
            "category_code": category_code,
            "start_date": date_range.start_date,
            "end_date": date_range.end_date,
            **platform_params
        }
        
        category_result = await db.execute_query(query_category, query_params)
        
        if not category_result:
            raise HTTPException(status_code=404, detail=f"Category {category_code} not found")
        
        category_data = category_result[0]
        category_name = category_data.get('category_name')
        category_revenue = float(category_data.get('category_revenue') or 0)
        
        # 2. Get top brands in this category
        query_brands = f"""
        SELECT 
            b.brand_name,
            b.brand_sk,
            SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as brand_revenue,
            COUNT(DISTINCT f.product_sk) as product_count,
            AVG(f.price_current) as avg_price
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
        JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
        {"JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk" if platform_filter else ""}
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE cat.category_code = :category_code 
          AND d.date_value >= :start_date 
          AND d.date_value <= :end_date
          {platform_filter}
        GROUP BY b.brand_name, b.brand_sk
        ORDER BY brand_revenue DESC
        LIMIT 10
        """
        
        brands_result = await db.execute_query(query_brands, query_params)
        
        top_brands = []
        for row in brands_result:
            brand_revenue = float(row.get('brand_revenue') or 0)
            top_brands.append({
                "brand_name": row['brand_name'],
                "brand_sk": row['brand_sk'],
                "revenue": brand_revenue,
                "revenue_percent": round((brand_revenue / category_revenue * 100) if category_revenue > 0 else 0, 2),
                "product_count": row.get('product_count', 0),
                "avg_price": float(row.get('avg_price') or 0)
            })
        
        # 3. Get top products with price and availability info
        query_products = f"""
        SELECT 
            p.global_product_id,
            p.product_name,
            b.brand_name,
            f.price_current,
            f.discount_pct,
            f.is_available,
            f.sold_count,
            f.rating_avg,
            pl.platform_name,
            SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as product_revenue
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
        JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE cat.category_code = :category_code 
          AND d.date_value >= :start_date 
          AND d.date_value <= :end_date
          {platform_filter}
        GROUP BY p.global_product_id, p.product_name, b.brand_name, 
                 f.price_current, f.discount_pct, f.is_available, f.sold_count, 
                 f.rating_avg, pl.platform_name
        ORDER BY product_revenue DESC
        LIMIT 20
        """
        
        products_result = await db.execute_query(query_products, query_params)
        
        top_products = []
        out_of_stock_products = []
        for row in products_result:
            product_revenue = float(row.get('product_revenue') or 0)
            product_metric = ProductMetrics(
                global_product_id=row['global_product_id'],
                product_name=row['product_name'],
                brand_name=row['brand_name'],
                category_name=category_name,
                platform_name=row['platform_name'],
                current_price=float(row.get('price_current') or 0),
                previous_price=0,  # TODO: get from price history
                price_change_percent=0,  # TODO: calculate
                revenue=product_revenue,
                orders_count=0,  # TODO
                avg_rating=float(row.get('rating_avg') or 0),
                sold_count=row.get('sold_count', 0) or 0,
                is_available=row.get('is_available', True),
                out_of_stock_reason=None,
                review_sentiment="neutral"  # TODO
            )
            
            if row.get('is_available'):
                top_products.append(product_metric)
            else:
                out_of_stock_products.append(product_metric)
        
        return CategoryDrilldown(
            category_name=category_name,
            category_revenue_metrics=RevenueMetrics(
                total_revenue=category_revenue,
                previous_period_revenue=0,
                revenue_change_percent=0,
                revenue_trend="stable",
                orders_count=0,
                avg_order_value=0
            ),
            top_brands=top_brands,
            top_products=top_products[:10],  # Top 10 available products
            price_changes=[],  # TODO: implement
            out_of_stock_products=out_of_stock_products,
            stock_changes=[],  # TODO: implement
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error in category drilldown: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ====================================
# ENDPOINT 4: PRODUCT DETAIL
# ====================================

@router.get("/product/{global_product_id}", response_model=ProductDrilldown)
async def get_product_detail(
    global_product_id: str,
    platform_code: Optional[str] = Query(None),
    days: int = Query(30, ge=7, le=365),
    db = Depends(get_db)
):
    """
    Get detailed product information including:
    - Price history (detection of price increases)
    - Availability history
    - Reviews and sentiment
    - Competitor prices
    - Sales trend
    
    Example: Analyst mở product "Brand X - Electronics" trên Lazada
    - See price increased 15% trong tuần cuối
    - See 3 variants out of stock từ tuần trước
    - See competitor prices (Tiki, FPTShop)
    """
    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # 1. Get product basic info
        query_product = """
        SELECT 
            p.global_product_id,
            p.product_name,
            b.brand_name,
            cat.category_name,
            p.seller_name
        FROM dwh_dim_product p
        LEFT JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
        LEFT JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
        WHERE p.global_product_id = :global_product_id
        LIMIT 1
        """
        
        product_result = await db.execute_query(query_product, {
            "global_product_id": global_product_id
        })
        
        if not product_result:
            raise HTTPException(status_code=404, detail=f"Product {global_product_id} not found")
        
        product_info = product_result[0]
        
        # 2. Get current price and availability for specified platform
        platform_filter = ""
        platform_params = {}
        if platform_code:
            platform_filter = "AND pl.platform_code = :platform_code"
            platform_params = {"platform_code": platform_code}
        
        query_current = f"""
        SELECT 
            f.price_current,
            f.price_original,
            f.discount_pct,
            f.is_available,
            f.rating_avg,
            f.sold_count,
            pl.platform_name,
            d.date_value
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE p.global_product_id = :global_product_id
          {platform_filter}
        ORDER BY d.date_value DESC
        LIMIT 1
        """
        
        current_result = await db.execute_query(query_current, {
            "global_product_id": global_product_id,
            **platform_params
        })
        
        current_data = current_result[0] if current_result else {}
        
        # 3. Get price history (detect increases)
        query_price_history = f"""
        SELECT 
            d.date_value,
            f.price_current,
            f.price_original,
            f.discount_pct,
            pl.platform_name
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE p.global_product_id = :global_product_id
          AND d.date_value >= :start_date
          AND d.date_value <= :end_date
          {platform_filter}
        ORDER BY d.date_value
        """
        
        price_history_result = await db.execute_query(query_price_history, {
            "global_product_id": global_product_id,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            **platform_params
        })
        
        price_history = [
            {
                "date": row['date_value'].isoformat() if hasattr(row['date_value'], 'isoformat') else str(row['date_value']),
                "price": float(row.get('price_current') or 0),
                "original_price": float(row.get('price_original') or 0),
                "discount_percent": float(row.get('discount_pct') or 0),
                "platform": row.get('platform_name')
            }
            for row in price_history_result
        ]
        
        # 4. Get availability history
        query_availability = f"""
        SELECT 
            d.date_value,
            f.is_available,
            pl.platform_name
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE p.global_product_id = :global_product_id
          AND d.date_value >= :start_date
          AND d.date_value <= :end_date
          {platform_filter}
        ORDER BY d.date_value
        """
        
        availability_result = await db.execute_query(query_availability, {
            "global_product_id": global_product_id,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            **platform_params
        })
        
        availability_history = [
            {
                "date": row['date_value'].isoformat() if hasattr(row['date_value'], 'isoformat') else str(row['date_value']),
                "available": row.get('is_available', True),
                "reason": "Out of Stock" if not row.get('is_available') else "Available",
                "platform": row.get('platform_name')
            }
            for row in availability_result
        ]
        
        # 5. Get competitor prices (same product on other platforms)
        query_competitors = """
        SELECT DISTINCT
            pl.platform_name,
            b.brand_name,
            f.price_current,
            f.rating_avg
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_product p ON f.product_sk = p.product_sk
        JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        WHERE p.global_product_id = :global_product_id
          AND d.date_value = :end_date
        ORDER BY f.price_current
        """
        
        competitors_result = await db.execute_query(query_competitors, {
            "global_product_id": global_product_id,
            "end_date": end_date.strftime("%Y-%m-%d")
        })
        
        competitor_prices = [
            {
                "platform": row.get('platform_name'),
                "brand": row.get('brand_name'),
                "price": float(row.get('price_current') or 0),
                "rating": float(row.get('rating_avg') or 0)
            }
            for row in competitors_result
        ]
        
        return ProductDrilldown(
            product_info=ProductMetrics(
                global_product_id=global_product_id,
                product_name=product_info.get('product_name'),
                brand_name=product_info.get('brand_name') or 'Unknown',
                category_name=product_info.get('category_name') or 'Unknown',
                platform_name=current_data.get('platform_name') or 'Unknown',
                current_price=float(current_data.get('price_current') or 0),
                previous_price=0,
                price_change_percent=0,
                revenue=0,
                orders_count=0,
                avg_rating=float(current_data.get('rating_avg') or 0),
                sold_count=current_data.get('sold_count', 0) or 0,
                is_available=current_data.get('is_available', True),
                out_of_stock_reason=None if current_data.get('is_available') else "Out of Stock",
                review_sentiment="neutral"
            ),
            price_history=price_history,
            availability_history=availability_history,
            reviews_summary={},  # TODO: implement
            competitor_prices=competitor_prices,
            sales_trend=[],  # TODO: implement
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error in product detail: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ====================================
# ENDPOINT 5: COMPARISON DRILL-DOWN
# ====================================

@router.get("/compare")
async def compare_metrics(
    metric_type: str = Query("revenue", description="revenue|products|rating"),
    group_by: str = Query("platform", description="platform|category|brand"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    filters: Optional[str] = Query(None),  # JSON filter string
    db = Depends(get_db)
):
    """
    Flexible comparison tool
    
    Examples:
    - GET /compare?metric_type=revenue&group_by=platform&start_date=2024-01-01&end_date=2024-12-31
    - GET /compare?metric_type=products&group_by=category&filters={"platform": "lazada"}
    - GET /compare?metric_type=rating&group_by=brand&filters={"category": "Electronics"}
    """
    try:
        date_range = DateRange(
            start_date=start_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            end_date=end_date or datetime.now().strftime("%Y-%m-%d")
        )
        
        # TODO: Implement flexible comparison logic
        return {
            "metric_type": metric_type,
            "group_by": group_by,
            "data": [],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in compare: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
