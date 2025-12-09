# app/api/v1/analytics.py
from datetime import date
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, Query, HTTPException
from app.api.dependencies import require_role
from app.schemas.analytics import (
    PlatformFilterItem,
    CategoryFilterItem,
    ProductFilterItem,
    OverviewKPIResponse,
    OverviewTrendResponse,
    PlatformComparisonItem,
    PlatformComparisonResponse,
    CategoryShareItem,
    TopProductItem,
    ProductTimeseriesResponse,
    ReviewSummaryResponse,
    PriceDistributionResponse,
    PriceVsRevenueItem,
    OverviewReportResponse,
    ProductReportResponse,
    RatingDistributionData,
    CriticalProductItem,
)
from app.services.analytics_service import AnalyticsService
from app.db_config import DATABASE_URL
try:
    from app.services.cached_analytics_service import CachedAnalyticsService
    USE_CACHE = True
except ImportError:
    CachedAnalyticsService = AnalyticsService
    USE_CACHE = False
import os
import asyncio
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["Analytics / Analyst"])

async def get_db():
    """
    Trả về connection pool cho analytics service.
    Service sẽ acquire connection từ pool khi cần.
    """
    try:
        from app.db_pool import get_pool
        pool = await get_pool()
        logger.debug(f"get_db: Successfully got pool {id(pool)}")
        return pool
    except Exception as e:
        logger.error(f"get_db: Failed to get pool: {e}")
        raise

async def get_analytics_service(db=Depends(get_db)) -> AnalyticsService:
    if USE_CACHE:
        return CachedAnalyticsService(db)
    return AnalyticsService(db)


# ====== FILTER / METADATA ======

@router.get("/filters/platforms", response_model=List[PlatformFilterItem],dependencies=[Depends(require_role("ANALYST"))])
async def list_platforms(
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_platform_filters()


@router.get("/filters/categories", response_model=List[CategoryFilterItem],dependencies=[Depends(require_role("ANALYST"))])
async def list_categories(
    platform_code: Optional[str] = Query(None),
    parent_category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_category_filters(platform_code, parent_category_key)


@router.get("/filters/products", response_model=List[ProductFilterItem],dependencies=[Depends(require_role("ANALYST"))])
async def search_products(
    q: str = Query(..., description="Từ khóa tìm sản phẩm"),
    platform_code: Optional[str] = Query(None),
    category_key: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.search_products(q, platform_code, category_key, limit)


# ====== OVERVIEW / KPI ======

@router.get("/overview/kpis", response_model=OverviewKPIResponse,dependencies=[Depends(require_role("ANALYST"))])
async def get_overview_kpis(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: Optional[str] = Query(None),
    category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_overview_kpis(from_date, to_date, platform_code, category_key)


@router.get("/overview/trends", response_model=OverviewTrendResponse,dependencies=[Depends(require_role("ANALYST"))])
async def get_overview_trends(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: Optional[str] = Query(None),
    category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_overview_trends(from_date, to_date, platform_code, category_key)


# ====== PLATFORM COMPARISON ======

@router.get("/platforms/comparison", response_model=PlatformComparisonResponse,dependencies=[Depends(require_role("ANALYST"))])
async def compare_platforms(
    from_date: date = Query(...),
    to_date: date = Query(...),
    category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_platform_comparison(from_date, to_date, category_key)


@router.get("/platforms/category-share", response_model=List[CategoryShareItem],dependencies=[Depends(require_role("ANALYST"))])
async def get_category_share(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: str = Query(...),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_category_share(from_date, to_date, platform_code)


# ====== PRODUCT PERFORMANCE ======

@router.get("/products/top", response_model=List[TopProductItem],dependencies=[Depends(require_role("ANALYST"))])
async def get_top_products(
    from_date: date = Query(...),
    to_date: date = Query(...),
    metric: str = Query("revenue", description="revenue|review_count|avg_rating|price_growth"),
    platform_code: Optional[str] = Query(None),
    category_key: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_top_products(
        from_date=from_date,
        to_date=to_date,
        metric=metric,
        platform_code=platform_code,
        category_key=category_key,
        limit=limit,
    )


@router.get("/products/{product_key}/timeseries", response_model=ProductTimeseriesResponse,dependencies=[Depends(require_role("ANALYST"))])
async def get_product_timeseries(
    product_key: str,
    platform_code: str = Query(...),
    from_date: date = Query(...),
    to_date: date = Query(...),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_product_timeseries(
        product_key=product_key,
        platform_code=platform_code,
        from_date=from_date,
        to_date=to_date,
    )


@router.get("/products/{product_key}/reviews/summary", response_model=ReviewSummaryResponse,dependencies=[Depends(require_role("ANALYST"))])
async def get_product_review_summary(
    product_key: str,
    platform_code: str = Query(...),
    from_date: date = Query(...),
    to_date: date = Query(...),
    top_n: int = Query(5, ge=1, le=20),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_review_summary(
        product_key=product_key,
        platform_code=platform_code,
        from_date=from_date,
        to_date=to_date,
        top_n=top_n,
    )


# ====== PRICING ANALYTICS ======

@router.get("/pricing/price-distribution", response_model=PriceDistributionResponse,dependencies=[Depends(require_role("ANALYST"))])
async def get_price_distribution(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: str = Query(...),
    category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_price_distribution(from_date, to_date, platform_code, category_key)


@router.get("/pricing/price-vs-revenue", response_model=List[PriceVsRevenueItem],dependencies=[Depends(require_role("ANALYST"))])
async def get_price_vs_revenue(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: str = Query(...),
    category_key: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_price_vs_revenue(
        from_date=from_date,
        to_date=to_date,
        platform_code=platform_code,
        category_key=category_key,
        limit=limit,
    )


# ====== REPORT APIs ======

@router.get(
    "/report/overview",
    response_model=OverviewReportResponse,
    dependencies=[Depends(require_role("ANALYST"))],
)
async def get_overview_report(
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    platform_code: Optional[str] = Query(
        None, description="tiki / lazada, nếu null thì tổng tất cả"
    ),
    category_key: Optional[str] = Query(
        None, description="category_sk, lấy từ /analytics/filters/categories"
    ),
    service: AnalyticsService = Depends(get_analytics_service),
):
    """
    API gom tất cả dữ liệu cần cho 1 report tổng quan:
      - KPIs
      - Trend theo ngày
      - So sánh giữa các platform
      - Tỷ trọng category theo platform (nếu có platform_code)
    """

    # chạy song song các query
    tasks = [
        service.get_overview_kpis(from_date, to_date, platform_code, category_key),
        service.get_overview_trends(from_date, to_date, platform_code, category_key),
        service.get_platform_comparison(from_date, to_date, category_key),
    ]
    if platform_code:
        tasks.append(service.get_category_share(from_date, to_date, platform_code))

    results = await asyncio.gather(*tasks)

    kpis = results[0]
    trends = results[1]
    platform_comparison_resp = results[2]

    # rút ra list PlatformComparisonItem từ PlatformComparisonResponse
    if isinstance(platform_comparison_resp, list):
        platforms = platform_comparison_resp
    elif isinstance(platform_comparison_resp, dict):
        platforms = platform_comparison_resp.get("platforms", [])
    else:
        # trường hợp là Pydantic model PlatformComparisonResponse
        platforms = getattr(platform_comparison_resp, "platforms", [])

    category_share = results[3] if platform_code else []

    return OverviewReportResponse(
        from_date=from_date,
        to_date=to_date,
        platform_code=platform_code,
        category_key=category_key,
        kpis=kpis,
        trends=trends,
        platform_comparison=platforms,   # <-- GIỜ LÀ LIST[PlatformComparisonItem]
        category_share=category_share,
    )

@router.get(
    "/report/product",
    response_model=ProductReportResponse,
    dependencies=[Depends(require_role("ANALYST"))],
)
async def get_product_report(
    product_key: str = Query(..., description="global product key, vd: tiki_123456"),
    platform_code: str = Query(..., description="tiki / lazada"),
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    service: AnalyticsService = Depends(get_analytics_service),
):
    """
    Report chi tiết cho 1 product:
      - Timeseries: giá / rating / review theo ngày
      - Review summary: tổng số review, breakdown rating, top review
    """
    # Tuần tự cũng được (an toàn tuyệt đối), hoặc dùng asyncio.gather nếu muốn nhanh hơn.
    timeseries = await service.get_product_timeseries(
        product_key=product_key,
        platform_code=platform_code,
        from_date=from_date,
        to_date=to_date,
    )

    review_summary = await service.get_review_summary(
        product_key=product_key,
        platform_code=platform_code,
        from_date=from_date,
        to_date=to_date,
        top_n=5,
    )

    return ProductReportResponse(
        product_key=product_key,
        platform_code=platform_code,
        from_date=from_date,
        to_date=to_date,
        timeseries=timeseries,
        review_summary=review_summary,
    )


# ====== PRODUCTS BY CATEGORY ======

@router.get("/products/by-category",dependencies=[Depends(require_role("ANALYST"))])
async def get_products_by_category_platform(
    platform_code: str = Query(..., description="Platform code: tiki / lazada"),
    category_id: Optional[int] = Query(
        None, description="Lọc theo category_sk (optional)"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=10000,
        description="Giới hạn số sản phẩm (default 100)",
    ),
    db=Depends(get_db),
):
    """
    Get products grouped by category for a specific platform.
    Returns list of products with category information.
    """
    params = [platform_code]
    category_filter = ""
    if category_id:
        category_filter = " AND c.category_sk = $2"
        params.append(category_id)

    sql = f"""
        SELECT
            pl.platform_code,
            pl.platform_name,
            c.category_sk AS category_id,
            COALESCE(c.category_std_key, 'Unknown') AS category_name,
            p.product_key,
            p.product_name,
            COALESCE(b.brand_name, 'Unknown') AS brand_name,
            AVG(f.avg_price) AS avg_price,
            MIN(f.min_price) AS min_price,
            MAX(f.max_price) AS max_price,
            SUM(COALESCE(f.total_review_count, 0)) AS total_reviews,
            AVG(f.avg_rating) AS avg_rating,
            COUNT(DISTINCT f.date_sk) AS days_tracked
        FROM dwh.fact_product_daily f
        JOIN dwh.dim_product p ON p.product_sk = f.product_sk
        JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
        LEFT JOIN dwh.dim_brand b ON b.brand_sk = p.brand_sk
        LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
        JOIN dwh.dim_date d ON d.date_sk = f.date_sk
        WHERE pl.platform_code = $1
        {category_filter}
        GROUP BY
            pl.platform_code,
            pl.platform_name,
            c.category_sk,
            c.category_std_key,
            p.product_key,
            p.product_name,
            b.brand_name
        ORDER BY
            c.category_sk,
            total_reviews DESC,
            avg_rating DESC
        LIMIT {limit};
    """

    try:
        rows = await db.fetch(sql, *params)
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


@router.get("/products/by-category-all-platforms",dependencies=[Depends(require_role("ANALYST"))])
async def get_products_by_category_all_platforms(
    category_id: Optional[int] = Query(
        None, description="Lọc theo category_sk (optional)"
    ),
    limit: int = Query(
        1000,
        ge=1,
        le=50000,
        description="Giới hạn số sản phẩm (default 1000)",
    ),
    db=Depends(get_db),
):
    """
    Get products grouped by category across all platforms.
    Returns list of products with platform and category information.
    Useful for comparing products across different platforms.
    """
    params = []
    category_filter = ""
    if category_id:
        category_filter = " WHERE c.category_sk = $1"
        params.append(category_id)

    sql = f"""
        SELECT
            pl.platform_code,
            pl.platform_name,
            c.category_sk AS category_id,
            COALESCE(c.category_std_key, 'Unknown') AS category_name,
            p.product_key,
            p.product_name,
            COALESCE(b.brand_name, 'Unknown') AS brand_name,
            AVG(f.avg_price) AS avg_price,
            MIN(f.min_price) AS min_price,
            MAX(f.max_price) AS max_price,
            SUM(COALESCE(f.total_review_count, 0)) AS total_reviews,
            AVG(f.avg_rating) AS avg_rating,
            COUNT(DISTINCT f.date_sk) AS days_tracked
        FROM dwh.fact_product_daily f
        JOIN dwh.dim_product p ON p.product_sk = f.product_sk
        JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
        LEFT JOIN dwh.dim_brand b ON b.brand_sk = p.brand_sk
        LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
        JOIN dwh.dim_date d ON d.date_sk = f.date_sk
        {category_filter}
        GROUP BY
            pl.platform_code,
            pl.platform_name,
            c.category_sk,
            c.category_std_key,
            p.product_key,
            p.product_name,
            b.brand_name
        ORDER BY
            c.category_sk,
            pl.platform_code,
            total_reviews DESC,
            avg_rating DESC
        LIMIT {limit};
    """

    try:
        rows = await db.fetch(sql, *params)
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


# ====== QUALITY & SENTIMENT ======

@router.get("/quality/rating-distribution", response_model=List[RatingDistributionData], dependencies=[Depends(require_role("ANALYST"))])
async def get_rating_distribution(
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    platform_code: Optional[str] = Query(None, description="tiki / lazada, nếu null thì tất cả"),
    category_key: Optional[str] = Query(None, description="category_sk, nếu null thì tất cả"),
    service: AnalyticsService = Depends(get_analytics_service),
):
    """
    Get rating distribution by rating buckets (1-5 stars).
    Used for histogram chart showing product count in each rating bucket.
    """
    return await service.get_rating_distribution(from_date, to_date, platform_code, category_key)


@router.get("/quality/critical-products", response_model=List[CriticalProductItem], dependencies=[Depends(require_role("ANALYST"))])
async def get_critical_products(
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    platform_code: Optional[str] = Query(None, description="tiki / lazada, nếu null thì tất cả"),
    category_key: Optional[str] = Query(None, description="category_sk, nếu null thì tất cả"),
    limit: int = Query(10, ge=1, le=50, description="Số lượng sản phẩm critical tối đa"),
    service: AnalyticsService = Depends(get_analytics_service),
):
    """
    Get products with critical issues (low rating < 3.5 or high negative sentiment).
    Sorted by negative_pct DESC, then avg_rating ASC.
    """
    return await service.get_critical_products(from_date, to_date, platform_code, category_key, limit)
