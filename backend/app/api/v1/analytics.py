# app/api/v1/analytics.py
from datetime import date
from typing import Optional, List

import asyncpg
from fastapi import APIRouter, Depends, Query

from schemas.analytics import (
    PlatformFilterItem,
    CategoryFilterItem,
    ProductFilterItem,
    OverviewKPIResponse,
    OverviewTrendResponse,
    PlatformComparisonItem,
    CategoryShareItem,
    TopProductItem,
    ProductTimeseriesResponse,
    ReviewSummaryResponse,
    PriceDistributionResponse,
    PriceVsRevenueItem,
    OverviewReportResponse,
    ProductReportResponse,
)
from app.services.analytics_service import AnalyticsService
import os

router = APIRouter(prefix="/analytics", tags=["Analytics / Analyst"])

# ========= DB CONFIG (giống ML) =========
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_dss"),
    "user": os.getenv("DB_USER", "dss_user"),
    "password": os.getenv("DB_PASSWORD", "IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4"),
}


async def get_db():
    """
    Tạo 1 kết nối asyncpg cho mỗi request analytics.
    """
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        await conn.close()


async def get_analytics_service(db=Depends(get_db)) -> AnalyticsService:
    return AnalyticsService(db)


# ====== FILTER / METADATA ======

@router.get("/filters/platforms", response_model=List[PlatformFilterItem])
async def list_platforms(
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_platform_filters()


@router.get("/filters/categories", response_model=List[CategoryFilterItem])
async def list_categories(
    platform_code: Optional[str] = Query(None),
    parent_category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_category_filters(platform_code, parent_category_key)


@router.get("/filters/products", response_model=List[ProductFilterItem])
async def search_products(
    q: str = Query(..., description="Từ khóa tìm sản phẩm"),
    platform_code: Optional[str] = Query(None),
    category_key: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.search_products(q, platform_code, category_key, limit)


# ====== OVERVIEW / KPI ======

@router.get("/overview/kpis", response_model=OverviewKPIResponse)
async def get_overview_kpis(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: Optional[str] = Query(None),
    category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_overview_kpis(from_date, to_date, platform_code, category_key)


@router.get("/overview/trends", response_model=OverviewTrendResponse)
async def get_overview_trends(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: Optional[str] = Query(None),
    category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_overview_trends(from_date, to_date, platform_code, category_key)


# ====== PLATFORM COMPARISON ======

@router.get("/platforms/comparison", response_model=List[PlatformComparisonItem])
async def compare_platforms(
    from_date: date = Query(...),
    to_date: date = Query(...),
    category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_platform_comparison(from_date, to_date, category_key)


@router.get("/platforms/category-share", response_model=List[CategoryShareItem])
async def get_category_share(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: str = Query(...),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_category_share(from_date, to_date, platform_code)


# ====== PRODUCT PERFORMANCE ======

@router.get("/products/top", response_model=List[TopProductItem])
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


@router.get("/products/{product_key}/timeseries", response_model=ProductTimeseriesResponse)
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


@router.get("/products/{product_key}/reviews/summary", response_model=ReviewSummaryResponse)
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

@router.get("/pricing/price-distribution", response_model=PriceDistributionResponse)
async def get_price_distribution(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: str = Query(...),
    category_key: Optional[str] = Query(None),
    service: AnalyticsService = Depends(get_analytics_service),
):
    return await service.get_price_distribution(from_date, to_date, platform_code, category_key)


@router.get("/pricing/price-vs-revenue", response_model=List[PriceVsRevenueItem])
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

@router.get("/report/overview", response_model=OverviewReportResponse)
async def get_overview_report(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: Optional[str] = Query(
        None, description="tiki / lazada, nếu null thì tổng tất cả"
    ),
    category_key: Optional[str] = Query(
        None, description="category_sk, lấy từ /filters/categories"
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
    # 1. KPIs
    kpis = await service.get_overview_kpis(
        from_date=from_date,
        to_date=to_date,
        platform_code=platform_code,
        category_key=category_key,
    )

    # 2. Trends
    trends = await service.get_overview_trends(
        from_date=from_date,
        to_date=to_date,
        platform_code=platform_code,
        category_key=category_key,
    )

    # 3. So sánh platform (không filter platform_code để vẫn thấy full)
    platform_comparison = await service.get_platform_comparison(
        from_date=from_date,
        to_date=to_date,
        category_key=category_key,
    )

    # 4. Category share theo platform (chỉ khi có platform_code)
    if platform_code:
        category_share = await service.get_category_share(
            from_date=from_date,
            to_date=to_date,
            platform_code=platform_code,
        )
    else:
        category_share = []

    return OverviewReportResponse(
        from_date=from_date,
        to_date=to_date,
        platform_code=platform_code,
        category_key=category_key,
        kpis=kpis,
        trends=trends,
        platform_comparison=platform_comparison,
        category_share=category_share,
    )


@router.get("/report/product", response_model=ProductReportResponse)
async def get_product_report(
    product_key: str = Query(..., description="global product key, vd: tiki_123456"),
    platform_code: str = Query(..., description="tiki / lazada"),
    from_date: date = Query(...),
    to_date: date = Query(...),
    service: AnalyticsService = Depends(get_analytics_service),
):
    """
    Report chi tiết cho 1 product:
      - Timeseries: giá / rating / review theo ngày
      - Review summary: tổng số review, breakdown rating, top review
    UI có thể dùng report này cho màn product-detail report.
    """

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
