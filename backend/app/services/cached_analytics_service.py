#!/usr/bin/env python3
"""
Cached Analytics Service Wrapper
Adds caching layer to analytics service for better performance
"""
from datetime import date
from typing import List, Optional

from services.analytics_service import AnalyticsService
from core.cache import cached

class CachedAnalyticsService(AnalyticsService):
    """
    Analytics service with caching
    
    Cache TTL Strategy:
    - Filters (platforms, categories): 1 hour (rarely change)
    - KPIs & Trends: 5 minutes (update frequently)
    - Top products: 10 minutes (semi-static)
    - Product details: 15 minutes (static)
    - Review summary: 30 minutes (mostly static)
    """
    
    @cached("platform_filters", ttl=3600)  # 1 hour
    async def get_platform_filters(self):
        """Get platform filters (cached for 1 hour)"""
        return await super().get_platform_filters()
    
    @cached("category_filters", ttl=3600)  # 1 hour
    async def get_category_filters(
        self,
        platform_code: Optional[str] = None,
        parent_category_key: Optional[str] = None,
    ):
        """Get category filters (cached for 1 hour)"""
        return await super().get_category_filters(platform_code, parent_category_key)
    
    @cached("overview_kpis", ttl=300)  # 5 minutes
    async def get_overview_kpis(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
    ):
        """Get overview KPIs (cached for 5 minutes)"""
        return await super().get_overview_kpis(from_date, to_date, platform_code, category_key)
    
    @cached("overview_trends", ttl=300)  # 5 minutes
    async def get_overview_trends(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
    ):
        """Get overview trends (cached for 5 minutes)"""
        return await super().get_overview_trends(from_date, to_date, platform_code, category_key)
    
    @cached("platform_comparison", ttl=300)  # 5 minutes
    async def get_platform_comparison(
        self,
        from_date: date,
        to_date: date,
        category_key: Optional[str] = None,
    ):
        """Get platform comparison (cached for 5 minutes)"""
        return await super().get_platform_comparison(from_date, to_date, category_key)
    
    @cached("category_share", ttl=300)  # 5 minutes
    async def get_category_share(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
    ):
        """Get category share (cached for 5 minutes)"""
        return await super().get_category_share(from_date, to_date, platform_code)
    
    @cached("top_products", ttl=600)  # 10 minutes
    async def get_top_products(
        self,
        from_date: date,
        to_date: date,
        metric: str = "revenue",
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
        limit: int = 20,
    ):
        """Get top products (cached for 10 minutes)"""
        return await super().get_top_products(
            from_date, to_date, metric, platform_code, category_key, limit
        )
    
    @cached("product_timeseries", ttl=900)  # 15 minutes
    async def get_product_timeseries(
        self,
        product_key: str,
        platform_code: str,
        from_date: date,
        to_date: date,
    ):
        """Get product timeseries (cached for 15 minutes)"""
        return await super().get_product_timeseries(
            product_key, platform_code, from_date, to_date
        )
    
    @cached("review_summary", ttl=1800)  # 30 minutes
    async def get_review_summary(
        self,
        product_key: str,
        platform_code: str,
        from_date: date,
        to_date: date,
        top_n: int = 5,
    ):
        """Get review summary (cached for 30 minutes)"""
        return await super().get_review_summary(
            product_key, platform_code, from_date, to_date, top_n
        )
    
    @cached("price_distribution", ttl=600)  # 10 minutes
    async def get_price_distribution(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
        category_key: Optional[str] = None,
    ):
        """Get price distribution (cached for 10 minutes)"""
        return await super().get_price_distribution(
            from_date, to_date, platform_code, category_key
        )
    
    @cached("price_vs_revenue", ttl=600)  # 10 minutes
    async def get_price_vs_revenue(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
        category_key: Optional[str] = None,
        limit: int = 100,
    ):
        """Get price vs revenue (cached for 10 minutes)"""
        return await super().get_price_vs_revenue(
            from_date, to_date, platform_code, category_key, limit
        )

