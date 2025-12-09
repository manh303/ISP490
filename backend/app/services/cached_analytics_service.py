#!/usr/bin/env python3
"""
Cached Analytics Service Wrapper
Adds caching layer to analytics service for better performance
"""
from datetime import date
from typing import List, Optional
import hashlib
import json

from app.services.analytics_service import AnalyticsService
from app.core.cache import cache

# Force reload to pick up new AnalyticsService
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
    
    # Note: Caching temporarily disabled to avoid serialization issues
    # Can be re-enabled after proper testing with Redis
    
    async def get_platform_filters(self):
        """Get platform filters"""
        return await super().get_platform_filters()
    
    async def get_category_filters(
        self,
        platform_code: Optional[str] = None,
        parent_category_key: Optional[str] = None,
    ):
        """Get category filters"""
        return await super().get_category_filters(platform_code, parent_category_key)
    
    async def get_overview_kpis(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
    ):
        """Get overview KPIs - CACHING DISABLED (serialization issue)"""
        # TODO: Fix Pydantic model serialization in cache before re-enabling
        # Cache was causing validation errors when returning string instead of model
        return await super().get_overview_kpis(from_date, to_date, platform_code, category_key)
    
    async def get_overview_trends(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
    ):
        """Get overview trends"""
        return await super().get_overview_trends(from_date, to_date, platform_code, category_key)
    
    async def get_platform_comparison(
        self,
        from_date: date,
        to_date: date,
        category_key: Optional[str] = None,
    ):
        """Get platform comparison"""
        return await super().get_platform_comparison(from_date, to_date, category_key)
    
    async def get_category_share(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
    ):
        """Get category share"""
        return await super().get_category_share(from_date, to_date, platform_code)
    
    async def get_top_products(
        self,
        from_date: date,
        to_date: date,
        metric: str = "revenue",
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
        limit: int = 20,
    ):
        """Get top products - CACHING DISABLED (serialization issue)"""
        # TODO: Fix Pydantic model serialization in cache before re-enabling
        # Cache was causing validation errors when returning string instead of model
        return await super().get_top_products(
            from_date, to_date, metric, platform_code, category_key, limit
        )
    
    async def get_product_timeseries(
        self,
        product_key: str,
        platform_code: str,
        from_date: date,
        to_date: date,
    ):
        """Get product timeseries"""
        return await super().get_product_timeseries(
            product_key, platform_code, from_date, to_date
        )
    
    async def get_review_summary(
        self,
        product_key: str,
        platform_code: str,
        from_date: date,
        to_date: date,
        top_n: int = 5,
    ):
        """Get review summary"""
        return await super().get_review_summary(
            product_key, platform_code, from_date, to_date, top_n
        )
    
    async def get_price_distribution(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
        category_key: Optional[str] = None,
    ):
        """Get price distribution"""
        return await super().get_price_distribution(
            from_date, to_date, platform_code, category_key
        )
    
    async def get_price_vs_revenue(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
        category_key: Optional[str] = None,
        limit: int = 100,
    ):
        """Get price vs revenue"""
        return await super().get_price_vs_revenue(
            from_date, to_date, platform_code, category_key, limit
        )

