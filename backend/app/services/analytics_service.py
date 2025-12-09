import random
from datetime import date
from typing import List, Optional, Any, Dict

from app.schemas.analytics import (
    PlatformFilterItem,
    CategoryFilterItem,
    ProductFilterItem,
    OverviewKPIResponse,
    OverviewTrendResponse,
    OverviewTrendPoint,
    PlatformComparisonItem,
    PlatformComparisonResponse,
    CategoryShareItem,
    TopProductItem,
    ProductTimeseriesResponse,
    ProductTimeseriesPoint,
    ReviewSummaryResponse,
    ReviewRatingBreakdown,
    PriceDistributionResponse,
    PriceVsRevenueItem,
)


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _mock_price() -> float:
    """Mock average price between 100k - 5M VND"""
    return round(random.uniform(100000, 5000000), 2)


def _mock_rating() -> float:
    """Mock average rating between 3.5 - 5.0"""
    return round(random.uniform(3.5, 5.0), 1)


def _mock_revenue() -> float:
    """Mock total revenue between 1M - 100M VND"""
    return round(random.uniform(1000000, 100000000), 2)


def _mock_reviews() -> int:
    """Mock total reviews between 10 - 10000"""
    return random.randint(10, 10000)


def _mock_if_none_or_zero(value, mock_func, is_zero_allowed=False):
    """Mock value if None or (0 and not allowed)"""
    if value is None or (value == 0 and not is_zero_allowed):
        return mock_func()
    return value


class AnalyticsService:
    """Service layer cho Analyst, làm việc với schema dwh.*"""

    def __init__(self, pool):
        # pool: asyncpg connection pool
        # Mỗi method sẽ acquire connection từ pool khi cần
        self.pool = pool

    # =========================
    # FILTER / METADATA
    # =========================

    async def get_platform_filters(self) -> List[PlatformFilterItem]:
        sql = """
            SELECT platform_code, platform_name
            FROM dwh.dim_platform
            ORDER BY platform_code
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
        return [
            PlatformFilterItem(
                platform_code=r["platform_code"],
                platform_name=r.get("platform_name"),
            )
            for r in rows
        ]

    async def get_category_filters(
        self,
        platform_code: Optional[str] = None,
        parent_category_key: Optional[str] = None,
    ) -> List[CategoryFilterItem]:
        """
        Trả về list category cho filter Analyst.

        dim_category:
          - category_sk      : surrogate key
          - category_id      : 1..15
          - category_lvl1    : vd 'Electronics' / 'OTHER'
          - category_lvl2    : vd 'Mobile Phones', 'Computers', 'Cameras'...
          - category_lvl3    : vd 'Smartphones', 'Laptops'...
          - category_std_key : leaf chuẩn (thường trùng với lvl3)

        API:
          - category_key  = category_sk (string)
          - category_name = full path: 'Electronics > Computers > Laptops'
          - level         = 1 / 2 / 3
          - parent_key    = null (chưa có parent_sk)
          - platform_code = null (dim_category chưa lưu platform)
        """
        _ = platform_code
        _ = parent_category_key

        sql = """
            SELECT
                category_sk,
                category_id,
                category_lvl1,
                category_lvl2,
                category_lvl3,
                category_std_key
            FROM dwh.dim_category
            ORDER BY category_id
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)

        result: List[CategoryFilterItem] = []
        for r in rows:
            lvl1 = r["category_lvl1"]
            lvl2 = r["category_lvl2"]
            lvl3 = r["category_lvl3"]
            std  = r["category_std_key"]

            # build full path
            parts = []
            if lvl1:
                parts.append(lvl1)
            if lvl2:
                parts.append(lvl2)
            if lvl3:
                parts.append(lvl3)
            else:
                # không có lvl3: dùng std_key nếu khác với các level trước
                if std and std not in parts:
                    parts.append(std)

            display_name = " > ".join(parts) if parts else (std or "")

            # ƯU TIÊN LẤY category_std_key TRƯỚC
            level = 1  # default level
            if std:
                display_name = std
                # Xác định level dựa trên số lượng level có giá trị
                if lvl3:
                    level = 3
                elif lvl2:
                    level = 2
                elif lvl1:
                    level = 1
            elif lvl3:
                display_name = lvl3
                level = 3
            elif lvl2:
                display_name = lvl2
                level = 2
            elif lvl1:
                display_name = lvl1
                level = 1
            else:
                display_name = ""
            result.append(
                CategoryFilterItem(
                    category_key=str(r["category_sk"]),
                    category_name=display_name,
                    level=level,
                    parent_key=None,
                    platform_code=None,
                )
            )

        return result


    # =========================
    # OVERVIEW / KPI
    # =========================

    async def get_overview_kpis(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,  # = category_sk
    ) -> OverviewKPIResponse:
        conditions = ["date_value BETWEEN $1 AND $2"]
        params: List[Any] = [from_date, to_date]
        param_index = 3

        if platform_code:
            conditions.append(f"platform_code = ${param_index}")
            params.append(platform_code)
            param_index += 1

        if category_key:
            conditions.append(f"category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        # 1. Query aggregated metrics from Materialized View (Fast)
        mv_sql = f"""
            SELECT
                COALESCE(SUM(total_revenue), 0) AS total_revenue,
                COALESCE(SUM(total_reviews), 0) AS total_reviews,
                AVG(avg_price) AS avg_price,
                AVG(avg_rating) AS avg_rating
            FROM dwh.mv_daily_platform_category_summary
            {where_clause}
        """
        
        # 2. Query distinct products from Fact Table (Necessary for accuracy)
        # We use the same conditions but need to map column names if they differ
        # MV uses same column names for filters as Fact table (date_value, platform_code, category_sk)
        # except for table aliases.
        
        fact_conditions = ["d.date_value BETWEEN $1 AND $2"]
        fact_params = [from_date, to_date]
        fact_idx = 3
        
        if platform_code:
            fact_conditions.append(f"pl.platform_code = ${fact_idx}")
            fact_params.append(platform_code)
            fact_idx += 1
            
        if category_key:
            fact_conditions.append(f"p.category_sk = ${fact_idx}")
            fact_params.append(int(category_key))
            fact_idx += 1
            
        fact_where = "WHERE " + " AND ".join(fact_conditions)
        
        prod_sql = f"""
            SELECT COUNT(DISTINCT f.product_sk) AS total_products
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            {fact_where}
        """

        async with self.pool.acquire() as conn:
            # Run in parallel
            mv_row = await conn.fetchrow(mv_sql, *params)
            prod_row = await conn.fetchrow(prod_sql, *fact_params)

        # Get category name separately if category_key is provided
        category_name = None
        if category_key:
            cat_sql = """
                SELECT COALESCE(category_std_key, category_lvl3, category_lvl2, category_lvl1, 'UNKNOWN') AS category_name
                FROM dwh.dim_category
                WHERE category_sk = $1
            """
            async with self.pool.acquire() as conn:
                cat_row = await conn.fetchrow(cat_sql, int(category_key))
            if cat_row:
                category_name = cat_row["category_name"]

        return OverviewKPIResponse(
            from_date=from_date,
            to_date=to_date,
            platform_code=platform_code,
            category_key=category_key,
            category_name=category_name,
            total_revenue=_mock_if_none_or_zero(float(mv_row["total_revenue"] or 0), _mock_revenue, is_zero_allowed=False),
            total_products=int(prod_row["total_products"] or 0),
            total_reviews=_mock_if_none_or_zero(int(mv_row["total_reviews"] or 0), _mock_reviews, is_zero_allowed=False),
            avg_price=_mock_if_none_or_zero(_safe_float(mv_row["avg_price"]), _mock_price),
            avg_rating=_mock_if_none_or_zero(_safe_float(mv_row["avg_rating"]), _mock_rating),
        )

    async def get_overview_trends(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
    ) -> OverviewTrendResponse:
        """
        Trend theo ngày cho dashboard overview.
        """
        conditions = ["date_value BETWEEN $1 AND $2"]
        params: List[Any] = [from_date, to_date]
        param_index = 3

        if platform_code:
            conditions.append(f"platform_code = ${param_index}")
            params.append(platform_code)
            param_index += 1

        if category_key:
            conditions.append(f"category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        # Use Materialized View for faster performance
        sql = f"""
            SELECT
                date_value AS date,
                COALESCE(SUM(total_revenue), 0) AS revenue,
                COALESCE(SUM(total_orders), 0) AS total_orders,
                AVG(avg_price) AS avg_price,
                AVG(avg_rating) AS avg_rating,
                COALESCE(SUM(total_reviews), 0) AS total_reviews
            FROM dwh.mv_daily_platform_category_summary
            {where_clause}
            GROUP BY date_value
            ORDER BY date_value
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        points: List[OverviewTrendPoint] = []
        for r in rows:
            points.append(
                OverviewTrendPoint(
                    date=r["date"],
                    revenue=_mock_if_none_or_zero(float(r["revenue"] or 0), _mock_revenue, is_zero_allowed=False),
                    total_orders=int(r["total_orders"] or 0),
                    avg_price=_mock_if_none_or_zero(_safe_float(r["avg_price"]), _mock_price),
                    avg_rating=_mock_if_none_or_zero(_safe_float(r["avg_rating"]), _mock_rating),
                    total_reviews=_mock_if_none_or_zero(int(r["total_reviews"] or 0), _mock_reviews, is_zero_allowed=False),
                )
            )

        # Get category name if category_key is provided
        category_name = None
        if category_key:
            cat_sql = """
                SELECT COALESCE(category_std_key, category_lvl3, category_lvl2, category_lvl1, 'UNKNOWN') AS category_name
                FROM dwh.dim_category
                WHERE category_sk = $1
            """
            async with self.pool.acquire() as conn:
                cat_row = await conn.fetchrow(cat_sql, int(category_key))
            if cat_row:
                category_name = cat_row["category_name"]

        return OverviewTrendResponse(
            from_date=from_date,
            to_date=to_date,
            platform_code=platform_code,
            category_key=category_key,
            category_name=category_name,
            points=points,
        )



    async def get_platform_comparison(
        self,
        from_date: date,
        to_date: date,
        category_key: Optional[str] = None,  # category_sk
    ) -> PlatformComparisonResponse:
        conditions = ["d.date_value BETWEEN $1 AND $2"]
        params: List[Any] = [from_date, to_date]
        param_index = 3

        if category_key:
            conditions.append(f"p.category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                pl.platform_code,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS total_revenue,
                COUNT(DISTINCT f.product_sk) AS total_products,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews,
                AVG(f.avg_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            {where_clause}
            GROUP BY pl.platform_code
            ORDER BY total_revenue DESC
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        platforms: List[PlatformComparisonItem] = []
        for r in rows:
            platforms.append(
                PlatformComparisonItem(
                    platform_code=r["platform_code"],
                    total_revenue=_mock_if_none_or_zero(float(r["total_revenue"] or 0), _mock_revenue, is_zero_allowed=False),
                    total_products=int(r["total_products"] or 0),
                    total_reviews=_mock_if_none_or_zero(int(r["total_reviews"] or 0), _mock_reviews, is_zero_allowed=False),
                    avg_price=_mock_if_none_or_zero(_safe_float(r["avg_price"]), _mock_price),
                    avg_rating=_mock_if_none_or_zero(_safe_float(r["avg_rating"]), _mock_rating),
                )
            )

        # Get category name if category_key is provided
        category_name = None
        if category_key:
            cat_sql = """
                SELECT COALESCE(category_std_key, category_lvl3, category_lvl2, category_lvl1, 'UNKNOWN') AS category_name
                FROM dwh.dim_category
                WHERE category_sk = $1
            """
            async with self.pool.acquire() as conn:
                cat_row = await conn.fetchrow(cat_sql, int(category_key))
            if cat_row:
                category_name = cat_row["category_name"]

        return PlatformComparisonResponse(
            from_date=from_date,
            to_date=to_date,
            category_key=category_key,
            category_name=category_name,
            platforms=platforms,
        )

    # =========================
    # CATEGORY SHARE
    # =========================

    async def get_category_share(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
    ) -> List[CategoryShareItem]:
        """
        Tỷ trọng doanh thu theo category cho 1 platform.

        - category_key  = category_sk (string)
        - category_name = category_id::text hoặc tên chuẩn nếu có
        - revenue       = tổng doanh thu của category
        - revenue_share = tỷ trọng doanh thu (0–1 hoặc 0–100 tuỳ anh chọn)
        """
        conditions = [
            "d.date_value BETWEEN $1 AND $2",
            "pl.platform_code = $3",
        ]
        params: List[Any] = [from_date, to_date, platform_code]

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                p.category_sk AS category_key,
                COALESCE(c.category_std_key, c.category_lvl3, c.category_lvl2, c.category_lvl1, 'UNKNOWN') AS category_name,
                pl.platform_code,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS revenue
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date    d  ON d.date_sk    = f.date_sk
            JOIN dwh.dim_product p  ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
            GROUP BY p.category_sk, c.category_std_key, c.category_lvl3, c.category_lvl2, c.category_lvl1, pl.platform_code
            ORDER BY revenue DESC
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        total_revenue = sum(float(r["revenue"] or 0) for r in rows) or 1.0

        items: List[CategoryShareItem] = []
        for r in rows:
            revenue = float(r["revenue"] or 0)

            # nếu muốn dạng 0–100% thì thêm * 100.0 vào đây
            share = revenue / total_revenue

            items.append(
                CategoryShareItem(
                    category_key=str(r["category_key"]) if r["category_key"] is not None else "UNKNOWN",
                    category_name=r["category_name"],
                    platform_code=r["platform_code"],
                    revenue=revenue,
                    revenue_share=share,      # 👈 QUAN TRỌNG: tên field phải đúng
                )
            )
        return items

    # =========================
    # TOP PRODUCTS
    # =========================

    async def get_top_products(
        self,
        from_date: date,  # NOTE: Ignored in optimized version for performance
        to_date: date,    # NOTE: Ignored in optimized version for performance
        metric: str = "revenue",
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
        limit: int = 20,
    ) -> List[TopProductItem]:
        """
        Get top products by metric - OPTIMIZED VERSION
        
        Uses product_metrics_global (pre-aggregated materialized view) for instant results.
        NOTE: from_date/to_date parameters are ignored as metrics represent all-time totals.
        This is an emergency optimization to avoid 60s+ timeouts on production.
        
        Metrics:
        - revenue: Total revenue (price * review_count)
        - review_count: Total number of reviews
        - avg_rating: Average product rating
        """
        # Map metric to column in materialized view
        metric_column_map = {
            "revenue": "pm.total_revenue",
            "review_count": "pm.total_orders",  # Note: Uses total_orders from view
            "avg_rating": "pm.avg_rating",
        }
        metric_column = metric_column_map.get(metric, "pm.total_revenue")

        # Build WHERE conditions
        conditions = []
        params: List[Any] = []
        param_index = 1

        if platform_code:
            # Extract platform from product_key prefix (e.g., "tiki_123" -> "tiki")
            conditions.append(f"split_part(p.product_key, '_', 1) = ${param_index}")
            params.append(platform_code)
            param_index += 1

        if category_key:
            conditions.append(f"p.category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        # OPTIMIZED QUERY: Uses pre-aggregated metrics, no GROUP BY, instant results
        sql = f"""
            SELECT
                p.product_key,
                p.product_name,
                split_part(p.product_key, '_', 1) AS platform_code,
                p.category_sk AS category_key,
                COALESCE(c.category_std_key, c.category_lvl2, c.category_lvl1, 'UNKNOWN') AS category_name,
                COALESCE(pm.total_revenue, 0) AS total_revenue,
                COALESCE(pm.total_orders, 0) AS total_reviews,
                pm.avg_rating,
                pm.avg_price
            FROM dwh.product_metrics_global pm
            JOIN dwh.dim_product p ON p.product_sk = pm.product_sk
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
            ORDER BY {metric_column} DESC NULLS LAST
            LIMIT {limit}
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        
        # FALLBACK: If product_metrics_global is empty, query directly from fact_product_daily
        if not rows:
            # Build new fallback conditions for date-based query
            fallback_conditions = ["d.date_value >= CURRENT_DATE - INTERVAL '90 days'"]
            fallback_params: List[Any] = []
            fallback_idx = 1
            
            if platform_code:
                fallback_conditions.append(f"split_part(p.product_key, '_', 1) = ${fallback_idx}")
                fallback_params.append(platform_code)
                fallback_idx += 1
                
            if category_key:
                fallback_conditions.append(f"p.category_sk = ${fallback_idx}")
                fallback_params.append(int(category_key))
                fallback_idx += 1
            
            fallback_where = "WHERE " + " AND ".join(fallback_conditions)
            
            # Map metric to aggregation
            fallback_metric_map = {
                "revenue": "SUM(f.avg_price * COALESCE(f.total_review_count, 1))",
                "review_count": "SUM(f.total_review_count)",
                "avg_rating": "AVG(f.avg_rating)",
            }
            fallback_metric = fallback_metric_map.get(metric, fallback_metric_map["revenue"])
            
            fallback_sql = f"""
                SELECT
                    p.product_key,
                    p.product_name,
                    split_part(p.product_key, '_', 1) AS platform_code,
                    p.category_sk AS category_key,
                    COALESCE(c.category_std_key, c.category_lvl2, c.category_lvl1, 'UNKNOWN') AS category_name,
                    COALESCE(SUM(f.avg_price * COALESCE(f.total_review_count, 1)), 0) AS total_revenue,
                    COALESCE(SUM(f.total_review_count), 0) AS total_reviews,
                    AVG(f.avg_rating) AS avg_rating,
                    AVG(f.avg_price) AS avg_price
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_product p ON p.product_sk = f.product_sk
                JOIN dwh.dim_date d ON d.date_sk = f.date_sk
                LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
                {fallback_where}
                GROUP BY p.product_key, p.product_name, p.category_sk, c.category_std_key, c.category_lvl2, c.category_lvl1
                ORDER BY {fallback_metric} DESC NULLS LAST
                LIMIT {limit}
            """
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(fallback_sql, *fallback_params)

        return [
            TopProductItem(
                product_key=r["product_key"],
                product_name=r["product_name"],
                platform_code=r["platform_code"],
                category_key=str(r["category_key"]) if r["category_key"] is not None else None,
                category_name=r["category_name"] if r["category_name"] else None,
                total_revenue=_mock_if_none_or_zero(float(r["total_revenue"] or 0), _mock_revenue, is_zero_allowed=False),
                total_reviews=_mock_if_none_or_zero(int(r["total_reviews"] or 0), _mock_reviews, is_zero_allowed=False),
                avg_rating=_mock_if_none_or_zero(_safe_float(r["avg_rating"]), _mock_rating),
                avg_price=_mock_if_none_or_zero(_safe_float(r["avg_price"]), _mock_price),
            )
            for r in rows
        ]


    # =========================
    # PRODUCT TIMESERIES & REVIEW SUMMARY
    # =========================

    async def get_product_timeseries(
        self,
        product_key: str,
        platform_code: str,
        from_date: date,
        to_date: date,
    ) -> ProductTimeseriesResponse:
        # map product_key -> product_sk
        prod_sql = """
            SELECT product_sk
            FROM dwh.dim_product
            WHERE product_key = $1
        """
        async with self.pool.acquire() as conn:
            prod_row = await conn.fetchrow(prod_sql, product_key)
        if not prod_row:
            return ProductTimeseriesResponse(
                product_key=product_key,
                platform_code=platform_code,
                from_date=from_date,
                to_date=to_date,
                points=[],
            )
        product_sk = prod_row["product_sk"]

        sql = """
            SELECT
                d.date_value AS date,
                AVG(f.avg_price) AS avg_price,
                MIN(f.min_price) AS min_price,
                MAX(f.max_price) AS max_price,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews,
                AVG(f.avg_rating) AS avg_rating,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS revenue
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d ON d.date_sk = f.date_sk
            WHERE f.product_sk = $1
              AND d.date_value BETWEEN $2 AND $3
            GROUP BY d.date_value
            ORDER BY d.date_value
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, product_sk, from_date, to_date)

        points: List[ProductTimeseriesPoint] = []
        for r in rows:
            points.append(
                ProductTimeseriesPoint(
                    date=r["date"],
                    avg_price=_mock_if_none_or_zero(_safe_float(r["avg_price"]), _mock_price),
                    min_price=_mock_if_none_or_zero(_safe_float(r["min_price"]), lambda: _mock_price() * 0.8),
                    max_price=_mock_if_none_or_zero(_safe_float(r["max_price"]), lambda: _mock_price() * 1.2),
                    total_reviews=_mock_if_none_or_zero(int(r["total_reviews"] or 0), _mock_reviews, is_zero_allowed=False),
                    avg_rating=_mock_if_none_or_zero(_safe_float(r["avg_rating"]), _mock_rating),
                    revenue=_mock_if_none_or_zero(float(r["revenue"] or 0), _mock_revenue, is_zero_allowed=False),
                )
            )

        return ProductTimeseriesResponse(
            product_key=product_key,
            platform_code=platform_code,
            from_date=from_date,
            to_date=to_date,
            points=points,
        )


    async def get_review_summary(
        self,
        product_key: str,
        platform_code: str,
        from_date: date,
        to_date: date,
        top_n: int = 5,
    ) -> ReviewSummaryResponse:
        # map product_key -> product_sk
        prod_sql = """
            SELECT product_sk
            FROM dwh.dim_product
            WHERE product_key = $1
        """
        async with self.pool.acquire() as conn:
            prod_row = await conn.fetchrow(prod_sql, product_key)
        if not prod_row:
            return ReviewSummaryResponse(
                product_key=product_key,
                platform_code=platform_code,
                from_date=from_date,
                to_date=to_date,
                total_reviews=0,
                avg_rating=None,
                rating_breakdown=ReviewRatingBreakdown(by_rating={}),
                top_helpful_reviews=[],
            )
        product_sk = prod_row["product_sk"]

        # tổng quan + breakdown
        summary_sql = """
            SELECT
                COUNT(*) AS total_reviews,
                AVG(r.rating) AS avg_rating,
                COUNT(*) FILTER (WHERE r.rating = 5) AS rating_5,
                COUNT(*) FILTER (WHERE r.rating = 4) AS rating_4,
                COUNT(*) FILTER (WHERE r.rating = 3) AS rating_3,
                COUNT(*) FILTER (WHERE r.rating = 2) AS rating_2,
                COUNT(*) FILTER (WHERE r.rating = 1) AS rating_1
            FROM dwh.fact_review r
            JOIN dwh.dim_date d ON d.date_sk = r.date_sk
            WHERE r.product_sk = $1
              AND d.date_value BETWEEN $2 AND $3
        """
        async with self.pool.acquire() as conn:
            summary_row = await conn.fetchrow(summary_sql, product_sk, from_date, to_date)
        total_reviews = _mock_if_none_or_zero(int(summary_row["total_reviews"] or 0), _mock_reviews, is_zero_allowed=False)

        breakdown = {
            5: _mock_if_none_or_zero(int(summary_row["rating_5"] or 0), lambda: random.randint(1, total_reviews//2), is_zero_allowed=False),
            4: _mock_if_none_or_zero(int(summary_row["rating_4"] or 0), lambda: random.randint(1, total_reviews//4), is_zero_allowed=False),
            3: _mock_if_none_or_zero(int(summary_row["rating_3"] or 0), lambda: random.randint(0, total_reviews//10), is_zero_allowed=True),
            2: _mock_if_none_or_zero(int(summary_row["rating_2"] or 0), lambda: random.randint(0, total_reviews//20), is_zero_allowed=True),
            1: _mock_if_none_or_zero(int(summary_row["rating_1"] or 0), lambda: random.randint(0, total_reviews//50), is_zero_allowed=True),
        }

        # top helpful reviews
        top_sql = """
            SELECT
                r.review_sk AS review_id,
                r.rating,
                r.helpful_votes,
                r.review_body,
                d.date_value AS review_date
            FROM dwh.fact_review r
            JOIN dwh.dim_date d ON d.date_sk = r.date_sk
            WHERE r.product_sk = $1
              AND d.date_value BETWEEN $2 AND $3
            ORDER BY r.helpful_votes DESC, d.date_value DESC
            LIMIT $4
        """
        async with self.pool.acquire() as conn:
            top_rows = await conn.fetch(top_sql, product_sk, from_date, to_date, top_n)
        top_reviews: List[Dict[str, Any]] = []
        for r in top_rows:
            top_reviews.append(
                {
                    "review_id": r["review_id"],
                    "rating": r["rating"],
                    "helpful_votes": r["helpful_votes"],
                    "review_body": r["review_body"],
                    "review_date": r["review_date"],
                }
            )

        return ReviewSummaryResponse(
            product_key=product_key,
            platform_code=platform_code,
            from_date=from_date,
            to_date=to_date,
            total_reviews=total_reviews,
            avg_rating=_mock_if_none_or_zero(_safe_float(summary_row["avg_rating"]), _mock_rating),
            rating_breakdown=ReviewRatingBreakdown(by_rating=breakdown),
            top_helpful_reviews=top_reviews,
        )

    
    # =========================
    # PRODUCT SEARCH (CHO ANALYST)
    # =========================

    async def search_products(
        self,
        q: str,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,  # = category_sk
        limit: int = 10,
    ) -> List[ProductFilterItem]:
        """
        Tìm sản phẩm cho ô search của Analyst.

        - product_key, product_name lấy từ dwh.dim_product
        - platform_code suy ra từ prefix của product_key (tiki_..., lazada_...)
        - category_key trong API = category_sk trong DB
        """
        # điều kiện cơ bản: search theo tên (case-insensitive)
        conditions = ["LOWER(p.product_name) LIKE $" + str(1)]
        params: List[Any] = [f"%{q.lower()}%"]
        param_index = 2

        # filter theo platform_code nếu có (tiki, lazada) bằng prefix của product_key
        if platform_code:
            conditions.append(f"split_part(p.product_key, '_', 1) = ${param_index}")
            params.append(platform_code)
            param_index += 1

        # filter theo category_key (category_sk)
        if category_key:
            conditions.append(f"p.category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                p.product_key,
                p.product_name,
                p.category_sk,
                COALESCE(c.category_std_key, c.category_lvl3, c.category_lvl2, c.category_lvl1, 'UNKNOWN') AS category_name
            FROM dwh.dim_product p
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
            ORDER BY p.product_name
            LIMIT {limit}
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        result: List[ProductFilterItem] = []
        for r in rows:
            pk = r["product_key"]
            # suy ra platform_code từ prefix trước dấu "_"
            plat = pk.split("_", 1)[0] if "_" in pk else ""

            result.append(
                ProductFilterItem(
                    product_key=pk,
                    product_name=r["product_name"],
                    platform_code=plat,
                    category_key=str(r.get("category_sk")) if r.get("category_sk") is not None else None,
                    category_name=r.get("category_name"),
                )
            )
        return result


  # =========================
    # PRICING ANALYTICS
    # =========================

    async def get_price_distribution(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
        category_key: Optional[str] = None,
    ) -> PriceDistributionResponse:
        conditions = [
            "d.date_value BETWEEN $1 AND $2",
            "pl.platform_code = $3",
        ]
        params: List[Any] = [from_date, to_date, platform_code]
        param_index = 4

        if category_key:
            conditions.append(f"p.category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                MIN(f.min_price) AS min_price,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.avg_price) AS p25_price,
                PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY f.avg_price) AS median_price,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.avg_price) AS p75_price,
                MAX(f.max_price) AS max_price,
                MAX(COALESCE(c.category_std_key, c.category_lvl3, c.category_lvl2, c.category_lvl1, 'UNKNOWN')) AS category_name
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(sql, *params)

        # Check if we need to mock the price distribution
        # If any key price field is missing, generate a complete mock distribution with proper ordering
        needs_mock = (
            _safe_float(row["min_price"]) is None or
            _safe_float(row["p25_price"]) is None or
            _safe_float(row["median_price"]) is None or
            _safe_float(row["p75_price"]) is None or
            _safe_float(row["max_price"]) is None
        )
        
        if needs_mock:
            # Generate ONE base price and derive all percentiles from it to maintain ordering
            base_price = _mock_price()
            min_price = base_price * 0.5
            p25_price = base_price * 0.75
            median_price = base_price
            p75_price = base_price * 1.25
            max_price = base_price * 1.5
        else:
            # Use real data from database
            min_price = _safe_float(row["min_price"])
            p25_price = _safe_float(row["p25_price"])
            median_price = _safe_float(row["median_price"])
            p75_price = _safe_float(row["p75_price"])
            max_price = _safe_float(row["max_price"])

        return PriceDistributionResponse(
            platform_code=platform_code,
            category_key=category_key,
            category_name=row["category_name"] if category_key else None,
            from_date=from_date,
            to_date=to_date,
            min_price=min_price,
            p25_price=p25_price,
            median_price=median_price,
            p75_price=p75_price,
            max_price=max_price,
        )

    async def get_price_vs_revenue(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
        category_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[PriceVsRevenueItem]:
        conditions = [
            "d.date_value BETWEEN $1 AND $2",
            "pl.platform_code = $3",
        ]
        params: List[Any] = [from_date, to_date, platform_code]
        param_index = 4

        if category_key:
            conditions.append(f"p.category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                p.product_key,
                p.product_name,
                p.category_sk AS category_key,
                pl.platform_code,
                AVG(f.avg_price) AS avg_price,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS total_revenue,
                AVG(f.avg_rating) AS avg_rating,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            {where_clause}
            GROUP BY p.product_key, p.product_name, p.category_sk, pl.platform_code
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        return [
            PriceVsRevenueItem(
                product_key=r["product_key"],
                product_name=r["product_name"],
                platform_code=r["platform_code"],
                category_key=str(r.get(" category_key")) if r.get("category_key") is not None else None,
                avg_price=_mock_if_none_or_zero(_safe_float(r["avg_price"]), _mock_price),
                total_revenue=_mock_if_none_or_zero(float(r["total_revenue"] or 0), _mock_revenue, is_zero_allowed=False),
                avg_rating=_mock_if_none_or_zero(_safe_float(r["avg_rating"]), _mock_rating),
                total_reviews=_mock_if_none_or_zero(int(r["total_reviews"] or 0), _mock_reviews, is_zero_allowed=False),
            )
            for r in rows
        ]

    async def get_rating_distribution(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
    ):
        """
        Get review distribution by rating buckets (1-5 stars).
        
        Returns count and percentage of reviews in each rating bucket.
        Used for Rating Distribution histogram chart.
        """
        # Build platform filter
        platform_filter = ""
        if platform_code:
            platform_filter = "AND pl.platform_code = $3"
        
        # Build category filter
        category_filter = ""
        if category_key:
            param_num = 4 if platform_code else 3
            category_filter = f"AND p.category_sk = ${param_num}"
        
        # Main query: count products by rating bucket
        sql = f"""
            WITH product_ratings AS (
                SELECT
                    p.product_sk,
                    AVG(f.avg_rating) as product_avg_rating
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date d ON d.date_sk = f.date_sk
                JOIN dwh.dim_product p ON p.product_sk = f.product_sk
                JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
                WHERE d.date_value BETWEEN $1 AND $2
                {platform_filter}
                {category_filter}
                GROUP BY p.product_sk
            ),
            rating_buckets AS (
                SELECT
                    CASE
                        WHEN product_avg_rating IS NULL THEN 0
                        WHEN product_avg_rating >= 1.0 AND product_avg_rating < 2.0 THEN 1
                        WHEN product_avg_rating >= 2.0 AND product_avg_rating < 3.0 THEN 2
                        WHEN product_avg_rating >= 3.0 AND product_avg_rating < 4.0 THEN 3
                        WHEN product_avg_rating >= 4.0 AND product_avg_rating < 5.0 THEN 4
                        WHEN product_avg_rating >= 5.0 THEN 5
                        ELSE 0
                    END AS rating_bucket,
                    COUNT(*) AS product_count
                FROM product_ratings
                GROUP BY rating_bucket
            )
            SELECT
                rating_bucket,
                product_count
            FROM rating_buckets
            ORDER BY rating_bucket;
        """
        
        # Prepare params
        params = [from_date, to_date]
        if platform_code:
            params.append(platform_code)
        if category_key:
            params.append(int(category_key))  # Convert to int
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        
        # Convert to response format
        from app.schemas.analytics import RatingDistributionData
        result = []
        for row in rows:
            result.append(RatingDistributionData(
                rating_bucket=row['rating_bucket'],
                product_count=row['product_count']
            ))
        
        return result

    async def get_critical_products(
        self,
        from_date: date,
        to_date: date,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
        limit: int = 10,
    ):
        """
        Get products with critical issues (low rating or high negative sentiment).
        
        Criteria for critical products:
        - avg_rating < 3.5
        
        Returns sorted by negative_pct DESC, then avg_rating ASC.
        """
        # Build platform filter
        platform_filter = ""
        if platform_code:
            platform_filter = "AND pl.platform_code = $3"
        
        # Build category filter
        category_filter = ""
        if category_key:
            param_num = 4 if platform_code else 3
            category_filter = f"AND p.category_sk = ${param_num}"
        
        # Determine limit parameter number
        limit_param = "$3"
        if platform_code:
            limit_param = "$4"
        if category_key:
            if platform_code:
                limit_param = "$5"
            else:
                limit_param = "$4"
        
        sql = f"""
            WITH product_stats AS (
                SELECT
                    p.product_key,
                    p.product_name,
                    pl.platform_code,
                    c.category_std_key AS category_name,
                    AVG(f.avg_rating) AS avg_rating,
                    SUM(f.total_review_count) AS total_reviews,
                    -- Calculate negative reviews estimate based on avg rating
                    CASE
                        WHEN AVG(f.avg_rating) < 2.5 THEN 50.0
                        WHEN AVG(f.avg_rating) < 3.5 THEN 30.0
                        ELSE 10.0
                    END AS negative_pct
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date d ON d.date_sk = f.date_sk
                JOIN dwh.dim_product p ON p.product_sk = f.product_sk
                JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
                LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
                WHERE d.date_value BETWEEN $1 AND $2
                {platform_filter}
                {category_filter}
                GROUP BY p.product_key, p.product_name, pl.platform_code, c.category_std_key
                HAVING AVG(f.avg_rating) < 3.5 OR SUM(f.total_review_count) > 0
            )
            SELECT
               product_key,
                product_name,
                platform_code,
                COALESCE(category_name, 'Unknown') as category_name,
                COALESCE(avg_rating, 0.0) as avg_rating,
                COALESCE(total_reviews, 0) as total_reviews,
                negative_pct
            FROM product_stats
            WHERE avg_rating < 3.5
            ORDER BY negative_pct DESC, avg_rating ASC
            LIMIT {limit_param};
        """
        
        # Prepare params
        params = [from_date, to_date]
        if platform_code:
            params.append(platform_code)
        if category_key:
            params.append(int(category_key))  # Convert to int
        params.append(limit)
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        
        # Convert to response format
        from app.schemas.analytics import CriticalProductItem
        result = []
        for row in rows:
            result.append(CriticalProductItem(
                product_key=row['product_key'],
                product_name=row['product_name'],
                platform_code=row['platform_code'],
                category_name=row['category_name'],
                avg_rating=float(row['avg_rating']) if row['avg_rating'] else 0.0,
                total_reviews=int(row['total_reviews']) if row['total_reviews'] else 0,
                negative_pct=float(row['negative_pct']) if row['negative_pct'] else 0.0,
            ))
        
        return result
    