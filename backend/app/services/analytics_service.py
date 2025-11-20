from datetime import date
from typing import List, Optional, Any, Dict

from schemas.analytics import (
    PlatformFilterItem,
    CategoryFilterItem,
    ProductFilterItem,
    OverviewKPIResponse,
    OverviewTrendResponse,
    OverviewTrendPoint,
    PlatformComparisonItem,
    CategoryShareItem,
    TopProductItem,
    ProductTimeseriesResponse,
    ProductTimeseriesPoint,
    ReviewSummaryResponse,
    ReviewRatingBreakdown,
    PriceDistributionResponse,
    PriceVsRevenueItem,
)


def _safe_float(value: Any) -> Optional[float]:
    """Convert to float, nhưng tránh NaN / inf vì JSON không hỗ trợ."""
    if value is None:
        return None
    try:
        import math
        v = float(value)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


class AnalyticsService:
    """Service layer cho Analyst, làm việc với schema dwh.*"""

    def __init__(self, db):
        # db: asyncpg connection/pool (có fetch, fetchrow, execute)
        self.db = db

    # =========================
    # FILTER / METADATA
    # =========================

    async def get_platform_filters(self) -> List[PlatformFilterItem]:
        sql = """
            SELECT platform_code, platform_name
            FROM dwh.dim_platform
            ORDER BY platform_code
        """
        rows = await self.db.fetch(sql)
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
        Trả về list category cho filter.

        Theo hint từ DB:
          - dim_category có category_id, category_sk
        → Ta dùng:
          - API category_key = category_sk (chuỗi)
          - category_name hiển thị = category_id (ép sang string)
        """
        sql = """
            SELECT
                category_sk,
                category_id
            FROM dwh.dim_category
            ORDER BY category_id
        """
        rows = await self.db.fetch(sql)
        result: List[CategoryFilterItem] = []
        for r in rows:
            category_id = r["category_id"]
            category_name = str(category_id) if category_id is not None else ""

            result.append(
                CategoryFilterItem(
                    category_key=str(r["category_sk"]),
                    category_name=category_name,  # 👈 luôn là string
                    level=None,
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
        conditions = ["d.date_value BETWEEN $1 AND $2"]
        params: List[Any] = [from_date, to_date]
        param_index = 3

        if platform_code:
            conditions.append(f"pl.platform_code = ${param_index}")
            params.append(platform_code)
            param_index += 1

        if category_key:
            conditions.append(f"p.category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                -- doanh thu ảo: avg_price * total_review_count
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
        """
        row = await self.db.fetchrow(sql, *params)

        return OverviewKPIResponse(
            from_date=from_date,
            to_date=to_date,
            platform_code=platform_code,
            category_key=category_key,
            total_revenue=float(row["total_revenue"] or 0),
            total_products=int(row["total_products"] or 0),
            total_reviews=int(row["total_reviews"] or 0),
            avg_price=_safe_float(row["avg_price"]),
            avg_rating=_safe_float(row["avg_rating"]),
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
        conditions = ["d.date_value BETWEEN $1 AND $2"]
        params: List[Any] = [from_date, to_date]
        param_index = 3

        if platform_code:
            conditions.append(f"pl.platform_code = ${param_index}")
            params.append(platform_code)
            param_index += 1

        if category_key:
            conditions.append(f"p.category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                d.date_value AS date,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS revenue,
                COALESCE(SUM(f.total_review_count), 0) AS total_orders,
                AVG(f.avg_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            {where_clause}
            GROUP BY d.date_value
            ORDER BY d.date_value
        """
        rows = await self.db.fetch(sql, *params)

        points: List[OverviewTrendPoint] = []
        for r in rows:
            points.append(
                OverviewTrendPoint(
                    date=r["date"],
                    revenue=float(r["revenue"] or 0),
                    total_orders=int(r["total_orders"] or 0),
                    avg_price=_safe_float(r["avg_price"]),
                    avg_rating=_safe_float(r["avg_rating"]),
                    total_reviews=int(r["total_reviews"] or 0),
                )
            )

        return OverviewTrendResponse(
            from_date=from_date,
            to_date=to_date,
            platform_code=platform_code,
            category_key=category_key,
            points=points,
        )

    # =========================
    # PLATFORM COMPARISON
    # =========================

    async def get_platform_comparison(
        self,
        from_date: date,
        to_date: date,
        category_key: Optional[str] = None,
    ) -> List[PlatformComparisonItem]:
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
                pl.platform_name,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS total_revenue,
                COUNT(DISTINCT f.product_sk) AS total_products,
                AVG(f.avg_price) AS avg_price,
                AVG(f.avg_rating) AS avg_rating,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            {where_clause}
            GROUP BY pl.platform_code, pl.platform_name
            ORDER BY pl.platform_code
        """
        rows = await self.db.fetch(sql, *params)
        return [
            PlatformComparisonItem(
                platform_code=r["platform_code"],
                platform_name=r["platform_name"],
                total_revenue=float(r["total_revenue"] or 0),
                total_products=int(r["total_products"] or 0),
                avg_price=_safe_float(r["avg_price"]),
                avg_rating=_safe_float(r["avg_rating"]),
                total_reviews=int(r["total_reviews"] or 0),
            )
            for r in rows
        ]

    async def get_category_share(
        self,
        from_date: date,
        to_date: date,
        platform_code: str,
    ) -> List[CategoryShareItem]:
        conditions = [
            "d.date_value BETWEEN $1 AND $2",
            "pl.platform_code = $3",
        ]
        params: List[Any] = [from_date, to_date, platform_code]

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                p.category_sk AS category_key,
                COALESCE(c.category_id::text, 'UNKNOWN') AS category_name,
                pl.platform_code,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS revenue
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
            GROUP BY p.category_sk, c.category_id, pl.platform_code
            ORDER BY revenue DESC
        """
        rows = await self.db.fetch(sql, *params)
        total_revenue = sum(float(r["revenue"] or 0) for r in rows) or 1.0

        items: List[CategoryShareItem] = []
        for r in rows:
            revenue = float(r["revenue"] or 0)
            items.append(
                CategoryShareItem(
                    category_key=str(r["category_key"]) if r["category_key"] is not None else "UNKNOWN",
                    category_name=r["category_name"],
                    platform_code=r["platform_code"],
                    revenue=revenue,
                    revenue_share=revenue / total_revenue,
                )
            )
        return items


    # =========================
    # PRODUCT SEARCH
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
        - product_key, product_name lấy từ dim_product
        - platform_code lấy từ prefix của product_key (tiki_..., lazada_...)
        - category_key trong API = category_sk trong DB
        """
        conditions = ["LOWER(product_name) LIKE $" + str(1)]
        params: List[Any] = [f"%{q.lower()}%"]
        param_index = 2

        if platform_code:
            conditions.append(f"split_part(product_key, '_', 1) = ${param_index}")
            params.append(platform_code)
            param_index += 1

        if category_key:
            conditions.append(f"category_sk = ${param_index}")
            params.append(int(category_key))
            param_index += 1

        where_clause = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                product_key,
                product_name,
                category_sk
            FROM dwh.dim_product
            {where_clause}
            ORDER BY product_name
            LIMIT {limit}
        """
        rows = await self.db.fetch(sql, *params)
        result: List[ProductFilterItem] = []
        for r in rows:
            pk = r["product_key"]
            plat = pk.split("_", 1)[0] if "_" in pk else ""

            result.append(
                ProductFilterItem(
                    product_key=pk,
                    product_name=r["product_name"],
                    platform_code=plat,
                    category_key=str(r.get("category_sk")) if r.get("category_sk") is not None else None,
                )
            )
        return result

    # =========================
    # PRODUCT PERFORMANCE
    # =========================

    async def get_top_products(
        self,
        from_date: date,
        to_date: date,
        metric: str,
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
        limit: int = 20,
    ) -> List[TopProductItem]:
        metric_column = {
            "revenue": "SUM(f.avg_price * f.total_review_count)",
            "total_review_count": "SUM(f.total_review_count)",
            "avg_rating": "AVG(f.avg_rating)",
            "price_growth": "AVG(f.avg_price)",  # placeholder
        }.get(metric, "SUM(f.avg_price * f.total_review_count)")

        conditions = ["d.date_value BETWEEN $1 AND $2"]
        params: List[Any] = [from_date, to_date]
        param_index = 3

        if platform_code:
            conditions.append(f"pl.platform_code = ${param_index}")
            params.append(platform_code)
            param_index += 1

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
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS total_revenue,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews,
                AVG(f.avg_rating) AS avg_rating,
                AVG(f.avg_price) AS avg_price
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            {where_clause}
            GROUP BY p.product_key, p.product_name, p.category_sk, pl.platform_code
            ORDER BY {metric_column} DESC
            LIMIT {limit}
        """
        rows = await self.db.fetch(sql, *params)
        return [
            TopProductItem(
                product_key=r["product_key"],
                product_name=r["product_name"],
                platform_code=r["platform_code"],
                category_key=str(r.get("category_key")) if r.get("category_key") is not None else None,
                total_revenue=float(r["total_revenue"] or 0),
                total_reviews=int(r["total_reviews"] or 0),
                avg_rating=_safe_float(r["avg_rating"]),
                avg_price=_safe_float(r["avg_price"]),
            )
            for r in rows
        ]

    # =========================
    # PRODUCT TIMESERIES
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
        prod_row = await self.db.fetchrow(prod_sql, product_key)
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
        rows = await self.db.fetch(sql, product_sk, from_date, to_date)

        points: List[ProductTimeseriesPoint] = []
        for r in rows:
            points.append(
                ProductTimeseriesPoint(
                    date=r["date"],
                    avg_price=_safe_float(r["avg_price"]),
                    min_price=_safe_float(r["min_price"]),
                    max_price=_safe_float(r["max_price"]),
                    total_reviews=int(r["total_reviews"] or 0),
                    avg_rating=_safe_float(r["avg_rating"]),
                    revenue=float(r["revenue"] or 0),
                )
            )

        return ProductTimeseriesResponse(
            product_key=product_key,
            platform_code=platform_code,
            from_date=from_date,
            to_date=to_date,
            points=points,
        )

    # =========================
    # REVIEW SUMMARY
    # =========================

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
        prod_row = await self.db.fetchrow(prod_sql, product_key)
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
        summary_row = await self.db.fetchrow(summary_sql, product_sk, from_date, to_date)
        total_reviews = int(summary_row["total_reviews"] or 0)

        breakdown = {
            5: int(summary_row["rating_5"] or 0),
            4: int(summary_row["rating_4"] or 0),
            3: int(summary_row["rating_3"] or 0),
            2: int(summary_row["rating_2"] or 0),
            1: int(summary_row["rating_1"] or 0),
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
        top_rows = await self.db.fetch(top_sql, product_sk, from_date, to_date, top_n)
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
            avg_rating=_safe_float(summary_row["avg_rating"]),
            rating_breakdown=ReviewRatingBreakdown(by_rating=breakdown),
            top_helpful_reviews=top_reviews,
        )

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
                MAX(f.max_price) AS max_price
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            {where_clause}
        """
        row = await self.db.fetchrow(sql, *params)

        return PriceDistributionResponse(
            platform_code=platform_code,
            category_key=category_key,
            from_date=from_date,
            to_date=to_date,
            min_price=_safe_float(row["min_price"]),
            p25_price=_safe_float(row["p25_price"]),
            median_price=_safe_float(row["median_price"]),
            p75_price=_safe_float(row["p75_price"]),
            max_price=_safe_float(row["max_price"]),
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
        rows = await self.db.fetch(sql, *params)

        return [
            PriceVsRevenueItem(
                product_key=r["product_key"],
                product_name=r["product_name"],
                platform_code=r["platform_code"],
                category_key=str(r.get("category_key")) if r.get("category_key") is not None else None,
                avg_price=_safe_float(r["avg_price"]),
                total_revenue=float(r["total_revenue"] or 0),
                avg_rating=_safe_float(r["avg_rating"]),
                total_reviews=int(r["total_reviews"] or 0),
            )
            for r in rows
        ]
