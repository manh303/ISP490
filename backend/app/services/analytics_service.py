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


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
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
        rows = await self.db.fetch(sql)

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

            # CHỈ LẤY TÊN Ở LEVEL CUỐI
            if lvl3:
                display_name = lvl3
                level = 3          # vd: Smartphones
            elif lvl2:
                display_name = lvl2
                level = 2         
            elif lvl1:
                display_name = lvl1
                level = 1         
            elif std:
                display_name = std           
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
                AVG(f.avg_rating) AS avg_rating,
                MAX(COALESCE(c.full_path, c.category_std_key, c.category_lvl1, 'UNKNOWN')) AS category_name
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
        """
        row = await self.db.fetchrow(sql, *params)

        return OverviewKPIResponse(
            from_date=from_date,
            to_date=to_date,
            platform_code=platform_code,
            category_key=category_key,
            category_name=row["category_name"] if category_key else None,
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
        category_key: Optional[str] = None,  # category_sk
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
        rows = await self.db.fetch(sql, *params)

        result: List[PlatformComparisonItem] = []
        for r in rows:
            result.append(
                PlatformComparisonItem(
                    platform_code=r["platform_code"],
                    total_revenue=float(r["total_revenue"] or 0),
                    total_products=int(r["total_products"] or 0),
                    total_reviews=int(r["total_reviews"] or 0),
                    avg_price=_safe_float(r["avg_price"]),
                    avg_rating=_safe_float(r["avg_rating"]),
                )
            )
        return result

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
                COALESCE(c.full_path, c.category_std_key, c.category_lvl1, 'UNKNOWN') AS category_name,
                pl.platform_code,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS revenue
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date    d  ON d.date_sk    = f.date_sk
            JOIN dwh.dim_product p  ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
            GROUP BY p.category_sk, c.full_path, c.category_std_key, c.category_lvl1, pl.platform_code
            ORDER BY revenue DESC
        """
        rows = await self.db.fetch(sql, *params)

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
        from_date: date,
        to_date: date,
        metric: str = "revenue",
        platform_code: Optional[str] = None,
        category_key: Optional[str] = None,
        limit: int = 20,
    ) -> List[TopProductItem]:
        """
        Lấy danh sách top products theo 1 metric:
        - revenue
        - review_count
        - avg_rating
        - price_growth (tạm thời cũng sort theo revenue hoặc avg_price tùy em)
        """
        metric_column_map = {
            "revenue": "SUM(f.avg_price * f.total_review_count)",
            "review_count": "SUM(f.total_review_count)",
            "avg_rating": "AVG(f.avg_rating)",
        }
        metric_column = metric_column_map.get(metric, metric_column_map["revenue"])

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
                pl.platform_code,
                p.category_sk AS category_key,
                COALESCE(c.full_path, c.category_std_key, c.category_lvl1, 'UNKNOWN') AS category_name,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS total_revenue,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews,
                AVG(f.avg_rating) AS avg_rating,
                AVG(f.avg_price) AS avg_price
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
            GROUP BY
                p.product_key,
                p.product_name,
                pl.platform_code,
                p.category_sk,
                c.full_path,
                c.category_std_key,
                c.category_lvl1
            ORDER BY {metric_column} DESC
            LIMIT {limit}
        """
        rows = await self.db.fetch(sql, *params)

        return [
            TopProductItem(
                product_key=r["product_key"],
                product_name=r["product_name"],
                platform_code=r["platform_code"],
                category_key=str(r["category_key"]) if r["category_key"] is not None else None,
                category_name=r["category_name"] if r["category_name"] else None,
                total_revenue=float(r["total_revenue"] or 0),
                total_reviews=int(r["total_reviews"] or 0),
                avg_rating=_safe_float(r["avg_rating"]),
                avg_price=_safe_float(r["avg_price"]),
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
        conditions = ["LOWER(product_name) LIKE $" + str(1)]
        params: List[Any] = [f"%{q.lower()}%"]
        param_index = 2

        # filter theo platform_code nếu có (tiki, lazada) bằng prefix của product_key
        if platform_code:
            conditions.append(f"split_part(product_key, '_', 1) = ${param_index}")
            params.append(platform_code)
            param_index += 1

        # filter theo category_key (category_sk)
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
            # suy ra platform_code từ prefix trước dấu "_"
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
                MAX(COALESCE(c.full_path, c.category_std_key, c.category_lvl1, 'UNKNOWN')) AS category_name
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
            {where_clause}
        """
        row = await self.db.fetchrow(sql, *params)

        return PriceDistributionResponse(
            platform_code=platform_code,
            category_key=category_key,
            category_name=row["category_name"] if category_key else None,
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
    