import os
from datetime import date
from typing import Optional, List

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
import io
import csv

from app.db_config import DATABASE_URL

router = APIRouter()

async def get_db():
    """
    Dependency mở 1 connection asyncpg, dùng xong thì đóng.
    """
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()

# =========================
#  HELPER: build CSV
# =========================
def _rows_to_csv(rows: List[asyncpg.Record], filename: str) -> StreamingResponse:
    if not rows:
        output = io.StringIO()
        output.write("")
        output.seek(0)
        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    dict_rows = [dict(r) for r in rows]
    fieldnames = list(dict_rows[0].keys())

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in dict_rows:
        writer.writerow(row)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ===================================================================
# 1) REPORT OVERVIEW: tổng quan theo ngày & platform (CSV)
#    GET /api/v1/reports/overview
# ===================================================================
@router.get("/overview")
async def export_overview_report(
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    platform_code: Optional[str] = Query(
        None, description="Lọc theo platform: tiki / lazada (optional)"
    ),
    db=Depends(get_db),
):
    """
    Report tổng quan theo ngày & platform, dựa trên dwh.fact_product_daily.
    Trả về CSV.
    """

    params = [from_date, to_date]
    plat_filter = ""
    if platform_code:
        plat_filter = " AND pl.platform_code = $3"
        params.append(platform_code)

    sql = f"""
        SELECT
            d.date_value                              AS full_date,
            pl.platform_code,
            pl.platform_name,
            COUNT(DISTINCT f.product_sk)              AS product_count,
            AVG(f.avg_price)                          AS avg_price,
            MIN(f.min_price)                          AS min_price,
            MAX(f.max_price)                          AS max_price,
            SUM(COALESCE(f.total_review_count, 0))    AS total_reviews,
            AVG(f.avg_rating)                         AS avg_rating
        FROM dwh.fact_product_daily f
        JOIN dwh.dim_product   p  ON p.product_sk   = f.product_sk
        JOIN dwh.dim_platform  pl ON pl.platform_sk = f.platform_sk
        JOIN dwh.dim_date      d  ON d.date_sk      = f.date_sk
        WHERE d.date_value BETWEEN $1 AND $2
        {plat_filter}
        GROUP BY d.date_value, pl.platform_code, pl.platform_name
        ORDER BY d.date_value, pl.platform_code;
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    filename = f"overview_{from_date}_to_{to_date}.csv"
    return _rows_to_csv(rows, filename)


# ===================================================================
# 2) REPORT PRODUCTS: top sản phẩm theo metric (CSV)
#    GET /api/v1/reports/products
# ===================================================================
@router.get("/products")
async def export_products_report(
    from_date: date = Query(...),
    to_date: date = Query(...),
    platform_code: Optional[str] = Query(
        None, description="Lọc theo platform: tiki / lazada (optional)"
    ),
    metric: str = Query(
        "revenue",
        description="revenue | reviews | rating | price",
    ),
    limit: int = Query(100, ge=1, le=1000),
    db=Depends(get_db),
):
    """
    Report top sản phẩm theo metric, trả về CSV:
    - revenue: giả lập = avg_price * total_review_count
    - reviews: tổng số review
    - rating: điểm rating trung bình
    - price: giá trung bình
    """

    metric = metric.lower()
    if metric not in {"revenue", "reviews", "rating", "price"}:
        raise HTTPException(
            status_code=400,
            detail="metric phải là 1 trong: revenue | reviews | rating | price",
        )

    # mapping metric -> biểu thức
    if metric == "revenue":
        metric_expr = "SUM(f.avg_price * COALESCE(f.total_review_count, 0))"
    elif metric == "reviews":
        metric_expr = "SUM(COALESCE(f.total_review_count, 0))"
    elif metric == "rating":
        metric_expr = "AVG(f.avg_rating)"
    else:  # price
        metric_expr = "AVG(f.avg_price)"

    params = [from_date, to_date]
    plat_filter = ""
    if platform_code:
        plat_filter = " AND pl.platform_code = $3"
        params.append(platform_code)

    sql = f"""
        SELECT
            p.product_key,
            p.product_name,
            pl.platform_code,
            pl.platform_name,
            COALESCE(b.brand_name, 'Unknown')              AS brand_name,
            COALESCE(c.full_path, c.category_std_key, c.category_lvl1, 'Unknown') AS category_name,
            {metric_expr}                                  AS metric_value,
            AVG(f.avg_price)                               AS avg_price,
            MIN(f.min_price)                               AS min_price,
            MAX(f.max_price)                               AS max_price,
            SUM(COALESCE(f.total_review_count, 0))         AS total_reviews,
            AVG(f.avg_rating)                              AS avg_rating
        FROM dwh.fact_product_daily f
        JOIN dwh.dim_product   p  ON p.product_sk   = f.product_sk
        JOIN dwh.dim_platform  pl ON pl.platform_sk = f.platform_sk
        LEFT JOIN dwh.dim_brand     b ON b.brand_sk     = p.brand_sk
        LEFT JOIN dwh.dim_category  c ON c.category_sk  = p.category_sk
        JOIN dwh.dim_date      d  ON d.date_sk      = f.date_sk
        WHERE d.date_value BETWEEN $1 AND $2
        {plat_filter}
        GROUP BY
            p.product_key,
            p.product_name,
            pl.platform_code,
            pl.platform_name,
            b.brand_name,
            c.full_path,
            c.category_std_key,
            c.category_lvl1
        ORDER BY metric_value DESC
        LIMIT {limit}
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    filename = f"products_{metric}_{from_date}_to_{to_date}.csv"
    return _rows_to_csv(rows, filename)

# ===================================================================
# 3) REVIEW REPORT: tổng hợp rating + sentiment theo ngày (CSV)
#    GET /api/v1/reports/reviews
# ===================================================================
@router.get(
    "/reviews",
    response_class=StreamingResponse,
    summary="Export Reviews/Sentiment Report (CSV)",
)
async def export_reviews_report(
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    platform_code: Optional[str] = Query(
        None, description="Lọc theo platform: tiki / lazada (optional)"
    ),
    min_reviews: int = Query(
        0,
        ge=0,
        description="Chỉ lấy những dòng có total_reviews >= giá trị này (mặc định 0)",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=50000,
        description="Giới hạn số dòng xuất CSV (default 100)",
    ),
    db=Depends(get_db),
):
    """
    Report review + sentiment theo ngày, dựa trên **dwh.fact_review_daily**:

    - agg_date
    - platform_code
    - product_key, product_name, brand, category
    - total_reviews, avg_rating, phân bố 1-5 sao
    - positive/neutral/negative_reviews
    - sentiment_score_avg, review_quality_score
    """

    params = [from_date, to_date, min_reviews]
    plat_filter = ""
    if platform_code:
        params.append(platform_code)
        plat_filter = f" AND r.platform_sk = ${len(params)}"

    sql = f"""
        SELECT
            d.date_value                      AS full_date,
            pl.platform_code,
            r.review_count,
            r.avg_rating,
            r.rating_1_count,
            r.rating_2_count,
            r.rating_3_count,
            r.rating_4_count,
            r.rating_5_count,
            r.avg_sentiment
        FROM dwh.fact_review_daily r
        JOIN dwh.dim_date d ON d.date_sk = r.date_sk
        JOIN dwh.dim_platform pl ON pl.platform_sk = r.platform_sk
        WHERE d.date_value BETWEEN $1 AND $2
          AND r.review_count >= $3
        {plat_filter}
        ORDER BY
            d.date_value,
            pl.platform_code,
            r.avg_rating DESC
        LIMIT {limit};
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    filename = f"reviews_{from_date}_to_{to_date}.csv"
    return _rows_to_csv(rows, filename)

@router.get(
    "/reviews-details",
    response_class=StreamingResponse,
    summary="Export Reviews Details (CSV)",
)
async def export_reviews_report_details(
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    platform_code: Optional[str] = Query(
        None, description="Lọc theo platform: tiki / lazada (optional)"
    ),
   
    
    limit: int = Query(
        100,
        ge=1,
        le=50000,
        description="Giới hạn số dòng xuất CSV (default 100)",
    ),
    db=Depends(get_db),
):
    params = [from_date, to_date]
    plat_filter = ""
    if platform_code:
        plat_filter = " AND rd.source_platform_std = $3"
        params.append(platform_code)
    sql = f"""
        SELECT
            rd.review_date                    AS full_date,
            rd.source_platform_std            AS platform_code,
            rd.global_product_id,
            p.product_name,
            rd.reviewer_name,
            rd.rating,
            rd.review_text,
            rd.sentiment_label
            
        FROM dwh.fact_reviews_detail rd
        LEFT JOIN dwh.dim_product p ON p.product_key = rd.global_product_id
        WHERE rd.review_date BETWEEN $1 AND $2
        {plat_filter}
        ORDER BY
            rd.review_date DESC
        LIMIT {limit};
    """
    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    filename = f"reviews_details{from_date}_to_{to_date}.csv"
    return _rows_to_csv(rows, filename)

@router.get(
    "/product-reviews-details",
    response_class=StreamingResponse,
    summary="Export Reviews Details for Specific Product (CSV)",
)
async def export_product_reviews_details(
    product_id: str = Query(..., description="Product ID (product_key)"),
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    platform_code: Optional[str] = Query(
        None, description="Lọc theo platform: tiki / lazada (optional)"
    ),
   
    
    limit: int = Query(
        100,
        ge=1,
        le=50000,
        description="Giới hạn số dòng xuất CSV (default 100)",
    ),
    db=Depends(get_db),
):
    params = [product_id, from_date, to_date]
    plat_filter = ""
    if platform_code:
        plat_filter = " AND rd.source_platform_std = $4"
        params.append(platform_code)
    sql = f"""
        SELECT
            rd.review_date                    AS full_date,
            rd.source_platform_std            AS platform_code,
            rd.global_product_id,
            p.product_name,
            rd.reviewer_name,
            rd.rating,
            rd.review_text,
            rd.sentiment_label
            
        FROM dwh.fact_reviews_detail rd
        LEFT JOIN dwh.dim_product p ON p.product_key = rd.global_product_id
        WHERE rd.global_product_id = $1
          AND rd.review_date BETWEEN $2 AND $3
        {plat_filter}
        ORDER BY
            rd.review_date DESC
        LIMIT {limit};
    """
    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    filename = f"product_{product_id}_reviews_{from_date}_to_{to_date}.csv"
    return _rows_to_csv(rows, filename)


# ===================================================================
# PRODUCTS BY CATEGORY - SPECIFIC PLATFORM
# GET /api/v1/reports/products-by-category
# ===================================================================
@router.get(
    "/products-by-category",
    response_class=StreamingResponse,
    summary="Export Products by Category for Specific Platform (CSV)",
)
async def export_products_by_category_platform(
    platform_code: str = Query(..., description="Platform code: tiki / lazada"),
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    category_id: Optional[str] = Query(
        None, description="Lọc theo category ID (optional)"
    ),
    limit: int = Query(
        100,
        ge=1,
        le=10000,
        description="Giới hạn số dòng xuất CSV (default 100)",
    ),
    db=Depends(get_db),
):
    """
    Export products grouped by category for a specific platform.
    Returns CSV with product details including category information.
    """
    params = [from_date, to_date, platform_code]
    category_filter = ""
    if category_id:
        category_filter = " AND c.category_id = $4"
        params.append(category_id)

    sql = f"""
        SELECT
            pl.platform_code,
            pl.platform_name,
            COALESCE(c.category_id::text, 'Unknown') AS category_id,
            COALESCE(c.category_name, 'Unknown') AS category_name,
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
        WHERE d.date_value BETWEEN $1 AND $2
          AND pl.platform_code = $3
        {category_filter}
        GROUP BY
            pl.platform_code,
            pl.platform_name,
            c.category_id,
            c.category_name,
            p.product_key,
            p.product_name,
            b.brand_name
        ORDER BY
            c.category_id,
            total_reviews DESC,
            avg_rating DESC
        LIMIT {limit};
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    filename = f"products_by_category_{platform_code}_{from_date}_to_{to_date}.csv"
    return _rows_to_csv(rows, filename)


# ===================================================================
# PRODUCTS BY CATEGORY - ALL PLATFORMS
# GET /api/v1/reports/products-by-category-all-platforms
# ===================================================================
@router.get(
    "/products-by-category-all-platforms",
    response_class=StreamingResponse,
    summary="Export Products by Category for All Platforms (CSV)",
)
async def export_products_by_category_all_platforms(
    from_date: date = Query(..., description="Ngày bắt đầu (YYYY-MM-DD)"),
    to_date: date = Query(..., description="Ngày kết thúc (YYYY-MM-DD)"),
    category_id: Optional[str] = Query(
        None, description="Lọc theo category ID (optional)"
    ),
    limit: int = Query(
        1000,
        ge=1,
        le=50000,
        description="Giới hạn số dòng xuất CSV (default 1000)",
    ),
    db=Depends(get_db),
):
    """
    Export products grouped by category across all platforms.
    Returns CSV with product details including platform and category information.
    Useful for comparing products across different platforms within same categories.
    """
    params = [from_date, to_date]
    category_filter = ""
    if category_id:
        category_filter = " AND c.category_id = $3"
        params.append(category_id)

    sql = f"""
        SELECT
            pl.platform_code,
            pl.platform_name,
            COALESCE(c.category_id::text, 'Unknown') AS category_id,
            COALESCE(c.category_name, 'Unknown') AS category_name,
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
        WHERE d.date_value BETWEEN $1 AND $2
        {category_filter}
        GROUP BY
            pl.platform_code,
            pl.platform_name,
            c.category_id,
            c.category_name,
            p.product_key,
            p.product_name,
            b.brand_name
        ORDER BY
            c.category_id,
            pl.platform_code,
            total_reviews DESC,
            avg_rating DESC
        LIMIT {limit};
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    filename = f"products_by_category_all_platforms_{from_date}_to_{to_date}.csv"
    return _rows_to_csv(rows, filename)