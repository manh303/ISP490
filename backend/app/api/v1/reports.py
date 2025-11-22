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
            d.date_value AS full_date,
            pl.platform_code,
            pl.platform_name,
            COUNT(DISTINCT f.product_sk)           AS product_count,
            AVG(f.avg_price)                       AS avg_price,
            MIN(f.min_price)                       AS min_price,
            MAX(f.max_price)                       AS max_price,
            SUM(COALESCE(f.total_review_count, 0)) AS total_reviews,
            AVG(f.avg_rating)                      AS avg_rating
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
        raise HTTPException(status_code=400, detail="metric phải là revenue|reviews|rating|price")

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
            COALESCE(b.brand_name, 'Unknown')                 AS brand_name,
            COALESCE(c.category_id::text, 'Unknown')          AS category_name,
            {metric_expr}                                     AS metric_value,
            AVG(f.avg_price)                                  AS avg_price,
            MIN(f.min_price)                                  AS min_price,
            MAX(f.max_price)                                  AS max_price,
            SUM(COALESCE(f.total_review_count, 0))            AS total_reviews,
            AVG(f.avg_rating)                                 AS avg_rating
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
            c.category_id
        ORDER BY metric_value DESC
        LIMIT {limit};
    """

    try:
        rows = await db.fetch(sql, *params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    filename = f"products_{metric}_{from_date}_to_{to_date}.csv"
    return _rows_to_csv(rows, filename)