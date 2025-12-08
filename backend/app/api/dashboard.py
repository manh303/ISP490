"""Dashboard & BI API"""
from fastapi import APIRouter
from typing import List, Dict, Any
from pydantic import BaseModel
from datetime import date
import psycopg2
import os

router = APIRouter(prefix="/api/v1/dashboard", tags=["Dashboard"])

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'dss_password_123')
}

class KPIResponse(BaseModel):
    total_products: int
    total_brands: int
    avg_price: float
    total_reviews: int

class PriceDistribution(BaseModel):
    price_range: str
    count: int

class TopProduct(BaseModel):
    product_sk: int
    product_name: str
    avg_price: float
    platform: str

@router.get("/kpis", response_model=KPIResponse)
def get_kpis():
    """Get key performance indicators"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM dwh_dim_product WHERE is_current = TRUE")
    total_products = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM dwh_dim_brand")
    total_brands = cur.fetchone()[0]
    
    cur.execute("SELECT AVG(price_current) FROM dwh_fact_product_daily WHERE price_current > 0")
    avg_price = float(cur.fetchone()[0] or 0)
    
    cur.execute("SELECT SUM(total_reviews) FROM dwh_fact_review_summary")
    total_reviews = cur.fetchone()[0] or 0
    
    conn.close()
    
    return KPIResponse(
        total_products=total_products,
        total_brands=total_brands,
        avg_price=round(avg_price, 2),
        total_reviews=total_reviews
    )

@router.get("/price-distribution", response_model=List[PriceDistribution])
def get_price_distribution():
    """Get price distribution by ranges"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            CASE 
                WHEN price_current < 1000000 THEN '< 1M'
                WHEN price_current < 5000000 THEN '1M - 5M'
                WHEN price_current < 10000000 THEN '5M - 10M'
                WHEN price_current < 20000000 THEN '10M - 20M'
                ELSE '> 20M'
            END as price_range,
            COUNT(*)
        FROM dwh_fact_product_daily
        WHERE price_current > 0
        GROUP BY price_range
        ORDER BY MIN(price_current)
    """)
    
    results = [PriceDistribution(price_range=r[0], count=r[1]) for r in cur.fetchall()]
    conn.close()
    return results

@router.get("/top-products", response_model=List[TopProduct])
def get_top_products(limit: int = 10):
    """Get top products by price"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            p.product_sk,
            p.product_name,
            AVG(f.price_current) as avg_price,
            pl.platform_name
        FROM dwh_dim_product p
        JOIN dwh_fact_product_daily f ON p.product_sk = f.product_sk
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        WHERE p.is_current = TRUE AND f.price_current > 0
        GROUP BY p.product_sk, p.product_name, pl.platform_name
        ORDER BY avg_price DESC
        LIMIT %s
    """, (limit,))
    
    results = []
    for row in cur.fetchall():
        results.append(TopProduct(
            product_sk=row[0],
            product_name=row[1],
            avg_price=float(row[2]),
            platform=row[3]
        ))
    
    conn.close()
    return results

@router.get("/platform-comparison")
def get_platform_comparison():
    """Compare metrics across platforms"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            pl.platform_name,
            COUNT(DISTINCT f.product_sk) as product_count,
            AVG(f.price_current) as avg_price,
            MIN(f.price_current) as min_price,
            MAX(f.price_current) as max_price
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        WHERE f.price_current > 0
        GROUP BY pl.platform_name
    """)
    
    results = []
    for row in cur.fetchall():
        results.append({
            "platform": row[0],
            "product_count": row[1],
            "avg_price": float(row[2]),
            "min_price": float(row[3]),
            "max_price": float(row[4])
        })
    
    conn.close()
    return results
