#!/usr/bin/env python3
"""
Reports API Endpoints
=====================
Provide reporting data and analytics reports
"""

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime
import pymongo
import psycopg2
import csv
import io
import json
import os

router = APIRouter()
logger = logging.getLogger(__name__)

# Database configurations
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://admin:admin_password@localhost:27017/')
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4')
}

class ReportsService:
    """Service for reports data"""

    def __init__(self):
        try:
            self.mongo_client = pymongo.MongoClient(MONGO_URL)
            self.mongo_db = self.mongo_client['ecommerce_dss']
        except Exception as e:
            logger.error(f"Failed to initialize reports service: {e}")

    def get_performance_reports(self) -> List[Dict[str, Any]]:
        """Get performance reports"""
        try:
            reports = list(self.mongo_db.performance_reports.find(
                {}, {"report_id": 1, "execution_date": 1, "pipeline_version": 1, "generated_at": 1}
            ).sort("generated_at", -1).limit(10))

            return reports

        except Exception as e:
            logger.error(f"Error getting performance reports: {e}")
            return []

# Initialize service
reports_service = ReportsService()

@router.get("/list")
async def get_reports_list():
    """Get list of available reports (MongoDB)"""
    try:
        performance_reports = reports_service.get_performance_reports()
        reports_list = {
            "performance_reports": performance_reports,
            "total_reports": len(performance_reports),
            "last_updated": datetime.now().isoformat()
        }
        return JSONResponse(content=reports_list)
    except Exception as e:
        logger.error(f"Error getting reports list: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get reports list: {str(e)}"
        )

@router.get("/export/products")
def export_products(format: str = "csv", platform: Optional[str] = None):
    """Export products to CSV/JSON"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    query = """
        SELECT 
            p.product_sk,
            p.global_product_id,
            p.product_name,
            b.brand_name,
            p.seller_name,
            pl.platform_name,
            f.price_current
        FROM dwh_dim_product p
        LEFT JOIN dwh_dim_brand b ON p.brand_sk = b.brand_sk
        LEFT JOIN dwh_fact_product_daily f ON p.product_sk = f.product_sk
        LEFT JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        WHERE p.is_current = TRUE
    """
    
    if platform:
        query += f" AND pl.platform_code = '{platform}'"
    
    cur.execute(query)
    rows = cur.fetchall()
    columns = ['product_sk', 'global_product_id', 'product_name', 'brand_name', 
               'seller_name', 'platform', 'price']
    
    conn.close()
    
    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=products_{datetime.now().strftime('%Y%m%d')}.csv"}
        )
    else:
        data = [dict(zip(columns, row)) for row in rows]
        return data

@router.get("/export/price-history/{product_sk}")
def export_price_history(product_sk: int, format: str = "csv"):
    """Export price history for a product"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            d.date_value,
            pl.platform_name,
            f.price_current,
            f.price_original,
            f.discount_pct
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_date d ON f.date_sk = d.date_sk
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        WHERE f.product_sk = %s
        ORDER BY d.date_value DESC
    """, (product_sk,))
    
    rows = cur.fetchall()
    columns = ['date', 'platform', 'price_current', 'price_original', 'discount_pct']
    
    conn.close()
    
    if not rows:
        raise HTTPException(404, "Product not found")
    
    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=price_history_{product_sk}.csv"}
        )
    else:
        data = [dict(zip(columns, row)) for row in rows]
        return data

@router.get("/export/analytics-summary")
def export_analytics_summary():
    """Export comprehensive analytics summary"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            pl.platform_name,
            COUNT(DISTINCT f.product_sk) as products,
            AVG(f.price_current) as avg_price,
            SUM(COALESCE(r.total_reviews, 0)) as total_reviews
        FROM dwh_fact_product_daily f
        JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
        LEFT JOIN dwh_fact_review_summary r ON f.product_sk = r.product_sk AND f.platform_sk = r.platform_sk
        WHERE f.price_current > 0
        GROUP BY pl.platform_name
    """)
    
    platform_data = []
    for row in cur.fetchall():
        platform_data.append({
            "platform": row[0],
            "products": row[1],
            "avg_price": float(row[2]),
            "total_reviews": row[3]
        })
    
    conn.close()
    
    return {
        "generated_at": datetime.now().isoformat(),
        "platform_summary": platform_data
    }

@router.post("/save")
def save_report(report_name: str, report_type: str, report_data: dict):
    """Save report configuration"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saved_reports (
            report_id SERIAL PRIMARY KEY,
            report_name VARCHAR(200),
            report_type VARCHAR(50),
            report_config JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    cur.execute("""
        INSERT INTO saved_reports (report_name, report_type, report_config)
        VALUES (%s, %s, %s)
        RETURNING report_id
    """, (report_name, report_type, json.dumps(report_data)))
    
    report_id = cur.fetchone()[0]
    conn.commit()
    conn.close()
    
    return {"report_id": report_id, "message": "Report saved successfully"}

@router.get("/saved")
def get_saved_reports():
    """Get list of saved reports"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT report_id, report_name, report_type, created_at
        FROM saved_reports
        ORDER BY created_at DESC
    """)
    
    results = []
    for row in cur.fetchall():
        results.append({
            "report_id": row[0],
            "report_name": row[1],
            "report_type": row[2],
            "created_at": row[3].isoformat()
        })
    
    conn.close()
    return results
