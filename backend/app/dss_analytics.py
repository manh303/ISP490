#!/usr/bin/env python3
"""
Decision Support System (DSS) Analytics
=======================================
Comprehensive analytics with charts, recommendations, and actionable insights
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import asyncio
import json

# Database
from databases import Database
from sqlalchemy import text

# Data processing
import pandas as pd
import numpy as np
from collections import defaultdict

# Simple auth dependency
try:
    from simple_auth import get_current_active_user
except:
    get_current_active_user = lambda: None

# ====================================
# PYDANTIC MODELS
# ====================================

class DSSMetrics(BaseModel):
    total_customers: int
    total_orders: int
    total_revenue: float
    avg_order_value: float
    top_products: List[Dict[str, Any]]
    revenue_trend: List[Dict[str, Any]]
    customer_segments: List[Dict[str, Any]]

class DSSRecommendation(BaseModel):
    type: str  # "alert", "opportunity", "action"
    priority: str  # "high", "medium", "low"
    title: str
    description: str
    metric_value: Optional[float] = None
    target_value: Optional[float] = None
    action_items: List[str]
    chart_data: Optional[Dict[str, Any]] = None

class DSSAction(BaseModel):
    action_id: str
    title: str
    description: str
    category: str  # "marketing", "inventory", "customer_service", "pricing"
    estimated_impact: str
    effort_level: str  # "low", "medium", "high"
    timeline: str
    kpis: List[str]

# ====================================
# ROUTER SETUP
# ====================================

dss_router = APIRouter(prefix="/api/v1/dss", tags=["Decision Support System"])

# ====================================
# UTILITY FUNCTIONS
# ====================================

async def get_postgres_session():
    """Get PostgreSQL session from main app"""
    # This will be injected from main app dependencies
    return None

def analyze_trends(data: List[Dict], value_field: str) -> Dict[str, Any]:
    """Analyze trends in data"""
    if len(data) < 2:
        return {"trend": "insufficient_data", "change_percent": 0}

    # Calculate trend
    recent_avg = np.mean([item[value_field] for item in data[-3:]])
    earlier_avg = np.mean([item[value_field] for item in data[:3]])

    if earlier_avg == 0:
        change_percent = 0
    else:
        change_percent = ((recent_avg - earlier_avg) / earlier_avg) * 100

    if change_percent > 10:
        trend = "increasing"
    elif change_percent < -10:
        trend = "decreasing"
    else:
        trend = "stable"

    return {
        "trend": trend,
        "change_percent": round(change_percent, 2),
        "recent_avg": round(recent_avg, 2),
        "earlier_avg": round(earlier_avg, 2)
    }

def generate_recommendations(metrics: Dict[str, Any]) -> List[DSSRecommendation]:
    """Generate intelligent recommendations based on metrics"""
    recommendations = []

    # Revenue recommendations
    if metrics.get('total_revenue', 0) < 1000000:  # Dưới 1M
        recommendations.append(DSSRecommendation(
            type="opportunity",
            priority="high",
            title="Tăng Doanh Thu",
            description="Doanh thu hiện tại thấp hơn mục tiêu. Cần chiến lược marketing mạnh mẽ hơn.",
            metric_value=metrics.get('total_revenue', 0),
            target_value=1000000,
            action_items=[
                "Chạy chiến dịch marketing targeted",
                "Tăng cross-selling và upselling",
                "Mở rộng sản phẩm hot trend",
                "Chương trình khuyến mãi hấp dẫn"
            ],
            chart_data={
                "type": "gauge",
                "current": metrics.get('total_revenue', 0),
                "target": 1000000,
                "unit": "VND"
            }
        ))

    # Average order value recommendations
    avg_order = metrics.get('avg_order_value', 0)
    if avg_order < 500000:  # Dưới 500k
        recommendations.append(DSSRecommendation(
            type="action",
            priority="medium",
            title="Tăng Giá Trị Đơn Hàng Trung Bình",
            description=f"AOV hiện tại {avg_order:,.0f} VND thấp. Cần tăng lên ít nhất 500k.",
            metric_value=avg_order,
            target_value=500000,
            action_items=[
                "Tạo combo sản phẩm hấp dẫn",
                "Freeship cho đơn hàng trên 500k",
                "Gợi ý sản phẩm liên quan",
                "Chương trình tích điểm"
            ]
        ))

    # Customer insights
    total_customers = metrics.get('total_customers', 0)
    total_orders = metrics.get('total_orders', 0)

    if total_customers > 0:
        orders_per_customer = total_orders / total_customers
        if orders_per_customer < 2:
            recommendations.append(DSSRecommendation(
                type="alert",
                priority="high",
                title="Tỷ Lệ Khách Hàng Quay Lại Thấp",
                description=f"Mỗi khách hàng chỉ mua {orders_per_customer:.1f} đơn. Cần tăng customer retention.",
                metric_value=orders_per_customer,
                target_value=3.0,
                action_items=[
                    "Email marketing nurturing campaigns",
                    "Chương trình loyalty rewards",
                    "Personalized product recommendations",
                    "After-sales service tốt hơn"
                ]
            ))

    # Product performance
    top_products = metrics.get('top_products', [])
    if len(top_products) < 10:
        recommendations.append(DSSRecommendation(
            type="opportunity",
            priority="medium",
            title="Mở Rộng Danh Mục Sản Phẩm",
            description="Số lượng sản phẩm bán chạy còn ít. Cần diversify portfolio.",
            action_items=[
                "Nghiên cứu thị trường sản phẩm mới",
                "A/B test sản phẩm trending",
                "Hợp tác với supplier mới",
                "Phân tích competitor products"
            ]
        ))

    return recommendations

def generate_action_plan() -> List[DSSAction]:
    """Generate actionable business plans"""
    return [
        DSSAction(
            action_id="MARKETING_001",
            title="Chiến Dịch Digital Marketing Q4",
            description="Tăng traffic và conversion thông qua Facebook Ads, Google Ads và TikTok Marketing",
            category="marketing",
            estimated_impact="Tăng 30-50% doanh thu",
            effort_level="medium",
            timeline="2-3 tháng",
            kpis=["ROAS", "Conversion Rate", "Traffic", "Brand Awareness"]
        ),
        DSSAction(
            action_id="INVENTORY_001",
            title="Tối Ưu Hóa Kho Hàng",
            description="Áp dụng ABC analysis và demand forecasting để tối ưu inventory",
            category="inventory",
            estimated_impact="Giảm 20% cost, tăng 15% availability",
            effort_level="high",
            timeline="1-2 tháng",
            kpis=["Inventory Turnover", "Stockout Rate", "Carrying Cost"]
        ),
        DSSAction(
            action_id="CUSTOMER_001",
            title="Chương Trình Loyalty VIP",
            description="Xây dựng hệ thống tích điểm và rewards cho khách hàng thân thiết",
            category="customer_service",
            estimated_impact="Tăng 25% customer retention",
            effort_level="medium",
            timeline="1 tháng",
            kpis=["Customer Lifetime Value", "Repeat Purchase Rate", "NPS Score"]
        ),
        DSSAction(
            action_id="PRICING_001",
            title="Dynamic Pricing Strategy",
            description="Áp dụng pricing algorithm dựa trên demand, competitor và season",
            category="pricing",
            estimated_impact="Tăng 10-15% profit margin",
            effort_level="high",
            timeline="2-4 tháng",
            kpis=["Profit Margin", "Price Competitiveness", "Demand Elasticity"]
        ),
        DSSAction(
            action_id="ANALYTICS_001",
            title="Real-time Business Intelligence",
            description="Nâng cấp dashboard với real-time alerts và predictive analytics",
            category="analytics",
            estimated_impact="Tăng 20% decision speed",
            effort_level="medium",
            timeline="1-2 tháng",
            kpis=["Decision Time", "Forecast Accuracy", "Alert Response Time"]
        )
    ]

# ====================================
# DSS ENDPOINTS
# ====================================

@dss_router.get("/dashboard", response_model=Dict[str, Any])
async def get_dss_dashboard(
    db: Database = Depends(get_postgres_session),
    current_user: dict = Depends(get_current_active_user)
):
    """
    Get comprehensive DSS dashboard with metrics, charts, and insights
    """
    try:

        # Get basic metrics
        dashboard_data = {
            "summary_metrics": {},
            "charts": {},
            "recommendations": [],
            "action_plans": [],
            "alerts": [],
            "timestamp": datetime.now().isoformat()
        }

        # Total customers
        cursor.execute("SELECT COUNT(DISTINCT customer_unique_id) FROM dw_ecommerce_data")
        total_customers = cursor.fetchone()[0] or 0

        # Total orders
        cursor.execute("SELECT COUNT(DISTINCT order_id) FROM dw_ecommerce_data")
        total_orders = cursor.fetchone()[0] or 0

        # Total revenue
        cursor.execute("SELECT SUM(payment_value) FROM dw_ecommerce_data WHERE payment_value IS NOT NULL")
        result = cursor.fetchone()
        total_revenue = float(result[0]) if result and result[0] else 0

        # Average order value
        cursor.execute("SELECT AVG(payment_value) FROM dw_ecommerce_data WHERE payment_value IS NOT NULL")
        result = cursor.fetchone()
        avg_order_value = float(result[0]) if result and result[0] else 0

        # Top products by revenue
        cursor.execute("""
            SELECT product_category_name, SUM(payment_value) as revenue,
                   COUNT(DISTINCT order_id) as orders
            FROM dw_ecommerce_data
            WHERE payment_value IS NOT NULL AND product_category_name IS NOT NULL
            GROUP BY product_category_name
            ORDER BY revenue DESC
            LIMIT 10
        """)
        top_products = []
        for row in cursor.fetchall():
            top_products.append({
                "category": row[0],
                "revenue": float(row[1]),
                "orders": row[2]
            })

        # Monthly revenue trend (simulated)
        cursor.execute("""
            SELECT
                EXTRACT(MONTH FROM order_purchase_timestamp) as month,
                SUM(payment_value) as revenue
            FROM dw_ecommerce_data
            WHERE payment_value IS NOT NULL
                AND order_purchase_timestamp IS NOT NULL
            GROUP BY EXTRACT(MONTH FROM order_purchase_timestamp)
            ORDER BY month
        """)
        revenue_trend = []
        for row in cursor.fetchall():
            revenue_trend.append({
                "month": int(row[0]),
                "revenue": float(row[1])
            })

        # Customer segments by value
        cursor.execute("""
            SELECT
                CASE
                    WHEN customer_revenue > 1000000 THEN 'VIP'
                    WHEN customer_revenue > 500000 THEN 'Premium'
                    WHEN customer_revenue > 100000 THEN 'Standard'
                    ELSE 'Basic'
                END as segment,
                COUNT(*) as customers,
                AVG(customer_revenue) as avg_revenue
            FROM (
                SELECT customer_unique_id, SUM(payment_value) as customer_revenue
                FROM dw_ecommerce_data
                WHERE payment_value IS NOT NULL
                GROUP BY customer_unique_id
            ) customer_totals
            GROUP BY segment
            ORDER BY avg_revenue DESC
        """)
        customer_segments = []
        for row in cursor.fetchall():
            customer_segments.append({
                "segment": row[0],
                "customers": row[1],
                "avg_revenue": float(row[2])
            })

        # Summary metrics
        dashboard_data["summary_metrics"] = {
            "total_customers": total_customers,
            "total_orders": total_orders,
            "total_revenue": total_revenue,
            "avg_order_value": avg_order_value,
            "revenue_per_customer": total_revenue / total_customers if total_customers > 0 else 0,
            "orders_per_customer": total_orders / total_customers if total_customers > 0 else 0
        }

        # Chart data
        dashboard_data["charts"] = {
            "top_products": top_products,
            "revenue_trend": revenue_trend,
            "customer_segments": customer_segments
        }

        # Generate recommendations
        dashboard_data["recommendations"] = [
            rec.dict() for rec in generate_recommendations(dashboard_data["summary_metrics"])
        ]

        # Get action plans
        dashboard_data["action_plans"] = [
            action.dict() for action in generate_action_plan()
        ]

        # Real-time alerts (from streaming data)
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM realtime_kafka_stream
                WHERE processed_at >= NOW() - INTERVAL '1 hour'
            """)
            recent_activity = cursor.fetchone()[0] or 0

            if recent_activity < 10:
                dashboard_data["alerts"].append({
                    "type": "warning",
                    "title": "Hoạt Động Real-time Thấp",
                    "message": f"Chỉ có {recent_activity} giao dịch trong 1 giờ qua",
                    "action": "Kiểm tra hệ thống streaming"
                })
        except:
            pass

        cursor.close()
        conn.close()

        return dashboard_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DSS Dashboard error: {str(e)}")

@dss_router.get("/recommendations")
async def get_recommendations(current_user: dict = Depends(get_current_active_user)):
    """Get AI-powered business recommendations"""
    try:
        conn = await get_postgres_connection()
        cursor = conn.cursor()

        # Get key metrics for recommendations
        cursor.execute("SELECT COUNT(DISTINCT customer_unique_id) FROM dw_ecommerce_data")
        total_customers = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(DISTINCT order_id) FROM dw_ecommerce_data")
        total_orders = cursor.fetchone()[0] or 0

        cursor.execute("SELECT SUM(payment_value) FROM dw_ecommerce_data WHERE payment_value IS NOT NULL")
        result = cursor.fetchone()
        total_revenue = float(result[0]) if result and result[0] else 0

        cursor.execute("SELECT AVG(payment_value) FROM dw_ecommerce_data WHERE payment_value IS NOT NULL")
        result = cursor.fetchone()
        avg_order_value = float(result[0]) if result and result[0] else 0

        metrics = {
            "total_customers": total_customers,
            "total_orders": total_orders,
            "total_revenue": total_revenue,
            "avg_order_value": avg_order_value
        }

        recommendations = generate_recommendations(metrics)

        cursor.close()
        conn.close()

        return {
            "recommendations": [rec.dict() for rec in recommendations],
            "total_count": len(recommendations),
            "generated_at": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Recommendations error: {str(e)}")

@dss_router.get("/actions")
async def get_action_plans(current_user: dict = Depends(get_current_active_user)):
    """Get actionable business plans"""
    try:
        actions = generate_action_plan()

        return {
            "action_plans": [action.dict() for action in actions],
            "total_count": len(actions),
            "categories": list(set(action.category for action in actions)),
            "generated_at": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Action plans error: {str(e)}")

@dss_router.get("/performance")
async def get_performance_metrics(current_user: dict = Depends(get_current_active_user)):
    """Get detailed performance analytics"""
    try:
        conn = await get_postgres_connection()
        cursor = conn.cursor()

        performance_data = {}

        # Sales performance
        cursor.execute("""
            SELECT
                product_category_name,
                COUNT(DISTINCT order_id) as orders,
                SUM(payment_value) as revenue,
                AVG(payment_value) as avg_order_value,
                COUNT(DISTINCT customer_unique_id) as unique_customers
            FROM dw_ecommerce_data
            WHERE payment_value IS NOT NULL AND product_category_name IS NOT NULL
            GROUP BY product_category_name
            ORDER BY revenue DESC
            LIMIT 15
        """)

        category_performance = []
        for row in cursor.fetchall():
            category_performance.append({
                "category": row[0],
                "orders": row[1],
                "revenue": float(row[2]),
                "avg_order_value": float(row[3]),
                "unique_customers": row[4],
                "revenue_per_customer": float(row[2]) / row[4] if row[4] > 0 else 0
            })

        performance_data["category_performance"] = category_performance

        # Geographic performance (by state)
        cursor.execute("""
            SELECT
                customer_state,
                COUNT(DISTINCT order_id) as orders,
                SUM(payment_value) as revenue,
                COUNT(DISTINCT customer_unique_id) as customers
            FROM dw_ecommerce_data
            WHERE payment_value IS NOT NULL AND customer_state IS NOT NULL
            GROUP BY customer_state
            ORDER BY revenue DESC
            LIMIT 10
        """)

        geo_performance = []
        for row in cursor.fetchall():
            geo_performance.append({
                "state": row[0],
                "orders": row[1],
                "revenue": float(row[2]),
                "customers": row[3]
            })

        performance_data["geographic_performance"] = geo_performance

        cursor.close()
        conn.close()

        return {
            "performance_metrics": performance_data,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Performance metrics error: {str(e)}")

# Export router
__all__ = ['dss_router']