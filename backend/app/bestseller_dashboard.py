#!/usr/bin/env python3
"""
Best Seller Dashboard - Real-time Analytics
==========================================
Real-time tracking and analytics for best-selling products with predictive insights
"""

from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json
import uuid
from collections import defaultdict, deque
import statistics

# Database
from databases import Database
from sqlalchemy import text

# Redis for caching
import redis.asyncio as redis

# Auth
try:
    from simple_auth import get_current_active_user
except:
    get_current_active_user = lambda: None

# ====================================
# ENUMS AND MODELS
# ====================================

class TimeFrame(str, Enum):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"

class TrendDirection(str, Enum):
    RISING = "rising"
    FALLING = "falling"
    STABLE = "stable"
    EXPLOSIVE = "explosive"  # Very fast growth
    DECLINING = "declining"  # Significant drop

class ProductMetric(str, Enum):
    SALES_VOLUME = "sales_volume"
    REVENUE = "revenue"
    CONVERSION_RATE = "conversion_rate"
    VIEW_TO_PURCHASE = "view_to_purchase"
    PROFIT_MARGIN = "profit_margin"

class BestSellerProduct(BaseModel):
    product_id: str
    product_name: str
    category: str
    brand: Optional[str]
    current_rank: int
    previous_rank: Optional[int]
    rank_change: int
    sales_volume: int
    revenue: float
    conversion_rate: float
    trend_direction: TrendDirection
    trend_percentage: float
    profit_margin: float
    stock_level: int
    estimated_days_to_stockout: Optional[int]
    last_updated: datetime

class TrendingProduct(BaseModel):
    product_id: str
    product_name: str
    category: str
    current_sales: int
    growth_rate: float
    velocity_score: float  # Custom metric combining growth + volume
    first_spike_date: datetime
    confidence_score: float

class CategoryPerformance(BaseModel):
    category: str
    total_products: int
    total_sales: int
    total_revenue: float
    avg_conversion_rate: float
    growth_rate: float
    top_product: BestSellerProduct
    market_share: float

class RealTimeMetrics(BaseModel):
    timestamp: datetime
    total_sales_today: int
    revenue_today: float
    top_selling_product: str
    fastest_growing_category: str
    active_customers: int
    conversion_rate: float

# ====================================
# ROUTER SETUP
# ====================================

bestseller_router = APIRouter(prefix="/api/v1/bestsellers", tags=["Best Seller Analytics"])

# ====================================
# WEBSOCKET CONNECTION MANAGER
# ====================================

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                # Remove dead connections
                self.active_connections.remove(connection)

manager = ConnectionManager()

# ====================================
# BEST SELLER SERVICE
# ====================================

class BestSellerService:
    def __init__(self):
        self.db = None  # Will be injected
        self.redis = None  # Will be injected
        self.cache_ttl = 300  # 5 minutes
        self.real_time_data = deque(maxlen=1000)  # Store last 1000 data points

    async def get_bestsellers(self, timeframe: TimeFrame = TimeFrame.DAY,
                            limit: int = 50, category: str = None) -> List[BestSellerProduct]:
        """Get current best-selling products"""

        # Build time condition
        time_conditions = {
            TimeFrame.HOUR: "o.order_date >= NOW() - INTERVAL '1 hour'",
            TimeFrame.DAY: "o.order_date >= NOW() - INTERVAL '1 day'",
            TimeFrame.WEEK: "o.order_date >= NOW() - INTERVAL '1 week'",
            TimeFrame.MONTH: "o.order_date >= NOW() - INTERVAL '1 month'",
            TimeFrame.QUARTER: "o.order_date >= NOW() - INTERVAL '3 months'",
            TimeFrame.YEAR: "o.order_date >= NOW() - INTERVAL '1 year'"
        }

        time_condition = time_conditions[timeframe]

        # Build category filter
        category_filter = ""
        params = {"limit": limit}
        if category:
            category_filter = "AND p.category = :category"
            params["category"] = category

        # Get current period sales
        current_query = f"""
        WITH current_sales AS (
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                p.brand,
                p.price,
                p.cost_price,
                p.stock_quantity,
                SUM(oi.quantity) as sales_volume,
                SUM(oi.quantity * oi.unit_price) as revenue,
                COUNT(DISTINCT o.order_id) as order_count,
                COUNT(DISTINCT o.customer_id) as unique_customers,
                (p.price - COALESCE(p.cost_price, p.price * 0.6)) / p.price * 100 as profit_margin_pct
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE {time_condition}
            {category_filter}
            GROUP BY p.product_id, p.product_name, p.category, p.brand, p.price, p.cost_price, p.stock_quantity
        ),
        previous_sales AS (
            SELECT
                p.product_id,
                SUM(oi.quantity) as prev_sales_volume,
                SUM(oi.quantity * oi.unit_price) as prev_revenue
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE {time_condition.replace('NOW()', 'NOW() - INTERVAL \'1 day\'')}
            {category_filter}
            GROUP BY p.product_id
        ),
        product_views AS (
            SELECT
                product_id,
                COUNT(*) as view_count
            FROM customer_behavior_logs
            WHERE behavior_type = 'product_view'
            AND timestamp >= NOW() - INTERVAL '1 day'
            GROUP BY product_id
        )
        SELECT
            cs.*,
            COALESCE(ps.prev_sales_volume, 0) as prev_sales_volume,
            COALESCE(pv.view_count, 0) as view_count,
            CASE
                WHEN COALESCE(pv.view_count, 0) > 0
                THEN (cs.sales_volume::float / pv.view_count * 100)
                ELSE 0
            END as conversion_rate,
            ROW_NUMBER() OVER (ORDER BY cs.sales_volume DESC) as current_rank
        FROM current_sales cs
        LEFT JOIN previous_sales ps ON cs.product_id = ps.product_id
        LEFT JOIN product_views pv ON cs.product_id = pv.product_id
        ORDER BY cs.sales_volume DESC
        LIMIT :limit
        """

        results = await self.db.fetch_all(current_query, params)

        # Get previous rankings for comparison
        previous_rankings = await self.get_previous_rankings(timeframe, category)

        bestsellers = []
        for i, row in enumerate(results):
            prev_rank = previous_rankings.get(row["product_id"])
            rank_change = (prev_rank - (i + 1)) if prev_rank else 0

            # Calculate trend
            current_sales = row["sales_volume"]
            prev_sales = row["prev_sales_volume"]
            trend_direction, trend_percentage = self.calculate_trend(current_sales, prev_sales)

            # Estimate days to stockout
            daily_avg_sales = current_sales / (1 if timeframe == TimeFrame.DAY else 7)
            days_to_stockout = None
            if daily_avg_sales > 0:
                days_to_stockout = int(row["stock_quantity"] / daily_avg_sales)

            bestsellers.append(BestSellerProduct(
                product_id=row["product_id"],
                product_name=row["product_name"],
                category=row["category"],
                brand=row["brand"],
                current_rank=i + 1,
                previous_rank=prev_rank,
                rank_change=rank_change,
                sales_volume=current_sales,
                revenue=float(row["revenue"]),
                conversion_rate=round(row["conversion_rate"], 2),
                trend_direction=trend_direction,
                trend_percentage=trend_percentage,
                profit_margin=round(row["profit_margin_pct"], 2),
                stock_level=row["stock_quantity"],
                estimated_days_to_stockout=days_to_stockout,
                last_updated=datetime.now()
            ))

        return bestsellers

    async def get_previous_rankings(self, timeframe: TimeFrame, category: str = None) -> Dict[str, int]:
        """Get previous period rankings for comparison"""

        # For simplicity, get rankings from stored bestseller_rankings table
        # In production, this would be more sophisticated
        query = """
        SELECT product_id, rank_position
        FROM bestseller_rankings
        WHERE timeframe = :timeframe
        AND date = CURRENT_DATE - INTERVAL '1 day'
        """

        if category:
            query += " AND category = :category"

        params = {"timeframe": timeframe.value}
        if category:
            params["category"] = category

        results = await self.db.fetch_all(query, params)
        return {row["product_id"]: row["rank_position"] for row in results}

    def calculate_trend(self, current: int, previous: int) -> tuple[TrendDirection, float]:
        """Calculate trend direction and percentage change"""

        if previous == 0:
            if current > 10:  # Arbitrary threshold for "explosive"
                return TrendDirection.EXPLOSIVE, 100.0
            elif current > 0:
                return TrendDirection.RISING, 100.0
            else:
                return TrendDirection.STABLE, 0.0

        percentage_change = ((current - previous) / previous) * 100

        if percentage_change > 50:
            return TrendDirection.EXPLOSIVE, percentage_change
        elif percentage_change > 10:
            return TrendDirection.RISING, percentage_change
        elif percentage_change < -30:
            return TrendDirection.DECLINING, percentage_change
        elif percentage_change < -10:
            return TrendDirection.FALLING, percentage_change
        else:
            return TrendDirection.STABLE, percentage_change

    async def get_trending_products(self, limit: int = 20) -> List[TrendingProduct]:
        """Get products with highest growth velocity"""

        query = """
        WITH product_metrics AS (
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                SUM(oi.quantity) as current_sales,
                COUNT(DISTINCT DATE(o.order_date)) as active_days,
                MIN(o.order_date) as first_sale_date,
                MAX(o.order_date) as last_sale_date
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_date >= NOW() - INTERVAL '7 days'
            GROUP BY p.product_id, p.product_name, p.category
            HAVING SUM(oi.quantity) >= 5  -- Minimum sales threshold
        ),
        growth_calc AS (
            SELECT
                *,
                current_sales / GREATEST(active_days, 1) as daily_avg_sales,
                CASE
                    WHEN active_days >= 3 THEN
                        (current_sales * LOG(current_sales + 1) / active_days)
                    ELSE current_sales
                END as velocity_score
            FROM product_metrics
        )
        SELECT
            *,
            -- Simple confidence based on consistency and volume
            LEAST(current_sales / 20.0, 1.0) * LEAST(active_days / 7.0, 1.0) as confidence_score
        FROM growth_calc
        ORDER BY velocity_score DESC
        LIMIT :limit
        """

        results = await self.db.fetch_all(query, {"limit": limit})

        trending = []
        for row in results:
            # Calculate growth rate (simplified)
            growth_rate = (row["current_sales"] / max(row["active_days"], 1)) * 100

            trending.append(TrendingProduct(
                product_id=row["product_id"],
                product_name=row["product_name"],
                category=row["category"],
                current_sales=row["current_sales"],
                growth_rate=round(growth_rate, 2),
                velocity_score=round(row["velocity_score"], 2),
                first_spike_date=row["first_sale_date"],
                confidence_score=round(row["confidence_score"], 2)
            ))

        return trending

    async def get_category_performance(self) -> List[CategoryPerformance]:
        """Get performance metrics by category"""

        query = """
        WITH category_metrics AS (
            SELECT
                p.category,
                COUNT(DISTINCT p.product_id) as total_products,
                SUM(oi.quantity) as total_sales,
                SUM(oi.quantity * oi.unit_price) as total_revenue,
                COUNT(DISTINCT o.order_id) as total_orders,
                AVG(CASE
                    WHEN pv.view_count > 0 THEN (oi.quantity::float / pv.view_count * 100)
                    ELSE 0
                END) as avg_conversion_rate
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            LEFT JOIN (
                SELECT product_id, COUNT(*) as view_count
                FROM customer_behavior_logs
                WHERE behavior_type = 'product_view'
                AND timestamp >= NOW() - INTERVAL '1 day'
                GROUP BY product_id
            ) pv ON p.product_id = pv.product_id
            WHERE o.order_date >= NOW() - INTERVAL '1 day'
            GROUP BY p.category
        ),
        previous_metrics AS (
            SELECT
                p.category,
                SUM(oi.quantity) as prev_total_sales
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_date >= NOW() - INTERVAL '2 days'
            AND o.order_date < NOW() - INTERVAL '1 day'
            GROUP BY p.category
        ),
        total_market AS (
            SELECT SUM(total_revenue) as market_total
            FROM category_metrics
        )
        SELECT
            cm.*,
            COALESCE(pm.prev_total_sales, 0) as prev_total_sales,
            tm.market_total,
            (cm.total_revenue / tm.market_total * 100) as market_share,
            CASE
                WHEN COALESCE(pm.prev_total_sales, 0) > 0 THEN
                    ((cm.total_sales - pm.prev_total_sales)::float / pm.prev_total_sales * 100)
                ELSE 0
            END as growth_rate
        FROM category_metrics cm
        LEFT JOIN previous_metrics pm ON cm.category = pm.category
        CROSS JOIN total_market tm
        ORDER BY cm.total_revenue DESC
        """

        results = await self.db.fetch_all(query)

        categories = []
        for row in results:
            # Get top product for this category
            top_product_query = """
            SELECT p.product_id, p.product_name, SUM(oi.quantity) as sales
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE p.category = :category
            AND o.order_date >= NOW() - INTERVAL '1 day'
            GROUP BY p.product_id, p.product_name
            ORDER BY sales DESC
            LIMIT 1
            """

            top_product_row = await self.db.fetch_one(top_product_query, {"category": row["category"]})

            # Create a simplified BestSellerProduct for top product
            top_product = None
            if top_product_row:
                top_product = BestSellerProduct(
                    product_id=top_product_row["product_id"],
                    product_name=top_product_row["product_name"],
                    category=row["category"],
                    brand=None,
                    current_rank=1,
                    previous_rank=None,
                    rank_change=0,
                    sales_volume=top_product_row["sales"],
                    revenue=0.0,
                    conversion_rate=0.0,
                    trend_direction=TrendDirection.STABLE,
                    trend_percentage=0.0,
                    profit_margin=0.0,
                    stock_level=0,
                    estimated_days_to_stockout=None,
                    last_updated=datetime.now()
                )

            categories.append(CategoryPerformance(
                category=row["category"],
                total_products=row["total_products"],
                total_sales=row["total_sales"],
                total_revenue=float(row["total_revenue"]),
                avg_conversion_rate=round(row["avg_conversion_rate"] or 0, 2),
                growth_rate=round(row["growth_rate"], 2),
                top_product=top_product,
                market_share=round(row["market_share"], 2)
            ))

        return categories

    async def get_real_time_metrics(self) -> RealTimeMetrics:
        """Get real-time dashboard metrics"""

        # Try to get from cache first
        cached = await self.redis.get("real_time_metrics") if self.redis else None
        if cached:
            return RealTimeMetrics(**json.loads(cached))

        # Calculate metrics
        today_sales_query = """
        SELECT
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.total_amount_vnd) as total_revenue,
            COUNT(DISTINCT o.customer_id) as active_customers,
            AVG(CASE WHEN s.session_id IS NOT NULL THEN 1.0 ELSE 0.0 END) as conversion_rate
        FROM orders o
        LEFT JOIN website_sessions s ON o.customer_id = s.customer_id
        WHERE DATE(o.order_date) = CURRENT_DATE
        """

        metrics_result = await self.db.fetch_one(today_sales_query)

        # Get top selling product today
        top_product_query = """
        SELECT p.product_name, SUM(oi.quantity) as sales
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        WHERE DATE(o.order_date) = CURRENT_DATE
        GROUP BY p.product_id, p.product_name
        ORDER BY sales DESC
        LIMIT 1
        """

        top_product_result = await self.db.fetch_one(top_product_query)

        # Get fastest growing category
        fastest_category_query = """
        SELECT
            p.category,
            SUM(oi.quantity) as today_sales,
            COALESCE(prev.yesterday_sales, 0) as yesterday_sales,
            CASE
                WHEN COALESCE(prev.yesterday_sales, 0) > 0 THEN
                    ((SUM(oi.quantity) - prev.yesterday_sales)::float / prev.yesterday_sales * 100)
                ELSE 100
            END as growth_rate
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        LEFT JOIN (
            SELECT
                p2.category,
                SUM(oi2.quantity) as yesterday_sales
            FROM products p2
            JOIN order_items oi2 ON p2.product_id = oi2.product_id
            JOIN orders o2 ON oi2.order_id = o2.order_id
            WHERE DATE(o2.order_date) = CURRENT_DATE - INTERVAL '1 day'
            GROUP BY p2.category
        ) prev ON p.category = prev.category
        WHERE DATE(o.order_date) = CURRENT_DATE
        GROUP BY p.category, prev.yesterday_sales
        ORDER BY growth_rate DESC
        LIMIT 1
        """

        fastest_category_result = await self.db.fetch_one(fastest_category_query)

        metrics = RealTimeMetrics(
            timestamp=datetime.now(),
            total_sales_today=metrics_result["total_orders"] or 0,
            revenue_today=float(metrics_result["total_revenue"] or 0),
            top_selling_product=top_product_result["product_name"] if top_product_result else "N/A",
            fastest_growing_category=fastest_category_result["category"] if fastest_category_result else "N/A",
            active_customers=metrics_result["active_customers"] or 0,
            conversion_rate=round(metrics_result["conversion_rate"] or 0, 2)
        )

        # Cache for 1 minute
        if self.redis:
            await self.redis.setex("real_time_metrics", 60, json.dumps(metrics.dict(), default=str))

        return metrics

    async def save_bestseller_rankings(self, timeframe: TimeFrame, rankings: List[BestSellerProduct]):
        """Save current rankings for historical comparison"""

        # Clear existing rankings for today
        await self.db.execute("""
            DELETE FROM bestseller_rankings
            WHERE date = CURRENT_DATE AND timeframe = :timeframe
        """, {"timeframe": timeframe.value})

        # Insert new rankings
        for ranking in rankings:
            await self.db.execute("""
                INSERT INTO bestseller_rankings (
                    product_id, category, rank_position, sales_volume, revenue,
                    timeframe, date, created_at
                ) VALUES (
                    :product_id, :category, :rank_position, :sales_volume, :revenue,
                    :timeframe, CURRENT_DATE, NOW()
                )
            """, {
                "product_id": ranking.product_id,
                "category": ranking.category,
                "rank_position": ranking.current_rank,
                "sales_volume": ranking.sales_volume,
                "revenue": ranking.revenue,
                "timeframe": timeframe.value
            })

    async def update_real_time_data(self):
        """Update real-time data and broadcast to WebSocket clients"""

        metrics = await self.get_real_time_metrics()
        self.real_time_data.append(metrics)

        # Broadcast to all connected clients
        await manager.broadcast(json.dumps(metrics.dict(), default=str))

# Global service instance
bestseller_service = BestSellerService()

# ====================================
# BACKGROUND TASKS
# ====================================

async def real_time_update_task():
    """Background task for real-time updates"""
    while True:
        try:
            await bestseller_service.update_real_time_data()
            await asyncio.sleep(30)  # Update every 30 seconds
        except Exception as e:
            print(f"Error in real-time update: {e}")
            await asyncio.sleep(60)  # Wait longer on error

# ====================================
# ENDPOINTS
# ====================================

@bestseller_router.get("/", response_model=List[BestSellerProduct])
async def get_bestsellers(
    timeframe: TimeFrame = TimeFrame.DAY,
    limit: int = 50,
    category: Optional[str] = None,
    current_user: Any = Depends(get_current_active_user)
):
    """Get current best-selling products"""
    return await bestseller_service.get_bestsellers(timeframe, limit, category)

@bestseller_router.get("/trending", response_model=List[TrendingProduct])
async def get_trending_products(
    limit: int = 20,
    current_user: Any = Depends(get_current_active_user)
):
    """Get trending products with high growth velocity"""
    return await bestseller_service.get_trending_products(limit)

@bestseller_router.get("/categories", response_model=List[CategoryPerformance])
async def get_category_performance(
    current_user: Any = Depends(get_current_active_user)
):
    """Get performance metrics by category"""
    return await bestseller_service.get_category_performance()

@bestseller_router.get("/metrics/real-time", response_model=RealTimeMetrics)
async def get_real_time_metrics(
    current_user: Any = Depends(get_current_active_user)
):
    """Get real-time dashboard metrics"""
    return await bestseller_service.get_real_time_metrics()

@bestseller_router.websocket("/ws/real-time")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and send periodic updates
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@bestseller_router.post("/save-rankings")
async def save_rankings(
    timeframe: TimeFrame,
    background_tasks: BackgroundTasks,
    current_user: Any = Depends(get_current_active_user)
):
    """Save current rankings for historical analysis"""

    async def save_task():
        rankings = await bestseller_service.get_bestsellers(timeframe, 100)
        await bestseller_service.save_bestseller_rankings(timeframe, rankings)

    background_tasks.add_task(save_task)
    return {"message": f"Ranking save task started for {timeframe.value}"}

@bestseller_router.get("/analytics/comparison")
async def get_comparison_analytics(
    timeframe1: TimeFrame = TimeFrame.DAY,
    timeframe2: TimeFrame = TimeFrame.WEEK,
    current_user: Any = Depends(get_current_active_user)
):
    """Compare performance across different timeframes"""

    period1_data = await bestseller_service.get_bestsellers(timeframe1, 20)
    period2_data = await bestseller_service.get_bestsellers(timeframe2, 20)

    # Create comparison metrics
    comparison = {
        "timeframe1": {
            "period": timeframe1.value,
            "total_sales": sum(p.sales_volume for p in period1_data),
            "total_revenue": sum(p.revenue for p in period1_data),
            "avg_conversion": statistics.mean([p.conversion_rate for p in period1_data if p.conversion_rate > 0] or [0]),
            "top_products": period1_data[:5]
        },
        "timeframe2": {
            "period": timeframe2.value,
            "total_sales": sum(p.sales_volume for p in period2_data),
            "total_revenue": sum(p.revenue for p in period2_data),
            "avg_conversion": statistics.mean([p.conversion_rate for p in period2_data if p.conversion_rate > 0] or [0]),
            "top_products": period2_data[:5]
        }
    }

    return comparison

@bestseller_router.get("/insights/ai")
async def get_ai_insights(
    current_user: Any = Depends(get_current_active_user)
):
    """Get AI-powered insights and recommendations"""

    # Get data for analysis
    bestsellers = await bestseller_service.get_bestsellers(TimeFrame.DAY, 10)
    trending = await bestseller_service.get_trending_products(10)
    categories = await bestseller_service.get_category_performance()

    insights = []

    # Stock alerts
    for product in bestsellers:
        if product.estimated_days_to_stockout and product.estimated_days_to_stockout <= 3:
            insights.append({
                "type": "stock_alert",
                "priority": "high",
                "title": f"Sản phẩm #{product.current_rank} sắp hết hàng",
                "description": f"{product.product_name} chỉ còn {product.estimated_days_to_stockout} ngày",
                "action": "Tăng cường nhập hàng ngay lập tức",
                "product_id": product.product_id
            })

    # Trending opportunities
    for product in trending:
        if product.confidence_score > 0.7 and product.growth_rate > 100:
            insights.append({
                "type": "trending_opportunity",
                "priority": "medium",
                "title": f"Cơ hội từ sản phẩm đang viral",
                "description": f"{product.product_name} tăng trưởng {product.growth_rate:.1f}%",
                "action": "Tăng inventory và marketing cho sản phẩm này",
                "product_id": product.product_id
            })

    # Category performance
    if categories:
        top_category = max(categories, key=lambda x: x.growth_rate)
        if top_category.growth_rate > 50:
            insights.append({
                "type": "category_growth",
                "priority": "medium",
                "title": f"Danh mục {top_category.category} đang bùng nổ",
                "description": f"Tăng trưởng {top_category.growth_rate:.1f}%, chiếm {top_category.market_share:.1f}% thị phần",
                "action": "Mở rộng sản phẩm trong danh mục này",
                "category": top_category.category
            })

    return {"insights": insights}