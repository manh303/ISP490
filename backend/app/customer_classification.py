#!/usr/bin/env python3
"""
Advanced Customer Classification System
======================================
Comprehensive customer segmentation with behavioral analysis and ML-powered insights
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json
import uuid
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

# Database
from databases import Database
from sqlalchemy import text

# Auth
try:
    from simple_auth import get_current_active_user
except:
    get_current_active_user = lambda: None

# ====================================
# ENUMS AND MODELS
# ====================================

class CustomerSegment(str, Enum):
    CHAMPION = "champion"           # Best customers - high value, high engagement
    LOYAL = "loyal"                # Regular valuable customers
    POTENTIAL_LOYALIST = "potential_loyalist"  # Good customers with potential
    NEW_CUSTOMER = "new_customer"   # Recent customers
    PROMISING = "promising"         # New customers with potential
    NEED_ATTENTION = "need_attention"  # Declining customers
    ABOUT_TO_SLEEP = "about_to_sleep"  # Risk of churning
    AT_RISK = "at_risk"            # High churn risk
    CANNOT_LOSE = "cannot_lose"     # High value but declining
    HIBERNATING = "hibernating"     # Low activity
    LOST = "lost"                  # Churned customers

class BehaviorType(str, Enum):
    BROWSER = "browser"             # High browsing, low purchase
    CONVERTER = "converter"         # High conversion rate
    BARGAIN_HUNTER = "bargain_hunter"  # Price sensitive
    LOYALIST = "loyalist"          # Brand/store loyal
    OCCASIONAL = "occasional"       # Infrequent purchases
    IMPULSE_BUYER = "impulse_buyer"  # Quick purchase decisions

class CustomerClassificationRequest(BaseModel):
    customer_id: Optional[str] = None
    include_predictions: bool = True
    include_recommendations: bool = True
    analysis_period_days: int = 365

class CustomerProfile(BaseModel):
    customer_id: str
    rfm_segment: CustomerSegment
    behavior_type: BehaviorType
    recency_score: int
    frequency_score: int
    monetary_score: int
    clv_prediction: float
    churn_probability: float
    recommended_actions: List[str]
    last_analysis: datetime

class SegmentAnalytics(BaseModel):
    segment: CustomerSegment
    customer_count: int
    avg_order_value: float
    avg_lifetime_value: float
    avg_recency_days: float
    total_revenue: float
    segment_percentage: float

# ====================================
# ROUTER SETUP
# ====================================

classification_router = APIRouter(prefix="/api/v1/customer-classification", tags=["Customer Classification"])

# ====================================
# CUSTOMER CLASSIFICATION SERVICE
# ====================================

class CustomerClassificationService:
    def __init__(self):
        self.db = None  # Will be injected
        self.scaler = StandardScaler()
        self.kmeans_model = None

    async def analyze_customer_rfm(self, customer_id: str = None, days: int = 365) -> List[Dict[str, Any]]:
        """Analyze RFM (Recency, Frequency, Monetary) for customers"""

        where_clause = ""
        params = {"days": days}

        if customer_id:
            where_clause = "AND o.customer_id = :customer_id"
            params["customer_id"] = customer_id

        rfm_query = f"""
        WITH customer_metrics AS (
            SELECT
                o.customer_id,
                MAX(o.order_date) as last_order_date,
                COUNT(DISTINCT o.order_id) as order_frequency,
                SUM(o.total_amount_vnd) as total_monetary,
                AVG(o.total_amount_vnd) as avg_order_value,
                DATE_PART('day', NOW() - MAX(o.order_date)) as recency_days,
                COUNT(DISTINCT DATE_TRUNC('month', o.order_date)) as active_months,
                MIN(o.order_date) as first_order_date,
                c.customer_segment,
                c.registration_date
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.order_date >= NOW() - INTERVAL '{days} days'
            {where_clause}
            GROUP BY o.customer_id, c.customer_segment, c.registration_date
        ),
        rfm_scores AS (
            SELECT
                *,
                NTILE(5) OVER (ORDER BY recency_days ASC) as recency_score,
                NTILE(5) OVER (ORDER BY order_frequency DESC) as frequency_score,
                NTILE(5) OVER (ORDER BY total_monetary DESC) as monetary_score
            FROM customer_metrics
        )
        SELECT
            customer_id,
            recency_days,
            order_frequency,
            total_monetary,
            avg_order_value,
            active_months,
            customer_segment as current_segment,
            recency_score,
            frequency_score,
            monetary_score,
            (recency_score + frequency_score + monetary_score) as total_rfm_score,
            CASE
                WHEN DATE_PART('day', NOW() - registration_date) <= 90 THEN TRUE
                ELSE FALSE
            END as is_new_customer,
            first_order_date,
            last_order_date
        FROM rfm_scores
        ORDER BY total_rfm_score DESC
        """

        results = await self.db.fetch_all(rfm_query, params)
        return [dict(row) for row in results]

    def classify_rfm_segment(self, recency: int, frequency: int, monetary: int, is_new: bool = False) -> CustomerSegment:
        """Classify customer segment based on RFM scores"""

        if is_new:
            if frequency >= 4 and monetary >= 4:
                return CustomerSegment.PROMISING
            else:
                return CustomerSegment.NEW_CUSTOMER

        # High value segments
        if recency >= 4 and frequency >= 4 and monetary >= 4:
            return CustomerSegment.CHAMPION
        elif recency >= 3 and frequency >= 3 and monetary >= 3:
            return CustomerSegment.LOYAL
        elif recency >= 3 and frequency <= 2 and monetary >= 4:
            return CustomerSegment.CANNOT_LOSE
        elif recency >= 4 and frequency <= 2 and monetary >= 3:
            return CustomerSegment.POTENTIAL_LOYALIST

        # Medium value segments
        elif recency >= 3 and frequency >= 3 and monetary <= 2:
            return CustomerSegment.NEED_ATTENTION
        elif recency <= 2 and frequency >= 3 and monetary >= 3:
            return CustomerSegment.AT_RISK
        elif recency <= 2 and frequency >= 2 and monetary >= 2:
            return CustomerSegment.ABOUT_TO_SLEEP

        # Low value segments
        elif recency <= 2 and frequency <= 2 and monetary >= 3:
            return CustomerSegment.HIBERNATING
        else:
            return CustomerSegment.LOST

    async def analyze_customer_behavior(self, customer_id: str) -> BehaviorType:
        """Analyze customer behavior patterns"""

        behavior_query = """
        WITH customer_behavior AS (
            SELECT
                COUNT(DISTINCT s.session_id) as total_sessions,
                COUNT(DISTINCT o.order_id) as total_orders,
                COALESCE(COUNT(DISTINCT o.order_id)::float / NULLIF(COUNT(DISTINCT s.session_id), 0), 0) as conversion_rate,
                AVG(o.total_amount_vnd) as avg_order_value,
                AVG(DATE_PART('epoch', o.order_date - s.session_start) / 60) as avg_decision_time_minutes,
                COUNT(CASE WHEN o.discount_amount_vnd > 0 THEN 1 END) as discounted_orders,
                COUNT(DISTINCT p.brand) as unique_brands_purchased,
                AVG(DATE_PART('day', LEAD(o.order_date) OVER (ORDER BY o.order_date) - o.order_date)) as avg_days_between_orders
            FROM customers c
            LEFT JOIN website_sessions s ON c.customer_id = s.customer_id
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN products p ON oi.product_id = p.product_id
            WHERE c.customer_id = :customer_id
            AND o.order_date >= NOW() - INTERVAL '365 days'
        )
        SELECT * FROM customer_behavior
        """

        result = await self.db.fetch_one(behavior_query, {"customer_id": customer_id})

        if not result:
            return BehaviorType.BROWSER

        # Classify behavior based on patterns
        conversion_rate = result["conversion_rate"] or 0
        avg_decision_time = result["avg_decision_time_minutes"] or 0
        discounted_ratio = (result["discounted_orders"] or 0) / max(result["total_orders"] or 1, 1)
        brand_diversity = result["unique_brands_purchased"] or 0

        if conversion_rate > 0.3:
            return BehaviorType.CONVERTER
        elif avg_decision_time < 10:  # Quick decisions
            return BehaviorType.IMPULSE_BUYER
        elif discounted_ratio > 0.7:  # Mostly uses discounts
            return BehaviorType.BARGAIN_HUNTER
        elif brand_diversity <= 2:  # Loyal to few brands
            return BehaviorType.LOYALIST
        elif result["total_orders"] and result["total_orders"] <= 3:
            return BehaviorType.OCCASIONAL
        else:
            return BehaviorType.BROWSER

    async def predict_customer_clv(self, customer_data: Dict[str, Any]) -> float:
        """Predict Customer Lifetime Value using simple heuristics"""

        # Simple CLV calculation based on available data
        avg_order_value = customer_data.get("avg_order_value", 0)
        order_frequency = customer_data.get("order_frequency", 0)
        active_months = customer_data.get("active_months", 1)

        # Estimate monthly purchase frequency
        monthly_frequency = order_frequency / max(active_months, 1)

        # Estimate annual CLV
        annual_clv = avg_order_value * monthly_frequency * 12

        # Apply segment multipliers
        current_segment = customer_data.get("current_segment", "")
        multiplier = {
            "VIP": 1.5,
            "Premium": 1.3,
            "Regular": 1.0,
            "Basic": 0.8
        }.get(current_segment, 1.0)

        return annual_clv * multiplier

    async def predict_churn_probability(self, customer_data: Dict[str, Any]) -> float:
        """Predict churn probability based on customer behavior"""

        recency_days = customer_data.get("recency_days", 365)
        order_frequency = customer_data.get("order_frequency", 0)
        total_monetary = customer_data.get("total_monetary", 0)

        # Simple churn prediction based on recency and activity
        if recency_days > 180:  # No order in 6 months
            base_churn = 0.8
        elif recency_days > 90:  # No order in 3 months
            base_churn = 0.6
        elif recency_days > 30:  # No order in 1 month
            base_churn = 0.3
        else:
            base_churn = 0.1

        # Adjust based on frequency and value
        if order_frequency > 10 and total_monetary > 5000000:  # High value customer
            base_churn *= 0.5
        elif order_frequency > 5:
            base_churn *= 0.7
        elif order_frequency <= 1:
            base_churn *= 1.3

        return min(base_churn, 1.0)

    def generate_customer_recommendations(self, segment: CustomerSegment, behavior: BehaviorType,
                                        clv: float, churn_prob: float) -> List[str]:
        """Generate actionable recommendations based on customer profile"""

        recommendations = []

        # Segment-based recommendations
        segment_recommendations = {
            CustomerSegment.CHAMPION: [
                "Offer exclusive VIP experiences and early access to new products",
                "Create loyalty program with premium benefits",
                "Request reviews and referrals from these valuable customers"
            ],
            CustomerSegment.LOYAL: [
                "Maintain engagement with personalized offers",
                "Cross-sell complementary products",
                "Invite to beta test new features"
            ],
            CustomerSegment.POTENTIAL_LOYALIST: [
                "Send targeted promotions to increase purchase frequency",
                "Offer membership or subscription programs",
                "Provide excellent customer service to build loyalty"
            ],
            CustomerSegment.NEW_CUSTOMER: [
                "Send welcome series with product education",
                "Offer first-purchase incentives",
                "Monitor engagement closely in first 90 days"
            ],
            CustomerSegment.NEED_ATTENTION: [
                "Send re-engagement campaigns",
                "Offer limited-time discounts to reactivate",
                "Survey for feedback on service improvement"
            ],
            CustomerSegment.AT_RISK: [
                "Immediate intervention with personalized offers",
                "Reach out via phone or email for feedback",
                "Provide special incentives to prevent churn"
            ],
            CustomerSegment.CANNOT_LOSE: [
                "High-priority retention campaign",
                "Assign dedicated customer success manager",
                "Offer exclusive deals and premium support"
            ],
            CustomerSegment.LOST: [
                "Win-back campaign with significant incentives",
                "Survey to understand reasons for leaving",
                "Consider if customer is worth re-acquisition cost"
            ]
        }

        recommendations.extend(segment_recommendations.get(segment, []))

        # Behavior-based recommendations
        behavior_recommendations = {
            BehaviorType.BARGAIN_HUNTER: [
                "Send notifications about sales and discounts",
                "Offer price-match guarantees",
                "Create bundle deals for better value"
            ],
            BehaviorType.IMPULSE_BUYER: [
                "Use urgency tactics in marketing (limited time offers)",
                "Show related products prominently",
                "Send flash sale notifications"
            ],
            BehaviorType.LOYALIST: [
                "Reward brand loyalty with exclusive benefits",
                "Introduce new products from preferred brands",
                "Create brand ambassador opportunities"
            ]
        }

        recommendations.extend(behavior_recommendations.get(behavior, []))

        # Risk-based recommendations
        if churn_prob > 0.7:
            recommendations.append("🚨 High churn risk - immediate retention action needed")
        elif churn_prob > 0.4:
            recommendations.append("⚠️ Medium churn risk - monitor closely and engage proactively")

        # Value-based recommendations
        if clv > 10000000:  # High CLV
            recommendations.append("💎 High-value customer - provide white-glove service")

        return recommendations[:5]  # Limit to top 5 recommendations

    async def classify_customer(self, customer_id: str, analysis_period_days: int = 365) -> CustomerProfile:
        """Complete customer classification and analysis"""

        # Get RFM analysis
        rfm_data = await self.analyze_customer_rfm(customer_id, analysis_period_days)

        if not rfm_data:
            raise HTTPException(status_code=404, detail="Customer not found or no order history")

        customer_data = rfm_data[0]

        # Classify segment
        rfm_segment = self.classify_rfm_segment(
            customer_data["recency_score"],
            customer_data["frequency_score"],
            customer_data["monetary_score"],
            customer_data["is_new_customer"]
        )

        # Analyze behavior
        behavior_type = await self.analyze_customer_behavior(customer_id)

        # Predict CLV and churn
        clv_prediction = await self.predict_customer_clv(customer_data)
        churn_probability = await self.predict_churn_probability(customer_data)

        # Generate recommendations
        recommendations = self.generate_customer_recommendations(
            rfm_segment, behavior_type, clv_prediction, churn_probability
        )

        # Save classification to database
        await self.save_customer_classification(
            customer_id, rfm_segment, behavior_type, customer_data,
            clv_prediction, churn_probability
        )

        return CustomerProfile(
            customer_id=customer_id,
            rfm_segment=rfm_segment,
            behavior_type=behavior_type,
            recency_score=customer_data["recency_score"],
            frequency_score=customer_data["frequency_score"],
            monetary_score=customer_data["monetary_score"],
            clv_prediction=clv_prediction,
            churn_probability=churn_probability,
            recommended_actions=recommendations,
            last_analysis=datetime.now()
        )

    async def save_customer_classification(self, customer_id: str, segment: CustomerSegment,
                                         behavior: BehaviorType, rfm_data: Dict,
                                         clv: float, churn_prob: float):
        """Save classification results to database"""

        query = """
        INSERT INTO customer_classifications (
            customer_id, rfm_segment, behavior_type, recency_score, frequency_score,
            monetary_score, clv_prediction, churn_probability, analysis_date,
            rfm_data, last_updated
        ) VALUES (
            :customer_id, :segment, :behavior, :recency_score, :frequency_score,
            :monetary_score, :clv, :churn_prob, NOW(), :rfm_data, NOW()
        )
        ON CONFLICT (customer_id)
        DO UPDATE SET
            rfm_segment = EXCLUDED.rfm_segment,
            behavior_type = EXCLUDED.behavior_type,
            recency_score = EXCLUDED.recency_score,
            frequency_score = EXCLUDED.frequency_score,
            monetary_score = EXCLUDED.monetary_score,
            clv_prediction = EXCLUDED.clv_prediction,
            churn_probability = EXCLUDED.churn_probability,
            analysis_date = EXCLUDED.analysis_date,
            rfm_data = EXCLUDED.rfm_data,
            last_updated = NOW()
        """

        await self.db.execute(query, {
            "customer_id": customer_id,
            "segment": segment.value,
            "behavior": behavior.value,
            "recency_score": rfm_data["recency_score"],
            "frequency_score": rfm_data["frequency_score"],
            "monetary_score": rfm_data["monetary_score"],
            "clv": clv,
            "churn_prob": churn_prob,
            "rfm_data": json.dumps(rfm_data)
        })

    async def get_segment_analytics(self) -> List[SegmentAnalytics]:
        """Get analytics for all customer segments"""

        query = """
        SELECT
            cc.rfm_segment,
            COUNT(*) as customer_count,
            AVG(cc.clv_prediction) as avg_lifetime_value,
            AVG(o.avg_order_value) as avg_order_value,
            AVG(o.recency_days) as avg_recency_days,
            SUM(o.total_monetary) as total_revenue,
            (COUNT(*)::float / (SELECT COUNT(*) FROM customer_classifications) * 100) as segment_percentage
        FROM customer_classifications cc
        LEFT JOIN (
            SELECT
                customer_id,
                AVG(total_amount_vnd) as avg_order_value,
                DATE_PART('day', NOW() - MAX(order_date)) as recency_days,
                SUM(total_amount_vnd) as total_monetary
            FROM orders
            WHERE order_date >= NOW() - INTERVAL '365 days'
            GROUP BY customer_id
        ) o ON cc.customer_id = o.customer_id
        GROUP BY cc.rfm_segment
        ORDER BY customer_count DESC
        """

        results = await self.db.fetch_all(query)

        return [
            SegmentAnalytics(
                segment=CustomerSegment(row["rfm_segment"]),
                customer_count=row["customer_count"],
                avg_order_value=row["avg_order_value"] or 0,
                avg_lifetime_value=row["avg_lifetime_value"] or 0,
                avg_recency_days=row["avg_recency_days"] or 0,
                total_revenue=row["total_revenue"] or 0,
                segment_percentage=row["segment_percentage"] or 0
            )
            for row in results
        ]

    async def bulk_classify_customers(self, background_tasks: BackgroundTasks):
        """Classify all customers in background"""

        # Get all customers with recent activity
        customers = await self.db.fetch_all("""
            SELECT DISTINCT customer_id
            FROM orders
            WHERE order_date >= NOW() - INTERVAL '365 days'
            LIMIT 1000
        """)

        async def classify_batch():
            for customer in customers:
                try:
                    await self.classify_customer(customer["customer_id"])
                except Exception as e:
                    print(f"Error classifying customer {customer['customer_id']}: {e}")
                    continue

        background_tasks.add_task(classify_batch)

        return {"message": f"Started classification for {len(customers)} customers"}

# Global service instance
classification_service = CustomerClassificationService()

# ====================================
# ENDPOINTS
# ====================================

@classification_router.post("/classify/{customer_id}")
async def classify_customer(
    customer_id: str,
    analysis_period_days: int = 365,
    current_user: Any = Depends(get_current_active_user)
):
    """Classify individual customer"""
    return await classification_service.classify_customer(customer_id, analysis_period_days)

@classification_router.get("/segments/analytics")
async def get_segment_analytics(
    current_user: Any = Depends(get_current_active_user)
):
    """Get analytics for all customer segments"""
    return await classification_service.get_segment_analytics()

@classification_router.post("/bulk-classify")
async def bulk_classify_customers(
    background_tasks: BackgroundTasks,
    current_user: Any = Depends(get_current_active_user)
):
    """Start bulk classification of all customers"""
    return await classification_service.bulk_classify_customers(background_tasks)

@classification_router.get("/customer/{customer_id}/profile")
async def get_customer_profile(
    customer_id: str,
    current_user: Any = Depends(get_current_active_user)
):
    """Get existing customer classification profile"""

    result = await classification_service.db.fetch_one("""
        SELECT * FROM customer_classifications
        WHERE customer_id = :customer_id
    """, {"customer_id": customer_id})

    if not result:
        # If no classification exists, perform it now
        return await classification_service.classify_customer(customer_id)

    return dict(result)

@classification_router.get("/segments/{segment}/customers")
async def get_customers_by_segment(
    segment: CustomerSegment,
    limit: int = 50,
    offset: int = 0,
    current_user: Any = Depends(get_current_active_user)
):
    """Get customers in specific segment"""

    customers = await classification_service.db.fetch_all("""
        SELECT cc.*, c.customer_name, c.email, c.registration_date
        FROM customer_classifications cc
        LEFT JOIN customers c ON cc.customer_id = c.customer_id
        WHERE cc.rfm_segment = :segment
        ORDER BY cc.clv_prediction DESC
        LIMIT :limit OFFSET :offset
    """, {"segment": segment.value, "limit": limit, "offset": offset})

    return {"customers": [dict(row) for row in customers]}

@classification_router.get("/high-risk-customers")
async def get_high_risk_customers(
    churn_threshold: float = 0.6,
    limit: int = 100,
    current_user: Any = Depends(get_current_active_user)
):
    """Get customers with high churn risk"""

    customers = await classification_service.db.fetch_all("""
        SELECT cc.*, c.customer_name, c.email
        FROM customer_classifications cc
        LEFT JOIN customers c ON cc.customer_id = c.customer_id
        WHERE cc.churn_probability >= :threshold
        ORDER BY cc.churn_probability DESC, cc.clv_prediction DESC
        LIMIT :limit
    """, {"threshold": churn_threshold, "limit": limit})

    return {"high_risk_customers": [dict(row) for row in customers]}