#!/usr/bin/env python3
"""
Feedback Analysis Dashboard with Sentiment Analysis
==================================================
Comprehensive feedback processing, sentiment analysis, and insights dashboard
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json
import uuid
import re
import statistics
from collections import Counter, defaultdict

# NLP Libraries
import nltk
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

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

class SentimentScore(str, Enum):
    VERY_POSITIVE = "very_positive"    # 0.8 - 1.0
    POSITIVE = "positive"              # 0.3 - 0.8
    NEUTRAL = "neutral"                # -0.3 - 0.3
    NEGATIVE = "negative"              # -0.8 - -0.3
    VERY_NEGATIVE = "very_negative"    # -1.0 - -0.8

class FeedbackCategory(str, Enum):
    PRODUCT_QUALITY = "product_quality"
    SHIPPING = "shipping"
    CUSTOMER_SERVICE = "customer_service"
    PRICING = "pricing"
    WEBSITE_UX = "website_ux"
    GENERAL = "general"

class FeedbackPriority(str, Enum):
    CRITICAL = "critical"      # Very negative + high-value customer
    HIGH = "high"             # Negative sentiment + specific issues
    MEDIUM = "medium"         # Mixed sentiment
    LOW = "low"              # Positive sentiment

class FeedbackSource(str, Enum):
    PRODUCT_REVIEW = "product_review"
    ORDER_FEEDBACK = "order_feedback"
    CUSTOMER_SUPPORT = "customer_support"
    SOCIAL_MEDIA = "social_media"
    SURVEY = "survey"
    EMAIL = "email"

class FeedbackAnalysisRequest(BaseModel):
    text: str = Field(..., min_length=5, max_length=5000)
    source: FeedbackSource = FeedbackSource.GENERAL
    customer_id: Optional[str] = None
    product_id: Optional[str] = None
    order_id: Optional[str] = None

class FeedbackResponse(BaseModel):
    feedback_id: str
    customer_id: Optional[str]
    product_id: Optional[str]
    order_id: Optional[str]
    original_text: str
    sentiment_score: float
    sentiment_label: SentimentScore
    confidence: float
    categories: List[FeedbackCategory]
    priority: FeedbackPriority
    key_phrases: List[str]
    emotions: Dict[str, float]
    action_required: bool
    recommended_response: str
    created_at: datetime

class SentimentAnalytics(BaseModel):
    period: str
    total_feedback: int
    sentiment_distribution: Dict[str, int]
    avg_sentiment_score: float
    trending_topics: List[Dict[str, Any]]
    category_breakdown: Dict[str, Dict[str, Any]]
    priority_distribution: Dict[str, int]

# ====================================
# ROUTER SETUP
# ====================================

feedback_router = APIRouter(prefix="/api/v1/feedback", tags=["Feedback Analysis"])

# ====================================
# FEEDBACK ANALYSIS SERVICE
# ====================================

class FeedbackAnalysisService:
    def __init__(self):
        self.db = None  # Will be injected
        self.vader_analyzer = SentimentIntensityAnalyzer()

        # Category keywords (Vietnamese + English)
        self.category_keywords = {
            FeedbackCategory.PRODUCT_QUALITY: {
                'vietnamese': ['chất lượng', 'sản phẩm', 'hàng', 'tốt', 'xấu', 'đẹp', 'kém', 'tệ', 'tuyệt vời'],
                'english': ['quality', 'product', 'item', 'good', 'bad', 'excellent', 'poor', 'terrible', 'amazing']
            },
            FeedbackCategory.SHIPPING: {
                'vietnamese': ['giao hàng', 'vận chuyển', 'ship', 'nhanh', 'chậm', 'đúng hẹn', 'trễ'],
                'english': ['shipping', 'delivery', 'fast', 'slow', 'on time', 'late', 'quick']
            },
            FeedbackCategory.CUSTOMER_SERVICE: {
                'vietnamese': ['nhân viên', 'phục vụ', 'tư vấn', 'hỗ trợ', 'thái độ', 'nhiệt tình'],
                'english': ['service', 'support', 'staff', 'help', 'attitude', 'friendly', 'rude']
            },
            FeedbackCategory.PRICING: {
                'vietnamese': ['giá', 'tiền', 'đắt', 'rẻ', 'phù hợp', 'hợp lý', 'khuyến mãi'],
                'english': ['price', 'cost', 'expensive', 'cheap', 'reasonable', 'discount', 'promotion']
            },
            FeedbackCategory.WEBSITE_UX: {
                'vietnamese': ['website', 'web', 'app', 'giao diện', 'dễ dùng', 'khó dùng', 'lỗi'],
                'english': ['website', 'app', 'interface', 'easy', 'difficult', 'bug', 'error', 'user-friendly']
            }
        }

        # Emotion keywords
        self.emotion_keywords = {
            'joy': ['happy', 'pleased', 'satisfied', 'hài lòng', 'vui', 'tuyệt vời'],
            'anger': ['angry', 'frustrated', 'annoyed', 'tức giận', 'bực mình', 'khó chịu'],
            'sadness': ['disappointed', 'sad', 'upset', 'thất vọng', 'buồn', 'tiếc'],
            'fear': ['worried', 'concerned', 'anxious', 'lo lắng', 'sợ', 'băn khoăn'],
            'surprise': ['surprised', 'amazed', 'shocked', 'ngạc nhiên', 'bất ngờ']
        }

    async def analyze_sentiment(self, text: str) -> Tuple[float, SentimentScore, float]:
        """Analyze sentiment using multiple methods"""

        # Clean text
        cleaned_text = self.clean_text(text)

        # VADER Sentiment Analysis
        vader_scores = self.vader_analyzer.polarity_scores(cleaned_text)
        vader_compound = vader_scores['compound']

        # TextBlob Analysis
        blob = TextBlob(cleaned_text)
        textblob_polarity = blob.sentiment.polarity

        # Combine scores (weighted average)
        combined_score = (vader_compound * 0.6) + (textblob_polarity * 0.4)

        # Determine sentiment label
        if combined_score >= 0.8:
            sentiment_label = SentimentScore.VERY_POSITIVE
        elif combined_score >= 0.3:
            sentiment_label = SentimentScore.POSITIVE
        elif combined_score >= -0.3:
            sentiment_label = SentimentScore.NEUTRAL
        elif combined_score >= -0.8:
            sentiment_label = SentimentScore.NEGATIVE
        else:
            sentiment_label = SentimentScore.VERY_NEGATIVE

        # Calculate confidence
        confidence = abs(combined_score)

        return combined_score, sentiment_label, confidence

    def clean_text(self, text: str) -> str:
        """Clean and preprocess text"""
        # Remove special characters but keep Vietnamese
        text = re.sub(r'[^\w\s\u00C0-\u017F\u1EA0-\u1EF9]', ' ', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text.lower()

    async def categorize_feedback(self, text: str) -> List[FeedbackCategory]:
        """Categorize feedback based on keywords"""
        categories = []
        cleaned_text = self.clean_text(text)

        for category, keywords in self.category_keywords.items():
            # Check Vietnamese and English keywords
            all_keywords = keywords['vietnamese'] + keywords['english']

            # Count keyword matches
            matches = sum(1 for keyword in all_keywords if keyword in cleaned_text)

            # If 2+ keywords match, include category
            if matches >= 2:
                categories.append(category)

        # Default to general if no specific category found
        if not categories:
            categories.append(FeedbackCategory.GENERAL)

        return categories

    async def extract_key_phrases(self, text: str) -> List[str]:
        """Extract key phrases from text"""
        blob = TextBlob(text)

        # Extract noun phrases
        noun_phrases = list(blob.noun_phrases)

        # Filter and clean phrases
        key_phrases = []
        for phrase in noun_phrases:
            if len(phrase.split()) >= 2 and len(phrase) <= 50:
                key_phrases.append(phrase)

        # Return top 5 most relevant phrases
        return key_phrases[:5]

    async def analyze_emotions(self, text: str) -> Dict[str, float]:
        """Analyze emotions in text"""
        emotions = {emotion: 0.0 for emotion in self.emotion_keywords.keys()}
        cleaned_text = self.clean_text(text)

        for emotion, keywords in self.emotion_keywords.items():
            # Count matches and calculate score
            matches = sum(1 for keyword in keywords if keyword in cleaned_text)
            emotions[emotion] = min(matches / 3.0, 1.0)  # Normalize to 0-1

        return emotions

    async def determine_priority(self, sentiment_score: float, sentiment_label: SentimentScore,
                               customer_id: str = None) -> FeedbackPriority:
        """Determine feedback priority"""

        # Check if customer is high-value
        is_high_value_customer = False
        if customer_id:
            customer_value = await self.db.fetch_val("""
                SELECT clv_prediction FROM customer_classifications
                WHERE customer_id = :customer_id
            """, {"customer_id": customer_id})

            is_high_value_customer = (customer_value or 0) > 5000000  # >5M VND CLV

        # Determine priority
        if sentiment_label in [SentimentScore.VERY_NEGATIVE, SentimentScore.NEGATIVE]:
            if is_high_value_customer:
                return FeedbackPriority.CRITICAL
            else:
                return FeedbackPriority.HIGH
        elif sentiment_label == SentimentScore.NEUTRAL:
            return FeedbackPriority.MEDIUM
        else:
            return FeedbackPriority.LOW

    async def generate_recommended_response(self, sentiment_label: SentimentScore,
                                          categories: List[FeedbackCategory],
                                          priority: FeedbackPriority) -> str:
        """Generate recommended response based on analysis"""

        responses = {
            SentimentScore.VERY_POSITIVE: "Cảm ơn bạn đã chia sẻ trải nghiệm tuyệt vời! Chúng tôi rất vui khi bạn hài lòng với dịch vụ.",
            SentimentScore.POSITIVE: "Cảm ơn bạn đã đánh giá tích cực! Chúng tôi sẽ tiếp tục cải thiện để phục vụ bạn tốt hơn.",
            SentimentScore.NEUTRAL: "Cảm ơn bạn đã phản hồi. Chúng tôi sẽ xem xét và cải thiện dịch vụ.",
            SentimentScore.NEGATIVE: "Chúng tôi xin lỗi về trải nghiệm không tốt. Vui lòng liên hệ để chúng tôi hỗ trợ giải quyết.",
            SentimentScore.VERY_NEGATIVE: "Chúng tôi rất xin lỗi về vấn đề này. Chúng tôi sẽ liên hệ trực tiếp để khắc phục ngay lập tức."
        }

        base_response = responses.get(sentiment_label, "Cảm ơn bạn đã phản hồi.")

        # Add category-specific response
        if FeedbackCategory.SHIPPING in categories:
            base_response += " Chúng tôi sẽ cải thiện quy trình giao hàng."
        elif FeedbackCategory.PRODUCT_QUALITY in categories:
            base_response += " Chúng tôi sẽ kiểm tra và cải thiện chất lượng sản phẩm."
        elif FeedbackCategory.CUSTOMER_SERVICE in categories:
            base_response += " Chúng tôi sẽ đào tạo nhân viên để phục vụ tốt hơn."

        return base_response

    async def process_feedback(self, feedback_request: FeedbackAnalysisRequest) -> FeedbackResponse:
        """Process and analyze feedback comprehensively"""

        feedback_id = str(uuid.uuid4())
        text = feedback_request.text

        # Analyze sentiment
        sentiment_score, sentiment_label, confidence = await self.analyze_sentiment(text)

        # Categorize feedback
        categories = await self.categorize_feedback(text)

        # Extract key phrases
        key_phrases = await self.extract_key_phrases(text)

        # Analyze emotions
        emotions = await self.analyze_emotions(text)

        # Determine priority
        priority = await self.determine_priority(
            sentiment_score, sentiment_label, feedback_request.customer_id
        )

        # Generate recommended response
        recommended_response = await self.generate_recommended_response(
            sentiment_label, categories, priority
        )

        # Determine if action is required
        action_required = sentiment_label in [SentimentScore.NEGATIVE, SentimentScore.VERY_NEGATIVE]

        # Save to database
        await self.save_feedback_analysis(
            feedback_id, feedback_request, sentiment_score, sentiment_label,
            confidence, categories, priority, key_phrases, emotions,
            action_required, recommended_response
        )

        return FeedbackResponse(
            feedback_id=feedback_id,
            customer_id=feedback_request.customer_id,
            product_id=feedback_request.product_id,
            order_id=feedback_request.order_id,
            original_text=text,
            sentiment_score=sentiment_score,
            sentiment_label=sentiment_label,
            confidence=confidence,
            categories=categories,
            priority=priority,
            key_phrases=key_phrases,
            emotions=emotions,
            action_required=action_required,
            recommended_response=recommended_response,
            created_at=datetime.now()
        )

    async def save_feedback_analysis(self, feedback_id: str, request: FeedbackAnalysisRequest,
                                   sentiment_score: float, sentiment_label: SentimentScore,
                                   confidence: float, categories: List[FeedbackCategory],
                                   priority: FeedbackPriority, key_phrases: List[str],
                                   emotions: Dict[str, float], action_required: bool,
                                   recommended_response: str):
        """Save feedback analysis to database"""

        query = """
        INSERT INTO feedback_analysis (
            feedback_id, customer_id, product_id, order_id, source, original_text,
            sentiment_score, sentiment_label, confidence, categories, priority,
            key_phrases, emotions, action_required, recommended_response, created_at
        ) VALUES (
            :feedback_id, :customer_id, :product_id, :order_id, :source, :original_text,
            :sentiment_score, :sentiment_label, :confidence, :categories, :priority,
            :key_phrases, :emotions, :action_required, :recommended_response, NOW()
        )
        """

        await self.db.execute(query, {
            "feedback_id": feedback_id,
            "customer_id": request.customer_id,
            "product_id": request.product_id,
            "order_id": request.order_id,
            "source": request.source.value,
            "original_text": request.text,
            "sentiment_score": sentiment_score,
            "sentiment_label": sentiment_label.value,
            "confidence": confidence,
            "categories": json.dumps([cat.value for cat in categories]),
            "priority": priority.value,
            "key_phrases": json.dumps(key_phrases),
            "emotions": json.dumps(emotions),
            "action_required": action_required,
            "recommended_response": recommended_response
        })

    async def get_sentiment_analytics(self, days: int = 30) -> SentimentAnalytics:
        """Get sentiment analytics for dashboard"""

        # Basic metrics
        total_feedback = await self.db.fetch_val("""
            SELECT COUNT(*) FROM feedback_analysis
            WHERE created_at >= NOW() - INTERVAL '%s days'
        """, (days,))

        # Sentiment distribution
        sentiment_dist = await self.db.fetch_all("""
            SELECT sentiment_label, COUNT(*) as count
            FROM feedback_analysis
            WHERE created_at >= NOW() - INTERVAL '%s days'
            GROUP BY sentiment_label
        """, (days,))

        sentiment_distribution = {row["sentiment_label"]: row["count"] for row in sentiment_dist}

        # Average sentiment score
        avg_sentiment = await self.db.fetch_val("""
            SELECT AVG(sentiment_score) FROM feedback_analysis
            WHERE created_at >= NOW() - INTERVAL '%s days'
        """, (days,)) or 0.0

        # Trending topics (most common key phrases)
        trending = await self.db.fetch_all("""
            SELECT phrase, COUNT(*) as frequency
            FROM (
                SELECT jsonb_array_elements_text(key_phrases) as phrase
                FROM feedback_analysis
                WHERE created_at >= NOW() - INTERVAL '%s days'
            ) phrases
            GROUP BY phrase
            ORDER BY frequency DESC
            LIMIT 10
        """, (days,))

        trending_topics = [{"topic": row["phrase"], "frequency": row["frequency"]} for row in trending]

        # Category breakdown
        category_data = await self.db.fetch_all("""
            SELECT category, sentiment_label, COUNT(*) as count, AVG(sentiment_score) as avg_score
            FROM (
                SELECT jsonb_array_elements_text(categories) as category, sentiment_label, sentiment_score
                FROM feedback_analysis
                WHERE created_at >= NOW() - INTERVAL '%s days'
            ) cat_data
            GROUP BY category, sentiment_label
        """, (days,))

        category_breakdown = defaultdict(lambda: {"positive": 0, "negative": 0, "neutral": 0, "avg_score": 0})
        for row in category_data:
            category = row["category"]
            sentiment = "positive" if row["sentiment_label"] in ["positive", "very_positive"] else \
                      "negative" if row["sentiment_label"] in ["negative", "very_negative"] else "neutral"
            category_breakdown[category][sentiment] += row["count"]
            category_breakdown[category]["avg_score"] = row["avg_score"]

        # Priority distribution
        priority_dist = await self.db.fetch_all("""
            SELECT priority, COUNT(*) as count
            FROM feedback_analysis
            WHERE created_at >= NOW() - INTERVAL '%s days'
            GROUP BY priority
        """, (days,))

        priority_distribution = {row["priority"]: row["count"] for row in priority_dist}

        return SentimentAnalytics(
            period=f"Last {days} days",
            total_feedback=total_feedback or 0,
            sentiment_distribution=sentiment_distribution,
            avg_sentiment_score=round(avg_sentiment, 2),
            trending_topics=trending_topics,
            category_breakdown=dict(category_breakdown),
            priority_distribution=priority_distribution
        )

    async def get_action_required_feedback(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get feedback that requires immediate action"""

        feedback = await self.db.fetch_all("""
            SELECT fa.*, c.customer_name, c.email
            FROM feedback_analysis fa
            LEFT JOIN customers c ON fa.customer_id = c.customer_id
            WHERE fa.action_required = TRUE
            AND fa.created_at >= NOW() - INTERVAL '7 days'
            ORDER BY fa.priority DESC, fa.created_at DESC
            LIMIT :limit
        """, {"limit": limit})

        return [dict(row) for row in feedback]

# Global service instance
feedback_service = FeedbackAnalysisService()

# ====================================
# ENDPOINTS
# ====================================

@feedback_router.post("/analyze", response_model=FeedbackResponse)
async def analyze_feedback(feedback_request: FeedbackAnalysisRequest):
    """Analyze feedback text with sentiment analysis"""
    return await feedback_service.process_feedback(feedback_request)

@feedback_router.get("/analytics")
async def get_sentiment_analytics(
    days: int = 30,
    current_user: Any = Depends(get_current_active_user)
):
    """Get sentiment analytics for dashboard"""
    return await feedback_service.get_sentiment_analytics(days)

@feedback_router.get("/action-required")
async def get_action_required_feedback(
    limit: int = 50,
    current_user: Any = Depends(get_current_active_user)
):
    """Get feedback that requires immediate action"""
    return await feedback_service.get_action_required_feedback(limit)

@feedback_router.get("/trends")
async def get_sentiment_trends(
    days: int = 30,
    current_user: Any = Depends(get_current_active_user)
):
    """Get sentiment trends over time"""

    trends = await feedback_service.db.fetch_all("""
        SELECT
            DATE(created_at) as date,
            AVG(sentiment_score) as avg_sentiment,
            COUNT(*) as total_feedback,
            COUNT(CASE WHEN sentiment_label IN ('positive', 'very_positive') THEN 1 END) as positive_count,
            COUNT(CASE WHEN sentiment_label IN ('negative', 'very_negative') THEN 1 END) as negative_count
        FROM feedback_analysis
        WHERE created_at >= NOW() - INTERVAL '%s days'
        GROUP BY DATE(created_at)
        ORDER BY date
    """, (days,))

    return {"trends": [dict(row) for row in trends]}

@feedback_router.get("/product/{product_id}/sentiment")
async def get_product_sentiment(
    product_id: str,
    current_user: Any = Depends(get_current_active_user)
):
    """Get sentiment analysis for specific product"""

    product_feedback = await feedback_service.db.fetch_all("""
        SELECT
            sentiment_label,
            sentiment_score,
            original_text,
            created_at,
            key_phrases
        FROM feedback_analysis
        WHERE product_id = :product_id
        ORDER BY created_at DESC
        LIMIT 100
    """, {"product_id": product_id})

    if not product_feedback:
        raise HTTPException(status_code=404, detail="No feedback found for this product")

    # Calculate summary
    scores = [row["sentiment_score"] for row in product_feedback]
    avg_sentiment = statistics.mean(scores)

    sentiment_counts = Counter(row["sentiment_label"] for row in product_feedback)

    return {
        "product_id": product_id,
        "total_feedback": len(product_feedback),
        "avg_sentiment_score": round(avg_sentiment, 2),
        "sentiment_distribution": dict(sentiment_counts),
        "recent_feedback": [dict(row) for row in product_feedback[:10]]
    }

@feedback_router.get("/dashboard-data")
async def get_dashboard_data(
    current_user: Any = Depends(get_current_active_user)
):
    """Get comprehensive dashboard data"""

    # Run multiple queries in parallel
    analytics_30d = await feedback_service.get_sentiment_analytics(30)
    analytics_7d = await feedback_service.get_sentiment_analytics(7)
    action_required = await feedback_service.get_action_required_feedback(10)

    # Recent feedback samples
    recent_feedback = await feedback_service.db.fetch_all("""
        SELECT * FROM feedback_analysis
        ORDER BY created_at DESC
        LIMIT 20
    """)

    return {
        "monthly_analytics": analytics_30d,
        "weekly_analytics": analytics_7d,
        "action_required": action_required,
        "recent_feedback": [dict(row) for row in recent_feedback]
    }