"""
Complete ML Models API
Sentiment Analysis + Price Optimization + Product Clustering
"""

from pathlib import Path
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import pickle
import psycopg2
from psycopg2.extras import Json
import yaml
import os

logger = logging.getLogger(__name__)

# ====================================
# REQUEST/RESPONSE MODELS
# ====================================

class SentimentAnalysisRequest(BaseModel):
    """Sentiment analysis request"""
    review_text: str = Field(..., min_length=10, description="Review text to analyze")
    rating: Optional[float] = Field(default=3.0, ge=1, le=5, description="Product rating")
    product_id: Optional[int] = Field(None, description="Product ID for context")

class SentimentAnalysisResponse(BaseModel):
    """Sentiment analysis response"""
    sentiment: str  # positive, negative, neutral
    confidence_score: float
    emotions: Dict[str, float]
    processed_at: datetime

class BatchSentimentRequest(BaseModel):
    """Batch sentiment analysis"""
    reviews: List[SentimentAnalysisRequest]

class BatchSentimentResponse(BaseModel):
    """Batch sentiment response"""
    total_processed: int
    successful: int
    results: List[SentimentAnalysisResponse]

class PricePredictionRequest(BaseModel):
    """Price optimization request"""
    product_id: int = Field(..., description="Product ID")
    current_price: float = Field(..., gt=0, description="Current price")
    avg_rating: float = Field(default=3.0, ge=1, le=5)
    review_count: int = Field(default=0, ge=0)
    sales_velocity: float = Field(default=0, ge=0)
    discount_pct: float = Field(default=0, ge=0, le=100)
    competitor_price: Optional[float] = Field(None, gt=0)

class PricePredictionResponse(BaseModel):
    """Price optimization response"""
    product_id: int
    current_price: float
    predicted_optimal_price: float
    price_change_percent: float
    confidence_score: float
    confidence_interval: Dict[str, float]
    recommendation: str
    prediction_at: datetime

class ProductClusterRequest(BaseModel):
    """Product clustering request"""
    product_id: int = Field(..., description="Product ID")
    price: float = Field(..., gt=0)
    rating: float = Field(default=3.0, ge=1, le=5)
    review_count: int = Field(default=0, ge=0)
    sales_velocity: float = Field(default=0, ge=0)

class ProductClusterResponse(BaseModel):
    """Product cluster response"""
    product_id: int
    cluster_id: int
    cluster_name: str
    segment_strategy: Optional[Dict[str, Any]]
    similar_products_count: int
    predicted_at: datetime

class ModelStatusResponse(BaseModel):
    """Model status response"""
    model_name: str
    model_type: str
    version: str
    status: str
    last_trained: datetime
    metrics: Dict[str, Any]

# ====================================
# ML MODELS API ROUTER
# ====================================

ml_models_router = APIRouter(prefix="/api/v1/ml-models", tags=["ML Models"])

# Database connection helper
def get_db_connection():
    """Get database connection"""
    config_path = Path(__file__).resolve().parents[2] / "ml" / "config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME', 'ecommerce_dss'),
        user=os.getenv('DB_USER', 'dss_user'),
        password=os.getenv('DB_PASSWORD', 'dss_password_123')
    )

def load_model_from_db(model_name: str):
    """Load model from database"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
        SELECT model_binary, performance_metrics, version
        FROM ml.models_storage
        WHERE model_name = %s AND status = 'active'
        ORDER BY created_at DESC
        LIMIT 1
        """

        cursor.execute(query, (model_name,))
        result = cursor.fetchone()

        if not result:
            raise ValueError(f"Model '{model_name}' not found")

        model_binary, metrics, version = result
        model = pickle.loads(model_binary)

        return model, metrics or {}

    finally:
        cursor.close()
        conn.close()

def load_artifact(model_type: str, artifact_name: str):
    """Load pickle artifact"""
    path = f'models/ml-models/{model_type}/{artifact_name}.pkl'
    if os.path.exists(path):
        with open(path, 'rb') as f:
            return pickle.load(f)
    return None

# ====================================
# SENTIMENT ANALYSIS ENDPOINTS
# ====================================

@ml_models_router.post("/sentiment/analyze", response_model=SentimentAnalysisResponse)
async def analyze_sentiment(request: SentimentAnalysisRequest) -> Dict[str, Any]:
    """
    Analyze sentiment of a review

    Returns: sentiment (positive/negative/neutral), confidence score, emotion breakdown
    """
    try:
        # Load model components
        model = load_model_from_db('sentiment_classifier_v1.0')
        vectorizer = load_artifact('sentiment', 'vectorizer')
        label_encoder = load_artifact('sentiment', 'label_encoder')

        if not all([model, vectorizer, label_encoder]):
            raise HTTPException(status_code=503, detail="Models not loaded")

        # Preprocess
        text_clean = request.review_text.lower()
        text_clean = ''.join(c for c in text_clean if c.isalnum() or c.isspace())

        # TF-IDF vectorization
        X_tfidf = vectorizer.transform([text_clean]).toarray()

        # Features
        text_length = len(request.review_text)
        word_count = len(request.review_text.split())
        exclamation_count = request.review_text.count('!')
        question_count = request.review_text.count('?')
        uppercase_ratio = sum(1 for c in request.review_text if c.isupper()) / (len(request.review_text) + 1)

        import numpy as np
        X_features = np.array([[
            request.rating, text_length, word_count,
            exclamation_count, question_count, uppercase_ratio
        ]])
        X_combined = np.hstack([X_tfidf, X_features])

        # Predict
        prediction = model.predict(X_combined)[0]
        probabilities = model.predict_proba(X_combined)[0]

        sentiment = label_encoder.inverse_transform([prediction])[0]

        return {
            "sentiment": sentiment,
            "confidence_score": float(probabilities[prediction]),
            "emotions": {
                label: float(prob)
                for label, prob in zip(label_encoder.classes_, probabilities)
            },
            "processed_at": datetime.now()
        }

    except Exception as e:
        logger.error(f"Sentiment analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@ml_models_router.post("/sentiment/batch", response_model=BatchSentimentResponse)
async def batch_sentiment_analysis(request: BatchSentimentRequest) -> Dict[str, Any]:
    """
    Batch sentiment analysis for multiple reviews
    """
    if len(request.reviews) > 100:
        raise HTTPException(status_code=400, detail="Batch size too large (max 100)")

    try:
        results = []
        successful = 0

        for review in request.reviews:
            try:
                # Call single analysis
                result = await analyze_sentiment(review)
                results.append(result)
                successful += 1
            except Exception as e:
                logger.warning(f"Failed to analyze review: {e}")

        return {
            "total_processed": len(request.reviews),
            "successful": successful,
            "results": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch analysis failed: {str(e)}")

# ====================================
# PRICE OPTIMIZATION ENDPOINTS
# ====================================

@ml_models_router.post("/price/predict", response_model=PricePredictionResponse)
async def predict_optimal_price(request: PricePredictionRequest) -> Dict[str, Any]:
    """
    Predict optimal price for a product

    Uses regression model to recommend price optimization
    """
    try:
        # Load model and scaler
        model = load_model_from_db('price_optimizer_v1.0')
        scaler = load_artifact('price', 'scaler')

        if not all([model, scaler]):
            raise HTTPException(status_code=503, detail="Price model not loaded")

        import numpy as np

        # Handle competitor price
        competitor_price = request.competitor_price or request.current_price * 1.05

        # Feature engineering (same as training)
        features = np.array([[
            np.log1p(request.current_price),
            request.avg_rating / 5.0,
            np.log1p(request.sales_velocity),
            request.current_price / (request.avg_rating + 1),
            (request.current_price - competitor_price) / (competitor_price + 1),
            request.review_count / 1000.0,
            request.discount_pct / 100.0,
            request.sales_velocity / 100.0
        ]])

        features_scaled = scaler.transform(features)
        predicted_price = float(model.predict(features_scaled)[0])

        price_change_percent = ((predicted_price - request.current_price) / request.current_price) * 100

        # Confidence calculation
        confidence = min(0.95, 0.5 + (abs(request.avg_rating - 3) / 2) * 0.1 + 0.2)

        # Confidence interval
        margin = request.current_price * 0.1
        lower_bound = max(request.current_price * 0.5, predicted_price - margin)
        upper_bound = predicted_price + margin

        # Recommendation
        if price_change_percent > 5:
            recommendation = "Increase Price"
        elif price_change_percent < -5:
            recommendation = "Decrease Price"
        else:
            recommendation = "Maintain Current Price"

        return {
            "product_id": request.product_id,
            "current_price": request.current_price,
            "predicted_optimal_price": predicted_price,
            "price_change_percent": float(price_change_percent),
            "confidence_score": float(confidence),
            "confidence_interval": {
                "lower": float(lower_bound),
                "upper": float(upper_bound)
            },
            "recommendation": recommendation,
            "prediction_at": datetime.now()
        }

    except Exception as e:
        logger.error(f"Price prediction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Price prediction failed: {str(e)}")

# ====================================
# PRODUCT CLUSTERING ENDPOINTS
# ====================================

@ml_models_router.post("/cluster/assign", response_model=ProductClusterResponse)
async def assign_product_cluster(request: ProductClusterRequest) -> Dict[str, Any]:
    """
    Assign product to cluster based on features

    Returns cluster ID, segment strategy, and similar products
    """
    try:
        # Load model and scaler
        model = load_model_from_db('product_clustering_v1.0')
        scaler = load_artifact('clustering', 'scaler')

        if not all([model, scaler]):
            raise HTTPException(status_code=503, detail="Clustering model not loaded")

        import numpy as np

        # Feature engineering
        features = np.array([[
            np.log1p(request.price),
            request.rating / 5.0,
            np.log1p(request.sales_velocity),
            np.log1p(0),  # engagement
            0,  # engagement_rate
            0,  # sales_per_review
            (request.rating / 5.0) * 0.6  # quality_score
        ]])

        features_scaled = scaler.transform(features)
        cluster_id = int(model.predict(features_scaled)[0])

        # Get cluster info from database
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            query = """
            SELECT cluster_name, COUNT(*) as product_count
            FROM ml.product_clusters
            WHERE cluster_number = %s
            GROUP BY cluster_name
            """

            cursor.execute(query, (cluster_id,))
            result = cursor.fetchone()

            cluster_name = result[0] if result else f"Cluster {cluster_id}"
            similar_products = result[1] if result else 0

        finally:
            cursor.close()
            conn.close()

        return {
            "product_id": request.product_id,
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "segment_strategy": {
                "target_segment": cluster_name,
                "product_count_in_segment": similar_products,
                "recommendation": f"Marketing strategy for {cluster_name}"
            },
            "similar_products_count": similar_products,
            "predicted_at": datetime.now()
        }

    except Exception as e:
        logger.error(f"Clustering failed: {e}")
        raise HTTPException(status_code=500, detail=f"Clustering failed: {str(e)}")

# ====================================
# MODEL MANAGEMENT ENDPOINTS
# ====================================

@ml_models_router.get("/models/status")
async def get_models_status() -> Dict[str, Any]:
    """
    Get status of all ML models

    Returns: Model names, versions, status, and metrics
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            query = """
            SELECT model_name, model_type, version, status, performance_metrics, created_at
            FROM ml.models_storage
            WHERE status = 'active'
            ORDER BY model_name
            """

            cursor.execute(query)
            results = cursor.fetchall()

            models = []
            for result in results:
                models.append({
                    "model_name": result[0],
                    "model_type": result[1],
                    "version": result[2],
                    "status": result[3],
                    "metrics": result[4] or {},
                    "last_trained": result[5].isoformat() if result[5] else None
                })

            return {
                "success": True,
                "total_models": len(models),
                "models": models,
                "timestamp": datetime.now().isoformat()
            }

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to get model status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get model status")

@ml_models_router.get("/models/{model_name}/info")
async def get_model_details(model_name: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific model
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            query = """
            SELECT model_id, model_name, model_type, version, performance_metrics,
                   hyperparameters, training_data_config, created_at
            FROM ml.models_storage
            WHERE model_name = %s
            ORDER BY created_at DESC
            LIMIT 1
            """

            cursor.execute(query, (model_name,))
            result = cursor.fetchone()

            if not result:
                raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found")

            return {
                "success": True,
                "model_id": result[0],
                "model_name": result[1],
                "model_type": result[2],
                "version": result[3],
                "performance_metrics": result[4] or {},
                "hyperparameters": result[5] or {},
                "training_config": result[6] or {},
                "trained_at": result[7].isoformat() if result[7] else None
            }

        finally:
            cursor.close()
            conn.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get model details: {e}")
        raise HTTPException(status_code=500, detail="Failed to get model details")

@ml_models_router.post("/models/reload")
async def reload_all_models() -> Dict[str, Any]:
    """
    Reload all models from database
    """
    try:
        # This would reload models in memory
        # For now, just verify they exist

        model_names = [
            'sentiment_classifier_v1.0',
            'price_optimizer_v1.0',
            'product_clustering_v1.0'
        ]

        loaded = []
        for model_name in model_names:
            try:
                load_model_from_db(model_name)
                loaded.append(model_name)
            except:
                pass

        return {
            "success": True,
            "models_loaded": len(loaded),
            "models": loaded,
            "reload_time": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reload failed: {str(e)}")

# Export router
__all__ = ['ml_models_router']
