# -*- coding: utf-8 -*-
"""
ML API Router - Integrated with actual ML pipeline
Provides endpoints for demand prediction and product recommendations
"""

import sys
import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import joblib
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel

# Try to import database
try:
    from databases import Database
    from sqlalchemy import text
    ASYNC_DB_AVAILABLE = True
except ImportError:
    ASYNC_DB_AVAILABLE = False

# Setup logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ml", tags=["ML Models"])

# ====================================
# CONFIGURATION & SETUP
# ====================================

# Get ML folder path (from backend/app/api/v1 -> project root)
# backend/app/api/v1 -> parent: backend/app/api/v1
#                    -> parent: backend/app/api
#                    -> parent: backend/app
#                    -> parent: backend
#                    -> parent: project_root
ml_folder = Path(__file__).resolve().parent.parent.parent.parent.parent / "ml"
models_dir = ml_folder / "models" / "ml-models"
data_dir = ml_folder / "data"

# Debug logging
logger.info(f"ML Folder Path: {ml_folder}")
logger.info(f"Models Dir: {models_dir}")
logger.info(f"Models Dir exists: {models_dir.exists()}")

# Add ML folder to path for imports
if str(ml_folder) not in sys.path:
    sys.path.insert(0, str(ml_folder))

# Load configuration
try:
    import yaml
    config_file = ml_folder / "config.yaml"
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    logger.info(f"✓ Loaded ML config from {config_file}")
except Exception as e:
    logger.warning(f"Failed to load config: {e}")
    config = {}

# Initialize models cache
loaded_models = {}

# ====================================
# PYDANTIC MODELS
# ====================================

class DemandPredictionRequest(BaseModel):
    """Demand prediction request"""
    product_id: int
    avg_price: float
    min_price: float
    max_price: float
    total_review_count: int
    day_of_week: int = 1
    month: int = 1
    year: int = 2024
    price_change_pct: float = 0.0
    price_volatility: float = 0.0
    review_ma7: float = 0.0
    review_ma30: float = 0.0
    avg_rating: float = 0.0


class DemandPredictionResponse(BaseModel):
    """Demand prediction response"""
    product_id: int
    predicted_demand: float
    confidence_interval: Dict[str, float]
    model_used: str
    timestamp: str


class RecommendationRequest(BaseModel):
    """Recommendation request"""
    product_id: int
    num_recommendations: int = 5


class RecommendationResponse(BaseModel):
    """Recommendation response"""
    product_id: int
    recommendations: List[Dict[str, Any]]
    model_used: str
    timestamp: str


class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str
    models_loaded: List[str]
    timestamp: str


class SentimentAnalysisRequest(BaseModel):
    """Market sentiment analysis request"""
    review_id: int
    product_id: int
    review_text: str
    rating: float
    review_length: int = 0
    

class SentimentAnalysisResponse(BaseModel):
    """Market sentiment analysis response"""
    review_id: int
    product_id: int
    sentiment_label: str  # positive, negative, neutral
    sentiment_score: float  # 0-1
    confidence: float
    model_used: str
    timestamp: str


class ProductSegmentRequest(BaseModel):
    """Product segment recommendation request"""
    product_id: int
    num_segments: int = 3


class ProductSegmentResponse(BaseModel):
    """Product segment recommendation response"""
    product_id: int
    segment_id: int
    segment_name: str
    characteristics: List[str]
    products_in_segment: List[Dict[str, Any]]
    model_used: str
    timestamp: str


# ====================================
# MODEL LOADING
# ====================================

def load_models():
    """Load trained ML models"""
    global loaded_models
    
    if loaded_models:  # Already loaded
        return
    
    try:
        # Load demand model
        demand_model_path = models_dir / "demand_linear.pkl"
        if demand_model_path.exists():
            loaded_models["demand"] = joblib.load(demand_model_path)
            logger.info(f"✓ Loaded demand model: {demand_model_path}")
        else:
            logger.warning(f"⚠ Demand model not found: {demand_model_path}")
        
        # Load recommendation model (nearest neighbors)
        nn_model_path = models_dir / "recommendation_nearest_neighbors.pkl"
        if nn_model_path.exists():
            loaded_models["nearest_neighbors"] = joblib.load(nn_model_path)
            logger.info(f"✓ Loaded nearest neighbors model: {nn_model_path}")
        else:
            logger.warning(f"⚠ Nearest neighbors model not found: {nn_model_path}")
        
        # Load KMeans model for clustering
        kmeans_model_path = models_dir / "recommendation_kmeans.pkl"
        if kmeans_model_path.exists():
            loaded_models["kmeans"] = joblib.load(kmeans_model_path)
            logger.info(f"✓ Loaded KMeans model: {kmeans_model_path}")
        else:
            logger.warning(f"⚠ KMeans model not found: {kmeans_model_path}")
        
        # Load Sentiment Analysis model (Classification)
        sentiment_model_path = models_dir / "sentiment_classifier.pkl"
        if sentiment_model_path.exists():
            loaded_models["sentiment"] = joblib.load(sentiment_model_path)
            logger.info(f"✓ Loaded Sentiment Analysis model: {sentiment_model_path}")
        else:
            logger.warning(f"⚠ Sentiment Analysis model not found: {sentiment_model_path}")
        
        if loaded_models:
            logger.info(f"✓ Successfully loaded models: {list(loaded_models.keys())}")
        else:
            logger.warning("⚠ No models loaded. ML endpoints will use mock data.")
    
    except Exception as e:
        logger.error(f"✗ Error loading models: {e}")
        # Continue anyway - will use mock data


# Load models on startup
load_models()


# ====================================
# UTILITY FUNCTIONS
# ====================================

def get_demand_data():
    """Load prepared demand data"""
    try:
        data_path = data_dir / "demand_prediction" / "train_demand_data.csv"
        if data_path.exists():
            return pd.read_csv(data_path)
    except Exception as e:
        logger.warning(f"Could not load demand data: {e}")
    return None


def get_recommendation_data():
    """Load prepared recommendation data"""
    try:
        data_path = data_dir / "product_recommendation" / "prepared_recommendation_data.csv"
        if data_path.exists():
            return pd.read_csv(data_path)
    except Exception as e:
        logger.warning(f"Could not load recommendation data: {e}")
    return None


# ====================================
# API ENDPOINTS
# ====================================

@router.get("/health", response_model=HealthCheckResponse)
async def ml_health_check():
    """Check ML service health"""
    return HealthCheckResponse(
        status="healthy" if loaded_models else "degraded",
        models_loaded=list(loaded_models.keys()),
        timestamp=datetime.now().isoformat()
    )


@router.post("/predict/demand", response_model=DemandPredictionResponse)
async def predict_demand(request: DemandPredictionRequest):
    """
    Predict product demand
    
    Features required:
    - avg_price: Average product price
    - min_price: Minimum price
    - max_price: Maximum price
    - total_review_count: Total reviews
    - day_of_week: Day of week (1-7)
    - month: Month (1-12)
    - year: Year
    - avg_rating: Average rating
    - price_change_pct: Price change percentage
    - price_volatility: Price volatility
    - review_ma7: 7-day review moving average
    - review_ma30: 30-day review moving average
    """
    try:
        if "demand" not in loaded_models:
            # Use mock prediction
            logger.warning("Demand model not loaded, using mock prediction")
            prediction = request.avg_price * request.total_review_count / 100 + np.random.normal(0, 10)
        else:
            model = loaded_models["demand"]
            features = np.array([[
                request.avg_price,
                request.min_price,
                request.max_price,
                request.avg_rating,
                request.total_review_count,
                request.day_of_week,
                request.month,
                request.year,
                request.price_change_pct,
                request.price_volatility,
                request.review_ma7,
                request.review_ma30
            ]])
            prediction = model.predict(features)[0]
        
        # Ensure non-negative prediction
        prediction = max(0, float(prediction))
        
        # Calculate confidence interval (±15%)
        confidence_low = prediction * 0.85
        confidence_high = prediction * 1.15
        
        return DemandPredictionResponse(
            product_id=request.product_id,
            predicted_demand=prediction,
            confidence_interval={
                "lower": float(confidence_low),
                "upper": float(confidence_high)
            },
            model_used="linear_regression" if "demand" in loaded_models else "mock",
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        logger.error(f"Demand prediction error: {e}")
        raise HTTPException(status_code=400, detail=f"Prediction failed: {str(e)}")


@router.post("/predict/batch-demand")
async def batch_predict_demand(requests: List[DemandPredictionRequest]):
    """Batch demand predictions for multiple products"""
    try:
        results = []
        
        for req in requests:
            try:
                response = await predict_demand(req)
                results.append({
                    "product_id": req.product_id,
                    "predicted_demand": response.predicted_demand,
                    "status": "success"
                })
            except Exception as e:
                results.append({
                    "product_id": req.product_id,
                    "error": str(e),
                    "status": "error"
                })
        
        return {"predictions": results, "timestamp": datetime.now().isoformat()}
    
    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=400, detail=f"Batch prediction failed: {str(e)}")


@router.post("/predict/recommendation", response_model=RecommendationResponse)
async def recommend_products(request: RecommendationRequest):
    """
    Recommend similar products based on features
    Uses trained nearest neighbors model
    """
    try:
        if "nearest_neighbors" not in loaded_models:
            logger.warning("Recommendation model not loaded, using mock recommendations")
            recommendations = [
                {
                    "product_sk": i,
                    "product_name": f"Similar Product {i}",
                    "category_sk": 1,
                    "similarity_score": 0.95 - (i * 0.05),
                    "rating": 4.5
                }
                for i in range(1, request.num_recommendations + 1)
            ]
        else:
            # Load product data
            rec_data = get_recommendation_data()
            
            if rec_data is None or rec_data.empty:
                # Use mock data if file not available
                recommendations = [
                    {
                        "product_sk": i,
                        "product_name": f"Recommended Product {i}",
                        "category_sk": 1,
                        "similarity_score": 0.9 - (i * 0.05),
                        "rating": 4.5
                    }
                    for i in range(1, request.num_recommendations + 1)
                ]
            else:
                # Find product in dataset
                product_matches = rec_data[
                    rec_data.get("global_product_id", rec_data.get("product_sk")) == request.product_id
                ]
                
                if product_matches.empty:
                    raise HTTPException(status_code=404, detail="Product not found")
                
                product_idx = product_matches.index[0]
                model = loaded_models["nearest_neighbors"]
                
                # Get neighbors
                distances, indices = model.kneighbors(
                    rec_data.iloc[[product_idx]],
                    n_neighbors=request.num_recommendations + 1
                )
                
                # Format recommendations (skip query product itself)
                recommendations = []
                for idx, distance in zip(indices[0][1:], distances[0][1:]):
                    rec_product = rec_data.iloc[idx]
                    recommendations.append({
                        "product_sk": int(rec_product.get("product_sk", idx)),
                        "product_name": rec_product.get("product_name", f"Product {idx}"),
                        "category_sk": int(rec_product.get("category_sk", 1)),
                        "similarity_score": float(1 - distance),
                        "rating": float(rec_product.get("avg_rating", 0))
                    })
        
        return RecommendationResponse(
            product_id=request.product_id,
            recommendations=recommendations,
            model_used="nearest_neighbors" if "nearest_neighbors" in loaded_models else "mock",
            timestamp=datetime.now().isoformat()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Recommendation error: {e}")
        raise HTTPException(status_code=400, detail=f"Recommendation failed: {str(e)}")


@router.get("/models/status")
async def get_models_status():
    """Get status of all loaded ML models"""
    try:
        model_info = []
        
        for model_name, model in loaded_models.items():
            model_info.append({
                "name": model_name,
                "loaded": True,
                "type": type(model).__name__
            })
        
        return {
            "total_models_loaded": len(loaded_models),
            "models": model_info,
            "models_directory": str(models_dir),
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Status check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
async def get_model_metrics():
    """Get model performance metrics from evaluation"""
    try:
        metrics_dir = Path(config.get("output", {}).get("metrics_dir", "logs/metrics"))
        
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "models_loaded": list(loaded_models.keys())
        }
        
        # Try to load stored metrics
        try:
            import json
            metrics_file = metrics_dir / "model_selection_summary.json"
            if metrics_file.exists():
                with open(metrics_file, 'r') as f:
                    metrics["model_selection"] = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load metrics: {e}")
        
        return metrics
    
    except Exception as e:
        logger.error(f"Metrics retrieval error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reload-models")
async def reload_models():
    """Reload all ML models"""
    try:
        global loaded_models
        loaded_models.clear()
        load_models()
        
        return {
            "status": "success",
            "message": "Models reloaded",
            "models_loaded": list(loaded_models.keys()),
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Model reload error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reload models: {str(e)}")


# ====================================
# NEW ML CONTRACT ENDPOINTS
# ====================================

@router.get("/models")
async def list_models():
    """
    GET /api/v1/ml/models
    List all ML models with latest version info
    
    Returns: {
        total_models: int,
        models: [
            {id, name, type, latest_version, status, last_trained_at}
        ]
    }
    """
    try:
        # Hardcoded models list (can be replaced with DB query)
        models_list = [
            {
                "id": 1,
                "name": "demand_linear_v1.0",
                "type": "demand_prediction",
                "latest_version": "1.0.0",
                "status": "active",
                "last_trained_at": datetime.now().isoformat()
            },
            {
                "id": 3,
                "name": "recommendation_nn_v1.0",
                "type": "product_recommendation",
                "latest_version": "1.0.0",
                "status": "active",
                "last_trained_at": datetime.now().isoformat()
            },
            {
                "id": 4,
                "name": "recommendation_kmeans_v1.0",
                "type": "product_recommendation",
                "latest_version": "1.0.0",
                "status": "active",
                "last_trained_at": datetime.now().isoformat()
            },
            {
                "id": 5,
                "name": "sentiment_classifier_v1.0",
                "type": "classification",
                "latest_version": "1.0.0",
                "status": "active",
                "last_trained_at": datetime.now().isoformat()
            },
            {
                "id": 6,
                "name": "customer_segmentation_v1.0",
                "type": "customer_segmentation",
                "latest_version": "1.0.0",
                "status": "active",
                "last_trained_at": datetime.now().isoformat()
            }
        ]
        
        return {
            "total_models": len(models_list),
            "models": models_list,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"List models error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/{model_id}/metrics")
async def get_model_metrics(model_id: int):
    """
    GET /api/v1/ml/models/{model_id}/metrics
    Get metrics and history for a specific model
    
    Returns: {
        model_id, model_name, accuracy, precision, recall, f1_score, metrics, trained_at, history
    }
    """
    try:
        # Mock data - in production, query from ml_model_registry
        model_info = {
            1: {
                "model_id": 1,
                "model_name": "demand_linear_v1.0",
                "accuracy": 0.8750,
                "precision": 0.8620,
                "recall": 0.8880,
                "f1_score": 0.8750,
                "metrics": {"rmse": 12.5, "mae": 8.3, "r2_score": 0.8750},
                "trained_at": datetime.now().isoformat(),
                "history": [
                    {"version": "1.0.0", "accuracy": 0.8750, "trained_at": datetime.now().isoformat()},
                    {"version": "0.9.0", "accuracy": 0.8500, "trained_at": (datetime.now() - timedelta(days=30)).isoformat()}
                ]
            },
            3: {
                "model_id": 3,
                "model_name": "recommendation_nn_v1.0",
                "accuracy": 0.7920,
                "precision": 0.7850,
                "recall": 0.8050,
                "f1_score": 0.7920,
                "metrics": {"precision_at_5": 0.82, "recall_at_5": 0.75, "nDCG": 0.79},
                "trained_at": datetime.now().isoformat(),
                "history": [
                    {"version": "1.0.0", "accuracy": 0.7920, "trained_at": datetime.now().isoformat()}
                ]
            },
            5: {
                "model_id": 5,
                "model_name": "sentiment_classifier_v1.0",
                "accuracy": 0.8650,
                "precision": 0.8580,
                "recall": 0.8720,
                "f1_score": 0.8650,
                "metrics": {"weighted_f1": 0.865, "macro_f1": 0.860, "roc_auc": 0.92},
                "trained_at": datetime.now().isoformat(),
                "history": [
                    {"version": "1.0.0", "accuracy": 0.8650, "trained_at": datetime.now().isoformat()}
                ]
            }
        }
        
        if model_id not in model_info:
            raise HTTPException(status_code=404, detail=f"Model {model_id} not found")
        
        return model_info[model_id]
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get model metrics error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/{model_id}/train")
async def trigger_model_training(model_id: int, request: Dict[str, Any]):
    """
    POST /api/v1/ml/models/{model_id}/train
    Trigger training/retraining for a model
    
    Body: {triggered_by: user_id, note: string}
    Returns: {model_id, status, message, job_id, timestamp}
    """
    try:
        triggered_by = request.get("triggered_by")
        note = request.get("note", "Manual retraining")
        
        # Mock job ID
        job_id = f"job_{model_id}_{datetime.now().timestamp():.0f}"
        
        logger.info(f"Training triggered for model {model_id} by user {triggered_by}: {note}")
        
        return {
            "model_id": model_id,
            "status": "training",
            "message": f"Training job started for model {model_id}",
            "job_id": job_id,
            "triggered_by": triggered_by,
            "note": note,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Trigger training error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations/sample")
async def get_recommendation_sample(
    product_sk: int = Query(..., description="Product SK"),
    limit: int = Query(10, ge=1, le=50, description="Number of recommendations")
):
    """
    GET /api/v1/ml/recommendations/sample?product_sk=...&limit=10
    Get product recommendations from ml_product_recommendations table
    
    Returns: {
        product_sk, recommendations: [
            {product_sk, product_name, category_sk, similarity_score, rating, recommendation_type}
        ], total_count, timestamp
    }
    """
    try:
        # Mock data - in production, query from ml_product_recommendations table
        mock_recommendations = [
            {
                "product_sk": 2,
                "product_name": "Similar Product 2",
                "category_sk": 1,
                "similarity_score": 0.92,
                "rating": 4.5,
                "recommendation_type": "content_based"
            },
            {
                "product_sk": 3,
                "product_name": "Similar Product 3",
                "category_sk": 1,
                "similarity_score": 0.885,
                "rating": 4.3,
                "recommendation_type": "content_based"
            },
            {
                "product_sk": 5,
                "product_name": "Similar Product 5",
                "category_sk": 2,
                "similarity_score": 0.865,
                "rating": 4.6,
                "recommendation_type": "collaborative"
            },
            {
                "product_sk": 7,
                "product_name": "Similar Product 7",
                "category_sk": 2,
                "similarity_score": 0.82,
                "rating": 4.4,
                "recommendation_type": "hybrid"
            },
            {
                "product_sk": 10,
                "product_name": "Similar Product 10",
                "category_sk": 3,
                "similarity_score": 0.795,
                "rating": 4.2,
                "recommendation_type": "collaborative"
            }
        ]
        
        recommendations = mock_recommendations[:limit]
        
        return {
            "product_sk": product_sk,
            "recommendations": recommendations,
            "total_count": len(recommendations),
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Get recommendations error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/price-predictions/sample")
async def get_price_predictions_sample(
    product_sk: int = Query(..., description="Product SK"),
    platform_sk: int = Query(..., description="Platform SK"),
    date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    limit: int = Query(7, ge=1, le=30, description="Number of predictions")
):
    """
    GET /api/v1/ml/price-predictions/sample?product_sk=...&platform_sk=...&date=...
    Get price predictions from ml_price_predictions table
    
    Returns: {
        product_sk, platform_sk, predictions: [
            {product_sk, platform_sk, prediction_date, predicted_price, 
             confidence_interval_lower, confidence_interval_upper, model_version}
        ], total_count, timestamp
    }
    """
    try:
        from datetime import datetime as dt
        
        start_date = dt.strptime(date, "%Y-%m-%d") if date else dt.now()
        
        # Mock data - in production, query from ml_price_predictions table
        mock_predictions = [
            {
                "product_sk": product_sk,
                "platform_sk": platform_sk,
                "prediction_date": (start_date + timedelta(days=i)).strftime("%Y-%m-%d"),
                "predicted_price": 250000.00 + (i * 2500),
                "confidence_interval_lower": 245000.00 + (i * 2500),
                "confidence_interval_upper": 255000.00 + (i * 2500),
                "model_version": "1.0.0"
            }
            for i in range(1, 8)
        ]
        
        predictions = mock_predictions[:limit]
        
        return {
            "product_sk": product_sk,
            "platform_sk": platform_sk,
            "predictions": predictions,
            "total_count": len(predictions),
            "timestamp": datetime.now().isoformat()
        }
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        logger.error(f"Get price predictions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ====================================
# SENTIMENT ANALYSIS ENDPOINT (Classification Model)
# ====================================

@router.post("/analyze/sentiment", response_model=SentimentAnalysisResponse)
async def analyze_sentiment(request: SentimentAnalysisRequest):
    """
    Analyze market sentiment from customer reviews
    
    Model Type: Classification Model
    Business Use: Market Sentiment Insight (Phân tích Cảm xúc từ Review)
    
    Returns sentiment classification (positive, negative, neutral) with confidence score
    """
    try:
        if "sentiment" not in loaded_models:
            # Mock sentiment analysis
            logger.warning("Sentiment model not loaded, using mock analysis")
            
            # Simple heuristic: rating-based classification
            if request.rating >= 4.0:
                sentiment_label = "positive"
                sentiment_score = min(request.rating / 5.0, 1.0)
            elif request.rating <= 2.0:
                sentiment_label = "negative"
                sentiment_score = 1.0 - (request.rating / 5.0)
            else:
                sentiment_label = "neutral"
                sentiment_score = 0.5
            
            confidence = 0.65  # Mock confidence
        else:
            # Use trained classification model
            model = loaded_models["sentiment"]
            
            # Prepare features (you may need to adjust based on actual model)
            # This is a simplified example - adjust feature engineering as needed
            from sklearn.feature_extraction.text import TfidfVectorizer
            
            # For mock, we'll use simple features
            features = np.array([[
                request.rating,
                request.review_length,
                len(request.review_text.split()),  # word count
                request.review_text.count('!'),     # exclamation marks
                request.review_text.count('?'),     # question marks
            ]])
            
            prediction = model.predict(features)[0]
            probabilities = model.predict_proba(features)[0]
            
            # Map prediction to label
            label_map = {0: "negative", 1: "neutral", 2: "positive"}
            sentiment_label = label_map.get(prediction, "neutral")
            sentiment_score = float(max(probabilities))
            confidence = float(max(probabilities))
        
        return SentimentAnalysisResponse(
            review_id=request.review_id,
            product_id=request.product_id,
            sentiment_label=sentiment_label,
            sentiment_score=sentiment_score,
            confidence=confidence,
            model_used="sentiment_classifier" if "sentiment" in loaded_models else "mock",
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        logger.error(f"Sentiment analysis error: {e}")
        raise HTTPException(status_code=400, detail=f"Sentiment analysis failed: {str(e)}")


@router.post("/analyze/batch-sentiment")
async def batch_analyze_sentiment(requests: List[SentimentAnalysisRequest]):
    """Batch sentiment analysis for multiple reviews"""
    try:
        results = []
        
        for req in requests:
            try:
                response = await analyze_sentiment(req)
                results.append({
                    "review_id": req.review_id,
                    "product_id": req.product_id,
                    "sentiment_label": response.sentiment_label,
                    "sentiment_score": response.sentiment_score,
                    "confidence": response.confidence,
                    "status": "success"
                })
            except Exception as e:
                results.append({
                    "review_id": req.review_id,
                    "product_id": req.product_id,
                    "error": str(e),
                    "status": "error"
                })
        
        return {"analyses": results, "total": len(results), "timestamp": datetime.now().isoformat()}
    
    except Exception as e:
        logger.error(f"Batch sentiment analysis error: {e}")
        raise HTTPException(status_code=400, detail=f"Batch analysis failed: {str(e)}")


# ====================================
# PRODUCT SEGMENT CLUSTERING ENDPOINT
# ====================================

@router.post("/segment/products", response_model=ProductSegmentResponse)
async def segment_products(request: ProductSegmentRequest):
    """
    Product Segment Recommendation using Clustering Model
    
    Model Type: Clustering Model (KMeans)
    Business Use: Product Segment Recommendation (Phân khúc Sản phẩm)
    
    Groups products into clusters/segments with similar characteristics
    """
    try:
        if "kmeans" not in loaded_models:
            # Mock product segmentation
            logger.warning("KMeans model not loaded, using mock segmentation")
            
            segment_names = ["Premium Products", "Mid-Range Products", "Budget Products"]
            segment_id = (request.product_id % request.num_segments)
            segment_name = segment_names[segment_id % len(segment_names)]
            
            characteristics = {
                0: ["High quality", "Premium pricing", "Exclusive features"],
                1: ["Good value", "Medium pricing", "Popular features"],
                2: ["Cost effective", "Budget pricing", "Basic features"]
            }
            
            products_in_segment = [
                {
                    "product_id": request.product_id + i,
                    "product_name": f"Product {request.product_id + i}",
                    "avg_price": 100000 + (segment_id * 50000),
                    "avg_rating": 4.0 + (segment_id * 0.3)
                }
                for i in range(1, 4)
            ]
        else:
            # Use trained KMeans model
            model = loaded_models["kmeans"]
            
            # Get product data for clustering features
            rec_data = get_recommendation_data()
            
            if rec_data is None or rec_data.empty:
                raise HTTPException(status_code=404, detail="Product data not available")
            
            # Find product in dataset
            product_matches = rec_data[
                rec_data.get("global_product_id", rec_data.get("product_sk")) == request.product_id
            ]
            
            if product_matches.empty:
                raise HTTPException(status_code=404, detail="Product not found")
            
            # Get cluster assignment
            product_idx = product_matches.index[0]
            product_features = rec_data.iloc[[product_idx]].values
            
            segment_id = int(model.predict(product_features)[0])
            
            # Get products in the same segment
            all_segments = model.predict(rec_data.values)
            segment_products_indices = np.where(all_segments == segment_id)[0]
            
            segment_names = [f"Segment {i}" for i in range(model.n_clusters)]
            segment_name = segment_names[segment_id] if segment_id < len(segment_names) else f"Segment {segment_id}"
            
            characteristics = [
                f"Cluster center distance: {model.inertia_:.2f}",
                f"Size: {len(segment_products_indices)} products",
                "Similar product characteristics"
            ]
            
            # Get top products in segment
            segment_df = rec_data.iloc[segment_products_indices].head(5)
            products_in_segment = [
                {
                    "product_id": int(row.get("product_sk", idx)),
                    "product_name": row.get("product_name", f"Product {idx}"),
                    "avg_price": float(row.get("avg_price", 0)),
                    "avg_rating": float(row.get("avg_rating", 0))
                }
                for idx, (_, row) in enumerate(segment_df.iterrows())
            ]
        
        return ProductSegmentResponse(
            product_id=request.product_id,
            segment_id=segment_id,
            segment_name=segment_name,
            characteristics=characteristics,
            products_in_segment=products_in_segment,
            model_used="kmeans" if "kmeans" in loaded_models else "mock",
            timestamp=datetime.now().isoformat()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Product segmentation error: {e}")
        raise HTTPException(status_code=400, detail=f"Segmentation failed: {str(e)}")
