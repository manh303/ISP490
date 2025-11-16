# -*- coding: utf-8 -*-
"""
ML API Schemas (Pydantic Models)
Contract definitions for ML endpoints
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field


# ====================================
# ML Model Registry Schemas
# ====================================

class MLModelMetrics(BaseModel):
    """Model metrics"""
    rmse: Optional[float] = None
    mae: Optional[float] = None
    r2_score: Optional[float] = None
    precision_at_5: Optional[float] = None
    recall_at_5: Optional[float] = None
    nDCG: Optional[float] = None
    silhouette_score: Optional[float] = None
    inertia: Optional[float] = None

    class Config:
        extra = "allow"  # Allow additional fields


class MLModelBase(BaseModel):
    """Base ML model info"""
    model_id: int = Field(..., description="Model ID")
    model_name: str = Field(..., description="Model name")
    model_type: str = Field(..., description="demand_prediction|product_recommendation|price_prediction|customer_segmentation")
    version: str = Field(..., description="Model version (e.g. 1.0.0)")
    status: str = Field(..., description="active|inactive|training|archived")
    description: Optional[str] = None
    model_path: Optional[str] = None
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    trained_at: Optional[datetime] = None
    updated_at: datetime
    created_at: datetime


class MLModelResponse(MLModelBase):
    """ML model response with metrics"""
    metrics: Optional[Dict[str, Any]] = None
    last_trained_at: Optional[datetime] = Field(None, description="Alias for trained_at")

    def __init__(self, **data):
        super().__init__(**data)
        # Set last_trained_at as alias for trained_at
        if self.trained_at and not data.get("last_trained_at"):
            self.last_trained_at = self.trained_at


class MLModelListResponse(BaseModel):
    """List of ML models"""
    id: int = Field(..., description="Model ID")
    name: str = Field(..., description="Model name")
    type: str = Field(..., description="Model type")
    latest_version: str = Field(..., description="Latest version")
    status: str = Field(..., description="Model status")
    last_trained_at: Optional[datetime] = Field(None, description="Last training timestamp")


class MLModelsListOutput(BaseModel):
    """Output for GET /api/v1/ml/models"""
    total_models: int
    models: List[MLModelListResponse]


# ====================================
# Model Training Schemas
# ====================================

class TrainModelRequest(BaseModel):
    """Request to train/retrain a model"""
    triggered_by: Optional[int] = Field(None, description="User ID who triggered training")
    note: Optional[str] = Field(None, description="Training note or reason")


class TrainModelResponse(BaseModel):
    """Response from model training request"""
    model_id: int
    status: str = "training"
    message: str
    job_id: Optional[str] = None
    timestamp: datetime


# ====================================
# Model Metrics Schemas
# ====================================

class ModelMetricsResponse(BaseModel):
    """Model metrics response"""
    model_id: int
    model_name: str
    accuracy: Optional[float]
    precision: Optional[float]
    recall: Optional[float]
    f1_score: Optional[float]
    metrics: Optional[Dict[str, Any]]
    trained_at: Optional[datetime]
    history: Optional[List[Dict[str, Any]]] = None  # Previous versions metrics


# ====================================
# Product Recommendation Schemas
# ====================================

class RecommendedProduct(BaseModel):
    """Recommended product info"""
    product_sk: int
    product_name: Optional[str] = None
    category_sk: Optional[int] = None
    similarity_score: float = Field(..., ge=0.0, le=1.0)
    rating: Optional[float] = None
    recommendation_type: Optional[str] = None


class RecommendationSampleResponse(BaseModel):
    """Response for GET /api/v1/ml/recommendations/sample"""
    product_sk: int
    recommendations: List[RecommendedProduct]
    total_count: int
    timestamp: datetime


# ====================================
# Price Prediction Schemas
# ====================================

class PricePrediction(BaseModel):
    """Single price prediction"""
    product_sk: int
    platform_sk: int
    prediction_date: str = Field(..., description="YYYY-MM-DD")
    predicted_price: float
    confidence_interval_lower: float
    confidence_interval_upper: float
    model_version: Optional[str] = None


class PricePredictionSampleResponse(BaseModel):
    """Response for GET /api/v1/ml/price-predictions/sample"""
    product_sk: int
    platform_sk: int
    predictions: List[PricePrediction]
    total_count: int
    timestamp: datetime


# ====================================
# Health Check Schemas
# ====================================

class MLHealthResponse(BaseModel):
    """ML service health status"""
    status: str = Field(..., description="healthy|degraded|unhealthy")
    models_loaded: List[str] = Field(..., description="List of loaded model names")
    models_active: int = Field(..., description="Number of active models in DB")
    timestamp: datetime
