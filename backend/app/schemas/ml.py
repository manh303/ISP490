# app/schemas/ml.py
from typing import Optional, Dict, Any, List
from datetime import date, datetime
from pydantic import BaseModel, Field


# --------- MODEL REGISTRY ---------

class MLModelBase(BaseModel):
    model_name: str = Field(..., description="Unique name of the model, e.g. 'price_forecast_xgb'")
    model_type: str = Field(..., description="Type of model: 'price', 'recommendation', 'sentiment', ...")
    model_version: str = Field(..., description="Model version tag, e.g. 'v1.0'")
    training_data_until: Optional[date] = Field(None, description="Data up to this date was used for training")
    metrics: Optional[Dict[str, Any]] = Field(None, description="Arbitrary metrics JSON (rmse, mae, f1, ...)")
    status: Optional[str] = Field("active", description="Model status: active, deprecated, training, ...")


class MLModelCreate(MLModelBase):
    pass


class MLModelUpdate(BaseModel):
    training_data_until: Optional[date] = None
    metrics: Optional[Dict[str, Any]] = None
    status: Optional[str] = None


class MLModelResponse(MLModelBase):
    model_sk: int
    created_at: datetime

    class Config:
        from_attributes = True


# --------- PRICE PREDICTION HISTORY ---------

class PricePredictionPoint(BaseModel):
    date: date
    platform_code: str
    product_key: str
    product_name: str
    model_name: str
    model_version: str
    predicted_price: float
    ci_lower: Optional[float] = None
    ci_upper: Optional[float] = None
    run_id: Optional[str] = None


class PricePredictionHistoryResponse(BaseModel):
    product_key: str
    platform_code: str
    model_name: Optional[str] = None
    model_version: Optional[str] = None
    points: List[PricePredictionPoint]


# --------- ONLINE PRICE PREDICTION (stub) ---------

class OnlinePricePredictionRequest(BaseModel):
    platform_code: str
    product_key: str
    current_price: Optional[float] = None
    avg_rating: Optional[float] = None
    review_count: Optional[int] = None

    # tuỳ chọn, có default luôn cho tiện FE
    model_name: Optional[str] = Field(
        "price_forecast_rf",
        description="Tên model, mặc định 'price_forecast_rf'"
    )
    model_version: Optional[str] = None

class OnlinePricePredictionResponse(BaseModel):
    predicted_price: float
    ci_lower: Optional[float] = None
    ci_upper: Optional[float] = None
    model_name: str
    model_version: str
    latency_ms: Optional[int] = None


# --------- RECOMMENDATIONS ---------

class RecommendationItem(BaseModel):
    rank: int
    recommended_product_key: str
    product_name: str
    similarity_score: Optional[float] = None
    min_price: Optional[float] = None
    avg_rating: Optional[float] = None


class RecommendationResponse(BaseModel):
    source_product_key: str
    platform_code: str
    model_name: str
    model_version: str
    date: date
    recommendations: List[RecommendationItem]


# --------- SENTIMENT (CLASSIFICATION) ---------

class SentimentSummaryItem(BaseModel):
    date: date
    product_key: str
    platform_code: str
    total_reviews: int
    positive: int
    negative: int
    neutral: int
    positive_ratio: float


class SentimentSummaryResponse(BaseModel):
    product_key: str
    platform_code: str
    model_name: str
    model_version: str
    from_date: date
    to_date: date
    points: List[SentimentSummaryItem]


class OnlineSentimentRequest(BaseModel):
    platform_code: str
    product_key: Optional[str] = None
    review_text: str
    model_name: Optional[str] = Field(
        "sentiment_tfidf_logreg",
        description="Tên model, mặc định 'sentiment_tfidf_logreg'"
    )
    model_version: Optional[str] = None


class OnlineSentimentResponse(BaseModel):
    label: str  # positive / negative / neutral
    score: float
    model_name: str
    model_version: str
    latency_ms: int


# --------- STATUS SUMMARY ---------

class MLStatusSummary(BaseModel):
    models_total: int
    models_active: int
    models_deprecated: int
    models_training: int
    predictions_last_7_days: int
    recommendations_last_7_days: int
    sentiment_reviews_last_7_days: int
