"""
ML API - API cho tất cả ML Models
Hỗ trợ: Model Registry, Price Predictions, Recommendations, Demand Forecast, Customer Segmentation
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

ml_router = APIRouter(prefix="/api/v1/ml", tags=["ML Models"])

# ========================================
# Database Connection
# ========================================

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com'),
            port=int(os.getenv('DB_PORT', 5432)),
            database=os.getenv('DB_NAME', 'ecommerce_dss_1'),
            user=os.getenv('DB_USER', 'dss_user'),
            password=os.getenv('DB_PASSWORD', '6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G')
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# ========================================
# Pydantic Models - Request/Response
# ========================================

class ModelRegistryResponse(BaseModel):
    model_id: int
    model_name: str
    model_type: str
    version: str
    status: str
    description: Optional[str]
    accuracy: Optional[float]
    precision: Optional[float]
    recall: Optional[float]
    f1_score: Optional[float]
    trained_at: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True

class PricePredictionResponse(BaseModel):
    prediction_id: int
    product_sk: int
    platform_sk: int
    prediction_date: date
    predicted_price: Decimal
    confidence_interval_lower: Decimal
    confidence_interval_upper: Decimal
    model_version: str
    created_at: datetime

    class Config:
        from_attributes = True

class RecommendationResponse(BaseModel):
    recommendation_id: int
    product_sk: int
    recommended_product_sk: int
    similarity_score: Decimal
    recommendation_type: str
    created_at: datetime

    class Config:
        from_attributes = True

class DemandForecastResponse(BaseModel):
    forecast_id: int
    product_sk: int
    forecast_date: date
    predicted_demand: int
    confidence_level: Decimal
    model_version: str
    created_at: datetime

    class Config:
        from_attributes = True

class CustomerSegmentResponse(BaseModel):
    segment_id: int
    segment_name: str
    segment_description: Optional[str]
    avg_purchase_value: Decimal
    purchase_frequency: Decimal
    created_at: datetime

    class Config:
        from_attributes = True

# ========================================
# 1. Model Registry Endpoints
# ========================================

@ml_router.get("/models", response_model=List[ModelRegistryResponse])
def get_all_models(
    model_type: Optional[str] = Query(None, description="Filter by model type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get all ML models with optional filters"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM ml.model_registry WHERE 1=1"
        params = []
        
        if model_type:
            query += " AND model_type = %s"
            params.append(model_type)
        
        if status:
            query += " AND status = %s"
            params.append(status)
        
        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, skip])
        
        cursor.execute(query, params)
        models = cursor.fetchall()
        cursor.close()
        
        return [ModelRegistryResponse(**model) for model in models]
    finally:
        conn.close()

@ml_router.get("/models/{model_id}", response_model=ModelRegistryResponse)
def get_model_by_id(model_id: int):
    """Get model details by ID"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM ml.model_registry WHERE model_id = %s", (model_id,))
        model = cursor.fetchone()
        cursor.close()
        
        if not model:
            raise HTTPException(status_code=404, detail=f"Model {model_id} not found")
        
        return ModelRegistryResponse(**model)
    finally:
        conn.close()

@ml_router.post("/models/create")
def create_model(
    model_name: str,
    model_type: str,
    version: str,
    description: Optional[str] = None,
    triggered_by: Optional[int] = None
):
    """Create new model registry entry"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ml.model_registry 
            (model_name, model_type, version, status, description, triggered_by)
            VALUES (%s, %s, %s, 'inactive', %s, %s)
            RETURNING model_id
        """, (model_name, model_type, version, description, triggered_by))
        
        model_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        return {"message": "Model created", "model_id": model_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

@ml_router.put("/models/{model_id}/status")
def update_model_status(model_id: int, status: str):
    """Update model status"""
    valid_statuses = ['active', 'inactive', 'training', 'archived']
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE ml.model_registry 
            SET status = %s, updated_at = NOW()
            WHERE model_id = %s
        """, (status, model_id))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Model {model_id} not found")
        
        conn.commit()
        cursor.close()
        
        return {"message": f"Model {model_id} status updated to {status}"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

@ml_router.put("/models/{model_id}/metrics")
def update_model_metrics(
    model_id: int,
    accuracy: Optional[float] = None,
    precision: Optional[float] = None,
    recall: Optional[float] = None,
    f1_score: Optional[float] = None
):
    """Update model performance metrics"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE ml.model_registry 
            SET accuracy = %s, precision = %s, recall = %s, f1_score = %s, 
                trained_at = NOW(), updated_at = NOW()
            WHERE model_id = %s
        """, (accuracy, precision, recall, f1_score, model_id))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=f"Model {model_id} not found")
        
        conn.commit()
        cursor.close()
        
        return {"message": f"Model {model_id} metrics updated"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

# ========================================
# 2. Price Prediction Endpoints
# ========================================

@ml_router.get("/price-predictions", response_model=List[PricePredictionResponse])
def get_price_predictions(
    product_sk: Optional[int] = Query(None),
    platform_sk: Optional[int] = Query(None),
    prediction_date: Optional[date] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get price predictions"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM ml.price_predictions WHERE 1=1"
        params = []
        
        if product_sk:
            query += " AND product_sk = %s"
            params.append(product_sk)
        
        if platform_sk:
            query += " AND platform_sk = %s"
            params.append(platform_sk)
        
        if prediction_date:
            query += " AND prediction_date = %s"
            params.append(prediction_date)
        
        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, skip])
        
        cursor.execute(query, params)
        predictions = cursor.fetchall()
        cursor.close()
        
        return [PricePredictionResponse(**pred) for pred in predictions]
    finally:
        conn.close()

@ml_router.post("/price-predictions")
def create_price_prediction(
    product_sk: int,
    platform_sk: int,
    prediction_date: date,
    predicted_price: float,
    confidence_lower: float,
    confidence_upper: float,
    model_version: str
):
    """Create price prediction"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ml.price_predictions
            (product_sk, platform_sk, prediction_date, predicted_price,
             confidence_interval_lower, confidence_interval_upper, model_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING prediction_id
        """, (product_sk, platform_sk, prediction_date, predicted_price, 
              confidence_lower, confidence_upper, model_version))
        
        prediction_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        return {"message": "Price prediction created", "prediction_id": prediction_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

# ========================================
# 3. Recommendation Endpoints
# ========================================

@ml_router.get("/recommendations", response_model=List[RecommendationResponse])
def get_recommendations(
    product_sk: Optional[int] = Query(None),
    recommendation_type: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get product recommendations"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM ml.product_recommendations WHERE 1=1"
        params = []
        
        if product_sk:
            query += " AND product_sk = %s"
            params.append(product_sk)
        
        if recommendation_type:
            query += " AND recommendation_type = %s"
            params.append(recommendation_type)
        
        query += " ORDER BY similarity_score DESC LIMIT %s OFFSET %s"
        params.extend([limit, skip])
        
        cursor.execute(query, params)
        recommendations = cursor.fetchall()
        cursor.close()
        
        return [RecommendationResponse(**rec) for rec in recommendations]
    finally:
        conn.close()

@ml_router.post("/recommendations")
def create_recommendation(
    product_sk: int,
    recommended_product_sk: int,
    similarity_score: float,
    recommendation_type: str
):
    """Create product recommendation"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ml.product_recommendations
            (product_sk, recommended_product_sk, similarity_score, recommendation_type)
            VALUES (%s, %s, %s, %s)
            RETURNING recommendation_id
        """, (product_sk, recommended_product_sk, similarity_score, recommendation_type))
        
        recommendation_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        return {"message": "Recommendation created", "recommendation_id": recommendation_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

# ========================================
# 4. Demand Forecast Endpoints
# ========================================

@ml_router.get("/demand-forecasts", response_model=List[DemandForecastResponse])
def get_demand_forecasts(
    product_sk: Optional[int] = Query(None),
    forecast_date: Optional[date] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get demand forecasts"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM ml.demand_forecast WHERE 1=1"
        params = []
        
        if product_sk:
            query += " AND product_sk = %s"
            params.append(product_sk)
        
        if forecast_date:
            query += " AND forecast_date = %s"
            params.append(forecast_date)
        
        query += " ORDER BY forecast_date DESC LIMIT %s OFFSET %s"
        params.extend([limit, skip])
        
        cursor.execute(query, params)
        forecasts = cursor.fetchall()
        cursor.close()
        
        return [DemandForecastResponse(**forecast) for forecast in forecasts]
    finally:
        conn.close()

@ml_router.post("/demand-forecasts")
def create_demand_forecast(
    product_sk: int,
    forecast_date: date,
    predicted_demand: int,
    confidence_level: float,
    model_version: str
):
    """Create demand forecast"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ml.demand_forecast
            (product_sk, forecast_date, predicted_demand, confidence_level, model_version)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING forecast_id
        """, (product_sk, forecast_date, predicted_demand, confidence_level, model_version))
        
        forecast_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        return {"message": "Demand forecast created", "forecast_id": forecast_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

# ========================================
# 5. Customer Segment Endpoints
# ========================================

@ml_router.get("/customer-segments", response_model=List[CustomerSegmentResponse])
def get_customer_segments(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get customer segments"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT * FROM ml.customer_segments 
            ORDER BY created_at DESC LIMIT %s OFFSET %s
        """, (limit, skip))
        
        segments = cursor.fetchall()
        cursor.close()
        
        return [CustomerSegmentResponse(**segment) for segment in segments]
    finally:
        conn.close()

@ml_router.post("/customer-segments")
def create_customer_segment(
    segment_name: str,
    segment_description: Optional[str] = None,
    avg_purchase_value: float = 0,
    purchase_frequency: float = 0
):
    """Create customer segment"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ml.customer_segments
            (segment_name, segment_description, avg_purchase_value, purchase_frequency)
            VALUES (%s, %s, %s, %s)
            RETURNING segment_id
        """, (segment_name, segment_description, avg_purchase_value, purchase_frequency))
        
        segment_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        return {"message": "Customer segment created", "segment_id": segment_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()

# ========================================
# 6. Health Check & Stats
# ========================================

@ml_router.get("/health")
def health_check():
    """Health check endpoint"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@ml_router.get("/stats")
def get_stats():
    """Get ML system statistics"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Model count
        cursor.execute("SELECT COUNT(*) FROM ml.model_registry")
        model_count = cursor.fetchone()[0]
        
        # Active models
        cursor.execute("SELECT COUNT(*) FROM ml.model_registry WHERE status = 'active'")
        active_models = cursor.fetchone()[0]
        
        # Price predictions count
        cursor.execute("SELECT COUNT(*) FROM ml.price_predictions")
        price_pred_count = cursor.fetchone()[0]
        
        # Recommendations count
        cursor.execute("SELECT COUNT(*) FROM ml.product_recommendations")
        recommendations_count = cursor.fetchone()[0]
        
        # Demand forecasts count
        cursor.execute("SELECT COUNT(*) FROM ml.demand_forecast")
        demand_count = cursor.fetchone()[0]
        
        # Customer segments count
        cursor.execute("SELECT COUNT(*) FROM ml.customer_segments")
        segments_count = cursor.fetchone()[0]
        
        cursor.close()
        
        return {
            "total_models": model_count,
            "active_models": active_models,
            "price_predictions": price_pred_count,
            "recommendations": recommendations_count,
            "demand_forecasts": demand_count,
            "customer_segments": segments_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
