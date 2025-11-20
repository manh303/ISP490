#!/usr/bin/env python3
"""
ML Prediction Endpoints for FastAPI Backend
Real-time ML inference API
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    from .ml_service import get_ml_service, MLInferenceService
except ImportError:
    from ml_service import get_ml_service, MLInferenceService

# Create ML router
ml_router = APIRouter(prefix="/api/v1/ml", tags=["Machine Learning"])

# ====================================
# PYDANTIC MODELS FOR ML REQUESTS
# ====================================

class CustomerFeaturesBase(BaseModel):
    """Base customer features for ML predictions"""
    customer_id: str = Field(..., description="Unique customer identifier")
    recency: Optional[float] = Field(default=0, description="Days since last purchase")
    frequency: Optional[float] = Field(default=0, description="Number of orders")
    monetary: Optional[float] = Field(default=0, description="Total spent")
    avg_order_value: Optional[float] = Field(default=0, description="Average order value")
    days_as_customer: Optional[float] = Field(default=0, description="Days as customer")
    avg_review_score: Optional[float] = Field(default=5, description="Average review score")
    total_orders: Optional[float] = Field(default=0, description="Total number of orders")
    total_spent: Optional[float] = Field(default=0, description="Total amount spent")
    days_since_last_order: Optional[float] = Field(default=0, description="Days since last order")
    order_frequency: Optional[float] = Field(default=0, description="Order frequency")

class CLVPredictionRequest(CustomerFeaturesBase):
    """Customer Lifetime Value prediction request"""
    pass

class ChurnPredictionRequest(CustomerFeaturesBase):
    """Churn prediction request"""
    pass

class CustomerSegmentationRequest(BaseModel):
    """Customer segmentation request"""
    customer_id: str = Field(..., description="Unique customer identifier")
    recency: float = Field(..., description="Days since last purchase")
    frequency: float = Field(..., description="Number of orders")
    monetary: float = Field(..., description="Total spent")

class ProductRecommendationRequest(BaseModel):
    """Product recommendation request"""
    customer_id: str = Field(..., description="Unique customer identifier")
    category: Optional[str] = Field(default="electronics", description="Product category")
    budget: Optional[float] = Field(default=500, description="Customer budget")
    previous_purchases: Optional[List[str]] = Field(default=[], description="Previous product IDs")
    limit: Optional[int] = Field(default=5, ge=1, le=20, description="Number of recommendations")

class BatchPredictionRequest(BaseModel):
    """Batch prediction request for multiple customers"""
    customers: List[CustomerFeaturesBase] = Field(..., description="List of customers")
    prediction_type: str = Field(..., description="Type of prediction: clv, churn, or segment")

# ====================================
# ML PREDICTION ENDPOINTS
# ====================================

@ml_router.post("/predict/clv")
async def predict_customer_lifetime_value(
    request: CLVPredictionRequest,
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Predict Customer Lifetime Value (CLV)

    Returns the predicted CLV with confidence scores and recommendations
    """
    try:
        customer_features = request.dict()
        result = await ml_service.predict_customer_lifetime_value(customer_features)

        return {
            "success": True,
            "prediction_type": "customer_lifetime_value",
            "customer_id": request.customer_id,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CLV prediction failed: {str(e)}")

@ml_router.post("/predict/churn")
async def predict_customer_churn(
    request: ChurnPredictionRequest,
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Predict customer churn probability

    Returns churn probability, risk level, and retention recommendations
    """
    try:
        customer_features = request.dict()
        result = await ml_service.predict_churn_probability(customer_features)

        return {
            "success": True,
            "prediction_type": "churn_prediction",
            "customer_id": request.customer_id,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Churn prediction failed: {str(e)}")

@ml_router.post("/predict/segment")
async def predict_customer_segment(
    request: CustomerSegmentationRequest,
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Predict customer segment using RFM analysis

    Returns customer segment with marketing strategy recommendations
    """
    try:
        customer_features = request.dict()
        result = await ml_service.predict_customer_segment(customer_features)

        return {
            "success": True,
            "prediction_type": "customer_segmentation",
            "customer_id": request.customer_id,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Customer segmentation failed: {str(e)}")

@ml_router.post("/recommend/products")
async def recommend_products(
    request: ProductRecommendationRequest,
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Generate personalized product recommendations

    Returns ranked list of recommended products with relevance scores
    """
    try:
        product_features = {
            'category': request.category,
            'budget': request.budget,
            'previous_purchases': request.previous_purchases
        }

        result = await ml_service.get_product_recommendations(
            request.customer_id,
            product_features
        )

        # Limit recommendations as requested
        if request.limit and 'recommendations' in result:
            result['recommendations'] = result['recommendations'][:request.limit]
            result['total_recommendations'] = len(result['recommendations'])

        return {
            "success": True,
            "prediction_type": "product_recommendations",
            "customer_id": request.customer_id,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Product recommendation failed: {str(e)}")

@ml_router.post("/predict/batch")
async def batch_predictions(
    request: BatchPredictionRequest,
    background_tasks: BackgroundTasks,
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Run batch predictions for multiple customers

    Supports CLV, churn, and segmentation predictions in batch
    """
    if len(request.customers) > 100:
        raise HTTPException(status_code=400, detail="Batch size too large. Maximum 100 customers.")

    try:
        results = []

        for customer in request.customers:
            customer_features = customer.dict()

            try:
                if request.prediction_type == "clv":
                    result = await ml_service.predict_customer_lifetime_value(customer_features)
                elif request.prediction_type == "churn":
                    result = await ml_service.predict_churn_probability(customer_features)
                elif request.prediction_type == "segment":
                    result = await ml_service.predict_customer_segment(customer_features)
                else:
                    raise ValueError(f"Invalid prediction type: {request.prediction_type}")

                results.append({
                    "customer_id": customer.customer_id,
                    "success": True,
                    "result": result
                })

            except Exception as e:
                results.append({
                    "customer_id": customer.customer_id,
                    "success": False,
                    "error": str(e)
                })

        # Calculate success rate
        successful = sum(1 for r in results if r["success"])
        success_rate = successful / len(results) if results else 0

        return {
            "success": True,
            "prediction_type": f"batch_{request.prediction_type}",
            "total_customers": len(request.customers),
            "successful_predictions": successful,
            "success_rate": success_rate,
            "results": results,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {str(e)}")

# ====================================
# ML MODEL MANAGEMENT ENDPOINTS
# ====================================

@ml_router.get("/models/status")
async def get_models_status(
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Get status of all available ML models

    Returns information about loaded models and their capabilities
    """
    try:
        models_info = ml_service.get_available_models()
        health_check = await ml_service.run_model_health_check()

        return {
            "success": True,
            "models_loaded": len(models_info),
            "models": models_info,
            "health_check": health_check,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Model status check failed: {str(e)}")

@ml_router.get("/models/{model_name}/info")
async def get_model_info(
    model_name: str,
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Get detailed information about a specific model
    """
    try:
        models_info = ml_service.get_available_models()

        if model_name not in models_info:
            raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found")

        return {
            "success": True,
            "model_name": model_name,
            "model_info": models_info[model_name],
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Model info retrieval failed: {str(e)}")

@ml_router.post("/models/reload")
async def reload_models(
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Reload all ML models (useful after model updates)
    """
    try:
        # Reload models
        ml_service.load_models()

        # Get updated status
        models_info = ml_service.get_available_models()

        return {
            "success": True,
            "message": "Models reloaded successfully",
            "models_loaded": len(models_info),
            "models": list(models_info.keys()),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Model reload failed: {str(e)}")

# ====================================
# ANALYTICS & INSIGHTS ENDPOINTS
# ====================================

@ml_router.post("/analytics/customer-insights")
async def get_customer_insights(
    request: CustomerFeaturesBase,
    ml_service: MLInferenceService = Depends(get_ml_service)
) -> Dict[str, Any]:
    """
    Get comprehensive customer insights combining multiple ML predictions

    Returns CLV, churn risk, segment, and recommendations in one call
    """
    try:
        customer_features = request.dict()
        insights = {}

        # Run all available predictions
        try:
            insights['clv'] = await ml_service.predict_customer_lifetime_value(customer_features)
        except Exception as e:
            insights['clv'] = {"error": str(e)}

        try:
            insights['churn'] = await ml_service.predict_churn_probability(customer_features)
        except Exception as e:
            insights['churn'] = {"error": str(e)}

        try:
            insights['segment'] = await ml_service.predict_customer_segment(customer_features)
        except Exception as e:
            insights['segment'] = {"error": str(e)}

        # Generate overall customer score
        overall_score = 0
        if 'predicted_clv' in insights.get('clv', {}):
            clv_score = min(insights['clv']['predicted_clv'] / 1000, 1.0)
            overall_score += clv_score * 0.4

        if 'churn_probability' in insights.get('churn', {}):
            churn_score = 1 - insights['churn']['churn_probability']
            overall_score += churn_score * 0.3

        if request.avg_review_score:
            review_score = request.avg_review_score / 5.0
            overall_score += review_score * 0.3

        # Generate comprehensive recommendation
        overall_recommendation = "Monitor customer behavior and maintain engagement"
        if overall_score >= 0.8:
            overall_recommendation = "High-value customer - provide VIP treatment and exclusive offers"
        elif overall_score >= 0.6:
            overall_recommendation = "Good customer - nurture relationship with targeted campaigns"
        elif overall_score >= 0.4:
            overall_recommendation = "Moderate customer - focus on increasing engagement and value"
        else:
            overall_recommendation = "At-risk customer - implement retention strategies immediately"

        return {
            "success": True,
            "customer_id": request.customer_id,
            "overall_score": round(overall_score, 3),
            "overall_recommendation": overall_recommendation,
            "detailed_insights": insights,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Customer insights generation failed: {str(e)}")

# Export the router
__all__ = ['ml_router']