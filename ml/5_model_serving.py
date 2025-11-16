# -*- coding: utf-8 -*-
"""
Step 5: Model Serving & Deployment
API for predictions and monitoring
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import joblib
import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Any
import uvicorn
from utils.logger import get_logger
from utils.db_connector import DWHConnector
import yaml
import asyncio

logger = get_logger("model_serving")

# Load config
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# Initialize FastAPI app
app = FastAPI(
    title="ML Model Serving API",
    description="Demand Prediction & Product Recommendation API",
    version="1.0.0"
)

# Global variables
loaded_models = {}
feature_scaler = None


# Pydantic models
class DemandPredictionRequest(BaseModel):
    """Demand prediction request"""
    product_id: int
    avg_price: float
    min_price: float
    max_price: float
    total_review_count: int
    day_of_week: int
    month: int
    year: int
    price_change_pct: float
    price_volatility: float = 0.0
    review_ma7: float = 0.0
    review_ma30: float = 0.0
    avg_rating: float = 0.0


class DemandPredictionResponse(BaseModel):
    """Demand prediction response"""
    product_id: int
    predicted_sales: float
    confidence_interval: Dict[str, float]
    model_used: str
    timestamp: str


class RecommendationRequest(BaseModel):
    """Recommendation request"""
    product_id: int
    category_id: int
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


# Load models at startup
@app.on_event("startup")
async def load_models():
    """Load trained models"""
    logger.info("Loading trained models...")
    
    model_dir = Path(config['output']['models_dir'])
    
    try:
        # Load demand models
        demand_model_path = model_dir / 'demand_linear.pkl'
        if demand_model_path.exists():
            loaded_models['demand'] = joblib.load(demand_model_path)
            logger.info(f"[OK] Loaded demand model: {demand_model_path}")
        else:
            logger.warning(f"[WARN] Demand model not found: {demand_model_path}")
        
        # Load recommendation models
        rec_model_path = model_dir / 'recommendation_kmeans.pkl'
        if rec_model_path.exists():
            loaded_models['recommendation'] = joblib.load(rec_model_path)
            logger.info(f"[OK] Loaded recommendation model: {rec_model_path}")
        else:
            logger.warning(f"[WARN] Recommendation model not found: {rec_model_path}")
        
        # Load nearest neighbors
        nn_model_path = model_dir / 'recommendation_nearest_neighbors.pkl'
        if nn_model_path.exists():
            loaded_models['nearest_neighbors'] = joblib.load(nn_model_path)
            logger.info(f"[OK] Loaded nearest neighbors model: {nn_model_path}")
        else:
            logger.warning(f"[WARN] Nearest neighbors model not found: {nn_model_path}")
        
        logger.info(f"[OK] All models loaded: {list(loaded_models.keys())}")
    
    except Exception as e:
        logger.error(f"[ERROR] Failed to load models: {e}")
        raise


# Health check endpoint
@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Health check endpoint"""
    return HealthCheckResponse(
        status="healthy" if loaded_models else "unhealthy",
        models_loaded=list(loaded_models.keys()),
        timestamp=datetime.now().isoformat()
    )


# Demand prediction endpoint
@app.post("/predict/demand", response_model=DemandPredictionResponse)
async def predict_demand(request: DemandPredictionRequest):
    """Predict product demand"""
    try:
        if 'demand' not in loaded_models:
            raise HTTPException(status_code=503, detail="Demand model not loaded")
        
        # Prepare features - match the trained model's feature columns
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
        
        # Make prediction
        model = loaded_models['demand']
        prediction = model.predict(features)[0]
        
        # Calculate confidence interval (example: ±10%)
        confidence_low = prediction * 0.9
        confidence_high = prediction * 1.1
        
        return DemandPredictionResponse(
            product_id=request.product_id,
            predicted_sales=float(prediction),
            confidence_interval={
                'lower': float(confidence_low),
                'upper': float(confidence_high)
            },
            model_used='linear',
            timestamp=datetime.now().isoformat()
        )
    
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# Recommendation endpoint
@app.post("/predict/recommendation", response_model=RecommendationResponse)
async def recommend_products(request: RecommendationRequest):
    """Recommend similar products"""
    try:
        if 'nearest_neighbors' not in loaded_models:
            raise HTTPException(status_code=503, detail="Recommendation model not loaded")
        
        # Load product data
        rec_dir = Path(config['data_extraction']['recommendation']['output_dir'])
        products_df = pd.read_csv(rec_dir / 'prepared_recommendation_data.csv')
        
        # Find product index
        product_idx = products_df[products_df['global_product_id'] == request.product_id].index
        
        if len(product_idx) == 0:
            raise HTTPException(status_code=404, detail="Product not found")
        
        # Get recommendations
        model = loaded_models['nearest_neighbors']
        distances, indices = model.kneighbors(
            products_df.iloc[product_idx[0]:product_idx[0]+1],
            n_neighbors=request.num_recommendations + 1
        )
        
        # Get recommended products (skip the first one which is the query product)
        recommendations = []
        for idx, distance in zip(indices[0][1:], distances[0][1:]):
            rec_product = products_df.iloc[idx]
            recommendations.append({
                'product_sk': int(rec_product['product_sk']),
                'product_name': rec_product.get('product_name', 'Unknown'),
                'category_sk': int(rec_product['category_sk']),
                'similarity_score': float(1 - distance),
                'rating': float(rec_product.get('avg_rating', 0))
            })
        
        return RecommendationResponse(
            product_id=request.product_id,
            recommendations=recommendations,
            model_used='nearest_neighbors',
            timestamp=datetime.now().isoformat()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Recommendation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# Batch prediction endpoint
@app.post("/predict/batch-demand")
async def batch_predict_demand(requests: List[DemandPredictionRequest]):
    """Batch demand predictions"""
    try:
        if 'demand' not in loaded_models:
            raise HTTPException(status_code=503, detail="Demand model not loaded")
        
        # Prepare features
        features_list = []
        for req in requests:
            features_list.append([
                req.avg_price, req.min_price, req.max_price,
                req.avg_rating, req.total_review_count,
                req.day_of_week, req.month, req.year,
                req.price_change_pct, req.price_volatility, 
                req.review_ma7, req.review_ma30
            ])
        
        features = np.array(features_list)
        
        # Make predictions
        model = loaded_models['demand']
        predictions = model.predict(features)
        
        results = []
        for req, pred in zip(requests, predictions):
            results.append({
                'product_id': req.product_id,
                'predicted_sales': float(pred),
                'timestamp': datetime.now().isoformat()
            })
        
        return {'predictions': results}
    
    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# Model monitoring endpoint
@app.get("/monitoring/metrics")
async def get_monitoring_metrics():
    """Get model performance metrics"""
    try:
        metrics_dir = Path(config['output']['metrics_dir'])
        
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'models_loaded': list(loaded_models.keys())
        }
        
        # Load stored metrics
        if (metrics_dir / 'model_selection_summary.json').exists():
            with open(metrics_dir / 'model_selection_summary.json', 'r') as f:
                metrics['model_selection'] = json.load(f)
        
        if (metrics_dir / 'demand_results.json').exists():
            with open(metrics_dir / 'demand_results.json', 'r') as f:
                metrics['demand_metrics'] = json.load(f)
        
        if (metrics_dir / 'recommendation_results.json').exists():
            with open(metrics_dir / 'recommendation_results.json', 'r') as f:
                metrics['recommendation_metrics'] = json.load(f)
        
        return metrics
    
    except Exception as e:
        logger.error(f"Metrics retrieval error: {e}")
        raise HTTPException(status_code=400, detail=str(e))


def main():
    """Start API server"""
    logger.info("[ML SERVING] Starting Model Serving API")
    logger.info("="*60)
    
    config_api = config['model_serving']['api']
    
    uvicorn.run(
        app,
        host=config_api['host'],
        port=config_api['port'],
        log_level="info"
    )


if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0', help='API host')
    parser.add_argument('--port', type=int, default=5000, help='API port')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers')
    
    args = parser.parse_args()
    
    config['model_serving']['api']['host'] = args.host
    config['model_serving']['api']['port'] = args.port
    config['model_serving']['api']['workers'] = args.workers
    
    main()
