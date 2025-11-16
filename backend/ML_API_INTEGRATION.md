# ML API Integration Guide

## Overview
ML API endpoints have been consolidated into a single router module that integrates with the actual ML pipeline from `/ml` folder.

## Architecture

### File Structure
```
backend/
├── app/
│   ├── main.py              # Main FastAPI app (includes ML router)
│   └── api/v1/
│       └── ml_api.py        # New ML API router ✨
│
../ml/                        # Source ML pipeline
├── models/ml-models/        # Trained model files (.pkl)
├── data/                    # Preprocessed data for inference
├── 5_model_serving.py       # Original ML serving code
└── config.yaml              # ML configuration
```

## ML API Endpoints

### Base Path
All ML endpoints are prefixed with `/api/v1/ml`

### Health & Status

#### Health Check
```
GET /api/v1/ml/health
```
Returns the health status of ML service and loaded models.

**Response:**
```json
{
  "status": "healthy",
  "models_loaded": ["demand", "nearest_neighbors"],
  "timestamp": "2024-11-16T10:30:00"
}
```

#### Models Status
```
GET /api/v1/ml/models/status
```
Returns detailed information about loaded models.

**Response:**
```json
{
  "total_models_loaded": 2,
  "models": [
    {"name": "demand", "loaded": true, "type": "LinearRegression"},
    {"name": "nearest_neighbors", "loaded": true, "type": "NearestNeighbors"}
  ],
  "models_directory": "path/to/models",
  "timestamp": "2024-11-16T10:30:00"
}
```

### Demand Prediction

#### Single Product Prediction
```
POST /api/v1/ml/predict/demand
```

**Request Body:**
```json
{
  "product_id": 123,
  "avg_price": 100.50,
  "min_price": 85.00,
  "max_price": 150.00,
  "total_review_count": 250,
  "day_of_week": 3,
  "month": 11,
  "year": 2024,
  "price_change_pct": 2.5,
  "price_volatility": 5.0,
  "review_ma7": 15.5,
  "review_ma30": 18.2,
  "avg_rating": 4.5
}
```

**Response:**
```json
{
  "product_id": 123,
  "predicted_demand": 156.42,
  "confidence_interval": {
    "lower": 132.96,
    "upper": 179.88
  },
  "model_used": "linear_regression",
  "timestamp": "2024-11-16T10:30:00"
}
```

#### Batch Prediction
```
POST /api/v1/ml/predict/batch-demand
```

**Request Body:**
```json
[
  {
    "product_id": 123,
    "avg_price": 100.50,
    "min_price": 85.00,
    "max_price": 150.00,
    "total_review_count": 250,
    "day_of_week": 3,
    "month": 11,
    "year": 2024,
    "price_change_pct": 2.5,
    "price_volatility": 5.0,
    "review_ma7": 15.5,
    "review_ma30": 18.2,
    "avg_rating": 4.5
  },
  {
    "product_id": 456,
    "avg_price": 75.00,
    ...
  }
]
```

**Response:**
```json
{
  "predictions": [
    {
      "product_id": 123,
      "predicted_demand": 156.42,
      "status": "success"
    },
    {
      "product_id": 456,
      "predicted_demand": 98.50,
      "status": "success"
    }
  ],
  "timestamp": "2024-11-16T10:30:00"
}
```

### Product Recommendations

#### Get Recommendations
```
POST /api/v1/ml/predict/recommendation
```

**Request Body:**
```json
{
  "product_id": 123,
  "num_recommendations": 5
}
```

**Response:**
```json
{
  "product_id": 123,
  "recommendations": [
    {
      "product_sk": 456,
      "product_name": "Similar Product A",
      "category_sk": 10,
      "similarity_score": 0.95,
      "rating": 4.5
    },
    {
      "product_sk": 789,
      "product_name": "Similar Product B",
      "category_sk": 10,
      "similarity_score": 0.92,
      "rating": 4.3
    }
  ],
  "model_used": "nearest_neighbors",
  "timestamp": "2024-11-16T10:30:00"
}
```

### Model Metrics & Management

#### Get Model Metrics
```
GET /api/v1/ml/metrics
```
Returns model performance metrics from evaluation.

#### Reload Models
```
POST /api/v1/ml/reload-models
```
Reloads all ML models from disk.

**Response:**
```json
{
  "status": "success",
  "message": "Models reloaded",
  "models_loaded": ["demand", "nearest_neighbors", "kmeans"],
  "timestamp": "2024-11-16T10:30:00"
}
```

## Features

### Model Integration
- **Automatic Model Loading**: Models are loaded from `/ml/models/ml-models/` on startup
- **Graceful Degradation**: If models aren't available, API uses mock predictions
- **Multiple Models**: Support for demand, nearest neighbors, and KMeans clustering
- **Fallback Data**: Can load preprocessed data from `/ml/data/` for inference

### Data Sources
- **Models**: Loaded from trained `.pkl` files in ML folder
- **Data**: Preprocessed CSV files in `/ml/data/`
- **Config**: Uses `/ml/config.yaml` for settings

### Error Handling
- All endpoints include proper error handling
- HTTPException with meaningful error messages
- Logging for debugging

### Performance
- Models cached in memory after loading
- Batch prediction support for efficiency
- Configurable confidence intervals

## Configuration

Edit `/ml/config.yaml` to customize:
- Database connection
- Model paths and formats
- Feature engineering options
- Monitoring settings

## Usage Example

### Python Client
```python
import requests
import json

BASE_URL = "http://localhost:8000/api/v1"

# Single prediction
response = requests.post(
    f"{BASE_URL}/ml/predict/demand",
    json={
        "product_id": 123,
        "avg_price": 100.50,
        "min_price": 85.00,
        "max_price": 150.00,
        "total_review_count": 250,
        "day_of_week": 3,
        "month": 11,
        "year": 2024,
        "price_change_pct": 2.5,
        "price_volatility": 5.0,
        "review_ma7": 15.5,
        "review_ma30": 18.2,
        "avg_rating": 4.5
    }
)

print(response.json())

# Get recommendations
response = requests.post(
    f"{BASE_URL}/ml/predict/recommendation",
    json={
        "product_id": 123,
        "num_recommendations": 5
    }
)

print(response.json())
```

## Troubleshooting

### Models Not Loading
- Check if model files exist in `/ml/models/ml-models/`
- Verify file names: `demand_linear.pkl`, `recommendation_nearest_neighbors.pkl`
- Check logs for import errors

### Prediction Errors
- Ensure all required features are provided in request
- Verify feature values are numeric
- Check database connectivity for data loading

### Configuration Issues
- Verify `/ml/config.yaml` exists and is valid YAML
- Check database credentials in config
- Ensure paths are correct (relative to `/ml/` folder)

## Integration with Main API

The ML router is included in `main.py`:
```python
from api.v1.ml_api import router as ml_router
app.include_router(ml_router, prefix="/api/v1", tags=["ML Predictions"])
```

This makes all ML endpoints available as part of the main FastAPI application.

## Next Steps

1. Train ML models using `/ml` pipeline scripts
2. Save trained models to `/ml/models/ml-models/`
3. Restart backend API to load models
4. Test endpoints via `/docs` (Swagger UI)
5. Monitor predictions and model performance
