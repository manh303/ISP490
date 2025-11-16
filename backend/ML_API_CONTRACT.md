# ML API Contract - Day 2

## Overview
Complete ML API contract definition with endpoint specifications, request/response formats, and sample data structure.

## Database Setup

### New Tables Created
1. **ml_model_registry** - ML model management and versioning
2. Updated **ml_product_recommendations** - Product recommendation data
3. Updated **ml_price_predictions** - Price forecast data

### Table Schemas

#### ml_model_registry
```sql
CREATE TABLE ml_model_registry (
    model_id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_type VARCHAR(50), -- demand_prediction|product_recommendation|price_prediction|customer_segmentation
    version VARCHAR(50),
    status VARCHAR(20) DEFAULT 'inactive', -- active|inactive|training|archived
    description TEXT,
    model_path VARCHAR(255),
    metrics JSONB,
    accuracy DECIMAL(5,4),
    precision DECIMAL(5,4),
    recall DECIMAL(5,4),
    f1_score DECIMAL(5,4),
    trained_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW(),
    triggered_by BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

#### Seed Data
Sample ML models have been inserted:
- demand_linear_v1.0 (active, accuracy: 0.875)
- recommendation_nn_v1.0 (active, accuracy: 0.792)
- recommendation_kmeans_v1.0 (active, accuracy: 0.765)
- customer_segmentation_v1.0 (active)

Sample product recommendations (25+ rows) with similarity scores
Sample price predictions (30+ rows) across products and platforms

---

## API Endpoints

### Base URL
```
http://localhost:8000/api/v1/ml
```

---

## 1. ML Models Endpoints

### 1.1 List All ML Models
```
GET /api/v1/ml/models
```

**Description:** Retrieve all available ML models with version info

**Response:**
```json
{
  "total_models": 4,
  "models": [
    {
      "id": 1,
      "name": "demand_linear_v1.0",
      "type": "demand_prediction",
      "latest_version": "1.0.0",
      "status": "active",
      "last_trained_at": "2025-11-16T10:30:00"
    },
    {
      "id": 3,
      "name": "recommendation_nn_v1.0",
      "type": "product_recommendation",
      "latest_version": "1.0.0",
      "status": "active",
      "last_trained_at": "2025-11-16T10:30:00"
    }
  ],
  "timestamp": "2025-11-16T10:35:00"
}
```

**Status Code:** 200 OK

---

### 1.2 Get Model Metrics
```
GET /api/v1/ml/models/{model_id}/metrics
```

**Parameters:**
- `model_id` (int, required): Model identifier

**Example:** `GET /api/v1/ml/models/1/metrics`

**Response:**
```json
{
  "model_id": 1,
  "model_name": "demand_linear_v1.0",
  "accuracy": 0.8750,
  "precision": 0.8620,
  "recall": 0.8880,
  "f1_score": 0.8750,
  "metrics": {
    "rmse": 12.5,
    "mae": 8.3,
    "r2_score": 0.8750
  },
  "trained_at": "2025-11-16T10:30:00",
  "history": [
    {
      "version": "1.0.0",
      "accuracy": 0.8750,
      "trained_at": "2025-11-16T10:30:00"
    },
    {
      "version": "0.9.0",
      "accuracy": 0.8500,
      "trained_at": "2025-10-17T10:30:00"
    }
  ]
}
```

**Status Codes:**
- 200 OK
- 404 Not Found (if model doesn't exist)
- 500 Internal Server Error

---

### 1.3 Trigger Model Training
```
POST /api/v1/ml/models/{model_id}/train
```

**Parameters:**
- `model_id` (int, required): Model identifier

**Request Body:**
```json
{
  "triggered_by": 1,
  "note": "Scheduled retraining on 2025-11-16"
}
```

**Response:**
```json
{
  "model_id": 1,
  "status": "training",
  "message": "Training job started for model 1",
  "job_id": "job_1_1731750600",
  "triggered_by": 1,
  "note": "Scheduled retraining on 2025-11-16",
  "timestamp": "2025-11-16T10:35:00"
}
```

**Status Code:** 200 OK

---

## 2. Product Recommendations Endpoint

### 2.1 Get Recommendation Samples
```
GET /api/v1/ml/recommendations/sample?product_sk=1&limit=10
```

**Query Parameters:**
- `product_sk` (int, required): Product surrogate key
- `limit` (int, optional, default=10, max=50): Number of recommendations

**Example:** 
```
GET /api/v1/ml/recommendations/sample?product_sk=1&limit=5
```

**Response:**
```json
{
  "product_sk": 1,
  "recommendations": [
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
    }
  ],
  "total_count": 3,
  "timestamp": "2025-11-16T10:35:00"
}
```

**Status Codes:**
- 200 OK
- 400 Bad Request (invalid parameters)
- 500 Internal Server Error

---

## 3. Price Predictions Endpoint

### 3.1 Get Price Prediction Samples
```
GET /api/v1/ml/price-predictions/sample?product_sk=1&platform_sk=1&date=2025-11-16&limit=7
```

**Query Parameters:**
- `product_sk` (int, required): Product surrogate key
- `platform_sk` (int, required): Platform surrogate key (1=Tiki, 2=Lazada)
- `date` (string, optional, format=YYYY-MM-DD): Start prediction date (default: today)
- `limit` (int, optional, default=7, max=30): Number of predictions

**Example:**
```
GET /api/v1/ml/price-predictions/sample?product_sk=2&platform_sk=2&date=2025-11-16&limit=5
```

**Response:**
```json
{
  "product_sk": 2,
  "platform_sk": 2,
  "predictions": [
    {
      "product_sk": 2,
      "platform_sk": 2,
      "prediction_date": "2025-11-17",
      "predicted_price": 350500.0,
      "confidence_interval_lower": 342500.0,
      "confidence_interval_upper": 358500.0,
      "model_version": "1.0.0"
    },
    {
      "product_sk": 2,
      "platform_sk": 2,
      "prediction_date": "2025-11-18",
      "predicted_price": 353000.0,
      "confidence_interval_lower": 345000.0,
      "confidence_interval_upper": 361000.0,
      "model_version": "1.0.0"
    }
  ],
  "total_count": 2,
  "timestamp": "2025-11-16T10:35:00"
}
```

**Status Codes:**
- 200 OK
- 400 Bad Request (invalid date format or parameters)
- 500 Internal Server Error

---

## 4. Health Check Endpoints

### 4.1 Check ML Service Health
```
GET /api/v1/ml/health
```

**Response:**
```json
{
  "status": "healthy",
  "models_loaded": ["demand", "nearest_neighbors", "kmeans"],
  "timestamp": "2025-11-16T10:35:00"
}
```

### 4.2 Get Model Status
```
GET /api/v1/ml/models/status
```

**Response:**
```json
{
  "total_models_loaded": 3,
  "models": [
    {
      "name": "demand",
      "loaded": true,
      "type": "LinearRegression"
    },
    {
      "name": "nearest_neighbors",
      "loaded": true,
      "type": "NearestNeighbors"
    },
    {
      "name": "kmeans",
      "loaded": true,
      "type": "KMeans"
    }
  ],
  "models_directory": "/path/to/models",
  "timestamp": "2025-11-16T10:35:00"
}
```

---

## 5. Legacy Prediction Endpoints

### 5.1 Predict Demand
```
POST /api/v1/ml/predict/demand
```

**Request:**
```json
{
  "product_id": 1,
  "avg_price": 250000.0,
  "min_price": 220000.0,
  "max_price": 280000.0,
  "total_review_count": 500,
  "day_of_week": 3,
  "month": 11,
  "year": 2025,
  "avg_rating": 4.5,
  "price_change_pct": 2.5,
  "price_volatility": 0.15,
  "review_ma7": 45.0,
  "review_ma30": 40.0
}
```

**Response:**
```json
{
  "product_id": 1,
  "predicted_demand": 350.5,
  "confidence_interval": {
    "lower": 297.925,
    "upper": 403.075
  },
  "model_used": "linear_regression",
  "timestamp": "2025-11-16T10:35:00"
}
```

### 5.2 Batch Demand Predictions
```
POST /api/v1/ml/predict/batch-demand
```

### 5.3 Product Recommendations
```
POST /api/v1/ml/predict/recommendation
```

---

## Testing Files

### 1. HTTP Test File (VS Code REST Client)
File: `backend/ml_api_test.http`

Contains ready-to-use test requests for all endpoints. Open in VS Code with REST Client extension and click "Send Request".

**Sample Requests:**
- List models
- Get model metrics
- Get recommendations
- Get price predictions
- etc.

### 2. Postman Collection
File: `backend/ML_API_Postman.json`

Import into Postman for organized endpoint testing:
1. Open Postman
2. File → Import → Select `ML_API_Postman.json`
3. Set `{{base_url}}` variable to `http://localhost:8000/api/v1`
4. Run requests

---

## Data Availability

### Available Product SKs
1, 2, 3, 4, 5, 7, 10, 11, 12, 14, 15, 16, 18

### Available Platforms
- Platform SK 1: Tiki
- Platform SK 2: Lazada

### Sample Data Counts
- Product Recommendations: 25+ records
- Price Predictions: 30+ records
- Registered Models: 4 models

---

## Implementation Notes

### Current Status
✅ All endpoints implemented with mock data
✅ Database schema ready
✅ Error handling with try/except blocks
✅ No 500 errors on failed queries (graceful degradation)
✅ Test files prepared

### Next Steps (for production)
1. Integrate actual database queries in endpoints
2. Implement real model training triggering
3. Add authentication/authorization
4. Add request validation
5. Implement pagination for large result sets
6. Add caching for frequently accessed models
7. Set up job queue for model training

---

## Error Handling

All endpoints return standardized error responses:

```json
{
  "detail": "Error message describing what went wrong"
}
```

**Common Error Codes:**
- 400: Bad Request (invalid parameters)
- 404: Not Found (model/product doesn't exist)
- 500: Internal Server Error

---

## Performance Considerations

- Endpoints return mock data for demo purposes
- Response time: < 100ms
- No database queries in current implementation
- Ready for async DB integration

---

## Versioning

- API Version: v1
- Contract Version: 1.0.0 (Day 2)
- Last Updated: 2025-11-16
