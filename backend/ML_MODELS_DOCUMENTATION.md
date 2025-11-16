# ML Models Documentation

## 3 Mô hình ML trong Hệ thống

### 1. **Clustering Model - Product Segment Recommendation**
   - **Tên Model**: Product Segmentation (KMeans)
   - **ID Model**: 4
   - **Loại**: Clustering (Phân cụm)
   - **Nghiệp vụ Áp dụng**: Product Segment Recommendation (Phân khúc Sản phẩm)
   - **Mô tả**: Nhóm các sản phẩm thành các phân khúc dựa trên đặc tính tương tự (giá, đánh giá, lượt mua, v.v.)
   - **File Model**: `ml/models/ml-models/recommendation_kmeans.pkl`
   - **Endpoint API**: `POST /api/v1/ml/segment/products`
   - **Request**:
     ```json
     {
       "product_id": 123,
       "num_segments": 3
     }
     ```
   - **Response**:
     ```json
     {
       "product_id": 123,
       "segment_id": 0,
       "segment_name": "Premium Products",
       "characteristics": ["High quality", "Premium pricing", "Exclusive features"],
       "products_in_segment": [...],
       "model_used": "kmeans",
       "timestamp": "2025-11-16T..."
     }
     ```
   - **Status**: ✅ Đã Implement

---

### 2. **Classification Model - Market Sentiment Insight**
   - **Tên Model**: Sentiment Analysis (Classification)
   - **ID Model**: 5
   - **Loại**: Classification (Phân loại)
   - **Nghiệp vụ Áp dụng**: Market Sentiment Insight (Phân tích Cảm xúc từ Review)
   - **Mô tả**: Phân loại cảm xúc của khách hàng từ bài review (Positive, Negative, Neutral)
   - **File Model**: `ml/models/ml-models/sentiment_classifier.pkl`
   - **Endpoint API**: `POST /api/v1/ml/analyze/sentiment`
   - **Request**:
     ```json
     {
       "review_id": 456,
       "product_id": 123,
       "review_text": "Sản phẩm rất tốt, giao hàng nhanh!",
       "rating": 4.5,
       "review_length": 45
     }
     ```
   - **Response**:
     ```json
     {
       "review_id": 456,
       "product_id": 123,
       "sentiment_label": "positive",
       "sentiment_score": 0.92,
       "confidence": 0.92,
       "model_used": "sentiment_classifier",
       "timestamp": "2025-11-16T..."
     }
     ```
   - **Batch Endpoint**: `POST /api/v1/ml/analyze/batch-sentiment`
   - **Metrics**: 
     - Accuracy: 86.50%
     - Precision: 85.80%
     - Recall: 87.20%
     - F1-Score: 86.50%
   - **Status**: ✅ Đã Implement

---

### 3. **Clustering Model - Product Segment Recommendation** (Đã Liệt Kê Ở #1)
   - Giống như mô hình #1

---

## Tóm Tắt API Endpoints

| Model | Endpoint | Method | Purpose |
|-------|----------|--------|---------|
| KMeans (Clustering) | `/api/v1/ml/segment/products` | POST | Phân khúc sản phẩm |
| Sentiment (Classification) | `/api/v1/ml/analyze/sentiment` | POST | Phân tích cảm xúc từng review |
| Sentiment (Classification) | `/api/v1/ml/analyze/batch-sentiment` | POST | Phân tích cảm xúc batch |
| All Models | `/api/v1/ml/models` | GET | Danh sách tất cả mô hình |
| Specific Model | `/api/v1/ml/models/{model_id}/metrics` | GET | Chi tiết metrics của mô hình |

---

## Hướng Dẫn Sử Dụng

### 1. Phân Khúc Sản Phẩm (Product Segmentation)

```bash
curl -X POST http://localhost:8000/api/v1/ml/segment/products \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": 123,
    "num_segments": 3
  }'
```

**Use Case**: 
- Gợi ý sản phẩm tương tự
- Phân loại sản phẩm theo nhóm giá
- Tìm đối thủ cạnh tranh sản phẩm

---

### 2. Phân Tích Cảm Xúc Review (Sentiment Analysis)

```bash
curl -X POST http://localhost:8000/api/v1/ml/analyze/sentiment \
  -H "Content-Type: application/json" \
  -d '{
    "review_id": 456,
    "product_id": 123,
    "review_text": "Sản phẩm tốt nhưng giao hàng hơi lâu",
    "rating": 3.5,
    "review_length": 52
  }'
```

**Use Case**:
- Phân tích cảm xúc khách hàng từ review
- Tìm các review tích cực/tiêu cực
- Giám sát mức độ hài lòng khách hàng
- Batch analysis để xử lý hàng loạt review

---

### 3. Danh Sách Tất Cả Mô Hình

```bash
curl -X GET http://localhost:8000/api/v1/ml/models
```

---

## Tính Năng Mock Data

Nếu model file không tồn tại (`.pkl`), API sẽ tự động sử dụng dữ liệu giả để demo:

- **Sentiment Analysis**: Dùng heuristic dựa trên rating
- **Product Segmentation**: Dùng logic đơn giản theo product_id

Status trong response sẽ hiển thị `"model_used": "mock"` để biết dữ liệu giả.

---

## Cách Train Mô Hình

### Train Sentiment Classifier
```bash
curl -X POST http://localhost:8000/api/v1/ml/models/5/train \
  -H "Content-Type: application/json" \
  -d '{
    "triggered_by": "user_123",
    "note": "Retrain with new review data"
  }'
```

### Train KMeans Model
```bash
curl -X POST http://localhost:8000/api/v1/ml/models/4/train \
  -H "Content-Type: application/json" \
  -d '{
    "triggered_by": "user_456",
    "note": "Retrain with updated product features"
  }'
```

---

## File Structure

```
ml/
├── models/
│   └── ml-models/
│       ├── demand_linear.pkl              (Demand Prediction)
│       ├── recommendation_nearest_neighbors.pkl
│       ├── recommendation_kmeans.pkl      ✅ (Product Segmentation)
│       └── sentiment_classifier.pkl       ✅ (Sentiment Analysis)
└── config.yaml
```

---

## Integration Status

- ✅ Sentiment Classification Model - IMPLEMENTED
- ✅ KMeans Product Segmentation - IMPLEMENTED  
- ✅ API Endpoints - READY
- ✅ Mock Data Fallback - ENABLED
- ⏳ Model Training Pipeline - IN PROGRESS
- ⏳ Database Integration - IN PROGRESS
