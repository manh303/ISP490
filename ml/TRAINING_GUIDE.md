# ML Training Pipeline Guide

## Tổng Quan

Pipeline này huấn luyện 3 mô hình ML chính:
1. **Sentiment Classification** - Phân loại cảm xúc từ review
2. **Product Clustering** - Phân cụm sản phẩm theo đặc tính
3. **Demand Prediction** - Dự đoán nhu cầu (tuỳ chọn)

---

## Cấu Trúc File

```
ml/
├── 1_data_extraction.py           # Trích xuất dữ liệu từ DWH
├── train_sentiment_classifier.py  # Train sentiment model
├── train_product_clustering.py    # Train clustering model
├── ml_pipeline.py                 # Orchestrate pipeline
├── config.yaml                    # Configuration
└── TRAINING_GUIDE.md              # Hướng dẫn này
```

---

## Cách Chạy Pipeline

### 1. Chạy Toàn Bộ Pipeline (Recommended)

```bash
cd ml/
python ml_pipeline.py
```

Điều này sẽ chạy các bước theo thứ tự:
1. Data Extraction
2. Train Sentiment Classifier
3. Train Product Clustering

### 2. Chạy Từng Bước Riêng Lẻ

**Trích xuất dữ liệu:**
```bash
python 1_data_extraction.py
```

**Train Sentiment Classifier:**
```bash
python train_sentiment_classifier.py
```

**Train Product Clustering:**
```bash
python train_product_clustering.py
```

---

## Chi Tiết Từng Mô Hình

### Mô Hình 1: Sentiment Classification

**File:** `train_sentiment_classifier.py`

**Input Data Source:**
- Table: `dwh.dim_review`
- Columns: review_id, review_text, rating, created_at
- Filter: review_length > 10 chars, limit 5000

**Features:**
- Text features: TF-IDF (100 features)
- Numeric features: rating, review_length, word_count, exclamation_count, question_count

**Model:**
- Algorithm: Random Forest Classifier
- n_estimators: 100
- max_depth: 20
- Classes: positive (rating >= 4.0), negative (rating <= 2.0), neutral

**Output Files:**
```
models/ml-models/
├── sentiment_classifier.pkl              # Trained model
├── sentiment_tfidf_vectorizer.pkl       # Text vectorizer
└── sentiment_label_encoder.pkl          # Label encoder

logs/metrics/
└── sentiment_metrics.json               # Performance metrics
```

**Expected Performance:**
- Accuracy: ~86%
- Precision: ~86%
- Recall: ~87%
- F1-Score: ~86%

**Usage in API:**
```bash
POST /api/v1/ml/analyze/sentiment
{
  "review_id": 456,
  "product_id": 123,
  "review_text": "Sản phẩm tốt, giao hàng nhanh!",
  "rating": 4.5,
  "review_length": 30
}
```

---

### Mô Hình 2: Product Clustering

**File:** `train_product_clustering.py`

**Input Data Source:**
- Table: `dwh.fact_review_daily_agg`
- Columns: global_product_id, avg_price, avg_rating, sentiment_score, etc.
- Filter: agg_date >= 90 days ago, active_days >= 7, limit 5000

**Features:**
- log_avg_price: Log-transformed average price
- price_range: max_price - min_price
- avg_rating: Average product rating
- sentiment_score: Positive reviews / total reviews
- log_engagement: Log-transformed reviews per day
- review_quality_score: Review quality metric

**Model:**
- Algorithm: KMeans
- Optimal K: Auto-detected (2-10 clusters)
- Evaluation: Silhouette Score, Davies-Bouldin Index

**Output Files:**
```
models/ml-models/
├── recommendation_kmeans.pkl            # Trained model
├── clustering_scaler.pkl                # Feature scaler
└── clustering_features.pkl              # Feature column names

logs/metrics/
└── clustering_metrics.json              # Performance metrics
```

**Expected Performance:**
- Silhouette Score: ~0.5-0.7
- Optimal K: 3-5 clusters

**Usage in API:**
```bash
POST /api/v1/ml/segment/products
{
  "product_id": 123,
  "num_segments": 3
}
```

---

## Data Quality Checks

Mỗi bước training sẽ check:

1. **Missing Values**
   ```
   Missing avg_price: 0
   Missing rating: 0
   ```

2. **Data Distribution**
   ```
   Sentiment distribution:
   - positive: 60%
   - neutral: 25%
   - negative: 15%
   ```

3. **Feature Statistics**
   ```
   Avg review length: 250 chars
   Avg rating: 3.8/5.0
   ```

---

## Configuration

File: `config.yaml`

**Sentiment Analysis Config:**
```yaml
sentiment:
  lookback_days: 90        # 3 months of review data
  output_dir: data/sentiment_analysis
  min_review_length: 10    # Minimum characters
  batch_size: 5000
```

**Product Clustering Config:**
```yaml
product_clustering:
  lookback_days: 90        # 3 months of product data
  output_dir: data/product_clustering
  min_active_days: 7       # Product must be active 7+ days
  batch_size: 5000
```

---

## Output Structure

```
ml/
├── data/
│   ├── sentiment_analysis/
│   │   └── raw_sentiment_data.csv
│   ├── product_clustering/
│   │   └── raw_clustering_data.csv
│   └── demand_prediction/
│       └── raw_demand_data.csv
│
├── models/
│   └── ml-models/
│       ├── sentiment_classifier.pkl
│       ├── sentiment_tfidf_vectorizer.pkl
│       ├── sentiment_label_encoder.pkl
│       ├── recommendation_kmeans.pkl
│       ├── clustering_scaler.pkl
│       └── clustering_features.pkl
│
└── logs/
    ├── ml_pipeline.log
    └── metrics/
        ├── sentiment_metrics.json
        └── clustering_metrics.json
```

---

## Troubleshooting

### Lỗi: "Database connection failed"
- Kiểm tra `config.yaml` - cập nhật host, port, credentials
- Đảm bảo PostgreSQL DWH đang chạy

### Lỗi: "No data extracted"
- Kiểm tra bảng DWH có dữ liệu không
- Điều chỉnh `lookback_days` trong config

### Lỗi: "Model file not found"
- Đảm bảo `models/ml-models/` directory tồn tại
- Kiểm tra đủ disk space

### Performance Thấp
- Điều chỉnh model hyperparameters trong `config.yaml`
- Tăng `lookback_days` để có nhiều training data hơn
- Kiểm tra feature engineering logic

---

## Best Practices

1. **Định kỳ Retrain**
   - Retrain mỗi tháng với dữ liệu mới
   - Monitor drift detection metrics

2. **Version Control Models**
   - Giữ history các version cũ
   - So sánh performance giữa versions

3. **Data Validation**
   - Luôn check data quality trước training
   - Đảm bảo balanced dataset cho classification

4. **Monitor Predictions**
   - Track prediction accuracy in production
   - Alert nếu accuracy drop > threshold

---

## API Integration

Sau khi training, models tự động available tại:

```
GET /api/v1/ml/models                 # List all models
GET /api/v1/ml/models/{id}/metrics    # Model metrics
POST /api/v1/ml/analyze/sentiment     # Single sentiment
POST /api/v1/ml/analyze/batch-sentiment # Batch sentiment
POST /api/v1/ml/segment/products      # Product segmentation
```

---

## Monitoring

### Logs
```
logs/ml_pipeline.log              # Training logs
logs/metrics/sentiment_metrics.json    # Sentiment metrics
logs/metrics/clustering_metrics.json   # Clustering metrics
```

### Metrics Được Lưu
```json
{
  "accuracy": 0.865,
  "precision": 0.858,
  "recall": 0.872,
  "f1_score": 0.865,
  "roc_auc": 0.92
}
```

---

## Next Steps

1. Chạy `python ml_pipeline.py` để train tất cả models
2. Kiểm tra output files tại `models/ml-models/`
3. Verify models loaded tại `/api/v1/ml/health`
4. Test API endpoints tại `/api/v1/ml/analyze/sentiment`
