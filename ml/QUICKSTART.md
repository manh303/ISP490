# ML Pipeline Quick Start

## Chạy Pipeline Ngay

### 1. Setup Environment
```bash
cd ml
pip install -r requirements.txt
```

### 2. Tùy chọn: Tạo Sample Data (nếu DWH chưa sẵn)
```bash
python demo_generate_sample_data.py
```

### 3. Chạy Full Pipeline
```bash
python 2_data_preparation.py
python 3_model_training.py
python 4_model_evaluation.py
python 5_model_serving.py
```

## Kết quả

**Best Models (Sample Data):**
- Demand Prediction: **Ridge Regression** (R² = 0.1566)
- Product Recommendation: **KMeans Clustering** (Silhouette = 0.2920)

## API Usage

Khi server đang chạy (`python 5_model_serving.py`):

### Health Check
```bash
curl http://localhost:5000/health
```

### Predict Demand
```bash
curl -X POST "http://localhost:5000/predict/demand" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": 1,
    "price_current": 500000,
    "price_original": 750000,
    "discount_pct": 33,
    "rating_avg": 4.5,
    "rating_count": 100,
    "review_count": 50,
    "day_of_week": 3,
    "month": 11,
    "year": 2025,
    "price_change_pct": 0.5,
    "sold_ma7": 10.5,
    "sold_ma30": 8.3,
    "sold_std7": 2.1,
    "sold_lag1": 12,
    "sold_lag7": 9.5,
    "sold_lag30": 8.0
  }'
```

### Get Recommendations
```bash
curl -X POST "http://localhost:5000/predict/recommendation" \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": 1,
    "category_id": 5,
    "num_recommendations": 5
  }'
```

### Get Metrics
```bash
curl http://localhost:5000/monitoring/metrics
```

## File Structure

```
ml/
├── demo_generate_sample_data.py    # Generate sample data
├── 1_data_extraction.py            # Extract from DWH (skip if using sample)
├── 2_data_preparation.py           # Prepare data
├── 3_model_training.py             # Train models
├── 4_model_evaluation.py           # Evaluate & select
├── 5_model_serving.py              # Start API
├── config.yaml                     # Configuration
└── logs/metrics/
    ├── demand_results.json         # Model metrics
    ├── model_selection_summary.json # Best models
    └── model_comparison_report.txt  # Detailed report
```

## Logs & Output

- Training logs: `logs/data_preparation.log`, `logs/model_training.log`
- Models: `models/ml-models/*.pkl`
- Results: `logs/metrics/`

## Note

- Sample data dùng 100 sản phẩm x 180 ngày = 18,000 records
- Actual DWH data sẽ có hàng triệu records, performance sẽ tốt hơn
- Thay đổi config.yaml để tùy chỉnh parameters
