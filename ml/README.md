# ML Pipeline: Demand Prediction & Product Recommendation

## 📋 Overview

ML pipeline gồm 5 bước chính:

1. **Data Extraction** - Lấy dữ liệu từ DWH
2. **Data Preparation** - Clean, preprocess, feature engineering
3. **Model Training** - Train nhiều models
4. **Model Evaluation** - So sánh và chọn best model
5. **Model Serving** - Deploy API & monitoring

## 🚀 Quick Start

### Setup

```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Setup configuration
# Edit config.yaml with your DWH connection details
```

### Run Pipeline

```bash
# Option 1: Run full pipeline
python 1_data_extraction.py
python 2_data_preparation.py
python 3_model_training.py
python 4_model_evaluation.py

# Option 2: Run with script (Unix/Linux)
chmod +x run_pipeline.sh
./run_pipeline.sh all

# Option 3: Run individual steps
./run_pipeline.sh extract
./run_pipeline.sh prepare
./run_pipeline.sh train
./run_pipeline.sh evaluate
```

## 📊 Pipeline Details

### Step 1: Data Extraction

**Input:** DWH Tables
- `dwh_fact_product_daily` - Dữ liệu bán hàng hàng ngày
- `dwh_fact_review_summary` - Dữ liệu review

**Output:**
- `data/demand_prediction/raw_demand_data.csv` - Raw data cho demand prediction
- `data/product_recommendation/raw_recommendation_data.csv` - Raw data cho recommendation

```bash
python 1_data_extraction.py
```

### Step 2: Data Preparation

**Features:**
- Missing value handling
- Outlier removal (IQR method)
- Feature engineering:
  - Moving averages (7-day, 30-day)
  - Lag features
  - Price features
  - Rating features

**Output:**
- `data/demand_prediction/train_demand_data.csv` - Training data
- `data/demand_prediction/test_demand_data.csv` - Test data
- `data/product_recommendation/prepared_recommendation_data.csv` - Prepared data

```bash
python 2_data_preparation.py
```

### Step 3: Model Training

**Demand Prediction Models:**
- XGBoost
- RandomForest
- LightGBM
- Linear Regression
- Ridge Regression

**Recommendation Models:**
- KMeans Clustering
- Nearest Neighbors

**Metrics:**
- MAE, RMSE, MAPE, R² (Demand)
- Silhouette Score, Davies-Bouldin Index (Recommendation)

**Output:**
- `models/ml-models/*.pkl` - Trained models
- `logs/metrics/demand_results.json` - Demand model results
- `logs/metrics/recommendation_results.json` - Recommendation model results

```bash
python 3_model_training.py
```

### Step 4: Model Evaluation

**Functionality:**
- Rank models by metrics
- Create comparison report
- Select best model

**Output:**
- `logs/metrics/model_selection_summary.json` - Best model selection
- `logs/metrics/model_comparison_report.txt` - Detailed report

```bash
python 4_model_evaluation.py
```

### Step 5: Model Serving

**API Endpoints:**

```bash
# Health check
GET /health

# Demand prediction
POST /predict/demand
{
  "product_id": 123,
  "price_current": 100000,
  "price_original": 150000,
  "discount_pct": 33.33,
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
}

# Batch demand predictions
POST /predict/batch-demand
[{...}, {...}]

# Product recommendation
POST /predict/recommendation
{
  "product_id": 123,
  "category_id": 5,
  "num_recommendations": 5
}

# Monitoring metrics
GET /monitoring/metrics
```

**Start API:**

```bash
# Default: localhost:5000
python 5_model_serving.py

# Custom host/port
python 5_model_serving.py --host 0.0.0.0 --port 8000
```

**Test API:**

```bash
# Health check
curl http://localhost:5000/health

# Get metrics
curl http://localhost:5000/monitoring/metrics

# Predict demand
curl -X POST "http://localhost:5000/predict/demand" \
  -H "Content-Type: application/json" \
  -d '{...}'
```

## 📁 Directory Structure

```
ml/
├── config.yaml                  # Configuration
├── requirements.txt             # Dependencies
├── 1_data_extraction.py        # Data extraction
├── 2_data_preparation.py       # Data preparation
├── 3_model_training.py         # Model training
├── 4_model_evaluation.py       # Model evaluation
├── 5_model_serving.py          # API serving
├── run_pipeline.sh             # Pipeline executor
├── utils/
│   ├── __init__.py
│   ├── db_connector.py         # DWH connection
│   ├── logger.py               # Logging
│   └── metrics.py              # Evaluation metrics
├── data/
│   ├── demand_prediction/
│   │   ├── raw_demand_data.csv
│   │   ├── train_demand_data.csv
│   │   └── test_demand_data.csv
│   ├── product_recommendation/
│   │   ├── raw_recommendation_data.csv
│   │   └── prepared_recommendation_data.csv
│   └── predictions/            # API predictions
├── models/
│   └── ml-models/
│       ├── demand_xgboost.pkl
│       ├── demand_random_forest.pkl
│       ├── recommendation_kmeans.pkl
│       └── recommendation_nearest_neighbors.pkl
└── logs/
    ├── ml_pipeline.log
    ├── metrics/
    │   ├── demand_results.json
    │   ├── recommendation_results.json
    │   ├── model_selection_summary.json
    │   └── model_comparison_report.txt
    └── plots/                  # Visualizations
```

## 🔧 Configuration (config.yaml)

```yaml
# Database Connection
database:
  host: ${DB_HOST}
  port: ${DB_PORT}
  database: ${DB_NAME}
  user: ${DB_USER}
  password: ${DB_PASSWORD}

# Data Extraction
data_extraction:
  demand:
    lookback_days: 180        # 6 months
    min_data_points: 30       # Min rows per product
  recommendation:
    lookback_days: 90
    min_interactions: 5       # Min reviews per product

# Data Preparation
data_preparation:
  train_test_split: 0.8
  temporal_split: true        # Time-series aware split
  test_days_forward: 30       # Test on future 30 days
  scaling: StandardScaler     # StandardScaler|MinMaxScaler|RobustScaler
  handle_missing: mean        # mean|median|forward_fill
  remove_outliers: true
  outlier_method: iqr         # iqr|zscore

# Model Training
model_training:
  demand_models:
    - XGBRegressor: {...}
    - RandomForestRegressor: {...}
    - LGBMRegressor: {...}
    - LinearRegression: {}
    - Ridge: {...}
  
  recommendation_models:
    - KMeans: {...}
    - NearestNeighbors: {...}
  
  n_jobs: -1                  # Use all cores
  cv_folds: 5

# Model Serving
model_serving:
  api:
    host: 0.0.0.0
    port: 5000
    workers: 4
    timeout: 30
```

## 📈 Model Performance Metrics

### Demand Prediction
- **MAE** (Mean Absolute Error) - Lỗi trung bình tuyệt đối
- **RMSE** (Root Mean Squared Error) - Căn bậc hai sai số bình phương trung bình
- **MAPE** (Mean Absolute Percentage Error) - Lỗi phần trăm trung bình
- **R²** (Coefficient of Determination) - Mức độ phù hợp của mô hình

### Product Recommendation
- **Silhouette Score** - Chất lượng clustering (0-1, cao hơn tốt hơn)
- **Davies-Bouldin Index** - Chất lượng clustering (thấp hơn tốt hơn)
- **Precision@K** - Độ chính xác top K recommendations
- **Recall@K** - Nhớ lại top K recommendations

## 🔍 Monitoring & Maintenance

### Drift Detection
- Monitor data distribution changes
- Alert if drift detected (threshold: 5%)

### Data Quality
- Missing value checks
- Outlier detection
- Data validation

### Performance Monitoring
- Log predictions every 24 hours
- Alert if MAE/RMSE exceeds threshold
- Track model performance over time

### Logs
```bash
# View logs
tail -f logs/ml_pipeline.log

# View training logs
tail -f logs/data_extraction.log
tail -f logs/data_preparation.log
tail -f logs/model_training.log
```

## 🐛 Troubleshooting

### Database Connection Failed
- Kiểm tra `.env` file
- Verify database credentials
- Test connection: `python -c "from utils.db_connector import DWHConnector; DWHConnector()"`

### Out of Memory
- Giảm batch size
- Giảm lookback days
- Giảm n_estimators trong model config

### Model Not Found When Serving
- Verify model files exist in `models/ml-models/`
- Run training step first: `python 3_model_training.py`

### API Connection Issues
- Check port availability: `netstat -an | grep 5000`
- Change port: `python 5_model_serving.py --port 8000`

## 📚 References

- [Demand Forecasting Best Practices](https://docs.example.com)
- [Recommendation Systems](https://docs.example.com)
- [XGBoost Documentation](https://xgboost.readthedocs.io)
- [Scikit-learn Guide](https://scikit-learn.org)

## 📞 Support

Issues? Check logs trong `logs/` directory hoặc tìm hiểu error messages.

## 📝 License

This project is part of the ecommerce DSS system.
