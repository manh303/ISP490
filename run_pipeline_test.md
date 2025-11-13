# Testing ML Models - Step by Step

## Current Status
✅ ML models created and deployed to spark-master
✅ DAG updated with ML tasks
❌ DWH/Datamart tables not yet created (need to run full pipeline)

## Steps to Test

### 1. Run Full Pipeline in Airflow
```bash
# Access Airflow UI
http://localhost:8080

# Trigger the DAG manually:
# 1. Go to DAGs page
# 2. Find "tiki_lazada_pipeline"
# 3. Click the "Play" button to trigger
# 4. Monitor progress in Graph view
```

### 2. Pipeline Flow
```
Crawlers (Tiki + Lazada)
  ↓
STG (Staging tables)
  ↓
ODS (Operational Data Store - Spark)
  ↓
DWH (Data Warehouse - Spark)
  ↓
Datamart (Analytics marts - Spark)
  ↓
ML Models (4 models in parallel)
  - Product Recommendation
  - Price Optimization
  - Demand Forecasting
  - Sales Forecasting
```

### 3. Check Results After Pipeline Completes

#### Option A: Test Locally
```bash
python test_ml_local.py
```

#### Option B: Check Database Directly
```python
import psycopg2
conn = psycopg2.connect(
    host="dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com",
    port=5432,
    database="ecommerce_dss",
    user="dss_user",
    password="IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4"
)
cur = conn.cursor()

# Check ML output tables
tables = [
    "mart_product_recommendations",
    "mart_price_optimization", 
    "mart_demand_forecast",
    "mart_sales_forecast_weekly",
    "mart_sales_trend",
    "mart_seasonality"
]

for table in tables:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"{table}: {cur.fetchone()[0]} rows")
```

#### Option C: Check in Spark Container
```bash
docker exec -it spark-master bash

# Run individual ML model
spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/product_recommendation.py \
  --pg-url jdbc:postgresql://dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com:5432/ecommerce_dss \
  --pg-user dss_user \
  --pg-pass IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4
```

### 4. Expected Output Tables

**mart_product_recommendations**
- source_product (VARCHAR)
- recommended_product (VARCHAR)
- recommended_product_sk (INT)
- score (FLOAT)

**mart_price_optimization**
- product_sk (INT)
- product_name (VARCHAR)
- current_price (DECIMAL)
- optimal_price (DECIMAL)
- expected_margin_change (FLOAT)
- recommendation (VARCHAR)
- price_position (VARCHAR)

**mart_demand_forecast**
- product_sk (INT)
- product_name (VARCHAR)
- recent_demand (FLOAT)
- baseline_demand (FLOAT)
- demand_trend (VARCHAR)
- forecast_7d (FLOAT)
- forecast_30d (FLOAT)
- quality_score (FLOAT)
- stock_recommendation (VARCHAR)

**mart_sales_forecast_weekly**
- year_num (INT)
- day_of_week (INT)
- avg_weekly_reviews (FLOAT)
- avg_weekly_rating (FLOAT)

**mart_sales_trend**
- year_num (INT)
- month_num (INT)
- total_monthly_reviews (BIGINT)
- avg_monthly_rating (FLOAT)
- prev_month_reviews (BIGINT)
- growth_rate (FLOAT)
- trend (VARCHAR)

**mart_seasonality**
- season (VARCHAR)
- avg_seasonal_reviews (FLOAT)
- avg_seasonal_rating (FLOAT)
- seasonality_index (FLOAT)

### 5. Troubleshooting

If pipeline fails at ML stage:
```bash
# Check Spark logs
docker logs spark-master

# Check Airflow task logs
# In Airflow UI → DAG → Task → Logs

# Verify ML files exist
docker exec spark-master ls -la /app/src/ml_models/

# Test database connection from Spark
docker exec spark-master python -c "import psycopg2; conn=psycopg2.connect(host='dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com',port=5432,database='ecommerce_dss',user='dss_user',password='IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4'); print('OK')"
```

## Next Steps After Testing

1. Create FastAPI endpoints to serve ML results
2. Build Grafana dashboards for visualization
3. Add real-time prediction endpoints
4. Implement model retraining schedule
