# ML Pipeline Quick Start Guide

## 🚀 Chạy Nhanh

### 1. Apply Airflow Optimization (QUAN TRỌNG!)

```bash
# Source env vars
source airflow/config/airflow_env_optimization.sh

# Restart Airflow
docker-compose restart airflow-scheduler airflow-webserver
```

### 2. Tạo Spark Jobs Pool

```bash
# Login vào Airflow
docker exec -it airflow-webserver bash

# Tạo pool
airflow pools set spark_jobs 2 "Pool for Spark jobs"

# Exit
exit
```

### 3. Trigger ML Pipeline

```bash
# Trigger DAG
docker exec airflow-webserver \
  airflow dags trigger ml_training_inference_pipeline

# Monitor logs
docker logs airflow-scheduler -f
```

### 4. Kiểm Tra Kết Quả

```bash
# Check models trained
ls -lh ml/models/

# Should see:
# sentiment_tfidf_logreg_v1.0.pkl
# content_recommender_tfidf_v1.0.pkl
# price_forecast_rf_v1.0.pkl

# Check database results
docker exec postgres psql -U dss_user -d ecommerce_dss -c \
  "SELECT * FROM ml.dim_ml_model;"

docker exec postgres psql -U dss_user -d ecommerce_dss -c \
  "SELECT COUNT(*) FROM ml.fact_review_sentiment;"
```

---

## 📋 Thay Đổi Chính

### ✅ ML Training DAG

**File:** `airflow/dags/ml_training_pipeline_dag.py`

**Changes:**
- Đổi tên DAG: `ml_training_inference_pipeline`
- Schedule: `0 3 * * *` (3AM daily)
- Sử dụng đúng 6 file ML:
  1. `train_sentiment_model.py`
  2. `train_recommender.py`
  3. `train_price_model.py`
  4. `run_sentiment_batch.py`
  5. `run_recommendations.py`
  6. `run_price_predictions.py`

**Flow:**
```
Setup → Check DWH Data → 
  Train 3 Models (Parallel) → Validate Models →
  Run 3 Inferences (Parallel) → Validate Results →
  Notify
```

### ✅ MinIO Pipeline DAG Fix

**File:** `airflow/dags/minio_pipeline_dag.py`

**Changes:**
- Thêm `execution_timeout=timedelta(hours=2)` cho Spark task
- Thêm `pool="spark_jobs"` để control concurrency

### ✅ Spark Job Optimization

**File:** `data-pipeline/src/spark_jobs/load_cleaned_from_minio.py`

**Changes:**
- Commit theo batch 1000 rows thay vì 1 lần
- Thêm progress logging
- Giảm database lock time

---

## 🔧 Troubleshooting

### Lỗi: "Job heartbeat got an exception"

**Giải pháp:**
```bash
# 1. Apply optimization env vars
source airflow/config/airflow_env_optimization.sh

# 2. Restart Airflow
docker-compose restart airflow-scheduler airflow-webserver

# 3. Check database connections
docker exec postgres psql -U dss_user -d ecommerce_dss -c \
  "SELECT count(*), state FROM pg_stat_activity GROUP BY state;"
```

### Lỗi: ML models không được tạo

**Check:**
```bash
# 1. Verify DWH có data
docker exec postgres psql -U dss_user -d ecommerce_dss -c \
  "SELECT COUNT(*) FROM dwh.fact_review;"

# 2. Check DATABASE_URL env var
docker exec airflow-scheduler printenv | grep DATABASE_URL

# 3. Check logs
docker logs airflow-scheduler -f | grep "train_sentiment"
```

### Lỗi: Task timeout

**Giải pháp:**
```python
# Tăng timeout trong task definition
task = BashOperator(
    task_id="...",
    bash_command="...",
    execution_timeout=timedelta(hours=2),  # Tăng timeout
)
```

---

## 📊 Expected Output

### Phase 1: Training (5-10 phút)
```
[INFO] train_sentiment_model: ✅ Complete
[INFO] train_recommender_model: ✅ Complete
[INFO] train_price_model: ✅ Complete
[INFO] Registered 3 models in ml.dim_ml_model
```

### Phase 2: Inference (10-15 phút)
```
[INFO] run_sentiment_batch: ✅ Scored 10,000 reviews
[INFO] run_recommendations: ✅ Generated 500,000 recommendations
[INFO] run_price_predictions: ✅ Predicted 50,000 prices
```

### Phase 3: Validation
```
[INFO] Models: 3/3 validated ✅
[INFO] Results: All fact tables populated ✅
```

---

## 📖 Chi Tiết

Xem file `ML_DAG_REFACTOR_SUMMARY.md` để biết thêm chi tiết về:
- Tất cả thay đổi
- Kiến trúc hệ thống
- Monitoring & troubleshooting
- Best practices

Xem file `airflow/config/AIRFLOW_OPTIMIZATION_GUIDE.md` để biết:
- Cấu hình Airflow chi tiết
- Database connection pooling
- Performance tuning
- Health check scripts

---

## ✅ Checklist

- [ ] Apply Airflow optimization env vars
- [ ] Create spark_jobs pool
- [ ] Restart Airflow services
- [ ] Trigger ML DAG
- [ ] Check models created
- [ ] Check ML results in database
- [ ] Monitor logs for errors
- [ ] Test MinIO DAG if needed

---

**Need Help?** Check `ML_DAG_REFACTOR_SUMMARY.md` or `airflow/config/AIRFLOW_OPTIMIZATION_GUIDE.md`

