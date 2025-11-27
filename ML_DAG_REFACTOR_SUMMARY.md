# ML DAG Refactor & MinIO Pipeline Fix - Summary

## 📋 Tổng Quan

Đã hoàn thành:
1. ✅ Refactor ML Training Pipeline DAG để phù hợp với 6 file ML thực tế
2. ✅ Fix lỗi database connection timeout trong MinIO Pipeline DAG
3. ✅ Tối ưu Spark job để giảm thời gian transaction
4. ✅ Tạo document hướng dẫn tối ưu Airflow

---

## 🎯 Vấn Đề 1: ML Training DAG Chưa Đúng

### ❌ Vấn đề cũ
- DAG cũ (`ml_training_pipeline_dag.py`) không khớp với 6 file ML thực tế
- Gọi các file không tồn tại: `1_data_extraction.py`, `train_sentiment_classifier.py`, `train_product_clustering.py`, `test_models.py`
- Không có phase inference/batch scoring

### ✅ Giải pháp mới

Đã refactor thành `ml_training_inference_pipeline` với flow đúng:

```
Phase 0: Setup & Validation
  └── setup_directories
  └── check_dwh_data

Phase 1: Model Training (Parallel)
  ├── train_sentiment_model.py      → sentiment_tfidf_logreg_v1.0.pkl
  ├── train_recommender.py           → content_recommender_tfidf_v1.0.pkl
  └── train_price_model.py           → price_forecast_rf_v1.0.pkl

Phase 2: Batch Inference (Parallel)
  ├── run_sentiment_batch.py         → ml.fact_review_sentiment
  ├── run_recommendations.py         → ml.fact_product_recommendation
  └── run_price_predictions.py       → ml.fact_price_prediction

Phase 3: Validation & Notification
  └── validate_ml_results
  └── notify_completion
```

### 📁 File Changes

#### 1. `airflow/dags/ml_training_pipeline_dag.py` (Refactored)

**Key Changes:**
- ✅ Đổi tên DAG: `ml_training_pipeline` → `ml_training_inference_pipeline`
- ✅ Schedule: `0 3 * * *` (3AM daily, sau khi DWH pipeline chạy xong lúc 2AM)
- ✅ Sử dụng đúng 6 file ML thực tế
- ✅ Thêm phase validation DWH data trước khi train
- ✅ Thêm phase batch inference sau khi train
- ✅ Pass environment variables (`DATABASE_URL`, `ML_MODEL_DIR`)

**Training Tasks:**
```python
train_sentiment = BashOperator(
    task_id='train_sentiment_model',
    bash_command='cd /app/ml && python train_sentiment_model.py',
    env={
        'PYTHONPATH': '/app/ml',
        'ML_MODEL_DIR': '/app/ml/models',
        'DATABASE_URL': os.getenv('DATABASE_URL', ''),
    },
)

train_recommender = BashOperator(...)
train_price = BashOperator(...)
```

**Inference Tasks:**
```python
run_sentiment_batch = BashOperator(
    task_id='run_sentiment_batch',
    bash_command='cd /app/ml && python run_sentiment_batch.py',
    env={...},
)

run_recommendations = BashOperator(...)
run_price_predictions = BashOperator(...)
```

**Validation:**
```python
def check_dwh_data(**context):
    """Check if DWH has sufficient data"""
    # Check fact_review (min 100 rows)
    # Check dim_product (min 50 rows)
    # Check fact_product_daily (min 100 rows)

def validate_trained_models(**context):
    """Validate model files exist"""
    # Check .pkl files in /app/ml/models

def validate_ml_results(**context):
    """Validate ML results in database"""
    # Check ml.dim_ml_model
    # Check ml.fact_review_sentiment
    # Check ml.fact_product_recommendation
    # Check ml.fact_price_prediction
```

---

## 🎯 Vấn Đề 2: MinIO Pipeline Lỗi Database Connection Timeout

### ❌ Vấn đề

```
[2025-11-23, 23:27:12 +07] {job.py:218} ERROR - Job heartbeat got an exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.9/site-packages/sqlalchemy/engine/base.py", line 3371, in _wrap_pool_connect
    return fn()
  ...
```

**Nguyên nhân:**
1. Spark job chạy quá lâu khi load 54,679 products vào `fact_product_daily`
2. Long-running database transaction (commit 1 lần toàn bộ 54K rows)
3. Airflow scheduler không thể heartbeat vì connection pool bị exhaust
4. Default job heartbeat interval (5s) quá ngắn cho Spark job

### ✅ Giải pháp

#### 1. Tối ưu Spark Job (`load_cleaned_from_minio.py`)

**Before:**
```python
# ❌ Commit 1 lần toàn bộ → long-running transaction
cur = conn.cursor()
execute_batch(cur, insert_fact_sql, rows, page_size=1000)
conn.commit()  # Lock database trong thời gian dài
cur.close()
```

**After:**
```python
# ✅ Commit theo batch nhỏ → giải phóng connection nhanh hơn
cur = conn.cursor()
batch_size = 1000
total_rows = len(rows)

for i in range(0, total_rows, batch_size):
    batch = rows[i:i+batch_size]
    execute_batch(cur, insert_fact_sql, batch, page_size=500)
    conn.commit()  # Commit từng batch
    if (i + batch_size) % 5000 == 0:
        print(f"  [PROGRESS] Loaded {min(i+batch_size, total_rows)}/{total_rows} rows...")

cur.close()
```

**Lợi ích:**
- Giảm database lock time
- Giải phóng connection nhanh hơn
- Progress logging để monitor
- Tránh connection pool exhaustion

#### 2. Tăng Timeout cho Spark Task (`minio_pipeline_dag.py`)

**Before:**
```python
spark_build_star_dwh = BashOperator(
    task_id="spark_build_star_dwh",
    bash_command="""...""",
)
```

**After:**
```python
spark_build_star_dwh = BashOperator(
    task_id="spark_build_star_dwh",
    bash_command="""...""",
    execution_timeout=timedelta(hours=2),  # ✅ Tăng timeout
    pool="spark_jobs",  # ✅ Sử dụng pool riêng
)
```

#### 3. Airflow Configuration Optimization

**File mới tạo:**
- `airflow/config/AIRFLOW_OPTIMIZATION_GUIDE.md` - Hướng dẫn chi tiết
- `airflow/config/airflow_env_optimization.sh` - Script set env vars

**Key Settings:**

```bash
# Scheduler
export AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC=30
export AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=30
export AIRFLOW__SCHEDULER__MAX_ACTIVE_TASKS_PER_DAG=16

# Database Connection Pool
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=10
export AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=20
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE=1800
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_PRE_PING=True

# Core
export AIRFLOW__CORE__PARALLELISM=32
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
```

---

## 📦 File Changes Summary

### ✅ Modified Files

1. **`airflow/dags/ml_training_pipeline_dag.py`**
   - Refactored toàn bộ DAG
   - Đổi tên: `ml_training_inference_pipeline`
   - Sử dụng đúng 6 file ML thực tế
   - Thêm validation và inference phases

2. **`airflow/dags/minio_pipeline_dag.py`**
   - Thêm `execution_timeout=timedelta(hours=2)` cho Spark task
   - Thêm `pool="spark_jobs"` để control concurrency

3. **`data-pipeline/src/spark_jobs/load_cleaned_from_minio.py`**
   - Tối ưu `load_fact_product_daily()` function
   - Commit theo batch nhỏ thay vì 1 lần
   - Thêm progress logging

### ✅ New Files

1. **`airflow/config/AIRFLOW_OPTIMIZATION_GUIDE.md`**
   - Hướng dẫn chi tiết tối ưu Airflow
   - Troubleshooting guide
   - Monitoring commands
   - Best practices

2. **`airflow/config/airflow_env_optimization.sh`**
   - Script để set env vars tối ưu
   - Dễ dàng apply settings

3. **`ML_DAG_REFACTOR_SUMMARY.md`** (file này)
   - Tổng hợp tất cả thay đổi

---

## 🚀 Cách Áp Dụng

### Bước 1: Apply Airflow Optimization

```bash
# Option 1: Source script
source airflow/config/airflow_env_optimization.sh

# Option 2: Add to docker-compose.yml
# Thêm env vars vào airflow-scheduler và airflow-webserver services

# Option 3: Add to .env file
cat airflow/config/airflow_env_optimization.sh >> .env
```

### Bước 2: Create Airflow Pool

```bash
# Login vào Airflow container
docker exec -it airflow-webserver bash

# Tạo pool
airflow pools set spark_jobs 2 "Pool for Spark jobs"

# Hoặc qua UI: Admin → Pools → Create
```

### Bước 3: Restart Airflow Services

```bash
docker-compose restart airflow-scheduler airflow-webserver
```

### Bước 4: Test ML Pipeline

```bash
# Trigger ML DAG manually
docker exec airflow-webserver \
  airflow dags trigger ml_training_inference_pipeline

# Monitor logs
docker logs airflow-scheduler -f

# Check task status
docker exec airflow-webserver \
  airflow tasks list ml_training_inference_pipeline
```

### Bước 5: Test MinIO Pipeline

```bash
# Trigger MinIO DAG
docker exec airflow-webserver \
  airflow dags trigger minio_ecommerce_dwh_pipeline

# Monitor Spark job
docker logs spark-master -f

# Check database progress
docker exec -it postgres psql -U dss_user -d ecommerce_dss_1 -c \
  "SELECT COUNT(*) FROM dwh.fact_product_daily;"
```

---

## 🔍 Monitoring & Troubleshooting

### Check DAG Status

```bash
# List all DAGs
docker exec airflow-webserver airflow dags list

# Check DAG structure
docker exec airflow-webserver \
  airflow dags show ml_training_inference_pipeline

# List tasks
docker exec airflow-webserver \
  airflow tasks list ml_training_inference_pipeline
```

### Monitor Logs

```bash
# Airflow scheduler logs
docker logs airflow-scheduler -f | grep "heartbeat"

# Spark logs
docker logs spark-master -f

# Database logs (nếu dùng Docker)
docker logs postgres -f
```

### Check Database

```sql
-- Check ML models registered
SELECT * FROM ml.dim_ml_model ORDER BY trained_at DESC;

-- Check sentiment predictions
SELECT COUNT(*) FROM ml.fact_review_sentiment;

-- Check recommendations
SELECT COUNT(*) FROM ml.fact_product_recommendation;

-- Check price predictions
SELECT COUNT(*) FROM ml.fact_price_prediction;

-- Check DWH data
SELECT COUNT(*) FROM dwh.fact_product_daily;
SELECT COUNT(*) FROM dwh.fact_review;
```

### Health Check Script

```bash
# Make script executable
chmod +x airflow/config/health_check.sh

# Run health check
./airflow/config/health_check.sh
```

---

## 📊 Expected Results

### ML Pipeline

**Phase 1: Training**
- ✅ `sentiment_tfidf_logreg_v1.0.pkl` (~5-10 MB)
- ✅ `content_recommender_tfidf_v1.0.pkl` (~2-5 MB)
- ✅ `price_forecast_rf_v1.0.pkl` (~10-20 MB)

**Phase 2: Inference**
- ✅ `ml.fact_review_sentiment`: ~N rows (số lượng reviews)
- ✅ `ml.fact_product_recommendation`: ~N*10 rows (mỗi product → 10 recommendations)
- ✅ `ml.fact_price_prediction`: ~N rows (số lượng products)

**Phase 3: Validation**
- ✅ `ml.dim_ml_model`: 3 models registered
- ✅ All model files exist
- ✅ All fact tables populated

### MinIO Pipeline

**Data Quality Report:**
```
Total records: 54,679
Valid records: 52,812 (96.6%)
Missing product_name: 0
Missing/invalid price: 1,981
Missing brand: 0
```

**DWH Load:**
- ✅ `dwh.dim_date`: 13 dates
- ✅ `dwh.dim_platform`: 3 platforms (tiki, lazada, shopee)
- ✅ `dwh.dim_category`: 13 categories
- ✅ `dwh.dim_brand`: 1,246 brands
- ✅ `dwh.dim_product`: 54,679 products
- ✅ `dwh.fact_product_daily`: ~54K rows
- ✅ `dwh.fact_review`: ~N reviews
- ✅ `dwh.fact_review_daily`: ~N aggregated reviews

---

## 🎓 Lessons Learned

### 1. Database Connection Management
- ❌ **Don't:** Commit large transactions in one go
- ✅ **Do:** Commit in batches and release connections frequently

### 2. Airflow Task Timeout
- ❌ **Don't:** Use default timeout for long-running jobs
- ✅ **Do:** Set appropriate `execution_timeout` for each task type

### 3. Connection Pool Sizing
- ❌ **Don't:** Use default pool size (5) for high concurrency
- ✅ **Do:** Adjust pool size based on workload (10-20)

### 4. Progress Monitoring
- ❌ **Don't:** Run silent long-running jobs
- ✅ **Do:** Add progress logging every N iterations

### 5. DAG Design
- ❌ **Don't:** Create DAGs that don't match actual code structure
- ✅ **Do:** Keep DAGs in sync with actual scripts and business logic

---

## 📚 References

- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [PostgreSQL Connection Pooling](https://www.postgresql.org/docs/current/runtime-config-connection.html)
- [SQLAlchemy Pooling](https://docs.sqlalchemy.org/en/14/core/pooling.html)

---

## ✅ Checklist

### Trước khi deploy
- [ ] Apply Airflow optimization env vars
- [ ] Create `spark_jobs` pool trong Airflow
- [ ] Test ML DAG với trigger manual
- [ ] Test MinIO DAG với trigger manual
- [ ] Monitor database connections
- [ ] Check Spark UI for resource usage

### Sau khi deploy
- [ ] Monitor scheduler logs cho heartbeat errors
- [ ] Check database query performance
- [ ] Verify ML models được train thành công
- [ ] Verify DWH data được load đầy đủ
- [ ] Set up alerts cho long-running tasks
- [ ] Document any additional optimizations needed

---

## 🎯 Next Steps

1. **Test thoroughly trong dev environment**
2. **Monitor performance metrics**
3. **Adjust timeouts/pool sizes nếu cần**
4. **Consider thêm data quality checks**
5. **Setup alerting cho pipeline failures**
6. **Document operational procedures**

---

**Created:** 2025-11-23  
**Author:** AI Assistant  
**Status:** ✅ Complete

