# Airflow ML Training Pipeline - Hướng Dẫn

## Tổng Quan

Hai DAG tự động hóa quá trình training và monitoring mô hình ML:

1. **`ml_training_pipeline`** - Train 3 mô hình ML hàng ngày
2. **`ml_monitoring_dag`** - Monitor performance hàng ngày

---

## File Cấu Trúc

```
airflow/dags/
├── ml_training_pipeline_dag.py    ✅ NEW - Training pipeline
├── ml_monitoring_dag.py           ✅ NEW - Monitoring pipeline
└── README_ML_DAG.md               ✅ NEW - Hướng dẫn này
```

---

## DAG 1: ML Training Pipeline

### Tên
`ml_training_pipeline`

### Schedule
- **Frequency**: Daily at 1:00 AM (UTC)
- **Timezone**: UTC
- **Catchup**: Disabled

### Tasks

```
start
  ↓
extract_data (Data from DWH)
  ↓
check_data (Verify data exists)
  ↓
┌─────────────────────────────────┐ (Parallel)
│                                 │
train_sentiment_classifier   train_product_clustering
│                                 │
└─────────────────────────────────┘
  ↓
test_models (Validate models)
  ↓
validate_models (Check files)
  ↓
get_model_metrics (Retrieve metrics)
  ↓
notify_completion (Send notification)
  ↓
end
```

### Task Details

#### 1. extract_data
- **Type**: BashOperator
- **Script**: `ml/1_data_extraction.py`
- **Input**: DWH tables
- **Output**: CSV files in `ml/data/`
- **Duration**: ~5-10 minutes

#### 2. check_data
- **Type**: PythonOperator
- **Purpose**: Verify data extraction successful
- **Check**: File existence and size
- **Fails if**: Data files missing

#### 3. train_sentiment_classifier
- **Type**: BashOperator
- **Script**: `ml/train_sentiment_classifier.py`
- **Model**: Random Forest Classifier
- **Features**: TF-IDF + numeric features
- **Output**: sentiment_classifier.pkl
- **Duration**: ~10-15 minutes

#### 4. train_product_clustering
- **Type**: BashOperator
- **Script**: `ml/train_product_clustering.py`
- **Model**: KMeans clustering
- **Features**: Price, rating, engagement, etc.
- **Output**: recommendation_kmeans.pkl
- **Duration**: ~5-10 minutes

#### 5. test_models
- **Type**: BashOperator
- **Script**: `ml/test_models.py`
- **Purpose**: Validate trained models
- **Checks**: Model loads, predictions work
- **Duration**: ~2 minutes

#### 6. validate_models
- **Type**: PythonOperator
- **Purpose**: Final model file validation
- **Checks**: All required files exist
- **Fails if**: Critical files missing

#### 7. get_model_metrics
- **Type**: PythonOperator
- **Purpose**: Extract performance metrics
- **Output**: Push to XCom for downstream tasks
- **Duration**: ~1 minute

#### 8. notify_completion
- **Type**: PythonOperator
- **Purpose**: Send completion notification
- **Actions**: Save notification log
- **Duration**: ~1 minute

### Total Runtime
Approximately 30-45 minutes

### Error Handling
- **Retries**: 2 attempts with 5-minute delay
- **Timeout**: 3 hours
- **On Failure**: Retries automatically

---

## DAG 2: ML Monitoring Pipeline

### Tên
`ml_monitoring_dag`

### Schedule
- **Frequency**: Daily at 2:00 AM (UTC) (after training completes)
- **Timezone**: UTC

### Tasks

```
start
  ↓
┌──────────────────────────┐ (Parallel)
│                          │
check_performance    check_files
│                          │
└──────────────────────────┘
  ↓
compare_baseline (Compare with baseline)
  ↓
generate_report (Create report)
  ↓
send_alerts (Send if issues)
  ↓
end
```

### Task Details

#### 1. check_performance
- **Type**: PythonOperator
- **Purpose**: Verify metrics above thresholds
- **Checks**:
  - Sentiment Accuracy >= 80%
  - Sentiment F1-Score >= 80%
  - Clustering Silhouette Score >= 0.4
- **Alert**: Warning if below threshold

#### 2. check_files
- **Type**: PythonOperator
- **Purpose**: Verify model files
- **Checks**:
  - All model files exist
  - Files not older than 36 hours
  - File sizes reasonable
- **Alert**: Warning if stale or missing

#### 3. compare_baseline
- **Type**: PythonOperator
- **Purpose**: Compare with baseline performance
- **Alert**: Warning if degraded > 5%

#### 4. generate_report
- **Type**: PythonOperator
- **Purpose**: Create comprehensive monitoring report
- **Output**: JSON report saved to `logs/monitoring_reports/`
- **Contents**:
  - Performance metrics
  - Threshold alerts
  - File status
  - Summary statistics

#### 5. send_alerts
- **Type**: PythonOperator
- **Purpose**: Send alert notifications
- **Triggers**: Only if issues detected
- **Channel**: Email (configured in Airflow)

### Total Runtime
Approximately 5-10 minutes

---

## Setup & Configuration

### 1. Copy DAG Files

```bash
cp ml_training_pipeline_dag.py /app/airflow/dags/
cp ml_monitoring_dag.py /app/airflow/dags/
```

### 2. Update Airflow Config

Edit `airflow.cfg`:

```ini
[core]
dags_folder = /app/airflow/dags
base_log_folder = /app/airflow/logs

[smtp]
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_user = your-email@gmail.com
smtp_password = your-password
smtp_mail_from = airflow@ecommerce.com
```

### 3. Setup Connections

```bash
# Postgres connection (for data extraction)
airflow connections add postgres_default \
  --conn-type postgres \
  --conn-host dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com \
  --conn-port 5432 \
  --conn-database ecommerce_dss \
  --conn-user dss_user \
  --conn-password [password]
```

### 4. Create Variables (Optional)

```bash
airflow variables set METRICS_THRESHOLD_ACCURACY 0.80
airflow variables set METRICS_THRESHOLD_F1 0.80
airflow variables set METRICS_THRESHOLD_SILHOUETTE 0.40
```

### 5. Restart Airflow

```bash
airflow webserver --port 8080 &
airflow scheduler &
```

---

## Running DAGs

### Manually Trigger

```bash
# Trigger training DAG
airflow dags trigger ml_training_pipeline

# Trigger monitoring DAG
airflow dags trigger ml_monitoring_dag
```

### Via Web UI

1. Open http://localhost:8080
2. Find DAG: `ml_training_pipeline` or `ml_monitoring_dag`
3. Click "Trigger DAG" button
4. Optional: Override config in JSON

### View Logs

```bash
# In web UI: Click task → Click "Log"

# Or via CLI:
airflow tasks logs ml_training_pipeline extract_data 2025-11-16
```

### Monitor Execution

```bash
# List recent runs
airflow dags list-runs ml_training_pipeline

# Get specific run details
airflow dags list-runs ml_training_pipeline --state success
```

---

## Expected Output

### After Training DAG

```
/app/ml/
├── data/
│   ├── sentiment_analysis/raw_sentiment_data.csv
│   └── product_clustering/raw_clustering_data.csv
├── models/ml-models/
│   ├── sentiment_classifier.pkl
│   ├── sentiment_tfidf_vectorizer.pkl
│   ├── sentiment_label_encoder.pkl
│   ├── recommendation_kmeans.pkl
│   ├── clustering_scaler.pkl
│   └── clustering_features.pkl
└── logs/
    ├── ml_pipeline.log
    └── metrics/
        ├── sentiment_metrics.json
        └── clustering_metrics.json
```

### After Monitoring DAG

```
/app/ml/logs/monitoring_reports/
├── monitoring_2025-11-16_020000.json
├── monitoring_2025-11-17_020000.json
└── ...
```

---

## Monitoring & Alerts

### Metrics Checked

| Metric | Type | Threshold | Action |
|--------|------|-----------|--------|
| Sentiment Accuracy | Classification | >= 80% | Warn if below |
| Sentiment F1-Score | Classification | >= 80% | Warn if below |
| Clustering Silhouette | Clustering | >= 0.4 | Warn if below |
| File Age | File | <= 36h | Alert if stale |
| Performance Degradation | Change | <= 5% | Alert if worse |

### Alert Channels

Currently configured for **Email**. To add more:

```python
# In send_alerts function, add:

# Slack
send_slack_alert(channel='#ml-alerts', message=f"ML Alert: {alert}")

# PagerDuty
trigger_pagerduty(severity='warning', title=f"ML Model Alert")

# Database Log
insert_into_alert_log(alert_data)
```

---

## Troubleshooting

### Issue: DAG not showing in Airflow UI

**Solution:**
- Verify file path: `ls /app/airflow/dags/ml_*.py`
- Check Python syntax: `python -m py_compile ml_training_pipeline_dag.py`
- Reload DAGs: `airflow dags list`
- Restart scheduler: `airflow scheduler`

### Issue: Task fails - "No such file"

**Solution:**
- Verify ML project path: `ls /app/ml/`
- Check Python path: `export PYTHONPATH=/app/ml`
- Verify scripts exist: `ls /app/ml/*.py`

### Issue: Data extraction fails

**Solution:**
- Test DB connection: `psql -h host -U user -d database`
- Check DWH tables: `SELECT COUNT(*) FROM dwh.fact_review_daily_agg`
- Verify config.yaml credentials

### Issue: Training takes too long

**Solution:**
- Reduce batch_size in config.yaml
- Reduce lookback_days (use less training data)
- Use smaller test data sample
- Check system resources: `top`, `df -h`

### Issue: Alerts not sending

**Solution:**
- Check Airflow SMTP config: `airflow config get-value smtp`
- Test email: `airflow dags test ml_training_pipeline 2025-11-16`
- Check logs: `tail -f /app/airflow/logs/scheduler/*.log`

---

## Best Practices

### 1. Schedule Coordination

```
DAG 1: Training    01:00 - 01:45 UTC
DAG 2: Monitoring  02:00 - 02:15 UTC
```

Ensure monitoring runs AFTER training completes.

### 2. Resource Management

- Max active runs: Set to 1 to prevent overlap
- Timeout: Set to 3 hours to catch stuck tasks
- Pool limits: Allocate resources properly

### 3. Data Retention

```bash
# Keep training data for 30 days
airflow dags trigger ml_training_pipeline \
  --conf '{"retention_days": 30}'

# Keep metrics for 1 year
find /app/ml/logs/metrics -mtime +365 -delete
```

### 4. Version Control

```bash
# Tag model versions with git
git tag -a ml-model-v1.0 -m "Sentiment classifier trained"
git push origin ml-model-v1.0
```

### 5. Testing

```bash
# Test DAG before deploying
airflow dags test ml_training_pipeline 2025-11-16

# Test individual task
airflow tasks test ml_training_pipeline extract_data 2025-11-16
```

---

## Performance Tuning

### Parallel Execution

```python
# In ml_training_pipeline_dag.py
check_data >> [train_sentiment, train_clustering]  # Parallel
[train_sentiment, train_clustering] >> test_models  # Then sequential
```

Training runs 2 models in parallel, saving ~10 minutes.

### Database Indexing

```sql
-- Add indexes for faster data extraction
CREATE INDEX idx_review_agg_date ON dwh.fact_review_daily_agg(agg_date);
CREATE INDEX idx_review_product_id ON dwh.fact_review_daily_agg(global_product_id);
```

### Resource Allocation

```yaml
# In airflow.cfg
[core]
parallelism = 4
dag_concurrency = 2
max_active_runs = 1
```

---

## Integration with API

After training, models are automatically available:

```bash
# Check models loaded
GET /api/v1/ml/health

# Get sentiment analysis
POST /api/v1/ml/analyze/sentiment
{
  "review_text": "Sản phẩm tốt!",
  "rating": 5.0
}

# Get product segment
POST /api/v1/ml/segment/products
{
  "product_id": 123,
  "num_segments": 3
}
```

---

## Advanced Usage

### Custom Metrics

Add custom metric checks:

```python
def check_custom_metric(**context):
    # Custom metric logic
    metric_value = compute_metric()
    if metric_value < THRESHOLD:
        raise AirflowException("Custom metric failed")
```

### Conditional Branching

```python
def decide_retrain(**context):
    # Check if retraining needed
    if performance_degraded:
        return 'retrain_task'
    return 'skip_retrain'

branch = BranchPythonOperator(
    task_id='branch_decision',
    python_callable=decide_retrain
)
```

### Database Logging

```python
def log_to_database(**context):
    conn = PostgresHook('postgres_default')
    conn.insert_rows(
        table='ml_training_log',
        rows=[(...training data...)]
    )
```

---

## Support & Documentation

- **Airflow Docs**: https://airflow.apache.org/docs/
- **ML Training Guide**: `/app/ml/TRAINING_GUIDE.md`
- **API Documentation**: `/app/backend/ML_API_DOCUMENTATION.md`
- **Issues**: Create issue in GitHub

---

**Last Updated**: 2025-11-16
**Status**: Ready for Production ✅
