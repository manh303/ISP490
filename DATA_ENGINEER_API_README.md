# Data Engineer API - Quick Start 🚀

## 📦 What You Have

✅ **Extended Meta Schema** - 15 tables for comprehensive monitoring  
✅ **Complete REST API** - 20+ endpoints for ETL, DQ, DB health  
✅ **Automated Scripts** - Setup, test, and metrics collection  
✅ **Full Documentation** - Guides and references  

---

## ⚡ Quick Setup (5 Minutes)

### Option 1: Automatic Setup (Recommended)

```bash
# Run the setup script
python setup_data_engineer_api.py
```

This will automatically:
1. ✅ Check database connection
2. ✅ Verify meta schema
3. ✅ Apply extended schema (if needed)
4. ✅ Verify all tables
5. ✅ Test API endpoint

**Expected output:**
```
============================================================
DATA ENGINEER API - QUICK SETUP
============================================================

============================================================
STEP 1: Checking Database Connection
============================================================
✅ Connected to PostgreSQL
ℹ️  Version: PostgreSQL 16.4 on x86_64-pc-linux-gnu...

============================================================
STEP 2: Checking Existing Meta Schema
============================================================
✅ Meta schema exists
ℹ️  Found 7 tables in meta schema
ℹ️  Need to apply extended schema (expecting 15 tables)

============================================================
STEP 3: Applying Extended Schema
============================================================
ℹ️  Reading database/schema/meta_schema_extended.sql...
ℹ️  Executing SQL...
✅ Extended schema applied successfully!
ℹ️  Total meta tables: 15

============================================================
STEP 4: Verifying Tables
============================================================
ℹ️  Found 15 tables:
  ✅ alert_config
  ✅ alert_history
  ✅ data_lineage
  ✅ data_quality_check_result
  ✅ data_quality_issue
  ✅ data_quality_rule
  ✅ db_connection_health
  ✅ etl_job
  ✅ etl_log
  ✅ etl_run
  ✅ pipeline_dependency
  ✅ query_performance
  ✅ schema_version
  ✅ storage_usage
  ✅ table_stats
✅ All expected tables present!

============================================================
STEP 5: Testing API Endpoint
============================================================
ℹ️  Calling http://localhost:8000/api/v1/data-engineer/health...
✅ API is responding!
ℹ️  Response: {'status': 'healthy', 'timestamp': '2025-11-24T01:30:00'}

============================================================
SETUP SUMMARY
============================================================
✅ Database Connection: True
✅ Meta Schema Check: False
✅ Apply Extended Schema: True
✅ Verify Tables: True
✅ API Test: True

============================================================
NEXT STEPS
============================================================
1. Restart backend:
   docker-compose restart backend

2. Visit API docs:
   http://localhost:8000/docs

3. Run metrics collector:
   python backend/scripts/collect_metadata_metrics.py

4. Read documentation:
   - DATA_ENGINEER_API_SETUP.md
   - backend/scripts/DATA_ENGINEER_QUICK_REFERENCE.md
============================================================
```

---

### Option 2: Manual Setup

```bash
# 1. Apply extended schema
psql "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss" \
  -f database/schema/meta_schema_extended.sql

# 2. Restart backend
docker-compose restart backend

# 3. Collect initial metrics
python backend/scripts/collect_metadata_metrics.py
```

---

## 🧪 Testing

### Test All Endpoints

```bash
python test_data_engineer_api.py
```

**Expected output:**
```
======================================================================
DATA ENGINEER API - ENDPOINT TESTS
======================================================================
Started at: 2025-11-24 01:30:00

======================================================================
TEST: API Health Check
Endpoint: /health
======================================================================
✅ Status: 200 OK

📄 Response:
{
  "status": "healthy",
  "timestamp": "2025-11-24T01:30:00Z"
}

======================================================================
TEST: Get All ETL Jobs Status
Endpoint: /etl/jobs
======================================================================
✅ Status: 200 OK
📊 Returned 2 items

📄 Sample (first item):
{
  "job_code": "MINIO_ECOMMERCE_DWH_PIPELINE",
  "job_name": "Ecommerce DSS - Full DWH (Star Schema)",
  "is_active": true,
  "last_run_date": "2025-11-23",
  "last_run_status": "SUCCESS",
  "last_run_duration_minutes": 15.5,
  "total_runs": 120,
  "success_rate": 95.8,
  "avg_duration_minutes": 14.2
}

... and 1 more items

... (more tests) ...

======================================================================
TEST SUMMARY
======================================================================
✅ Health Check
✅ ETL Jobs
✅ ETL Runs
✅ Table Health (All)
✅ Table Health (DWH)
✅ Table Growth
✅ Data Quality Issues
✅ Data Quality Summary
✅ Database Health
✅ Data Lineage
✅ Alert Summary
✅ Alert History
✅ Pipeline Performance
✅ Data Volume

📊 Results: 14/14 tests passed (100.0%)

🎉 All tests passed!
======================================================================
```

---

## 📊 Using the API

### Via cURL

```bash
# Health check
curl http://localhost:8000/api/v1/data-engineer/health

# Get ETL jobs
curl http://localhost:8000/api/v1/data-engineer/etl/jobs | jq

# Get table health
curl http://localhost:8000/api/v1/data-engineer/tables/health | jq

# Get data quality issues
curl "http://localhost:8000/api/v1/data-engineer/data-quality/issues?status=OPEN" | jq

# Get database health
curl http://localhost:8000/api/v1/data-engineer/database/health | jq
```

### Via Python

```python
import requests

BASE_URL = "http://localhost:8000/api/v1/data-engineer"

# Get ETL jobs
response = requests.get(f"{BASE_URL}/etl/jobs")
jobs = response.json()

for job in jobs:
    print(f"{job['job_code']}: {job['success_rate']:.1f}% success rate")

# Get table health
response = requests.get(f"{BASE_URL}/tables/health", params={"schema_name": "dwh"})
tables = response.json()

for table in tables:
    print(f"{table['table_name']}: {table['row_count']:,} rows, {table['health_status']}")
```

### Via Browser

Visit **Interactive API Documentation:**

http://localhost:8000/docs

Look for the **"Data Engineer"** section to try all endpoints.

---

## 🔄 Scheduled Metrics Collection

### Option 1: Using Cron

```bash
# Edit crontab
crontab -e

# Add line (run every 15 minutes)
*/15 * * * * cd /path/to/project/backend/scripts && python collect_metadata_metrics.py >> /var/log/metadata_collector.log 2>&1
```

### Option 2: Using Airflow

Create `airflow/dags/metadata_collection_dag.py`:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    'metadata_collection',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    tags=['monitoring', 'metadata']
) as dag:
    
    collect_metrics = BashOperator(
        task_id='collect_metadata_metrics',
        bash_command='cd /app/backend/scripts && python collect_metadata_metrics.py'
    )
```

---

## 📚 Documentation

| File | Description |
|------|-------------|
| `DATA_ENGINEER_API_SETUP.md` | **Complete setup guide** with step-by-step instructions |
| `DATA_ENGINEER_API_IMPLEMENTATION_SUMMARY.md` | **Implementation overview** and what's included |
| `backend/scripts/DATA_ENGINEER_QUICK_REFERENCE.md` | **Quick reference** for common commands and queries |

---

## 🎯 Common Use Cases

### 1. Monitor Daily ETL Success

```bash
curl http://localhost:8000/api/v1/data-engineer/etl/jobs | \
  jq '.[] | {job: .job_code, success_rate: .success_rate}'
```

### 2. Find Stale Tables

```bash
curl "http://localhost:8000/api/v1/data-engineer/tables/health?stale_hours=12" | \
  jq '.[] | select(.health_status == "STALE") | {table: .table_name, age_hours: .freshness_hours}'
```

### 3. Check Critical Data Quality Issues

```bash
curl "http://localhost:8000/api/v1/data-engineer/data-quality/issues?severity=CRITICAL&status=OPEN" | \
  jq '.[] | {table: (.schema_name + "." + .table_name), issue: .issue_description}'
```

### 4. View Data Lineage

```bash
curl "http://localhost:8000/api/v1/data-engineer/lineage/table/dwh/fact_product_daily?direction=both" | \
  jq '.[] | {from: (.source_schema + "." + .source_table), to: (.target_schema + "." + .target_table), type: .transformation_type}'
```

### 5. Track Pipeline Performance

```bash
curl "http://localhost:8000/api/v1/data-engineer/stats/pipeline-performance?days=7" | \
  jq 'group_by(.job_code) | map({job: .[0].job_code, runs: length, avg_duration: (map(.avg_duration_minutes) | add / length)})'
```

---

## 🚨 Troubleshooting

### Problem: API returns 404

**Solution:**
```bash
# 1. Check if data_engineer.py exists
ls backend/app/api/v1/data_engineer.py

# 2. Check if it's registered in main.py
grep -A 5 "data_engineer" backend/app/main.py

# 3. Restart backend
docker-compose restart backend

# 4. Check logs
docker logs ecommerce-dss-project-backend-1
```

---

### Problem: No data in tables

**Solution:**
```bash
# 1. Run metrics collector
python backend/scripts/collect_metadata_metrics.py

# 2. Check if it succeeded
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM meta.table_stats;"

# 3. If empty, trigger ETL pipeline in Airflow UI
```

---

### Problem: Stale data warnings

**Solution:**
```bash
# Trigger ETL pipeline to refresh data
# Go to Airflow UI: http://localhost:8081
# Manually trigger: minio_ecommerce_dwh_pipeline
```

---

## 🎉 Success Checklist

- [ ] ✅ Extended schema applied (15 tables in meta)
- [ ] ✅ Backend restarted and responding
- [ ] ✅ Health endpoint returns 200 OK
- [ ] ✅ All test endpoints pass
- [ ] ✅ Metrics collector runs successfully
- [ ] ✅ Data visible in API responses
- [ ] ✅ API docs accessible at /docs
- [ ] ✅ (Optional) Metrics collection scheduled

---

## 📞 Quick Commands Reference

```bash
# Setup
python setup_data_engineer_api.py

# Test
python test_data_engineer_api.py

# Collect metrics
python backend/scripts/collect_metadata_metrics.py

# Restart backend
docker-compose restart backend

# View logs
docker logs -f ecommerce-dss-project-backend-1

# Check database
psql "$DATABASE_URL" -c "\dt meta.*"
```

---

## 🌟 What's Next?

1. **Customize Alerts** - Add project-specific thresholds
2. **Create Dashboard** - Visualize metrics with Grafana
3. **Add Notifications** - Setup email/Slack alerts
4. **Optimize Queries** - Add caching for frequent requests
5. **Extend Monitoring** - Add more custom metrics

---

**Total Setup Time:** ~5 minutes ⚡  
**Total Test Time:** ~1 minute ⚡  
**Status:** ✅ Ready to use!

---

**Created:** 2025-11-24  
**Version:** 1.0.0  
**License:** MIT

