# Data Engineer API - Step by Step Setup Guide

## 🎯 Mục Tiêu

Tạo API hoàn chỉnh cho Data Engineer để monitor:
- ETL pipeline status & history
- Table health & freshness
- Data quality issues
- Database performance
- Data lineage
- Alerts & notifications

---

## 📋 STEP 1: Setup Extended Meta Schema

### 1.1 Review Current Schema

Schema `meta` hiện tại đã có:
- ✅ `etl_job` - Định nghĩa ETL jobs
- ✅ `etl_run` - Lịch sử runs
- ✅ `etl_log` - Logs chi tiết
- ✅ `table_stats` - Thống kê tables
- ✅ `data_quality_issue` - DQ issues
- ✅ `data_quality_rule` - DQ rules
- ✅ `data_quality_check_result` - DQ results

### 1.2 Apply Extended Schema

```bash
# Connect to Render database
psql "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

# Run extended schema
\i database/schema/meta_schema_extended.sql
```

Hoặc qua Python:

```bash
cd backend/scripts
python3 << EOF
import psycopg2
import os

DB_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

with open('../../database/schema/meta_schema_extended.sql', 'r') as f:
    sql = f.read()

conn = psycopg2.connect(DB_URL)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute(sql)
    print("✅ Extended schema applied!")
conn.close()
EOF
```

### 1.3 Verify Tables Created

```sql
-- Check all meta tables
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'meta' 
ORDER BY table_name;
```

**Expected output:**
```
 table_name                  
-----------------------------
 alert_config
 alert_history
 data_lineage
 data_quality_check_result
 data_quality_issue
 data_quality_rule
 db_connection_health
 etl_job
 etl_log
 etl_run
 pipeline_dependency
 query_performance
 schema_version
 storage_usage
 table_stats
(15 rows) ✅
```

---

## 📋 STEP 2: Integrate API with FastAPI Backend

### 2.1 Check Backend Structure

```bash
backend/
├── app/
│   ├── api/
│   │   ├── v1/
│   │   │   ├── __init__.py
│   │   │   ├── analytics.py         # Existing
│   │   │   ├── data_engineer.py     # ✅ NEW!
│   │   │   └── ...
│   │   └── router.py
│   ├── main.py
│   └── ...
```

### 2.2 Register Data Engineer Router

Edit `backend/app/api/router.py`:

```python
from fastapi import APIRouter
from app.api.v1 import analytics, data_engineer  # Add data_engineer

api_router = APIRouter()

# Existing routes
api_router.include_router(analytics.router, prefix="/v1")

# ✅ Add new Data Engineer routes
api_router.include_router(data_engineer.router, prefix="/v1")
```

### 2.3 Update Requirements

Add to `backend/requirements.txt` if not present:

```txt
fastapi>=0.104.0
psycopg2-binary>=2.9.9
pydantic>=2.0.0
```

### 2.4 Restart Backend

```bash
cd backend
# If using Docker
docker-compose restart backend

# If running locally
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

---

## 📋 STEP 3: Setup Metrics Collection

### 3.1 Make Script Executable

```bash
chmod +x backend/scripts/collect_metadata_metrics.py
```

### 3.2 Test Manual Run

```bash
cd backend/scripts
python collect_metadata_metrics.py
```

**Expected output:**
```
============================================================
METADATA METRICS COLLECTOR
============================================================
Started at: 2025-11-24 01:30:00

[INFO] Collecting database health metrics...
  ✅ Database health: HEALTHY (12.5% connections used)
[INFO] Collecting table statistics...
  ✅ Collected stats for 25 tables
[INFO] Checking data freshness...
  ✅ dwh.fact_product_daily: Fresh (2.3 hours old)
  ✅ dwh.fact_review: Fresh (2.3 hours old)
  ⚠️  ml.fact_review_sentiment: No freshness data
[INFO] Collecting storage usage...
  ✅ Collected storage usage for 25 tables

✅ Collection completed in 3.45 seconds
```

### 3.3 Schedule with Cron (Optional)

```bash
# Edit crontab
crontab -e

# Add line to run every 15 minutes
*/15 * * * * cd /path/to/project/backend/scripts && python collect_metadata_metrics.py >> /var/log/metadata_collector.log 2>&1
```

Or use Airflow DAG:

```python
# airflow/dags/metadata_collection_dag.py
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

## 📋 STEP 4: Test API Endpoints

### 4.1 Check API Documentation

Visit: `http://localhost:8000/docs`

Look for new section: **Data Engineer**

### 4.2 Test Key Endpoints

#### Health Check

```bash
curl http://localhost:8000/api/v1/data-engineer/health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-24T01:30:00Z"
}
```

#### Get ETL Jobs Status

```bash
curl http://localhost:8000/api/v1/data-engineer/etl/jobs
```

**Response:**
```json
[
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
  },
  {
    "job_code": "ML_TRAINING_PIPELINE",
    "job_name": "ML Training Pipeline",
    "is_active": true,
    "last_run_date": "2025-11-23",
    "last_run_status": "SUCCESS",
    "last_run_duration_minutes": 25.3,
    "total_runs": 45,
    "success_rate": 88.9,
    "avg_duration_minutes": 23.7
  }
]
```

#### Get Table Health

```bash
curl "http://localhost:8000/api/v1/data-engineer/tables/health?schema_name=dwh"
```

**Response:**
```json
[
  {
    "schema_name": "dwh",
    "table_name": "dim_product",
    "row_count": 54679,
    "size_mb": 12.5,
    "last_loaded_at": "2025-11-23T17:30:00Z",
    "freshness_hours": 2.3,
    "health_status": "HEALTHY"
  },
  {
    "schema_name": "dwh",
    "table_name": "fact_product_daily",
    "row_count": 54679,
    "size_mb": 8.7,
    "last_loaded_at": "2025-11-23T17:30:00Z",
    "freshness_hours": 2.3,
    "health_status": "HEALTHY"
  }
]
```

#### Get Data Quality Issues

```bash
curl "http://localhost:8000/api/v1/data-engineer/data-quality/issues?status=OPEN"
```

#### Get Database Health

```bash
curl http://localhost:8000/api/v1/data-engineer/database/health
```

#### Get Data Lineage

```bash
curl "http://localhost:8000/api/v1/data-engineer/lineage/table/dwh/fact_product_daily?direction=both"
```

---

## 📋 STEP 5: Create Monitoring Dashboard (Optional)

### 5.1 Create Simple Dashboard

```bash
touch backend/app/templates/data_engineer_dashboard.html
```

**Content:**

```html
<!DOCTYPE html>
<html>
<head>
    <title>Data Engineer Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial; margin: 20px; }
        .metric-card { 
            border: 1px solid #ddd; 
            padding: 15px; 
            margin: 10px; 
            border-radius: 5px;
            display: inline-block;
            min-width: 200px;
        }
        .status-healthy { color: green; }
        .status-degraded { color: orange; }
        .status-down { color: red; }
    </style>
</head>
<body>
    <h1>🔧 Data Engineer Dashboard</h1>
    
    <div id="metrics"></div>
    <canvas id="pipelineChart" width="800" height="400"></canvas>
    
    <script>
        // Fetch and display metrics
        async function loadDashboard() {
            // ETL Jobs
            const jobs = await fetch('/api/v1/data-engineer/etl/jobs').then(r => r.json());
            
            // Table Health
            const tables = await fetch('/api/v1/data-engineer/tables/health').then(r => r.json());
            
            // Database Health
            const dbHealth = await fetch('/api/v1/data-engineer/database/health').then(r => r.json());
            
            // Display metrics
            document.getElementById('metrics').innerHTML = `
                <div class="metric-card">
                    <h3>Database Status</h3>
                    <p class="status-${dbHealth.status.toLowerCase()}">${dbHealth.status}</p>
                    <p>${dbHealth.active_connections} active connections</p>
                    <p>${dbHealth.connection_usage_pct}% usage</p>
                </div>
                
                <div class="metric-card">
                    <h3>ETL Pipelines</h3>
                    <p>${jobs.length} jobs configured</p>
                    <p>${jobs.filter(j => j.last_run_status === 'SUCCESS').length} successful</p>
                </div>
                
                <div class="metric-card">
                    <h3>Tables</h3>
                    <p>${tables.length} tables monitored</p>
                    <p>${tables.filter(t => t.health_status === 'HEALTHY').length} healthy</p>
                    <p>${tables.filter(t => t.health_status === 'STALE').length} stale</p>
                </div>
            `;
            
            // Draw chart
            const ctx = document.getElementById('pipelineChart').getContext('2d');
            new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: jobs.map(j => j.job_code),
                    datasets: [{
                        label: 'Success Rate (%)',
                        data: jobs.map(j => j.success_rate),
                        backgroundColor: 'rgba(75, 192, 192, 0.6)'
                    }]
                },
                options: {
                    scales: {
                        y: { beginAtZero: true, max: 100 }
                    }
                }
            });
        }
        
        loadDashboard();
        setInterval(loadDashboard, 60000); // Refresh every minute
    </script>
</body>
</html>
```

### 5.2 Add Route to Serve Dashboard

In `backend/app/main.py`:

```python
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")

@app.get("/dashboard/data-engineer", response_class=HTMLResponse)
async def data_engineer_dashboard(request: Request):
    return templates.TemplateResponse("data_engineer_dashboard.html", {"request": request})
```

Visit: `http://localhost:8000/dashboard/data-engineer`

---

## 📋 STEP 6: Integrate với Airflow DAGs

### 6.1 Update Existing DAGs để Log Metrics

Edit `airflow/dags/minio_pipeline_dag.py`:

```python
# At the end of successful run
def log_to_meta(**context):
    import psycopg2
    import os
    
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    
    ti = context['ti']
    run_id = ti.xcom_pull(task_ids='etl_run_start')
    
    # Get rows processed from spark task
    # (You would extract this from task logs or XCom)
    rows_processed = 54679  # Example
    
    cur = conn.cursor()
    cur.execute("""
        UPDATE meta.etl_run
        SET rows_written = %s
        WHERE run_id = %s;
    """, (rows_processed, run_id))
    conn.commit()
    cur.close()
    conn.close()

log_metrics_task = PythonOperator(
    task_id='log_metrics',
    python_callable=log_to_meta
)

# Add to DAG flow
spark_build_star_dwh >> log_metrics_task >> etl_run_finish_task
```

---

## 📋 STEP 7: Setup Alerts (Optional)

### 7.1 Create Alert Check Script

```bash
touch backend/scripts/check_alerts.py
```

```python
#!/usr/bin/env python3
import psycopg2
import os
import smtplib
from email.mime.text import MIMEText

DB_URL = os.getenv("DATABASE_URL")

def check_alerts():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    
    # Check ETL failures
    cur.execute("""
        SELECT j.job_name, r.error_message
        FROM meta.etl_run r
        JOIN meta.etl_job j ON r.job_id = j.job_id
        WHERE r.status = 'FAILED'
          AND r.started_at >= NOW() - INTERVAL '1 hour'
    """)
    
    for job_name, error in cur.fetchall():
        send_alert(f"ETL Failure: {job_name}", error)
    
    # Check stale data
    cur.execute("""
        SELECT schema_name, table_name, issue_description
        FROM meta.data_quality_issue
        WHERE status = 'OPEN'
          AND severity = 'CRITICAL'
          AND detected_at >= NOW() - INTERVAL '1 hour'
    """)
    
    for schema, table, desc in cur.fetchall():
        send_alert(f"Data Quality Issue: {schema}.{table}", desc)
    
    cur.close()
    conn.close()

def send_alert(subject, message):
    # Implement your notification logic
    # (Email, Slack, PagerDuty, etc.)
    print(f"🚨 ALERT: {subject}")
    print(f"   {message}")

if __name__ == "__main__":
    check_alerts()
```

### 7.2 Schedule Alert Checks

```bash
# Run every 15 minutes
*/15 * * * * cd /path/to/project/backend/scripts && python check_alerts.py
```

---

## 📋 STEP 8: Documentation & Testing

### 8.1 Test All Endpoints

Create test script:

```bash
touch backend/tests/test_data_engineer_api.py
```

```python
import requests

BASE_URL = "http://localhost:8000/api/v1/data-engineer"

def test_all_endpoints():
    endpoints = [
        "/health",
        "/etl/jobs",
        "/etl/runs/MINIO_ECOMMERCE_DWH_PIPELINE",
        "/tables/health",
        "/tables/health?schema_name=dwh",
        "/database/health",
        "/data-quality/issues",
        "/data-quality/summary",
        "/lineage/table/dwh/fact_product_daily",
        "/alerts/summary",
        "/stats/pipeline-performance",
        "/stats/data-volume",
    ]
    
    for endpoint in endpoints:
        url = BASE_URL + endpoint
        try:
            response = requests.get(url)
            status = "✅" if response.status_code == 200 else "❌"
            print(f"{status} {endpoint}: {response.status_code}")
        except Exception as e:
            print(f"❌ {endpoint}: {e}")

if __name__ == "__main__":
    test_all_endpoints()
```

Run:

```bash
python backend/tests/test_data_engineer_api.py
```

### 8.2 Update API Documentation

API docs auto-generated at: `http://localhost:8000/docs`

Add descriptions in code using docstrings (already done ✅)

---

## 📊 SUMMARY

### ✅ What You Now Have

1. **Extended Meta Schema** (15 tables total)
   - Database health monitoring
   - Schema version tracking
   - Data lineage
   - Pipeline dependencies
   - Alert configuration & history
   - Query performance tracking
   - Storage usage monitoring

2. **Comprehensive API** (20+ endpoints)
   - ETL job monitoring
   - Table health & freshness
   - Data quality tracking
   - Database health
   - Data lineage visualization
   - Alert management
   - Performance statistics

3. **Automated Metrics Collection**
   - Database connection health
   - Table statistics
   - Data freshness checks
   - Storage usage

4. **Optional Dashboard**
   - Real-time metrics
   - Charts & visualizations
   - Status indicators

---

## 🎯 Next Steps

1. **Customize for Your Needs**
   - Add more specific data quality rules
   - Configure alerts for your SLAs
   - Add more lineage relationships

2. **Integrate with Monitoring Tools**
   - Grafana dashboards
   - Prometheus metrics export
   - Slack/Email notifications

3. **Add Authentication**
   - JWT tokens
   - Role-based access control
   - API keys

4. **Scale Up**
   - Cache frequent queries
   - Add pagination for large results
   - Implement rate limiting

---

## 📚 API Endpoint Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | API health check |
| `/etl/jobs` | GET | All ETL jobs status |
| `/etl/runs/{job_code}` | GET | Run history for job |
| `/etl/logs/{run_id}` | GET | Detailed logs for run |
| `/tables/health` | GET | Table health status |
| `/tables/growth/{schema}/{table}` | GET | Growth history |
| `/data-quality/issues` | GET | DQ issues |
| `/data-quality/summary` | GET | DQ summary stats |
| `/database/health` | GET | DB connection health |
| `/lineage/table/{schema}/{table}` | GET | Data lineage |
| `/alerts/summary` | GET | Alert summary |
| `/alerts/history` | GET | Alert history |
| `/stats/pipeline-performance` | GET | Pipeline stats |
| `/stats/data-volume` | GET | Volume trends |

---

**Created:** 2025-11-24  
**Status:** ✅ Complete & Ready to Use

