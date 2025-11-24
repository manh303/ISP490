# Airflow Optimization Guide

## 🎯 Giải quyết Lỗi "Job heartbeat got an exception"

### Nguyên nhân
- Spark job chạy quá lâu (> 5 phút) khi load fact_product_daily
- Airflow job heartbeat không thể kết nối database do connection pool exhaust
- Long-running database transaction blocking connections

### ✅ Giải pháp đã áp dụng

#### 1. Tối ưu Spark Job (`load_cleaned_from_minio.py`)
```python
# Thay vì commit 1 lần toàn bộ:
execute_batch(cur, insert_fact_sql, rows, page_size=1000)
conn.commit()

# → Commit theo batch nhỏ để giải phóng connection:
batch_size = 1000
for i in range(0, total_rows, batch_size):
    batch = rows[i:i+batch_size]
    execute_batch(cur, insert_fact_sql, batch, page_size=500)
    conn.commit()  # Commit từng batch
```

**Lợi ích:**
- Giảm thời gian lock database
- Giải phóng connection nhanh hơn
- Tránh long-running transaction

#### 2. Tăng Execution Timeout cho Spark Task
```python
spark_build_star_dwh = BashOperator(
    task_id="spark_build_star_dwh",
    execution_timeout=timedelta(hours=2),  # Tăng timeout
    pool="spark_jobs",  # Sử dụng pool riêng
    # ...
)
```

### 🔧 Cấu hình Airflow bổ sung (Khuyến nghị)

#### 1. Tăng Job Heartbeat Interval

Thêm vào `airflow/config/airflow.cfg` hoặc set env vars:

```ini
[scheduler]
# Tăng interval giữa các heartbeat (default: 5s)
job_heartbeat_sec = 30

# Tăng thời gian timeout cho scheduler heartbeat (default: 30s)  
scheduler_heartbeat_sec = 30

# Tăng số lượng task instances có thể chạy đồng thời
max_active_tasks_per_dag = 16
```

**Environment Variables:**
```bash
export AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC=30
export AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=30
export AIRFLOW__SCHEDULER__MAX_ACTIVE_TASKS_PER_DAG=16
```

#### 2. Database Connection Pool Configuration

```ini
[database]
# Tăng connection pool size
sql_alchemy_pool_size = 10
sql_alchemy_max_overflow = 20
sql_alchemy_pool_recycle = 1800
sql_alchemy_pool_pre_ping = True

# Connection timeout
sql_alchemy_connect_args = {"connect_timeout": 30}
```

**Environment Variables:**
```bash
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=10
export AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=20
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE=1800
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_PRE_PING=True
```

#### 3. Tạo Airflow Pool cho Spark Jobs

```bash
# Login vào Airflow webserver container
docker exec -it airflow-webserver bash

# Tạo pool
airflow pools set spark_jobs 2 "Pool for Spark jobs to control concurrency"
```

Hoặc qua UI: Admin → Pools → Create

**Pool Settings:**
- Name: `spark_jobs`
- Slots: `2`
- Description: "Pool for Spark jobs"

### 📊 Monitoring

#### 1. Check Airflow Logs
```bash
# Logs cho task
docker logs airflow-scheduler -f | grep "heartbeat"

# Logs cho database connection
docker logs airflow-scheduler -f | grep "sqlalchemy"
```

#### 2. Check Database Connections
```sql
-- PostgreSQL: Check active connections
SELECT count(*), state
FROM pg_stat_activity
WHERE datname = 'ecommerce_dss'
GROUP BY state;

-- Check long-running queries
SELECT pid, now() - query_start AS duration, query
FROM pg_stat_activity
WHERE state = 'active'
ORDER BY duration DESC;
```

#### 3. Spark Job Monitoring
```bash
# Access Spark UI
http://localhost:8081  # Spark Master UI
http://localhost:4040  # Spark Application UI (when job running)

# Check Spark logs
docker logs spark-master -f
```

### 🚀 Best Practices

#### 1. Batch Processing Strategy
```python
# Chia lớn workload thành các batch nhỏ
BATCH_SIZE = 1000
COMMIT_EVERY = 1000  # Commit sau mỗi 1000 rows

for i in range(0, total_rows, BATCH_SIZE):
    batch = rows[i:i+BATCH_SIZE]
    process_batch(batch)
    if i % COMMIT_EVERY == 0:
        conn.commit()
```

#### 2. Connection Management
```python
def get_conn_with_retry(max_retries=3):
    """Get database connection với retry logic"""
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
```

#### 3. Progress Logging
```python
# Log progress để monitor và debug
total = len(items)
for i, item in enumerate(items):
    process_item(item)
    if (i + 1) % 1000 == 0:
        print(f"[PROGRESS] {i+1}/{total} ({(i+1)/total*100:.1f}%)")
```

### 🐛 Troubleshooting

#### Lỗi: "too many connections"
```sql
-- Check max connections
SHOW max_connections;

-- Increase max_connections (requires restart)
ALTER SYSTEM SET max_connections = 200;

-- Or in postgresql.conf
max_connections = 200
```

#### Lỗi: "connection timeout"
```python
# Tăng timeout trong connection string
DATABASE_URL = "postgresql://user:pass@host:5432/db?connect_timeout=30"
```

#### Lỗi: "idle in transaction"
```sql
-- Check idle transactions
SELECT pid, state, query_start, state_change
FROM pg_stat_activity
WHERE state = 'idle in transaction';

-- Kill idle transactions
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'idle in transaction'
  AND state_change < NOW() - INTERVAL '5 minutes';
```

### 📚 References

- [Airflow Configuration Reference](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)
- [SQLAlchemy Connection Pooling](https://docs.sqlalchemy.org/en/14/core/pooling.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [PostgreSQL Connection Pooling](https://www.postgresql.org/docs/current/runtime-config-connection.html)

---

## 🔍 Health Check Script

```bash
#!/bin/bash
# airflow/scripts/health_check.sh

echo "=== Airflow Health Check ==="

echo -e "\n1. Airflow Scheduler Status:"
docker ps | grep airflow-scheduler

echo -e "\n2. Database Connections:"
docker exec airflow-scheduler \
  psql $DATABASE_URL -c \
  "SELECT count(*), state FROM pg_stat_activity WHERE datname = 'ecommerce_dss' GROUP BY state;"

echo -e "\n3. Active DAG Runs:"
docker exec airflow-scheduler \
  airflow dags list-runs --state running

echo -e "\n4. Task Instance Status:"
docker exec airflow-scheduler \
  airflow tasks states-for-dag-run minio_ecommerce_dwh_pipeline <DAG_RUN_ID>

echo -e "\n5. Spark Jobs:"
docker ps | grep spark

echo "=== Health Check Complete ==="
```

**Usage:**
```bash
chmod +x airflow/scripts/health_check.sh
./airflow/scripts/health_check.sh
```

