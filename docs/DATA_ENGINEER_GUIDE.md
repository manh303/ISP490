# 📘 Hướng Dẫn Hoàn Chỉnh Cho Data Engineer

## 🎯 Mục Tiêu
Data Engineer đảm bảo data pipeline chạy ổn định, dữ liệu sạch, đầy đủ và kịp thời từ Crawler → MinIO → Staging → ODS → DWH.

---

## 📋 Mục Lục
1. [Setup & Cấu Hình Ban Đầu](#1-setup--cấu-hình-ban-đầu)
2. [Monitoring Pipeline Hàng Ngày](#2-monitoring-pipeline-hàng-ngày)
3. [Theo Dõi ETL Runs & Logs](#3-theo-dõi-etl-runs--logs)
4. [Giám Sát Data Volume & Freshness](#4-giám-sát-data-volume--freshness)
5. [Xử Lý Data Quality Issues](#5-xử-lý-data-quality-issues)
6. [Troubleshooting & Recovery](#6-troubleshooting--recovery)
7. [Tối Ưu Performance](#7-tối-ưu-performance)
8. [Best Practices & Checklist](#8-best-practices--checklist)

---

## 1. Setup & Cấu Hình Ban Đầu

### 1.1. Kiểm Tra Kết Nối Hệ Thống

```bash
# Kiểm tra Airflow
curl http://localhost:8080/health

# Kiểm tra Spark Master
curl http://localhost:8081

# Kiểm tra MinIO
curl http://localhost:9000/minio/health/live

# Kiểm tra PostgreSQL
psql -h localhost -p 5433 -U dss_user -d ecommerce_dss_1 -c "SELECT version();"
```

### 1.2. Kiểm Tra Schema Meta

```sql
-- Kết nối database
psql -h localhost -p 5433 -U dss_user -d ecommerce_dss_1

-- Kiểm tra schema meta tồn tại
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'meta';

-- Kiểm tra các bảng meta
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'meta'
ORDER BY table_name;

-- Kết quả mong đợi:
-- - etl_job
-- - etl_run
-- - etl_log
-- - table_stats
-- - data_quality_issue
```

### 1.3. Kiểm Tra DAG Trong Airflow

1. Truy cập Airflow UI: `http://localhost:8080`
2. Đăng nhập: `admin` / `admin123`
3. Tìm DAG: `minio_ecommerce_dwh_pipeline`
4. Kiểm tra:
   - ✅ DAG không bị paused
   - ✅ Schedule: `0 2 * * *` (chạy 2h sáng mỗi ngày)
   - ✅ Latest run có status hợp lệ

### 1.4. Kiểm Tra MinIO Buckets

```bash
# Sử dụng MinIO Client hoặc Python
python3 << EOF
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

buckets = client.list_buckets()
for bucket in buckets:
    print(f"Bucket: {bucket.name}")
    
# Kết quả mong đợi:
# - crawler-data (raw JSONL files)
# - cleaned-data (processed parquet)
# - processed-reviews (review parquet)
EOF
```

---

## 2. Monitoring Pipeline Hàng Ngày

### 2.1. Checklist Buổi Sáng (Sau 2h sáng)

#### Bước 1: Kiểm Tra DAG Status

```sql
-- Query ETL runs trong 24h qua
SELECT 
    er.run_id,
    ej.job_code,
    ej.job_name,
    er.run_date,
    er.started_at,
    er.finished_at,
    er.status,
    er.rows_read,
    er.rows_written,
    er.error_message,
    EXTRACT(EPOCH FROM (er.finished_at - er.started_at)) / 60 as duration_minutes
FROM meta.etl_run er
JOIN meta.etl_job ej ON er.job_id = ej.job_id
WHERE er.run_date >= CURRENT_DATE - INTERVAL '1 day'
ORDER BY er.started_at DESC;
```

**Kết quả mong đợi:**
- Status: `SUCCESS`
- Duration: < 60 phút (tùy volume)
- rows_written > 0

#### Bước 2: Kiểm Tra Airflow UI

1. Mở DAG `minio_ecommerce_dwh_pipeline`
2. Xem Graph View → kiểm tra tất cả tasks:
   - ✅ `start` → `etl_run_start`
   - ✅ `crawl_lazada`, `crawl_tiki` (song song)
   - ✅ `crawl_lazada_reviews`, `crawl_tiki_reviews`
   - ✅ `wait_raw_ready`, `wait_reviews_ready` (sensors)
   - ✅ `upload_to_minio`
   - ✅ `spark_build_star_dwh`
   - ✅ `ml_price_optimization`, `ml_inventory_recommendation`, `ml_customer_segment`
   - ✅ `etl_run_finish` → `end`

3. Nếu có task FAILED:
   - Click vào task → View Log
   - Ghi lại error message
   - Xem phần [Troubleshooting](#6-troubleshooting--recovery)

#### Bước 3: Kiểm Tra Data Volume

```sql
-- Tổng quan volume theo layer
SELECT 
    table_schema,
    table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = schemaname AND table_name = tablename) as column_count
FROM pg_tables
WHERE table_schema IN ('staging', 'ods', 'dwh', 'ml')
ORDER BY table_schema, table_name;
```

---

## 3. Theo Dõi ETL Runs & Logs

### 3.1. Query ETL Overview (Theo Ngày)

```sql
-- Tổng quan ETL runs cho một ngày cụ thể
WITH daily_runs AS (
    SELECT 
        er.run_date,
        ej.job_code,
        ej.job_name,
        COUNT(*) as total_runs,
        COUNT(CASE WHEN er.status = 'SUCCESS' THEN 1 END) as success_count,
        COUNT(CASE WHEN er.status = 'FAILED' THEN 1 END) as failed_count,
        COUNT(CASE WHEN er.status = 'RUNNING' THEN 1 END) as running_count,
        SUM(er.rows_read) as total_rows_read,
        SUM(er.rows_written) as total_rows_written,
        AVG(EXTRACT(EPOCH FROM (er.finished_at - er.started_at)) / 60) as avg_duration_minutes
    FROM meta.etl_run er
    JOIN meta.etl_job ej ON er.job_id = ej.job_id
    WHERE er.run_date = '2025-11-23'  -- Thay đổi ngày
    GROUP BY er.run_date, ej.job_code, ej.job_name
)
SELECT * FROM daily_runs;
```

### 3.2. Chi Tiết Một ETL Run

```sql
-- Xem chi tiết run cụ thể
SELECT 
    er.run_id,
    ej.job_code,
    ej.job_name,
    er.run_date,
    er.started_at,
    er.finished_at,
    er.status,
    er.rows_read,
    er.rows_written,
    er.error_message,
    er.airflow_run_id,
    EXTRACT(EPOCH FROM (er.finished_at - er.started_at)) / 60 as duration_minutes
FROM meta.etl_run er
JOIN meta.etl_job ej ON er.job_id = ej.job_id
WHERE er.run_id = 123;  -- Thay đổi run_id
```

### 3.3. Xem Logs Chi Tiết (Từ Airflow)

**Cách 1: Qua Airflow UI**
1. Mở DAG → Click vào run date
2. Click vào task cần xem
3. Click "Log" button
4. Copy log để phân tích

**Cách 2: Qua File System (Nếu có access)**

```bash
# Logs thường ở:
/opt/airflow/logs/dag_id=minio_ecommerce_dwh_pipeline/
```

### 3.4. Query ETL Logs Từ Database (Nếu có bảng meta.etl_log)

```sql
-- Xem logs chi tiết của một run
SELECT 
    log_id,
    run_id,
    log_level,
    log_message,
    created_at
FROM meta.etl_log
WHERE run_id = 123
ORDER BY created_at;
```

---

## 4. Giám Sát Data Volume & Freshness

### 4.1. Snapshot Volume Theo Ngày

```sql
-- Volume snapshot cho một ngày
WITH table_stats AS (
    SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size_bytes,
        (xpath('/row/cnt/text()', query_to_xml(
            format('select count(*) as cnt from %I.%I', schemaname, tablename),
            false, true, ''
        )))[1]::text::int as row_count
    FROM pg_tables
    WHERE schemaname IN ('staging', 'ods', 'dwh', 'ml')
)
SELECT 
    schemaname as layer,
    tablename,
    size_bytes,
    row_count,
    CASE 
        WHEN schemaname = 'staging' THEN 'Raw data from crawlers'
        WHEN schemaname = 'ods' THEN 'Cleaned operational data'
        WHEN schemaname = 'dwh' THEN 'Star schema warehouse'
        WHEN schemaname = 'ml' THEN 'ML model outputs'
    END as description
FROM table_stats
ORDER BY schemaname, tablename;
```

### 4.2. Kiểm Tra Freshness (Last Loaded At)

```sql
-- Kiểm tra freshness của fact tables
SELECT 
    'dwh.fact_product_daily' as table_name,
    MAX(date_sk) as latest_date_sk,
    (SELECT date_value FROM dwh.dim_date WHERE date_sk = MAX(fpd.date_sk)) as latest_date,
    COUNT(*) as total_rows,
    COUNT(DISTINCT date_sk) as distinct_dates
FROM dwh.fact_product_daily fpd

UNION ALL

SELECT 
    'dwh.fact_review_daily' as table_name,
    MAX(date_sk) as latest_date_sk,
    (SELECT date_value FROM dwh.dim_date WHERE date_sk = MAX(frd.date_sk)) as latest_date,
    COUNT(*) as total_rows,
    COUNT(DISTINCT date_sk) as distinct_dates
FROM dwh.fact_review_daily frd;
```

### 4.3. Volume History (Trend Theo Ngày)

```sql
-- Volume history cho một bảng (nếu có bảng meta.table_stats)
SELECT 
    snapshot_date,
    schema_name,
    table_name,
    row_count,
    size_bytes,
    last_loaded_at
FROM meta.table_stats
WHERE schema_name = 'dwh' 
  AND table_name = 'fact_product_daily'
  AND snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY snapshot_date DESC;
```

**Nếu chưa có bảng meta.table_stats**, có thể tính từ fact tables:

```sql
-- Tính volume history từ fact_product_daily
SELECT 
    dd.date_value as snapshot_date,
    COUNT(*) as row_count,
    COUNT(DISTINCT fpd.product_sk) as distinct_products,
    COUNT(DISTINCT fpd.platform_sk) as distinct_platforms
FROM dwh.fact_product_daily fpd
JOIN dwh.dim_date dd ON fpd.date_sk = dd.date_sk
WHERE dd.date_value >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dd.date_value
ORDER BY dd.date_value DESC;
```

---

## 5. Xử Lý Data Quality Issues

### 5.1. Kiểm Tra Data Quality Issues

```sql
-- Xem danh sách issues chưa xử lý
SELECT 
    issue_id,
    schema_name,
    table_name,
    issue_type,
    severity,
    status,
    affected_rows,
    issue_description,
    detected_at,
    resolved_at
FROM meta.data_quality_issue
WHERE status = 'OPEN'
ORDER BY severity DESC, detected_at DESC;
```

### 5.2. Phân Loại Issues Thường Gặp

#### Issue Type 1: Missing/Null Values

```sql
-- Kiểm tra null values trong fact_product_daily
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN min_price IS NULL THEN 1 END) as null_min_price,
    COUNT(CASE WHEN max_price IS NULL THEN 1 END) as null_max_price,
    COUNT(CASE WHEN avg_price IS NULL THEN 1 END) as null_avg_price,
    COUNT(CASE WHEN product_sk IS NULL THEN 1 END) as null_product_sk
FROM dwh.fact_product_daily
WHERE date_sk = (SELECT MAX(date_sk) FROM dwh.fact_product_daily);
```

#### Issue Type 2: Invalid Prices (Negative, Zero, Outlier)

```sql
-- Kiểm tra giá không hợp lệ
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN min_price < 0 THEN 1 END) as negative_min_price,
    COUNT(CASE WHEN min_price = 0 THEN 1 END) as zero_min_price,
    COUNT(CASE WHEN min_price > 100000000 THEN 1 END) as outlier_min_price,
    COUNT(CASE WHEN max_price < min_price THEN 1 END) as invalid_price_range
FROM dwh.fact_product_daily
WHERE date_sk = (SELECT MAX(date_sk) FROM dwh.fact_product_daily);
```

#### Issue Type 3: Invalid Dates

```sql
-- Kiểm tra dates không hợp lệ
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN date_sk IS NULL THEN 1 END) as null_date_sk,
    COUNT(CASE WHEN date_sk NOT IN (SELECT date_sk FROM dwh.dim_date) THEN 1 END) as invalid_date_sk
FROM dwh.fact_product_daily;
```

#### Issue Type 4: Duplicate Records

```sql
-- Kiểm tra duplicates trong fact_product_daily
SELECT 
    date_sk,
    product_sk,
    platform_sk,
    COUNT(*) as duplicate_count
FROM dwh.fact_product_daily
GROUP BY date_sk, product_sk, platform_sk
HAVING COUNT(*) > 1;
```

### 5.3. Tạo Data Quality Issue (Manual)

```sql
-- Tạo issue mới khi phát hiện vấn đề
INSERT INTO meta.data_quality_issue (
    schema_name,
    table_name,
    issue_type,
    severity,
    status,
    affected_rows,
    issue_description,
    detected_at
) VALUES (
    'dwh',
    'fact_product_daily',
    'INVALID_DATA',
    'HIGH',
    'OPEN',
    150,
    'Found 150 rows with negative prices in latest snapshot',
    NOW()
);
```

### 5.4. Resolve Issue

```sql
-- Đánh dấu issue đã được xử lý
UPDATE meta.data_quality_issue
SET 
    status = 'RESOLVED',
    resolved_at = NOW(),
    resolution_notes = 'Fixed by updating price cleaning logic in Spark job'
WHERE issue_id = 123;
```

---

## 6. Troubleshooting & Recovery

### 6.1. DAG Failed - Crawler Issues

**Triệu chứng:**
- Task `crawl_lazada` hoặc `crawl_tiki` FAILED
- Log: "Không tìm thấy script" hoặc "Playwright error"

**Giải pháp:**

```bash
# 1. Kiểm tra script tồn tại
docker exec airflow-worker ls -la /app/crawlers/lazada/runners/
docker exec airflow-worker ls -la /app/crawlers/tiki/

# 2. Kiểm tra Chrome/Chromium
docker exec airflow-worker which chromium-browser
docker exec airflow-worker which chromedriver

# 3. Test crawl thủ công
docker exec airflow-worker python /app/crawlers/lazada/runners/lazada_with_cookies.py

# 4. Nếu cần, reinstall playwright
docker exec airflow-worker pip install playwright
docker exec airflow-worker playwright install chromium
```

### 6.2. DAG Failed - Spark Job Issues

**Triệu chứng:**
- Task `spark_build_star_dwh` FAILED
- Log: "Initial job has not accepted any resources"

**Giải pháp:**

```bash
# 1. Kiểm tra Spark workers đang chạy
docker exec spark-master spark-submit --master spark://spark-master:7077 --version

# 2. Kiểm tra Spark UI
# Mở: http://localhost:8081
# Xem Workers tab → phải có 2 workers

# 3. Kiểm tra resource allocation
# Đảm bảo DAG config:
# --num-executors 2
# --executor-cores 1
# --executor-memory 1g

# 4. Test Spark job thủ công
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 1g \
  /app/src/spark_jobs/load_cleaned_from_minio.py
```

### 6.3. DAG Failed - Database Connection Issues

**Triệu chứng:**
- Task `spark_build_star_dwh` FAILED
- Log: "Connection refused" hoặc "Authentication failed"

**Giải pháp:**

```bash
# 1. Kiểm tra PostgreSQL đang chạy
docker ps | grep postgres

# 2. Test connection từ Spark container
docker exec spark-master psql -h postgres -p 5432 -U dss_user -d ecommerce_dss_1 -c "SELECT 1;"

# 3. Kiểm tra environment variables
docker exec spark-master env | grep -E "DB_|DATABASE"

# 4. Kiểm tra JDBC driver
docker exec spark-master ls -la /opt/spark/jars/postgresql-*.jar
```

### 6.4. Data Missing - Sensor Timeout

**Triệu chứng:**
- Task `wait_raw_ready` hoặc `wait_reviews_ready` TIMEOUT
- Log: "Sensor timeout after 1800 seconds"

**Giải pháp:**

```bash
# 1. Kiểm tra data có tồn tại không
docker exec airflow-worker ls -la /app/data/outputs/tiki/date=2025-11-23/
docker exec airflow-worker ls -la /app/data/outputs/lazada/date=2025-11-23/

# 2. Nếu data chưa có, kiểm tra crawler logs
# Xem log của task crawl_lazada hoặc crawl_tiki

# 3. Nếu crawler chạy quá lâu, tăng timeout:
# Trong DAG: timeout=3600 (1 giờ thay vì 30 phút)

# 4. Manual trigger crawler nếu cần
docker exec airflow-worker python /app/crawlers/tiki/tiki_crawler.py
```

### 6.5. Recovery - Re-run Failed DAG

**Cách 1: Qua Airflow UI**
1. Mở DAG `minio_ecommerce_dwh_pipeline`
2. Click vào run date bị failed
3. Click "Clear" trên các tasks failed
4. DAG sẽ tự động re-run

**Cách 2: Qua Airflow CLI**

```bash
# Clear failed tasks
docker exec airflow-scheduler airflow tasks clear \
  minio_ecommerce_dwh_pipeline \
  --start-date 2025-11-23 \
  --end-date 2025-11-23

# Trigger DAG lại
docker exec airflow-scheduler airflow dags trigger \
  minio_ecommerce_dwh_pipeline \
  --run-id manual_recovery_$(date +%Y%m%d_%H%M%S)
```

### 6.6. Recovery - Backfill Missing Data

```sql
-- 1. Xác định ngày nào thiếu data
SELECT 
    date_value,
    CASE WHEN EXISTS (
        SELECT 1 FROM dwh.fact_product_daily fpd
        JOIN dwh.dim_date dd ON fpd.date_sk = dd.date_sk
        WHERE dd.date_value = dim_date.date_value
    ) THEN 'HAS_DATA' ELSE 'MISSING' END as status
FROM dwh.dim_date
WHERE date_value >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date_value DESC;

-- 2. Nếu thiếu, trigger DAG cho ngày đó
-- Qua Airflow UI: Trigger DAG với execution_date = ngày thiếu
```

---

## 7. Tối Ưu Performance

### 7.1. Tối Ưu Spark Job

**Vấn đề:** Spark job chạy quá lâu (> 60 phút)

**Giải pháp:**

1. **Tăng số partitions:**
```python
# Trong load_cleaned_from_minio.py
df = df.repartition(200)  # Thay vì mặc định
```

2. **Tối ưu shuffle partitions:**
```bash
# Trong DAG, thêm config:
--conf spark.sql.shuffle.partitions=200
```

3. **Cache intermediate DataFrames:**
```python
# Cache DataFrame được dùng nhiều lần
df_cleaned.cache()
df_dedup.cache()
```

4. **Tăng executor memory (nếu có resource):**
```bash
# Nếu workers có nhiều memory hơn, tăng:
--executor-memory 1.5g
```

### 7.2. Tối Ưu Database Queries

**Vấn đề:** Load dimensions quá chậm

**Giải pháp:**

```sql
-- 1. Tạo indexes cho foreign keys
CREATE INDEX IF NOT EXISTS idx_fact_product_daily_product_sk 
ON dwh.fact_product_daily(product_sk);

CREATE INDEX IF NOT EXISTS idx_fact_product_daily_platform_sk 
ON dwh.fact_product_daily(platform_sk);

CREATE INDEX IF NOT EXISTS idx_fact_product_daily_date_sk 
ON dwh.fact_product_daily(date_sk);

-- 2. Analyze tables sau khi load
ANALYZE dwh.fact_product_daily;
ANALYZE dwh.dim_product;
ANALYZE dwh.dim_platform;
```

### 7.3. Tối Ưu MinIO Upload

**Vấn đề:** Upload MinIO quá chậm

**Giải pháp:**

```python
# Trong upload_to_minio function, sử dụng parallel upload
from concurrent.futures import ThreadPoolExecutor

def upload_file(client, bucket, remote_path, local_path):
    client.fput_object(bucket, remote_path, local_path)

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = []
    for jsonl_file in output_dir.rglob(f"**/date={date}/*.jsonl"):
        relative = jsonl_file.relative_to(output_dir)
        future = executor.submit(
            upload_file, 
            client, 
            bucket, 
            str(relative).replace("\\", "/"), 
            str(jsonl_file)
        )
        futures.append(future)
    
    for future in futures:
        future.result()
```

---

## 8. Best Practices & Checklist

### 8.1. Daily Checklist (Sau 2h sáng)

- [ ] Kiểm tra DAG status trong Airflow UI
- [ ] Query ETL runs trong 24h qua → đảm bảo SUCCESS
- [ ] Kiểm tra data volume → so sánh với ngày trước
- [ ] Kiểm tra freshness → latest date = hôm qua
- [ ] Xem data quality issues → resolve nếu có
- [ ] Kiểm tra Spark job duration → < 60 phút
- [ ] Review error logs nếu có warning

### 8.2. Weekly Checklist

- [ ] Review volume trends (7 ngày) → phát hiện anomaly
- [ ] Analyze data quality issues → tìm pattern
- [ ] Review Spark job performance → tối ưu nếu cần
- [ ] Backup critical tables (nếu cần)
- [ ] Update documentation nếu có thay đổi

### 8.3. Monthly Checklist

- [ ] Review ETL job performance trends
- [ ] Analyze data quality metrics
- [ ] Review và optimize database indexes
- [ ] Cleanup old data (nếu có policy)
- [ ] Review và update DAG configurations

### 8.4. Best Practices

1. **Luôn kiểm tra logs trước khi re-run:**
   - Đọc error message kỹ
   - Tìm root cause, không chỉ fix symptom

2. **Document mọi issue:**
   - Ghi lại vào meta.data_quality_issue
   - Note resolution steps

3. **Monitor proactively:**
   - Setup alerts (nếu có) cho failed runs
   - Check volume trends để phát hiện sớm

4. **Test changes trước khi deploy:**
   - Test Spark job locally trước
   - Test DAG với test data

5. **Backup trước khi thay đổi lớn:**
   - Backup DWH tables trước khi alter schema
   - Backup DAG code trước khi update

---

## 9. API Endpoints (Khi Backend Implement)

Khi backend đã implement các API cho Data Engineer, sử dụng:

### 9.1. ETL Overview
```bash
GET /api/v1/data/etl/overview?date=2025-11-23
```

### 9.2. ETL Runs
```bash
GET /api/v1/data/etl/runs?job_code=MINIO_ECOMMERCE_DWH_PIPELINE&from_date=2025-11-20&to_date=2025-11-23
```

### 9.3. Data Volume
```bash
GET /api/v1/data/volume/snapshot?date=2025-11-23
```

### 9.4. Data Quality
```bash
GET /api/v1/data/quality/issues?status=OPEN&layer=dwh
```

---

## 10. Tài Liệu Tham Khảo

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Spark Documentation](https://spark.apache.org/docs/)
- [MinIO Documentation](https://min.io/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

---

## 📞 Liên Hệ & Hỗ Trợ

Nếu gặp vấn đề không giải quyết được:
1. Check logs chi tiết
2. Review documentation
3. Liên hệ team lead hoặc admin

---

**Last Updated:** 2025-11-23  
**Version:** 1.0

