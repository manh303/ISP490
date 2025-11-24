# 📋 Data Engineer - Next Steps

## ✅ Đã Hoàn Thành

- [x] Setup schema META (7 bảng)
- [x] Setup schema DWH (8 bảng)
- [x] Setup schema ML (3 bảng)

---

## 🚀 Bước Tiếp Theo

### 1. Cấu Hình Airflow

**Cách 1: Update Docker Compose Environment**

Thêm vào `docker-compose.yml` trong phần `x-airflow-common-env`:

```yaml
x-airflow-common-env:
  &airflow-common-env
  # ... các biến khác ...
  
  # Database Render
  DATABASE_URL: postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss
  DB_HOST: dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com
  DB_PORT: "5432"
  DB_NAME: ecommerce_dss
  DB_USER: dss_user
  DB_PASSWORD: IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4
```

**Cách 2: Update file .env**

```bash
# Copy file .env.render
cp .env.render .env

# Hoặc thêm vào .env hiện tại
cat .env.render >> .env
```

**Cách 3: Set trong Airflow UI**

1. Mở Airflow UI: http://localhost:8080
2. Admin → Connections
3. Tìm connection `postgres_default`
4. Update:
   - Host: `dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com`
   - Schema: `ecommerce_dss`
   - Login: `dss_user`
   - Password: `IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4`
   - Port: `5432`

### 2. Restart Airflow

```bash
# Restart để load biến môi trường mới
docker-compose restart airflow-scheduler airflow-worker airflow-webserver
```

### 3. Test Connection

```bash
# Test từ Airflow worker
docker exec airflow-worker python -c "
import os
import psycopg2
db_url = os.getenv('DATABASE_URL')
conn = psycopg2.connect(db_url)
print('✅ Kết nối thành công!')
conn.close()
"
```

### 4. Chạy DAG

1. Mở Airflow UI: http://localhost:8080
2. Tìm DAG: `minio_ecommerce_dwh_pipeline`
3. Unpause nếu đang pause
4. Click "Trigger DAG"
5. Chọn execution_date (hoặc để mặc định)
6. Click "Trigger"

### 5. Monitor DAG Run

**Theo dõi qua Airflow UI:**
- Graph View: Xem flow tasks
- Log: Click vào từng task để xem log

**Theo dõi qua Database:**

```sql
-- Xem ETL run mới nhất
SELECT 
    er.run_id,
    ej.job_code,
    er.run_date,
    er.started_at,
    er.finished_at,
    er.status,
    er.rows_written,
    EXTRACT(EPOCH FROM (er.finished_at - er.started_at)) / 60 as duration_minutes
FROM meta.etl_run er
JOIN meta.etl_job ej ON er.job_id = ej.job_id
ORDER BY er.started_at DESC
LIMIT 5;
```

---

## 📊 Monitoring Hàng Ngày

### Check ETL Status

```bash
# Chạy script kiểm tra
python database/scripts/check_schemas_render.py
```

Hoặc query trực tiếp:

```sql
-- ETL runs trong 24h qua
SELECT 
    ej.job_code,
    er.run_date,
    er.status,
    er.started_at,
    er.finished_at,
    EXTRACT(EPOCH FROM (er.finished_at - er.started_at)) / 60 as duration_minutes
FROM meta.etl_run er
JOIN meta.etl_job ej ON er.job_id = ej.job_id
WHERE er.started_at >= NOW() - INTERVAL '24 hours'
ORDER BY er.started_at DESC;
```

### Check Data Volume

```sql
-- Volume snapshot
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname IN ('meta', 'dwh', 'ml')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Check Data Freshness

```sql
-- Latest data date
SELECT 
    'fact_product_daily' as table_name,
    MAX(dd.date_value) as latest_date,
    CURRENT_DATE - MAX(dd.date_value) as days_behind
FROM dwh.fact_product_daily fpd
JOIN dwh.dim_date dd ON fpd.date_sk = dd.date_sk;
```

### Check Data Quality

```sql
-- Open issues
SELECT 
    issue_id,
    schema_name,
    table_name,
    issue_type,
    severity,
    affected_rows,
    issue_description,
    detected_at
FROM meta.data_quality_issue
WHERE status = 'OPEN'
ORDER BY severity DESC, detected_at DESC;
```

---

## 🔧 Sử Dụng Queries Monitoring

Tất cả queries đã chuẩn bị sẵn trong:
```
database/scripts/data_engineer_queries.sql
```

**Cách dùng:**

```bash
# Kết nối psql
psql "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

# Chạy query cụ thể (copy từ file)
```

Hoặc dùng Python script:

```python
import psycopg2

DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Chạy query
cur.execute("""
    SELECT job_code, status, started_at 
    FROM meta.etl_run 
    ORDER BY started_at DESC 
    LIMIT 5;
""")

for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
```

---

## 📚 Tài Liệu Tham Khảo

1. **Hướng dẫn Data Engineer:**
   - `docs/DATA_ENGINEER_GUIDE.md`

2. **Queries monitoring:**
   - `database/scripts/data_engineer_queries.sql`

3. **Setup Render:**
   - `database/scripts/RENDER_SETUP_GUIDE.md`

4. **Architecture:**
   - `docs/architecture.txt`

---

## ✅ Checklist Hoàn Thành

- [ ] Airflow đã được cấu hình với database Render
- [ ] Test connection thành công
- [ ] DAG chạy thành công ít nhất 1 lần
- [ ] Dữ liệu đã được load vào DWH (fact_product_daily có data)
- [ ] Meta logging hoạt động (meta.etl_run có records)
- [ ] Queries monitoring chạy thành công

---

## 🎓 Daily Workflow

### Buổi Sáng (Sau 2h sáng khi DAG chạy xong)

1. **Check DAG status:**
   ```bash
   python database/scripts/check_schemas_render.py
   ```

2. **Review ETL runs:**
   - Mở Airflow UI
   - Xem latest run của `minio_ecommerce_dwh_pipeline`
   - Nếu FAILED → xem log và troubleshoot

3. **Check data volume:**
   ```sql
   -- Số dòng mới hôm qua
   SELECT COUNT(*) 
   FROM dwh.fact_product_daily 
   WHERE date_sk = (SELECT date_sk FROM dwh.dim_date WHERE date_value = CURRENT_DATE - 1);
   ```

4. **Check data quality:**
   ```sql
   SELECT * FROM meta.data_quality_issue WHERE status = 'OPEN';
   ```

### Khi Có Issue

1. Xem log chi tiết trong Airflow
2. Query `meta.etl_run` để xem error_message
3. Troubleshoot theo hướng dẫn trong `docs/DATA_ENGINEER_GUIDE.md`
4. Fix và re-run DAG

---

**Good luck! 🚀**

Nếu gặp vấn đề, tham khảo:
- Airflow logs
- `meta.etl_run` table
- `docs/DATA_ENGINEER_GUIDE.md` phần Troubleshooting

