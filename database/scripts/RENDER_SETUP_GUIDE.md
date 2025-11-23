# 🚀 Hướng Dẫn Setup Database Render

## 📋 Thông Tin Database

**Database URL:**
```
postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss
```

**Connection Details:**
- Host: `dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com`
- Port: `5432` (default)
- Database: `ecommerce_dss`
- User: `dss_user`
- Password: `IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4`

---

## 🔧 Cài Đặt Requirements

```bash
# Cài đặt psycopg2 (PostgreSQL driver)
pip install psycopg2-binary
```

---

## 📝 Các Bước Setup

### Bước 1: Kiểm Tra Kết Nối

```bash
# Test kết nối database
python -c "import psycopg2; conn = psycopg2.connect('postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss'); print('✅ Kết nối thành công!'); conn.close()"
```

### Bước 2: Kiểm Tra Schemas Hiện Tại

```bash
python database/scripts/check_schemas_render.py
```

**Kết quả mong đợi:**
- Hiển thị schemas và bảng đang tồn tại
- Báo thiếu schema nào (nếu có)

### Bước 3: Tạo Schema META (Nếu Chưa Có)

```bash
python database/scripts/setup_meta_schema_render.py
```

**Script này sẽ:**
- Tạo schema `meta`
- Tạo 7 bảng: etl_job, etl_run, etl_log, table_stats, data_quality_issue, data_quality_rule, data_quality_check_result
- Insert 2 ETL jobs mặc định

### Bước 4: Tạo Schema DWH + ML (Nếu Chưa Có)

```bash
python database/scripts/setup_dwh_schema_render.py
```

**Script này sẽ:**
- Tạo schema `dwh` và `ml`
- Tạo 5 dimension tables trong dwh
- Tạo 3 fact tables trong dwh
- Tạo 3 bảng ML

### Bước 5: Kiểm Tra Lại

```bash
python database/scripts/check_schemas_render.py
```

**Kết quả mong đợi:**
- ✅ Tất cả schemas đã tồn tại
- ✅ Tất cả bảng đã được tạo
- Hiển thị số dòng trong mỗi bảng

---

## 🔍 Kiểm Tra Thủ Công (Qua psql)

### Kết nối qua psql

```bash
psql "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"
```

### Các lệnh kiểm tra

```sql
-- Xem tất cả schemas
\dn

-- Xem bảng trong schema meta
\dt meta.*

-- Xem bảng trong schema dwh
\dt dwh.*

-- Xem bảng trong schema ml
\dt ml.*

-- Xem cấu trúc bảng
\d dwh.fact_product_daily

-- Kiểm tra ETL jobs
SELECT * FROM meta.etl_job;

-- Kiểm tra số dòng
SELECT 
    schemaname,
    tablename,
    COUNT(*) as row_count
FROM pg_tables
WHERE schemaname IN ('meta', 'dwh', 'ml')
GROUP BY schemaname, tablename;
```

---

## 🔧 Troubleshooting

### Lỗi: Connection timeout

```
Giải pháp:
1. Kiểm tra firewall/network
2. Kiểm tra Render database có đang chạy
3. Kiểm tra IP whitelist (nếu có)
```

### Lỗi: Permission denied

```
Giải pháp:
1. Kiểm tra user có quyền CREATE SCHEMA
2. Liên hệ admin để grant permissions:

GRANT CREATE ON DATABASE ecommerce_dss TO dss_user;
```

### Lỗi: Table already exists

```
Giải pháp:
Script sử dụng CREATE IF NOT EXISTS nên không sao.
Nếu muốn recreate:

-- ⚠️ CẨN THẬN: Xóa toàn bộ dữ liệu
DROP SCHEMA IF EXISTS meta CASCADE;
DROP SCHEMA IF EXISTS dwh CASCADE;
DROP SCHEMA IF EXISTS ml CASCADE;

-- Sau đó chạy lại scripts
```

---

## 📊 Monitoring

### Kiểm tra kích thước database

```sql
SELECT 
    pg_size_pretty(pg_database_size('ecommerce_dss')) as database_size;
```

### Kiểm tra kích thước từng schema

```sql
SELECT 
    schema_name,
    COUNT(*) as table_count,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as total_size
FROM pg_tables
WHERE schemaname IN ('meta', 'dwh', 'ml')
GROUP BY schema_name;
```

### Kiểm tra connections

```sql
SELECT 
    datname,
    usename,
    application_name,
    state,
    query
FROM pg_stat_activity
WHERE datname = 'ecommerce_dss';
```

---

## 🚀 Next Steps

Sau khi setup xong schemas:

1. **Chạy Airflow DAG** `minio_ecommerce_dwh_pipeline` để load dữ liệu
2. **Monitor ETL runs** qua `meta.etl_run`
3. **Check data quality** qua `meta.data_quality_issue`
4. **Setup backend API** để expose data cho frontend

---

## 📝 Notes

- Database trên Render có giới hạn connections (thường 20-100)
- Nên đóng connections sau khi dùng xong
- Backup định kỳ (Render có auto backup)
- Monitor storage usage (Render free tier có giới hạn)

---

## ✅ Checklist

Setup hoàn chỉnh khi:

- [ ] Kết nối database thành công
- [ ] Schema meta đã được tạo (7 bảng)
- [ ] Schema dwh đã được tạo (8 bảng)
- [ ] Schema ml đã được tạo (3 bảng)
- [ ] ETL jobs mặc định đã được insert
- [ ] Foreign keys và indexes đã được tạo
- [ ] Test query thành công

---

**Last Updated:** 2025-11-23

