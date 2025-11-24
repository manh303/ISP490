# 📋 Hướng Dẫn Setup Schemas

## 🎯 Mục Tiêu
Tạo đầy đủ các schemas (meta, dwh, ml) cho hệ thống E-commerce DSS.

---

## 📝 Các Bước Thực Hiện

### Bước 1: Kiểm Tra Schemas Hiện Tại

```bash
# Kết nối database
psql -h localhost -p 5433 -U dss_user -d ecommerce_dss

# Chạy script kiểm tra
\i database/scripts/check_schemas.sql
```

Hoặc:

```bash
psql -h localhost -p 5433 -U dss_user -d ecommerce_dss -f database/scripts/check_schemas.sql
```

### Bước 2: Tạo Schema META (Nếu Chưa Có)

```bash
psql -h localhost -p 5433 -U dss_user -d ecommerce_dss -f database/schema/meta_schema.sql
```

**Schema meta bao gồm:**
- ✅ `meta.etl_job` - Định nghĩa các ETL jobs
- ✅ `meta.etl_run` - Lịch sử các lần chạy ETL
- ✅ `meta.etl_log` - Log chi tiết từng bước
- ✅ `meta.table_stats` - Thống kê volume và freshness
- ✅ `meta.data_quality_issue` - Các vấn đề về chất lượng dữ liệu
- ✅ `meta.data_quality_rule` - Định nghĩa rules kiểm tra
- ✅ `meta.data_quality_check_result` - Kết quả kiểm tra

### Bước 3: Tạo Schema DWH (Nếu Chưa Có)

```bash
psql -h localhost -p 5433 -U dss_user -d ecommerce_dss -f database/schema/datawarehouse.sql
```

**Schema dwh bao gồm:**

**Dimensions:**
- ✅ `dwh.dim_date` - Bảng ngày tháng
- ✅ `dwh.dim_platform` - Platform (Tiki, Lazada)
- ✅ `dwh.dim_brand` - Thương hiệu
- ✅ `dwh.dim_category` - Danh mục sản phẩm
- ✅ `dwh.dim_product` - Sản phẩm

**Facts:**
- ✅ `dwh.fact_product_daily` - Dữ liệu sản phẩm theo ngày
- ✅ `dwh.fact_review` - Review chi tiết
- ✅ `dwh.fact_review_daily` - Review tổng hợp theo ngày

**Schema ml bao gồm:**
- ✅ `ml.dim_ml_model` - Định nghĩa ML models
- ✅ `ml.fact_price_prediction` - Dự đoán giá
- ✅ `ml.fact_product_recommendation` - Gợi ý sản phẩm

### Bước 4: Kiểm Tra Lại

```bash
psql -h localhost -p 5433 -U dss_user -d ecommerce_dss -f database/scripts/check_schemas.sql
```

Tất cả các bảng phải có status ✅.

---

## 🔍 Kiểm Tra Chi Tiết

### Kiểm Tra Schema META

```sql
-- Xem các bảng trong schema meta
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'meta'
ORDER BY table_name;

-- Kiểm tra ETL job mặc định
SELECT * FROM meta.etl_job;
```

### Kiểm Tra Schema DWH

```sql
-- Xem các bảng trong schema dwh
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'dwh'
ORDER BY table_name;

-- Kiểm tra cấu trúc fact_product_daily
\d dwh.fact_product_daily

-- Kiểm tra foreign keys
SELECT 
    tc.table_name, 
    kcu.column_name, 
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY' 
  AND tc.table_schema = 'dwh'
ORDER BY tc.table_name;
```

### Kiểm Tra Indexes

```sql
-- Xem indexes trong schema dwh
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname IN ('dwh', 'ml')
ORDER BY schemaname, tablename, indexname;
```

---

## ⚠️ Lưu Ý

1. **Nếu schema đã tồn tại:**
   - Script sử dụng `CREATE TABLE IF NOT EXISTS` nên an toàn
   - Không làm mất dữ liệu hiện có
   - Chỉ tạo các bảng chưa có

2. **Nếu cần xóa và tạo lại:**
   ```sql
   -- ⚠️ CẨN THẬN: Xóa toàn bộ dữ liệu
   DROP SCHEMA IF EXISTS meta CASCADE;
   DROP SCHEMA IF EXISTS dwh CASCADE;
   DROP SCHEMA IF EXISTS ml CASCADE;
   
   -- Sau đó chạy lại các script tạo schema
   ```

3. **Permissions:**
   - Đảm bảo user `dss_user` có quyền CREATE SCHEMA và CREATE TABLE
   ```sql
   GRANT CREATE ON DATABASE ecommerce_dss TO dss_user;
   GRANT ALL PRIVILEGES ON SCHEMA meta TO dss_user;
   GRANT ALL PRIVILEGES ON SCHEMA dwh TO dss_user;
   GRANT ALL PRIVILEGES ON SCHEMA ml TO dss_user;
   ```

---

## 📊 So Sánh Với Code

Schema DWH trong `database/schema/datawarehouse.sql` phải khớp với:
- `data-pipeline/src/spark_jobs/load_cleaned_from_minio.py` (STAR_SCHEMA_SQL_TEMPLATE)
- `airflow/dags/minio_pipeline_dag.py` (sử dụng meta.etl_job, meta.etl_run)

Nếu có khác biệt, cần cập nhật cho đồng bộ.

---

## ✅ Checklist

- [ ] Schema meta đã được tạo
- [ ] Tất cả 7 bảng trong meta đã tồn tại
- [ ] Schema dwh đã được tạo
- [ ] Tất cả 5 dimension tables đã tồn tại
- [ ] Tất cả 3 fact tables đã tồn tại
- [ ] Schema ml đã được tạo
- [ ] Tất cả 3 bảng ML đã tồn tại
- [ ] Foreign keys đã được tạo đúng
- [ ] Indexes đã được tạo đúng
- [ ] ETL job mặc định đã được insert

---

**Last Updated:** 2025-11-23

