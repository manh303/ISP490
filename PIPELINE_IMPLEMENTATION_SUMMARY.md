# Data Pipeline Implementation Summary

## Tổng Quan

Đã tạo một luồng xử lý dữ liệu hoàn chỉnh (End-to-End Data Pipeline) với các bước sau:

```
Crawl → Upload MinIO → Spark Cleaning → Standardization → 
Data Quality → Deduplication → Category Mapping → 
Identifier Sync → Load Warehouse
```

## Files Đã Tạo

### 1. Airflow DAG Orchestration

**File**: `airflow/dags/complete_data_pipeline.py`

Phân bố công việc thành các giai đoạn:

- **Stage 1 - Data Crawling**: Crawl từ Lazada, Tiki, Shopee
- **Stage 2 - MinIO Upload**: Upload dữ liệu thô vào MinIO
- **Stage 3 - Spark Processing**: 
  - Cleaning
  - Standardization
  - Data Quality Check
  - Deduplication
- **Stage 4 - Category Mapping**: Ánh xạ danh mục chuẩn
- **Stage 5 - Identifier Sync**: Đồng bộ định danh sản phẩm
- **Final - Validation**: Kiểm tra chất lượng đầu ra

### 2. Spark Processing Jobs

Các script xử lý dữ liệu lớn bằng Spark:

#### `data-pipeline/src/spark_jobs/data_cleaning.py`
- Xóa bản ghi không hợp lệ
- Làm sạch giá trị NULL
- Chuẩn hóa text data
- Xóa dữ liệu ngoại lệ (giá âm, rating ngoài 0-5)

#### `data-pipeline/src/spark_jobs/data_standardization.py`
- Chuẩn hóa định dạng giá (VND)
- Chuẩn hóa định dạng datetime (ISO 8601)
- Chuẩn hóa tên danh mục
- Chuẩn hóa rating (scale 0-5)
- Sắp xếp lại cột nhất quán

#### `data-pipeline/src/spark_jobs/data_quality.py`
- Kiểm tra giá trị NULL
- Kiểm tra phạm vi giá trị
- Phát hiện bản ghi trùng
- Tính toán thống kê cột
- Kiểm tra tính mới của dữ liệu
- Tạo báo cáo chất lượng JSON

#### `data-pipeline/src/spark_jobs/deduplication.py`
- Loại bỏ bản ghi trùng chính xác
- Loại bỏ gần đúp (fuzzy matching)
- Xác định sản phẩm cross-platform
- Thêm metadata dedup

#### `data-pipeline/src/spark_jobs/category_mapping.py`
- Ánh xạ danh mục thô → danh mục chuẩn
- Thêm phân cấp danh mục (level 1, level 2)
- Xử lý danh mục không được ánh xạ
- Ghi lại phân bố danh mục

#### `data-pipeline/src/spark_jobs/identifier_synchronization.py`
- Tạo global product ID (MD5 hash)
- Tạo source-specific identifiers
- Xác định sản phẩm trên nhiều nền tảng
- Tạo bảng mapping định danh
- Thêm metadata đồng bộ

### 3. Support Scripts

#### `scripts/upload_to_minio.py`
- Upload files từ thư mục địa phương lên MinIO
- Hỗ trợ cấu trúc thư mục phân cấp
- Kiểm tra bucket tồn tại, tạo nếu cần

#### `scripts/load_to_warehouse.py`
- Load dữ liệu JSON từ MinIO/filesystem
- Tạo bảng staging PostgreSQL
- Insert/upsert records với conflict handling
- Hỗ trợ bulk insert 1000 records/batch

#### `scripts/validate_pipeline.py`
- Kiểm tra file tồn tại
- Kiểm tra cấu trúc JSON
- Kiểm tra cột bắt buộc
- Tạo báo cáo validation JSON
- Xác định trạng thái overall (PASS/WARN/FAIL)

### 4. Docker Configuration

#### `docker-compose.pipeline.yml`

Stack hoàn chỉnh với:

**Databases**:
- PostgreSQL (ecommerce_dss_1)
- Redis (caching)

**Storage**:
- MinIO (S3-compatible object storage)

**Streaming** (Optional):
- Zookeeper
- Kafka

**Computation**:
- Spark Master + 2 Workers
- Spark History Server

**Orchestration**:
- Airflow Database (PostgreSQL)
- Airflow Webserver (port 8080)
- Airflow Scheduler
- Airflow Worker (Celery)

**Monitoring** (Optional):
- Prometheus
- Grafana

### 5. Documentation & Scripts

#### `PIPELINE_GUIDE.md`
- Kiến trúc pipeline chi tiết
- Hướng dẫn quick start
- Cách chạy từng stage
- Troubleshooting guide
- Performance tips

#### `start_pipeline.sh` (Linux/Mac)
- `./start_pipeline.sh start` - Start pipeline
- `./start_pipeline.sh stop` - Stop pipeline
- `./start_pipeline.sh restart` - Restart
- `./start_pipeline.sh status` - Kiểm tra trạng thái
- `./start_pipeline.sh logs [service]` - Xem logs
- `./start_pipeline.sh init` - Init MinIO buckets
- `./start_pipeline.sh trigger` - Trigger DAG

#### `start_pipeline.ps1` (Windows)
- Phiên bản PowerShell tương tự

## Kiến Trúc Chi Tiết

### Data Flow

```
Raw Data (JSON)
      ↓
   MinIO
      ↓
Spark Cleaning
   ↓
Spark Standardization
   ↓
Data Quality Check → Quality Report
   ↓
Spark Deduplication
   ↓
Category Mapping
   ↓
Identifier Synchronization
      ↓
PostgreSQL Staging Table
      ↓
Validation Report
```

### File Organization in MinIO

```
s3://raw-data/
  ├── lazada/2025-01-15/
  ├── tiki/2025-01-15/
  └── shopee/2025-01-15/

s3://processed-data/
  ├── cleaned/2025-01-15/
  ├── standardized/2025-01-15/
  ├── quality-reports/2025-01-15/
  ├── deduplicated/2025-01-15/
  ├── category-mapped/2025-01-15/
  ├── synchronized/2025-01-15/
  └── identifier-mappings/2025-01-15/
```

## Ports & Access Points

| Service | Port | Access |
|---------|------|--------|
| PostgreSQL | 5433 | postgres://dss_user@localhost:5433/ecommerce_dss_1 |
| MinIO API | 9000 | http://localhost:9000 |
| MinIO Console | 9001 | http://localhost:9001 |
| Spark Master | 7077 | spark://spark-master:7077 |
| Spark Master UI | 8081 | http://localhost:8081 |
| Spark Worker 1 UI | 8082 | http://localhost:8082 |
| Spark Worker 2 UI | 8083 | http://localhost:8083 |
| Spark History | 18080 | http://localhost:18080 |
| Airflow Web | 8080 | http://localhost:8080 |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3001 | http://localhost:3001 |

## Quick Start Commands

### Start Pipeline

**Linux/Mac**:
```bash
chmod +x start_pipeline.sh
./start_pipeline.sh start
```

**Windows**:
```powershell
.\start_pipeline.ps1 -Action start
```

**Manual Docker**:
```bash
docker-compose -f docker-compose.pipeline.yml up -d
```

### Initialize MinIO Buckets

```bash
./start_pipeline.sh init
# hoặc
docker-compose -f docker-compose.pipeline.yml exec minio \
  mc mb minio/raw-data minio/processed-data
```

### Trigger Pipeline

```bash
./start_pipeline.sh trigger
# hoặc vào http://localhost:8080 và trigger DAG manually
```

### View Logs

```bash
./start_pipeline.sh logs airflow-scheduler
./start_pipeline.sh logs spark-master
```

### Check Status

```bash
./start_pipeline.sh status
docker-compose -f docker-compose.pipeline.yml ps
```

## Customization

### 1. Category Mapping

Tạo file `config/category_mapping.json`:

```json
{
  "mappings": {
    "Electronics": ["điện tử", "electronics"],
    "Fashion": ["thời trang", "fashion"],
    "...": ["..."]
  }
}
```

### 2. Data Quality Thresholds

Sửa trong `data_quality.py`:
- Null percentage thresholds
- Value range validations
- Duplicate percentage limits

### 3. Spark Resources

Sửa trong `docker-compose.pipeline.yml`:
```yaml
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=4g
```

### 4. Crawl Schedule

Sửa trong `complete_data_pipeline.py`:
```python
schedule_interval='@daily'  # Change to '@hourly', etc.
```

## Monitoring & Observability

### Metrics Available

- Spark Job Metrics (UI + History Server)
- Airflow Task Duration & Status
- PostgreSQL Connection Pool
- MinIO Upload/Download Rates
- Prometheus Metrics

### Grafana Dashboards

- Spark Cluster Overview
- Pipeline Execution Times
- Data Quality Trends
- Resource Utilization

## Performance Considerations

1. **Spark Partitioning**: Dữ liệu phân vùng theo ngày
2. **Batch Processing**: Insert 1000 records/batch vào DB
3. **MinIO Optimization**: Sử dụng multipart upload
4. **Caching**: Redis cho metadata caching
5. **Parallel Processing**: 2 Spark workers

## Scaling Recommendations

- **More Data**: Thêm Spark workers trong docker-compose
- **Real-time**: Enable Kafka streaming (optional)
- **Cloud**: Port to AWS EMR/GCP Dataproc
- **Monitoring**: Thêm ELK stack cho centralized logging

## Troubleshooting

### Spark Jobs Fail
```bash
docker logs pipeline-spark-master
# Check http://localhost:8081
```

### MinIO Connection Issues
```bash
docker exec pipeline-minio curl -f http://localhost:9000/minio/health/live
docker exec pipeline-minio mc ls minio/
```

### Airflow DAG Not Running
```bash
docker logs pipeline-airflow-scheduler
docker exec pipeline-airflow-webserver airflow dags test <dag_id> 2025-01-15
```

## Next Steps

1. ✅ Deploy pipeline infrastructure
2. ✅ Configure crawlers
3. ✅ Set up category mapping
4. ⬜ Monitor pipeline executions
5. ⬜ Tune performance based on metrics
6. ⬜ Implement incremental loading
7. ⬜ Add data quality alerting

## Support Files Structure

```
ecommerce-dss-project/
├── airflow/dags/
│   └── complete_data_pipeline.py
├── data-pipeline/src/spark_jobs/
│   ├── data_cleaning.py
│   ├── data_standardization.py
│   ├── data_quality.py
│   ├── deduplication.py
│   ├── category_mapping.py
│   └── identifier_synchronization.py
├── scripts/
│   ├── upload_to_minio.py
│   ├── load_to_warehouse.py
│   └── validate_pipeline.py
├── docker-compose.pipeline.yml
├── start_pipeline.sh
├── start_pipeline.ps1
├── PIPELINE_GUIDE.md
└── PIPELINE_IMPLEMENTATION_SUMMARY.md
```

## Conclusion

Pipeline hoàn chỉnh được xây dựng với:
- ✅ Orchestration (Airflow)
- ✅ Distributed Processing (Spark)
- ✅ Object Storage (MinIO)
- ✅ Data Warehouse (PostgreSQL)
- ✅ Monitoring (Prometheus + Grafana)
- ✅ Easy Deployment (Docker Compose)

Sẵn sàng cho production data processing!
