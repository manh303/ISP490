# 🇻🇳 HƯỚNG DẪN CHẠY DỰ ÁN E-COMMERCE DSS - CHI TIẾT

## 📋 Tổng Quan Dự Án
Dự án phân tích dữ liệu thương mại điện tử Việt Nam với pipeline xử lý dữ liệu real-time: **CRAWL → API → KAFKA → SPARK → DATA WAREHOUSE → ANALYTICS**

**Các platform Việt Nam được hỗ trợ:**
- Shopee Vietnam (35.2% thị phần)
- Lazada Vietnam (28.5% thị phần)
- Tiki Vietnam (15.8% thị phần)
- Sendo Vietnam (10.3% thị phần)
- FPT Shop (5.2% thị phần)
- CellphoneS (3.5% thị phần)

---

## 🔧 1. YÊU CẦU HỆ THỐNG

### Phần mềm cần thiết:
- **Docker Desktop** (phiên bản mới nhất)
- **Docker Compose** v2.0+
- **Git** để clone project
- **Windows 10/11** hoặc **Linux/MacOS**
- **RAM tối thiểu**: 8GB (khuyến nghị 16GB)
- **Ổ cứng trống**: 20GB

### Kiểm tra Docker:
```bash
docker --version
docker-compose --version
```

---

## 🚀 2. CÀI ĐẶT VÀ KHỞI ĐỘNG DỰ ÁN

### Bước 1: Clone dự án
```bash
git clone <repository-url>
cd ecommerce-dss-project
```

### Bước 2: Khởi động toàn bộ hệ thống
```bash
# Khởi động tất cả services
docker-compose up -d

# Kiểm tra trạng thái containers
docker-compose ps
```

### Bước 3: Chờ các services khởi động (5-10 phút)
```bash
# Theo dõi logs
docker-compose logs -f

# Kiểm tra health các services chính
docker-compose logs airflow-webserver
docker-compose logs kafka
docker-compose logs postgres
```

---

## 🌐 3. TRUY CẬP CÁC SERVICES

### 🔗 **Airflow Web UI** (Quản lý pipeline chính)
- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: admin
- **Chức năng**: Giám sát và chạy các DAG pipeline

### 📊 **Backend API**
- **URL**: http://localhost:8000
- **Health Check**: http://localhost:8000/health
- **API Docs**: http://localhost:8000/docs

### 🎯 **Frontend Dashboard**
- **URL**: http://localhost:3000
- **Chức năng**: Dashboard hiển thị analytics

### 📈 **Monitoring Services**
- **Grafana**: http://localhost:3001
- **Prometheus**: http://localhost:9090

### 🗄️ **Database Access**
- **PostgreSQL**: localhost:5432
  - Database: `ecommerce_dss`
  - Username: `dss_user`
  - Password: `dss_password_123`
- **MongoDB**: localhost:27017
  - Username: `admin`
  - Password: `admin_password`
- **Redis**: localhost:6379

---

## 🔄 4. CHẠY PIPELINE VIETNAM E-COMMERCE

### Phương pháp 1: Qua Airflow Web UI (Khuyến nghị)

1. **Truy cập Airflow**: http://localhost:8080
2. **Tìm DAG**: `vietnam_complete_ecommerce_pipeline`
3. **Bật DAG**: Click toggle để enable
4. **Chạy manual**: Click "Trigger DAG"
5. **Theo dõi**: Click vào DAG run để xem progress

### Phương pháp 2: Qua Command Line

```bash
# Trigger pipeline chính
docker exec ecommerce-dss-project-airflow-webserver-1 airflow dags trigger vietnam_complete_ecommerce_pipeline

# Kiểm tra trạng thái
docker exec ecommerce-dss-project-airflow-webserver-1 airflow dags state vietnam_complete_ecommerce_pipeline

# Xem danh sách runs
docker exec ecommerce-dss-project-airflow-webserver-1 airflow dags list-runs -d vietnam_complete_ecommerce_pipeline
```

### Pipeline khác có sẵn:
- `test_vietnam_pipeline`: Test hệ thống
- `vietnam_simple_pipeline`: Demo đơn giản

---

## 📊 5. LUỒNG XỬ LÝ DỮ LIỆU CHI TIẾT

### Giai đoạn 1: CRAWL (Thu thập dữ liệu)
- **Mô tả**: Thu thập dữ liệu từ 6 platform Việt Nam
- **Thời gian**: 2-3 phút
- **Output**: Dữ liệu được lưu trong MongoDB collections
- **Kiểm tra**: MongoDB có collections `crawled_*_products`

### Giai đoạn 2: API (Xử lý qua API)
- **Mô tả**: Chuẩn hóa và làm sạch dữ liệu qua backend API
- **Thời gian**: 1-2 phút
- **Output**: Dữ liệu được xử lý và chuẩn hóa
- **Kiểm tra**: Collections `api_processed_*`

### Giai đoạn 3: KAFKA (Streaming)
- **Mô tả**: Stream dữ liệu qua Kafka topics
- **Topics**: vietnam_products, vietnam_customers, vietnam_sales_events
- **Thời gian**: 30 giây - 1 phút
- **Kiểm tra**: Kafka topics có messages

### Giai đoạn 4: SPARK (Xử lý real-time)
- **Mô tả**: Xử lý stream data với Spark
- **Thời gian**: 30 giây - 2 phút
- **Output**: Dữ liệu được process và load vào warehouse
- **Kiểm tra**: PostgreSQL vietnam_dw schema

### Giai đoạn 5: DATA WAREHOUSE (Lưu trữ)
- **Mô tả**: Load data vào PostgreSQL data warehouse
- **Schema**: vietnam_dw với các bảng fact và dimension
- **Kiểm tra**: Tables có dữ liệu

### Giai đoạn 6: VALIDATION & ANALYTICS
- **Mô tả**: Validate pipeline và tạo báo cáo
- **Output**: Health score và comprehensive report
- **Kiểm tra**: Pipeline alerts trong MongoDB

---

## 🔍 6. GIÁM SÁT VÀ TROUBLESHOOTING

### Kiểm tra logs chi tiết:
```bash
# Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Data pipeline logs
docker-compose logs data-pipeline

# Kafka logs
docker-compose logs kafka

# Database logs
docker-compose logs postgres mongodb
```

### Kiểm tra dữ liệu:

**PostgreSQL:**
```bash
# Kết nối PostgreSQL
docker exec -it ecommerce-dss-project-postgres-1 psql -U dss_user -d ecommerce_dss

# Kiểm tra tables
\dt vietnam_dw.*

# Đếm records
SELECT COUNT(*) FROM vietnam_dw.fact_sales_vn;
```

**MongoDB:**
```bash
# Kết nối MongoDB
docker exec -it ecommerce-dss-project-mongodb-1 mongosh -u admin -p admin_password

# Kiểm tra collections
use vietnam_ecommerce_dss
show collections

# Đếm documents
db.crawled_shopee_products.countDocuments()
```

**Kafka:**
```bash
# Kiểm tra topics
docker exec ecommerce-dss-project-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# Xem messages
docker exec ecommerce-dss-project-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic vietnam_products --from-beginning --max-messages 5
```

---

## 🔧 7. CÁC LỆNH HỮU ÍCH

### Quản lý containers:
```bash
# Dừng tất cả
docker-compose down

# Khởi động lại
docker-compose restart

# Xóa tất cả và rebuild
docker-compose down -v
docker-compose up --build -d

# Xem resource usage
docker stats
```

### Backup và restore:
```bash
# Backup PostgreSQL
docker exec ecommerce-dss-project-postgres-1 pg_dump -U dss_user ecommerce_dss > backup.sql

# Backup MongoDB
docker exec ecommerce-dss-project-mongodb-1 mongodump --username admin --password admin_password --out /tmp/backup
```

---

## 🎯 8. TESTING VÀ VALIDATION

### Test pipeline đơn giản:
```bash
# Chạy test pipeline
docker exec ecommerce-dss-project-airflow-webserver-1 airflow dags trigger test_vietnam_pipeline

# Kiểm tra kết quả
docker exec ecommerce-dss-project-airflow-webserver-1 airflow tasks states-for-dag-run test_vietnam_pipeline <run_id>
```

### Test components riêng lẻ:

**Test Spark processor:**
```bash
docker exec ecommerce-dss-project-data-pipeline-1 python simple_spark_processor.py
```

**Test Kafka producer:**
```bash
cd streaming
python kafka_producer_vietnam.py --duration 30 --customer-rate 2 --product-rate 3
```

---

## 📈 9. PHÂN TÍCH DỮ LIỆU

### Truy vấn analytics cơ bản:

**Top selling products:**
```sql
SELECT
    product_name,
    SUM(quantity) as total_sold,
    SUM(total_amount) as revenue
FROM vietnam_dw.fact_sales_vn f
JOIN vietnam_dw.dim_product_vn p ON f.product_key = p.product_key
GROUP BY product_name
ORDER BY total_sold DESC
LIMIT 10;
```

**Revenue by platform:**
```sql
SELECT
    platform,
    COUNT(*) as orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM vietnam_dw.fact_sales_vn
GROUP BY platform
ORDER BY total_revenue DESC;
```

**Daily trends:**
```sql
SELECT
    DATE(order_date) as date,
    COUNT(*) as daily_orders,
    SUM(total_amount) as daily_revenue
FROM vietnam_dw.fact_sales_vn
WHERE order_date >= NOW() - INTERVAL '7 days'
GROUP BY DATE(order_date)
ORDER BY date;
```

---

## 🚨 10. TROUBLESHOOTING THƯỜNG GẶP

### Lỗi memory/resource:
```bash
# Tăng memory cho Docker Desktop
# Settings > Resources > Memory > 8GB+

# Restart Docker Desktop
```

### Pipeline fails:
1. Kiểm tra logs Airflow
2. Verify data files trong /data directory
3. Check database connections
4. Restart failed tasks từ Airflow UI

### Kafka connection issues:
```bash
# Restart Kafka
docker-compose restart kafka zookeeper

# Kiểm tra network
docker network ls
docker network inspect ecommerce-dss-project_default
```

### Database connection issues:
```bash
# Reset databases
docker-compose down
docker volume prune
docker-compose up -d
```

---

## 📞 11. HỖ TRỢ VÀ LIÊN HỆ

- **Documentation**: Xem thêm các file .md trong thư mục docs/
- **Logs**: Tất cả logs được lưu trong containers, truy cập qua docker-compose logs
- **Health checks**: Tất cả services có health endpoints
- **Monitoring**: Sử dụng Grafana dashboard để theo dõi real-time

---

## 🎉 12. KẾT LUẬN

Sau khi hoàn thành hướng dẫn này, bạn sẽ có:
- ✅ Hệ thống E-commerce DSS hoạt động đầy đủ
- ✅ Pipeline xử lý dữ liệu Vietnam real-time
- ✅ Data warehouse với dữ liệu từ 6 platform lớn nhất VN
- ✅ Dashboard monitoring và analytics
- ✅ Khả năng mở rộng và tùy chỉnh

**Thời gian setup đầy đủ**: 15-30 phút (tùy theo tốc độ internet và hardware)

**Lưu ý**: Đây là dự án demo/development. Để production cần cấu hình bảo mật và performance tuning thêm.