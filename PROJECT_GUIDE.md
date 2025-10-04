# 🇻🇳 VIETNAM E-COMMERCE STREAMING DATA PLATFORM
## Hướng dẫn chi tiết từng bước chạy dự án

---

## 📋 **TỔNG QUAN DỰ ÁN**

Dự án này xây dựng một **Real-time Streaming Data Platform** cho Vietnam E-commerce với:
- 🏭 **Data Pipeline**: Kafka → Spark → Data Warehouse → Data Lake
- 📊 **Data Warehouse**: PostgreSQL (vietnam_dw schema)
- 🗄️ **Data Lake**: MongoDB (vietnam_ecommerce_lake database)
- ⚡ **Streaming**: Apache Kafka + Custom Processors
- 🔄 **Orchestration**: Apache Airflow
- 📈 **Monitoring**: Prometheus + Grafana

---

## 🔧 **KIỂM TRA HIỆN TRẠNG DỰ ÁN**

### **Bước 1: Kiểm tra Docker Containers**
```bash
# Kiểm tra tất cả containers đang chạy
docker ps

# Kết quả mong đợi:
# - ecommerce-dss-project-postgres-1    (PostgreSQL)
# - ecommerce-dss-project-mongodb-1     (MongoDB)
# - kafka                               (Apache Kafka)
# - spark-master                        (Spark Master)
# - spark-worker-1                      (Spark Worker)
# - ecommerce-dss-project-airflow-*     (Airflow components)
```

### **Bước 2: Kiểm tra Services Status**
```bash
# 🔍 Kafka status
docker exec kafka ls /opt/bitnami/kafka/bin/

# 🔍 PostgreSQL status
docker exec ecommerce-dss-project-postgres-1 psql -U dss_user -d ecommerce_dss -c "SELECT version();"

# 🔍 MongoDB status
docker exec ecommerce-dss-project-mongodb-1 mongosh --eval "db.adminCommand('ismaster')"

# 🔍 Spark status
curl -s http://localhost:8081 | grep -q "Spark Master"
```

---

## 📊 **KIỂM TRA DATA WAREHOUSE (PostgreSQL)**

### **Bước 3: Truy cập PostgreSQL**
```bash
# Kết nối vào PostgreSQL
docker exec -it ecommerce-dss-project-postgres-1 psql -U dss_user -d ecommerce_dss
```

### **Trong PostgreSQL psql:**
```sql
-- ✅ BƯỚC QUAN TRỌNG: Set search path để thấy tables
SET search_path = vietnam_dw, public;

-- 📋 Xem tất cả tables
\d

-- 📊 Xem tổng quan dữ liệu
SELECT * FROM v_all_tables;

-- 👥 Kiểm tra customers (1,000 records)
SELECT COUNT(*), province FROM dim_customer_vn GROUP BY province ORDER BY COUNT(*) DESC LIMIT 5;

-- 📦 Kiểm tra products (500 records)
SELECT COUNT(*), category FROM dim_product_vn GROUP BY category ORDER BY COUNT(*) DESC LIMIT 5;

-- 💰 Kiểm tra sales
SELECT COUNT(*) FROM fact_sales_vn;

-- 🎯 Kiểm tra activities (10,000 records)
SELECT COUNT(*), activity_type FROM fact_customer_activity_vn GROUP BY activity_type;

-- Thoát psql
\q
```

**📈 Kết quả mong đợi:**
- ✅ **dim_customer_vn**: 1,000 customers
- ✅ **dim_product_vn**: 500 products
- ✅ **fact_customer_activity_vn**: 10,000 activities
- ⚠️ **fact_sales_vn**: 0-N sales (tùy streaming)

---

## 🗄️ **KIỂM TRA DATA LAKE (MongoDB)**

### **Bước 4: Truy cập MongoDB**
```bash
# Kết nối vào MongoDB
docker exec -it ecommerce-dss-project-mongodb-1 mongosh vietnam_ecommerce_lake
```

### **Trong MongoDB shell:**
```javascript
// 📋 Xem tất cả collections
show collections

// 📊 Kiểm tra sample data
db.analytics_trends.find().pretty()
db.ml_predictions.find().pretty()

// Thoát MongoDB
exit
```

---

## ⚡ **STREAMING PIPELINE STATUS**

### **Bước 5: Kiểm tra Kafka Producer**
```bash
# Kiểm tra producer đang chạy
tasklist | findstr python

# Nếu chưa chạy, khởi động producer:
python streaming/kafka_producer_vietnam.py
```

**📊 Producer sẽ tạo:**
- 🔄 **78 records/second** tổng cộng
- 👥 **5 customers/second**
- 📦 **3 products/second**
- 💰 **20 sales events/second**
- 🎯 **50 activities/second**

### **Bước 6: Kiểm tra Data Processor**
```bash
# Kiểm tra processor đang chạy trong container
docker exec ecommerce-dss-project-data-pipeline-1 ps aux | grep simple_spark_processor

# Nếu chưa chạy, khởi động processor:
docker exec -d ecommerce-dss-project-data-pipeline-1 python simple_spark_processor.py
```

### **Bước 7: Verify Streaming Data Flow**
```bash
# Kiểm tra dữ liệu sales mới trong PostgreSQL
docker exec ecommerce-dss-project-postgres-1 psql -U dss_user -d ecommerce_dss -c "
SET search_path = vietnam_dw, public;
SELECT COUNT(*) as total_sales, MAX(created_at) as latest_record FROM fact_sales_vn;
"

# Theo dõi real-time (chạy nhiều lần để thấy tăng)
watch -n 5 'docker exec ecommerce-dss-project-postgres-1 psql -U dss_user -d ecommerce_dss -c "SELECT COUNT(*) FROM vietnam_dw.fact_sales_vn;"'
```

---

## 🔄 **AIRFLOW ORCHESTRATION**

### **Bước 8: Truy cập Airflow Web UI**
```bash
# Mở browser và truy cập:
http://localhost:8080

# Default credentials:
Username: airflow
Password: airflow
```

### **Available DAGs:**
- ✅ **simple_vietnam_automation**: Health checks every 15 minutes
- ✅ **vietnam_automated_streaming_pipeline**: Full pipeline automation
- ✅ **optimized_ecommerce_pipeline**: Optimized processing

### **Bước 9: Chạy DAG thủ công**
1. Vào Airflow UI → DAGs
2. Chọn `simple_vietnam_automation`
3. Click **"Trigger DAG"**
4. Theo dõi execution trong Graph View

---

## 📈 **MONITORING & ANALYTICS**

### **Bước 10: Kiểm tra System Health**
```bash
# 🔍 Kafka topics
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 🔍 Database sizes
docker exec ecommerce-dss-project-postgres-1 psql -U dss_user -d ecommerce_dss -c "
SELECT tablename, pg_size_pretty(pg_total_relation_size('vietnam_dw.'||tablename)) as size
FROM pg_tables WHERE schemaname = 'vietnam_dw'
ORDER BY pg_total_relation_size('vietnam_dw.'||tablename) DESC;
"

# 🔍 Container resources
docker stats --no-stream
```

### **Bước 11: Business Analytics Queries**
```sql
-- Trong PostgreSQL:
SET search_path = vietnam_dw, public;

-- 📊 Daily sales performance
SELECT
    sale_date,
    COUNT(*) as orders,
    SUM(total_amount_vnd) as revenue_vnd,
    AVG(total_amount_vnd) as avg_order_value
FROM fact_sales_vn
WHERE sale_date >= CURRENT_DATE - 7
GROUP BY sale_date
ORDER BY sale_date DESC;

-- 👥 Top customers by province
SELECT
    province,
    COUNT(*) as customer_count,
    customer_segment
FROM dim_customer_vn
GROUP BY province, customer_segment
ORDER BY customer_count DESC
LIMIT 10;

-- 📦 Product category performance
SELECT
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price_vnd
FROM dim_product_vn
GROUP BY category
ORDER BY product_count DESC;
```

---

## 🚀 **FULL SYSTEM RESTART**

### **Nếu cần restart toàn bộ hệ thống:**

```bash
# 1. Stop tất cả
docker-compose down

# 2. Start infrastructure
docker-compose up -d

# 3. Wait for services (2-3 minutes)
sleep 120

# 4. Start Kafka producer
python streaming/kafka_producer_vietnam.py &

# 5. Start data processor
docker exec -d ecommerce-dss-project-data-pipeline-1 python simple_spark_processor.py

# 6. Verify all components
python restart_vietnam_pipeline.py
```

---

## 🎯 **PROJECT STATUS SUMMARY**

### **✅ HOÀN THÀNH:**
- [x] **Docker Infrastructure**: All containers running
- [x] **Data Warehouse**: PostgreSQL với vietnam_dw schema
- [x] **Sample Data**: 1,000 customers, 500 products, 10,000 activities
- [x] **Kafka Streaming**: Producer generating 78 records/second
- [x] **Data Processing**: Spark-like processor active
- [x] **Airflow DAGs**: 3 working automation workflows
- [x] **Data Lake Structure**: MongoDB collections ready
- [x] **Analytics Views**: Business intelligence queries
- [x] **Monitoring**: Health checks and status views

### **⚠️ CẦN THEO DÕI:**
- [ ] **Sales Streaming**: fact_sales_vn records (depends on processor)
- [ ] **Real-time ML**: Model predictions
- [ ] **Advanced Analytics**: Trend analysis

### **🔗 ACCESS POINTS:**
- **PostgreSQL**: `docker exec -it ecommerce-dss-project-postgres-1 psql -U dss_user -d ecommerce_dss`
- **MongoDB**: `docker exec -it ecommerce-dss-project-mongodb-1 mongosh vietnam_ecommerce_lake`
- **Airflow**: http://localhost:8080 (airflow/airflow)
- **Spark Master**: http://localhost:8081
- **Kafka UI**: http://localhost:8090

---

## 🆘 **TROUBLESHOOTING**

### **Vấn đề thường gặp:**

1. **"Did not find any relations" trong PostgreSQL:**
   ```sql
   SET search_path = vietnam_dw, public;
   ```

2. **Kafka producer không chạy:**
   ```bash
   python streaming/kafka_producer_vietnam.py
   ```

3. **Containers không start:**
   ```bash
   docker-compose down && docker-compose up -d
   ```

4. **Airflow DAG lỗi:**
   - Check logs: http://localhost:8080 → DAGs → Logs
   - Restart: `docker-compose restart airflow-webserver`

---

**🎉 Dự án Vietnam E-commerce Streaming Platform đã hoạt động hoàn chỉnh!**

**📞 Support**: Kiểm tra từng bước theo guide này để verify system status.