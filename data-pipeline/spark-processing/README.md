# Spark Data Processing for E-commerce (Tiki & Lazada)

Hệ thống xử lý dữ liệu Apache Spark cho các nền tảng thương mại điện tử Việt Nam (Tiki, Lazada, FPTShop, CellphoneS).

## 📁 Cấu Trúc Thư Mục

```
spark-processing/
├── simple/                    # Phiên bản đơn giản
│   └── simple_ecommerce_processor.py
├── advanced/                  # Phiên bản nâng cao
│   └── advanced_ecommerce_processor.py
├── configs/                   # File cấu hình
│   ├── simple_config.yaml
│   └── advanced_config.json
├── utils/                     # Utilities
│   ├── spark_utils.py
│   └── data_schemas.py
├── requirements.txt
└── README.md
```

## 🚀 Cài Đặt

### 1. Prerequisites

```bash
# Java 8 hoặc 11
sudo apt-get install openjdk-11-jdk

# Python 3.8+
python3 --version

# Apache Spark 3.5.0
wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

### 2. Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. JDBC Drivers (Nếu sử dụng Database)

```bash
# PostgreSQL
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P $SPARK_HOME/jars/

# MySQL
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.2.0.jar -P $SPARK_HOME/jars/
```

## 💡 Sử Dụng

### Phiên Bản Đơn Giản

**Tính năng:**
- Đọc dữ liệu từ JSON, CSV, Database
- Làm sạch và chuẩn hóa dữ liệu
- Feature engineering cơ bản
- Thống kê và phân tích đơn giản

**Chạy:**

```bash
# Với cấu hình mặc định
cd simple/
python simple_ecommerce_processor.py

# Với cấu hình tùy chỉnh
python simple_ecommerce_processor.py --config ../configs/simple_config.yaml
```

**Input Data Format:**

```json
{
  "id": "product_123",
  "name": "iPhone 15 Pro Max",
  "url": "https://tiki.vn/iphone-15-pro-max",
  "price": {
    "current": 29990000,
    "original": 34990000,
    "currency": "VND"
  },
  "rating": {
    "score": 4.5,
    "count": 1250
  },
  "category": "Điện thoại di động",
  "brand": "Apple",
  "sold_count": 500,
  "source": "tiki"
}
```

### Phiên Bản Nâng Cao

**Tính năng:**
- Schema evolution và multi-source processing
- Outlier detection và data quality checks
- Advanced feature engineering
- Machine Learning (K-Means clustering, Random Forest)
- Advanced analytics và market insights
- Performance monitoring

**Chạy:**

```bash
cd advanced/
python advanced_ecommerce_processor.py

# Với config tùy chỉnh
python advanced_ecommerce_processor.py --config ../configs/advanced_config.json
```

**Features được tạo:**
- `price_vs_category_avg`: So sánh giá với trung bình category
- `popularity_score`: Điểm phổ biến dựa trên rating và số lượt bán
- `value_score`: Tỷ lệ chất lượng/giá
- `brand_market_share`: Thị phần của brand
- Time-based features: giờ, ngày trong tuần, tháng

## 📊 Kết Quả Đầu Ra

### Simple Processing
```
outputs/simple_processing/
├── processed_tiki_simple/          # Dữ liệu Tiki đã xử lý
├── processed_lazada_simple/        # Dữ liệu Lazada đã xử lý
└── processed_combined_simple/      # Dữ liệu kết hợp
```

### Advanced Processing
```
outputs/advanced_processing/
├── processed_data/                 # Dữ liệu chính (partitioned)
│   ├── platform=tiki/
│   ├── platform=lazada/
│   └── platform=fptshop/
├── customer_segments/              # Kết quả phân khúc khách hàng
├── ml_models/                      # Trained ML models
└── analytics_results.json         # Insights và analytics
```

## 🔧 Cấu Hình

### Simple Config (YAML)

```yaml
spark:
  app_name: "SimpleEcommerceProcessor"
  configs:
    spark.sql.adaptive.enabled: "true"

data_sources:
  tiki:
    json_path: "data-collection/data/tiki_products.json"
    platform: "tiki"

processing:
  remove_duplicates: true
  handle_outliers: true
  text_cleaning: true
```

### Advanced Config (JSON)

```json
{
  "processing": {
    "ml_analysis": {
      "customer_segmentation": {
        "enabled": true,
        "algorithm": "kmeans",
        "num_clusters": 5
      },
      "price_prediction": {
        "enabled": true,
        "algorithm": "random_forest"
      }
    }
  }
}
```

## 📈 Machine Learning Features

### Customer Segmentation
- **Thuật toán:** K-Means Clustering
- **Features:** current_price, rating_score, popularity_score, value_score
- **Output:** 5 segments với đặc điểm riêng biệt

### Price Prediction
- **Thuật toán:** Random Forest Regression
- **Features:** rating_score, rating_count, sold_count, brand_avg_price
- **Metric:** RMSE evaluation

### Market Insights
- Top performing categories
- Brand performance analysis
- Platform comparison
- Trend analysis

## 🎯 Use Cases

### 1. Price Monitoring
```python
# So sánh giá giữa các platform
price_comparison = df.groupBy("name", "platform").agg(avg("current_price"))
```

### 2. Market Analysis
```python
# Phân tích thị phần brand
brand_analysis = df.groupBy("brand").agg(
    count("*").alias("product_count"),
    avg("rating_score").alias("avg_rating")
)
```

### 3. Product Recommendation
```python
# Sản phẩm có value score cao
high_value_products = df.filter(col("value_score") > 0.8)
```

## 🔍 Data Quality Checks

### Phiên Bản Đơn Giản
- Remove duplicates
- Handle missing values
- Basic outlier detection
- Text normalization

### Phiên Bản Nâng Cao
- Fuzzy duplicate detection
- Advanced outlier treatment (IQR method)
- Data integrity validation
- Schema evolution handling

## 📊 Performance Optimization

### Spark Configurations
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Memory Management
- Broadcast joins cho lookup tables
- Caching cho frequently accessed DataFrames
- Partitioning theo platform và category

## 🐛 Troubleshooting

### Common Issues

**1. OutOfMemoryError**
```bash
# Tăng memory cho driver và executor
spark-submit --driver-memory 4g --executor-memory 8g your_script.py
```

**2. Schema Mismatch**
```python
# Sử dụng schema evolution
df = spark.read.option("mergeSchema", "true").json(path)
```

**3. Slow Performance**
```python
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Optimize shuffle partitions
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

## 📝 Monitoring và Logging

### Performance Metrics
- Processing time per stage
- Memory usage tracking
- Data quality scores
- Record counts

### Quality Metrics
- Duplicate percentage
- Null value ratios
- Outlier detection results
- Schema validation results

## 🔄 Scheduled Processing

### Cron Job Example
```bash
# Chạy hàng ngày lúc 2:00 AM
0 2 * * * /usr/bin/python3 /path/to/advanced_ecommerce_processor.py
```

### Airflow DAG Example
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def run_spark_processing():
    # Import và chạy processor
    pass

dag = DAG('ecommerce_processing', schedule_interval='@daily')
task = PythonOperator(task_id='spark_processing', python_callable=run_spark_processing)
```

## 🚀 Next Steps

1. **Real-time Processing:** Implement Spark Streaming
2. **Advanced ML:** Deep learning models, NLP
3. **Data Visualization:** Grafana/Tableau integration
4. **API Integration:** REST API cho insights
5. **Cloud Deployment:** AWS EMR, Azure HDInsight

## 📞 Support

Để được hỗ trợ:
1. Check troubleshooting section
2. Review logs trong `outputs/logs/`
3. Check Spark UI tại `http://localhost:4040`

---

**Phát triển bởi:** E-commerce Analytics Team
**Phiên bản:** 2.0.0
**Cập nhật:** November 2024