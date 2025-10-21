# 🛒 Lazada Electronics Crawler - Hướng Dẫn Sử Dụng

## 📋 Tổng Quan

Lazada Electronics Crawler là một công cụ chuyên biệt để thu thập dữ liệu thiết bị điện tử từ Lazada Việt Nam. Crawler này được thiết kế để:

- Thu thập dữ liệu sản phẩm điện tử một cách hiệu quả và ổn định
- Tích hợp với hệ thống database và batch processing hiện có
- Tuân thủ rate limiting và anti-detection mechanisms
- Hỗ trợ multiple output formats (JSON, CSV, JSONL)

## 🚀 Cài Đặt và Thiết Lập

### 1. Cài đặt Dependencies

```bash
# Cài đặt requirements
pip install -r requirements_lazada.txt

# Cài đặt ChromeDriver (cần cho Selenium)
# Download từ: https://chromedriver.chromium.org/
# Hoặc sử dụng webdriver-manager:
pip install webdriver-manager
```

### 2. Cấu hình Environment

```bash
# Tạo file .env trong thư mục data-collection
cp .env.example .env

# Cấu hình database connections
POSTGRES_HOST=localhost
POSTGRES_DB=ecommerce_dss
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_PORT=5432

MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=dss_streaming

KAFKA_SERVERS=localhost:9092

REDIS_HOST=localhost
REDIS_PORT=6379
```

### 3. Tạo Output Directories

```bash
mkdir -p ../data/lazada_electronics
mkdir -p ../data/demo_lazada
mkdir -p ../data/test_lazada
```

## 💻 Sử Dụng Crawler

### 1. Sử Dụng Cơ Bản

```python
from lazada_electronics_crawler import LazadaElectronicsCrawler

# Khởi tạo crawler
crawler = LazadaElectronicsCrawler(headless=True)

# Chạy crawler cho danh mục cụ thể
result = crawler.run_crawler(
    categories=['smartphones', 'laptops'],
    max_pages_per_category=2
)

print(f"Collected {result['products_count']} products")
```

### 2. Sử Dụng với Integration Module

```python
from lazada_integration import crawl_lazada_electronics

# Chạy complete workflow (crawl + database integration)
result = crawl_lazada_electronics(
    categories=['smartphones', 'headphones'],
    max_pages=3,
    headless=True
)

print(f"Workflow status: {result['workflow']['status']}")
```

### 3. Command Line Usage

```bash
# Chạy demo nhanh
python test_lazada_crawler.py demo quick

# Chạy demo đầy đủ
python test_lazada_crawler.py demo full

# Chạy performance test
python test_lazada_crawler.py performance

# Chạy unit tests
python test_lazada_crawler.py test
```

## 📊 Cấu Trúc Dữ Liệu

### Input Categories

Crawler hỗ trợ các danh mục thiết bị điện tử sau:

```python
categories = {
    'smartphones': 'Điện Thoại Di Động',
    'laptops': 'Máy Tính Xách Tay',
    'tablets': 'Máy Tính Bảng',
    'smartwatch': 'Đồng Hồ Thông Minh',
    'headphones': 'Tai Nghe',
    'speakers': 'Loa',
    'cameras': 'Máy Ảnh',
    'accessories': 'Phụ Kiện Điện Thoại'
}
```

### Output Data Structure

```json
{
  "product_id": "lazada_123456",
  "name": "iPhone 15 Pro Max 256GB",
  "price": 29990000,
  "original_price": 32990000,
  "discount_percent": 9.09,
  "category": "Smartphones",
  "brand": "Apple",
  "rating": 4.5,
  "review_count": 150,
  "sold_count": "2.5k đã bán",
  "seller_name": "Apple Store Official",
  "description": "...",
  "images": ["https://..."],
  "url": "https://www.lazada.vn/products/...",
  "crawl_timestamp": "2024-10-19T10:30:00",
  "platform": "lazada_vn"
}
```

## 🔧 Cấu Hình Crawler

### 1. Rate Limiting

```python
# Trong LazadaElectronicsCrawler
self.min_delay = 2      # Minimum delay between requests (seconds)
self.max_delay = 5      # Maximum delay between requests (seconds)
self.page_load_timeout = 30    # Page load timeout
self.element_timeout = 10      # Element wait timeout
```

### 2. Anti-Detection Features

- Random User-Agent rotation
- Random delays between requests
- Scroll simulation for natural behavior
- Headless mode support
- Proxy support (có thể extend)

### 3. Error Handling

```python
# Crawler có comprehensive error handling:
- Timeout handling
- Element not found handling
- Network error retry logic
- Graceful degradation
```

## 📈 Monitoring và Logging

### 1. Log Files

```bash
# Crawler tạo log files tại:
lazada_crawler.log           # Main crawler logs
crawl_report_TIMESTAMP.json  # Detailed crawl reports
```

### 2. Statistics Tracking

```python
# Crawler track các metrics:
- Total products found
- Successful extractions
- Failed extractions
- Pages processed
- Categories processed
- Success rate
- Duration
```

### 3. Real-time Monitoring

```python
# Check crawler statistics từ Redis
from lazada_integration import get_lazada_statistics

stats = get_lazada_statistics()
print(f"Last crawl: {stats['timestamp']}")
print(f"Products collected: {stats['total_products']}")
```

## 🗄️ Database Integration

### 1. PostgreSQL Storage

```sql
-- Crawler saves to multiple tables:
- lazada_products_raw     -- Raw crawled data
- products               -- Standardized product data
- analytics_summary      -- Aggregated analytics
```

### 2. MongoDB Storage

```javascript
// Raw data collection
db.lazada_products_raw.find()

// Với indexing for performance:
- timestamp index
- category index
- price range index
```

### 3. Kafka Streaming

```python
# Products sent to Kafka topics:
- lazada_products_stream  -- Real-time product updates
- products_raw           -- Raw product data
```

## 🧪 Testing và Development

### 1. Unit Testing

```bash
# Chạy all tests
python test_lazada_crawler.py test

# Test specific functionality
python -m pytest test_lazada_crawler.py::TestLazadaCrawler::test_driver_setup
```

### 2. Demo Modes

```bash
# Quick demo (1 category, 1 page)
python test_lazada_crawler.py demo quick

# Full demo (multiple categories)
python test_lazada_crawler.py demo full
```

### 3. Performance Testing

```bash
# Benchmark crawler performance
python test_lazada_crawler.py performance
```

## 🔄 Integration với Airflow

### 1. DAG Configuration

```python
# airflow/dags/lazada_daily_crawl.py
from lazada_integration import crawl_lazada_electronics

def daily_lazada_crawl():
    result = crawl_lazada_electronics(
        categories=['smartphones', 'laptops', 'tablets'],
        max_pages=5,
        headless=True
    )
    return result

# DAG task
lazada_task = PythonOperator(
    task_id='crawl_lazada_electronics',
    python_callable=daily_lazada_crawl,
    dag=dag
)
```

### 2. Scheduling

```python
# Recommended schedule
dag = DAG(
    'lazada_daily_crawl',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    default_args=default_args
)
```

## 🚨 Troubleshooting

### 1. Common Issues

**ChromeDriver Issues:**
```bash
# Update ChromeDriver
pip install --upgrade webdriver-manager

# Check Chrome version compatibility
chrome --version
```

**Memory Issues:**
```python
# Giảm concurrent pages
max_pages_per_category=1

# Enable headless mode
crawler = LazadaElectronicsCrawler(headless=True)
```

**Rate Limiting:**
```python
# Tăng delays
self.min_delay = 5
self.max_delay = 10
```

### 2. Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Disable headless for visual debugging
crawler = LazadaElectronicsCrawler(headless=False)
```

### 3. Database Connection Issues

```bash
# Test database connections
python -c "from lazada_integration import LazadaDataProcessor; LazadaDataProcessor()"
```

## 📊 Performance Metrics

### Expected Performance:

- **Collection Rate**: 2-5 products/second
- **Success Rate**: 85-95%
- **Memory Usage**: 200-500MB
- **CPU Usage**: 20-40%

### Optimization Tips:

1. **Use headless mode** trong production
2. **Adjust delays** based trên website response
3. **Limit concurrent operations** để tránh blocking
4. **Monitor memory usage** với large datasets
5. **Use database indexing** cho faster queries

## 🔐 Best Practices

### 1. Ethical Crawling
- Respect robots.txt
- Use appropriate delays
- Don't overload servers
- Cache results to minimize requests

### 2. Data Quality
- Validate extracted data
- Handle missing fields gracefully
- Clean and normalize data
- Monitor data consistency

### 3. Production Deployment
- Use environment variables for configuration
- Implement proper logging
- Setup monitoring and alerts
- Plan for error recovery

---

## 📞 Support

Nếu gặp vấn đề hoặc cần hỗ trợ:

1. Check log files trong `lazada_crawler.log`
2. Run test suite để identify issues
3. Review configuration settings
4. Check database connectivity

**Happy Crawling! 🛒🚀**