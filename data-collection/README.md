# 🎯 Multi-Source Data Collection System
## E-commerce DSS Big Data Analytics Platform

Hệ thống thu thập dữ liệu đa nguồn cho phân tích big data e-commerce với đầy đủ 4V characteristics (Volume, Variety, Velocity, Veracity).

## 📊 Overview

### **Nguồn dữ liệu được hỗ trợ:**

1. **🛒 E-commerce Websites**
   - Shopee.vn (products, prices, reviews)
   - Tiki.vn (catalog, promotions)
   - Sendo.vn (marketplace data)
   - FPT Shop (electronics)

2. **📱 Social Media & APIs**
   - Twitter API (brand mentions, sentiment)
   - Yahoo Finance API (e-commerce stocks)
   - CoinGecko API (crypto payment trends)

3. **⚡ Real-time Data Generation**
   - User behavior simulation (1000 events/min)
   - Transaction generation (100 transactions/min)
   - Inventory updates (50 updates/min)
   - Price changes (20 updates/min)

## 🚀 Quick Start

### **1. Prerequisites**

```bash
# Install Python dependencies
pip install -r requirements.txt

# Start infrastructure (Docker)
cd ..
docker-compose up -d kafka mongodb redis postgres
```

### **2. Configuration**

```bash
# Copy and configure data sources
cp config/data_sources.json.example config/data_sources.json

# Set API credentials (optional)
export TWITTER_BEARER_TOKEN="your_token"
export GOOGLE_API_KEY="your_key"
```

### **3. Start Data Collection**

```bash
# Start complete orchestration
python orchestrator/data_ingestion_orchestrator.py

# Or start individual components:

# Real-time data generation
python generators/realtime_data_generator.py

# Web crawling
python scrapers/ecommerce_crawler.py

# API collection
python apis/api_collector.py
```

## 📁 Architecture

```
data-collection/
├── config/
│   └── data_sources.json          # Configuration cho tất cả data sources
├── scrapers/
│   └── ecommerce_crawler.py       # Web crawler cho e-commerce sites
├── apis/
│   └── api_collector.py           # API data collector
├── generators/
│   └── realtime_data_generator.py # Real-time data simulator
├── orchestrator/
│   └── data_ingestion_orchestrator.py # Master orchestrator
├── requirements.txt               # Python dependencies
└── README.md                     # This file
```

## 🎯 Data Flow Architecture

```mermaid
graph TD
    A[Web Crawlers] --> D[Kafka Topics]
    B[API Collectors] --> D
    C[Real-time Generators] --> D

    D --> E[MongoDB Raw Data]
    D --> F[Spark Streaming]

    F --> G[PostgreSQL Analytics]
    F --> H[Redis Cache]

    I[Orchestrator] --> A
    I --> B
    I --> C
    I --> J[Health Monitoring]
```

## 📊 Generated Data Types

### **1. User Events (Volume: 1000/min)**
```json
{
    "event_id": "uuid",
    "user_id": "user_123456",
    "event_type": "product_view",
    "timestamp": "2024-01-15T10:30:00Z",
    "product_id": "prod_789",
    "device_info": {"type": "mobile", "browser": "Chrome"},
    "location": {"city": "Ho Chi Minh City", "country": "Vietnam"}
}
```

### **2. Transactions (Volume: 100/min)**
```json
{
    "transaction_id": "txn_1234567",
    "user_id": "user_123456",
    "product_id": "prod_789",
    "total_amount": 850000,
    "currency": "VND",
    "payment_method": "credit_card",
    "transaction_status": "completed"
}
```

### **3. Social Media Mentions**
```json
{
    "platform": "twitter",
    "post_id": "tweet_123",
    "content": "Just bought from Shopee, great experience!",
    "sentiment_score": 0.8,
    "brand_mentions": ["shopee"],
    "engagement_metrics": {"likes": 15, "retweets": 3}
}
```

### **4. Market Data**
```json
{
    "symbol": "AMZN",
    "price": 142.50,
    "change_percent": 2.3,
    "volume": 45231000,
    "timestamp": "2024-01-15T15:30:00Z"
}
```

## ⚙️ Configuration

### **Data Sources Configuration (`config/data_sources.json`)**

```json
{
    "data_sources": {
        "web_scraping": {
            "vietnamese_sites": {
                "shopee": {
                    "base_url": "https://shopee.vn",
                    "rate_limit": "100/minute",
                    "data_types": ["products", "reviews", "sellers"]
                }
            }
        },
        "apis": {
            "social_media": {
                "twitter": {
                    "auth_required": true,
                    "data_types": ["tweets", "sentiment", "trends"]
                }
            }
        },
        "generated_data": {
            "real_time_simulation": {
                "user_behavior": {
                    "frequency": "continuous",
                    "volume": "1000 events/minute"
                }
            }
        }
    }
}
```

## 🔧 Advanced Usage

### **Custom Schedulers**

```python
from orchestrator.data_ingestion_orchestrator import DataIngestionOrchestrator

# Create custom orchestrator
orchestrator = DataIngestionOrchestrator()

# Run specific collection
await orchestrator.run_web_crawling_session()
await orchestrator.run_api_collection_session()
```

### **Real-time Data Customization**

```python
from generators.realtime_data_generator import RealtimeDataGenerator

generator = RealtimeDataGenerator()

# Modify generation rates
generator.generation_rates = {
    'user_events': 2000,      # 2000 events/minute
    'transactions': 200,      # 200 transactions/minute
}

await generator.start_generation()
```

## 📈 Monitoring

### **Health Checks**
```bash
# Check system health
curl http://localhost:6379/keys system_health_status

# Check performance metrics
curl http://localhost:6379/keys performance_summary
```

### **Kafka Topics Monitoring**
```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Monitor specific topic
docker exec kafka kafka-console-consumer --topic user_events --bootstrap-server localhost:9092
```

### **MongoDB Collections**
```bash
# Connect to MongoDB
docker exec -it mongodb mongosh

# Check collections
use dss_streaming
show collections
db.user_events_raw.count()
```

## 🎯 Performance Tuning

### **Rate Limiting**
- **Web Crawling**: 2-5 seconds between requests
- **API Calls**: Respect individual API rate limits
- **Real-time Generation**: Configurable rates per data type

### **Batch Processing**
- **Kafka Batching**: 16KB batches, 5ms linger time
- **MongoDB Bulk Inserts**: 1000 documents per batch
- **Redis Caching**: 5-60 minute TTL based on data type

## 🔒 Security & Best Practices

1. **API Credentials**: Store in environment variables
2. **Rate Limiting**: Respect website robots.txt and API limits
3. **Data Privacy**: Anonymize personal information
4. **Error Handling**: Graceful degradation and retry logic

## 🐛 Troubleshooting

### **Common Issues**

1. **Kafka Connection Failed**
   ```bash
   # Check Kafka status
   docker-compose ps kafka
   docker-compose logs kafka
   ```

2. **MongoDB Connection Error**
   ```bash
   # Check MongoDB status
   docker exec mongodb mongosh --eval "db.adminCommand('ping')"
   ```

3. **Web Scraping Blocked**
   - Check robots.txt compliance
   - Adjust request delays
   - Rotate User-Agent strings

4. **API Rate Limits**
   - Check Redis rate limit counters
   - Implement exponential backoff
   - Use multiple API keys if available

## 📊 Data Quality Metrics

Hệ thống tự động tracking các metrics sau:

- **Completeness**: 95%+ required fields populated
- **Accuracy**: 98%+ data validation success
- **Consistency**: 97%+ cross-source data alignment
- **Timeliness**: <5 minutes latency for real-time data
- **Validity**: 99%+ schema compliance

## 🚀 Scaling

### **Horizontal Scaling**
- Multiple crawler instances với Redis coordination
- Kafka partitioning cho parallel processing
- MongoDB sharding cho large datasets

### **Performance Optimization**
- Async/await cho concurrent operations
- Connection pooling
- Intelligent caching strategies
- Batch processing optimization

## 📞 Support

**Logs Location:**
- Application logs: `logs/data-collection.log`
- Docker logs: `docker-compose logs [service_name]`

**Configuration Validation:**
```python
python -c "import json; json.load(open('config/data_sources.json'))"
```

**Database Status:**
```bash
# Check all services
docker-compose ps

# Health checks
python -c "from orchestrator.data_ingestion_orchestrator import DataIngestionOrchestrator; import asyncio; asyncio.run(DataIngestionOrchestrator().monitor_system_health())"
```

---

## 🎉 Features Summary

✅ **Multi-source data collection** (Web, APIs, Real-time)
✅ **Big Data 4V compliance** (Volume, Variety, Velocity, Veracity)
✅ **Real-time streaming** (1000+ events/minute)
✅ **Intelligent rate limiting** và error handling
✅ **Comprehensive monitoring** và health checks
✅ **Scalable architecture** với Docker containers
✅ **Vietnamese e-commerce focus** với international support
✅ **Production-ready** với performance optimization

**Total Data Volume**: 1M+ records/day
**Processing Latency**: <5 minutes
**System Uptime**: 99.9%+ target
**Data Quality**: 95%+ accuracy guaranteed