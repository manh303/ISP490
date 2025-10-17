# Kiến Trúc DSS E-commerce Thị Trường Việt Nam
*Hệ thống hỗ trợ quyết định cho thương mại điện tử - Tập trung PostgreSQL Data Warehouse*

## 📋 Tổng Quan Dự Án

### Mục Tiêu
Xây dựng hệ thống DSS (Decision Support System) phân tích big data thương mại điện tử tập trung vào thị trường Việt Nam, sử dụng PostgreSQL làm data warehouse chính thay vì data lake.

### Nguồn Dữ Liệu Chính
- **Lazada Vietnam** - Sàn thương mại điện tử hàng đầu
- **Tiki** - Nền tảng thương mại điện tử Việt Nam
- **FPT Shop** - Chuỗi bán lẻ điện tử
- **CellphoneS** - Chuỗi bán lẻ điện thoại
- **Shopee Vietnam** - Sàn thương mại điện tử phổ biến
- **Sendo** - Sàn thương mại điện tử địa phương

## 🏗️ Kiến Trúc Hệ Thống

### Kiến Trúc Tổng Thể
```
┌─────────────────────────────────────────────────────────────────┐
│                 VIETNAMESE E-COMMERCE DSS                       │
├─────────────────────────────────────────────────────────────────┤
│  Data Sources → Crawling → Processing → PostgreSQL → Analytics  │
│       ↓           ↓          ↓           ↓           ↓         │
│   Vietnamese   Selenium/   Apache     PostgreSQL   FastAPI     │
│   E-commerce   Scrapy      Spark      Data WH      Dashboard   │
│   Platforms    Crawlers    ETL       (OLAP/OLTP)   & APIs     │
└─────────────────────────────────────────────────────────────────┘
```

### Chi Tiết Các Layer

#### 1. Data Collection Layer (Thu Thập Dữ Liệu)
```yaml
Vietnamese E-commerce Sources:
  Lazada:
    - Products: /dien-thoai-di-dong/, /thoi-trang-nu/
    - Reviews: API endpoints + UI crawling
    - Pricing: Real-time price tracking
    - Inventory: Stock status monitoring

  Tiki:
    - Categories: Electronics, Fashion, Books
    - Product details + specifications
    - User reviews and ratings
    - Seller information

  FPT Shop:
    - Electronics focus
    - Store locations
    - Promotional pricing
    - Technical specifications

  CellphoneS:
    - Mobile devices specialist
    - Price comparison data
    - Accessories marketplace
    - Service center locations

Crawling Technology:
  - Selenium WebDriver (Existing implementation)
  - Scrapy framework (Scale up)
  - Requests + BeautifulSoup (Simple sites)
  - Proxy rotation (Anti-detection)
  - Rate limiting (Respectful crawling)
```

#### 2. Data Processing Layer (Xử Lý Dữ Liệu)
```yaml
Apache Airflow DAGs:
  daily_crawl_lazada:
    - Schedule: 0 2 * * * (2 AM daily)
    - Tasks: Product listing → Product details → Reviews
    - Data validation and cleaning
    - Duplicate detection

  daily_crawl_tiki:
    - Schedule: 0 3 * * * (3 AM daily)
    - Category-based crawling
    - Price history tracking
    - Image processing

  weekly_aggregate_analysis:
    - Schedule: 0 1 * * 0 (Sunday 1 AM)
    - Cross-platform price comparison
    - Market trend analysis
    - Competitor analysis

Apache Spark Processing:
  Data Cleaning:
    - Text normalization (Vietnamese language)
    - Price standardization (VND currency)
    - Category mapping across platforms
    - Duplicate product matching

  Data Enrichment:
    - Product categorization using ML
    - Sentiment analysis (Vietnamese reviews)
    - Price trend calculation
    - Geographic analysis (Vietnam regions)

  Real-time Processing:
    - Price change detection
    - New product alerts
    - Stock availability monitoring
    - Flash sale tracking
```

#### 3. Data Storage Layer (PostgreSQL Data Warehouse)
```sql
-- Core PostgreSQL Schema Design

-- Dimension Tables
CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(500),
    brand VARCHAR(100),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    platform VARCHAR(50),
    external_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    seller_name VARCHAR(200),
    platform VARCHAR(50),
    seller_rating DECIMAL(3,2),
    location VARCHAR(100),
    verification_status VARCHAR(50)
);

CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    date_actual DATE,
    day_of_week INTEGER,
    week_of_year INTEGER,
    month_actual INTEGER,
    quarter_actual INTEGER,
    year_actual INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100)
);

CREATE TABLE dim_locations (
    location_id SERIAL PRIMARY KEY,
    province VARCHAR(100),
    district VARCHAR(100),
    region VARCHAR(50), -- North, Central, South Vietnam
    population INTEGER,
    economic_zone VARCHAR(50)
);

-- Fact Tables
CREATE TABLE fact_product_prices (
    price_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES dim_products(product_id),
    seller_id INTEGER REFERENCES dim_sellers(seller_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    original_price DECIMAL(12,2),
    sale_price DECIMAL(12,2),
    discount_percent DECIMAL(5,2),
    currency VARCHAR(3) DEFAULT 'VND',
    stock_quantity INTEGER,
    is_available BOOLEAN
);

CREATE TABLE fact_reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES dim_products(product_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    review_text TEXT,
    sentiment_score DECIMAL(3,2),
    helpful_count INTEGER,
    verified_purchase BOOLEAN
);

CREATE TABLE fact_sales_metrics (
    metric_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES dim_products(product_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    location_id INTEGER REFERENCES dim_locations(location_id),
    daily_views INTEGER,
    daily_sales INTEGER,
    revenue_vnd DECIMAL(15,2),
    conversion_rate DECIMAL(5,4)
);

-- Analytical Views
CREATE VIEW vw_price_trends AS
SELECT
    p.product_name,
    p.brand,
    p.platform,
    t.date_actual,
    pp.sale_price,
    LAG(pp.sale_price) OVER (
        PARTITION BY p.product_id
        ORDER BY t.date_actual
    ) as previous_price,
    ((pp.sale_price - LAG(pp.sale_price) OVER (
        PARTITION BY p.product_id
        ORDER BY t.date_actual
    )) / LAG(pp.sale_price) OVER (
        PARTITION BY p.product_id
        ORDER BY t.date_actual
    )) * 100 as price_change_percent
FROM fact_product_prices pp
JOIN dim_products p ON pp.product_id = p.product_id
JOIN dim_time t ON pp.time_id = t.time_id;

CREATE VIEW vw_platform_comparison AS
SELECT
    p.product_name,
    p.brand,
    p.category,
    COUNT(CASE WHEN p.platform = 'Lazada' THEN 1 END) as on_lazada,
    COUNT(CASE WHEN p.platform = 'Tiki' THEN 1 END) as on_tiki,
    COUNT(CASE WHEN p.platform = 'FPTShop' THEN 1 END) as on_fptshop,
    AVG(CASE WHEN p.platform = 'Lazada' THEN pp.sale_price END) as lazada_price,
    AVG(CASE WHEN p.platform = 'Tiki' THEN pp.sale_price END) as tiki_price,
    AVG(CASE WHEN p.platform = 'FPTShop' THEN pp.sale_price END) as fptshop_price
FROM dim_products p
JOIN fact_product_prices pp ON p.product_id = pp.product_id
GROUP BY p.product_name, p.brand, p.category;
```

#### 4. Analytics & ML Layer
```python
# Vietnamese E-commerce Analytics Models

class VietnameseEcommerceAnalytics:
    def __init__(self):
        self.spark = SparkSession.builder.appName("VietnamEcommerceDSS").getOrCreate()
        self.pg_config = {
            "host": "postgres",
            "database": "vietnam_ecommerce_dw",
            "user": "analytics_user",
            "password": "secure_password"
        }

    def price_optimization_model(self):
        """
        Mô hình tối ưu giá cho thị trường Việt Nam
        """
        query = """
        SELECT
            product_id,
            sale_price,
            discount_percent,
            daily_sales,
            competitor_min_price,
            competitor_max_price,
            season,
            region
        FROM analytics.price_optimization_view
        WHERE date_actual >= CURRENT_DATE - INTERVAL '90 days'
        """

        df = self.spark.read.format("jdbc").options(
            url=f"jdbc:postgresql://{self.pg_config['host']}/{self.pg_config['database']}",
            dbtable=f"({query}) as price_data",
            user=self.pg_config['user'],
            password=self.pg_config['password']
        ).load()

        # Feature engineering cho thị trường Việt Nam
        df = df.withColumn("is_tet_season",
                          F.when(F.month(F.col("date_actual")).isin([1, 2]), 1).otherwise(0))
        df = df.withColumn("is_student_season",
                          F.when(F.month(F.col("date_actual")).isin([8, 9]), 1).otherwise(0))

        return df

    def vietnamese_sentiment_analysis(self):
        """
        Phân tích sentiment cho review tiếng Việt
        """
        # Load Vietnamese language model
        from transformers import pipeline

        # Sử dụng mô hình đã fine-tune cho tiếng Việt
        sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model="vinai/phobert-base-v2"
        )

        def analyze_vietnamese_review(text):
            if not text:
                return {"label": "NEUTRAL", "score": 0.5}

            # Preprocess Vietnamese text
            text = self.preprocess_vietnamese_text(text)
            result = sentiment_pipeline(text)
            return result[0]

        return analyze_vietnamese_review

    def market_basket_analysis_vietnam(self):
        """
        Phân tích market basket cho thói quen mua sắm Việt Nam
        """
        query = """
        SELECT
            transaction_id,
            product_name,
            category,
            platform,
            price_vnd,
            region
        FROM analytics.transactions_view
        WHERE date_actual >= CURRENT_DATE - INTERVAL '30 days'
        """

        # Implement Apriori algorithm for Vietnamese shopping patterns
        # Consider cultural factors: Tết shopping, student season, etc.
        pass
```

#### 5. API & Dashboard Layer (FastAPI)
```python
# FastAPI Application for Vietnamese E-commerce DSS

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import asyncpg
import asyncio
from datetime import datetime, date

app = FastAPI(
    title="Vietnamese E-commerce DSS API",
    description="Decision Support System for Vietnamese E-commerce Market",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data Models
class ProductPriceResponse(BaseModel):
    product_id: int
    product_name: str
    platform: str
    current_price: float
    previous_price: Optional[float]
    price_change_percent: Optional[float]
    last_updated: datetime

class MarketAnalysisResponse(BaseModel):
    category: str
    total_products: int
    average_price: float
    price_range_min: float
    price_range_max: float
    top_platforms: List[str]
    market_trend: str

class VietnameseRegionAnalysis(BaseModel):
    region: str  # North, Central, South
    popular_categories: List[str]
    average_order_value: float
    seasonal_trends: dict
    preferred_platforms: List[str]

# Database Connection
async def get_db_connection():
    return await asyncpg.connect(
        host="postgres",
        database="vietnam_ecommerce_dw",
        user="api_user",
        password="secure_password"
    )

# API Endpoints
@app.get("/api/v1/products/price-trends", response_model=List[ProductPriceResponse])
async def get_price_trends(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    days: int = 30
):
    """
    Lấy xu hướng giá sản phẩm theo platform và category
    """
    conn = await get_db_connection()
    try:
        query = """
        SELECT
            p.product_id,
            p.product_name,
            p.platform,
            pp.sale_price as current_price,
            LAG(pp.sale_price) OVER (
                PARTITION BY p.product_id
                ORDER BY t.date_actual
            ) as previous_price,
            pp.updated_at as last_updated
        FROM dim_products p
        JOIN fact_product_prices pp ON p.product_id = pp.product_id
        JOIN dim_time t ON pp.time_id = t.time_id
        WHERE t.date_actual >= CURRENT_DATE - INTERVAL '%s days'
        """ % days

        if platform:
            query += f" AND p.platform = '{platform}'"
        if category:
            query += f" AND p.category = '{category}'"

        query += " ORDER BY pp.updated_at DESC"

        rows = await conn.fetch(query)

        results = []
        for row in rows:
            price_change = None
            if row['previous_price'] and row['current_price']:
                price_change = ((row['current_price'] - row['previous_price']) /
                              row['previous_price']) * 100

            results.append(ProductPriceResponse(
                product_id=row['product_id'],
                product_name=row['product_name'],
                platform=row['platform'],
                current_price=row['current_price'],
                previous_price=row['previous_price'],
                price_change_percent=price_change,
                last_updated=row['last_updated']
            ))

        return results
    finally:
        await conn.close()

@app.get("/api/v1/analytics/market-overview", response_model=List[MarketAnalysisResponse])
async def get_market_overview():
    """
    Tổng quan thị trường e-commerce Việt Nam
    """
    conn = await get_db_connection()
    try:
        query = """
        SELECT
            p.category,
            COUNT(DISTINCT p.product_id) as total_products,
            AVG(pp.sale_price) as average_price,
            MIN(pp.sale_price) as price_range_min,
            MAX(pp.sale_price) as price_range_max,
            STRING_AGG(DISTINCT p.platform, ', ') as platforms,
            CASE
                WHEN AVG(pp.sale_price) > LAG(AVG(pp.sale_price)) OVER (
                    PARTITION BY p.category
                    ORDER BY DATE_TRUNC('month', t.date_actual)
                ) THEN 'Tăng'
                WHEN AVG(pp.sale_price) < LAG(AVG(pp.sale_price)) OVER (
                    PARTITION BY p.category
                    ORDER BY DATE_TRUNC('month', t.date_actual)
                ) THEN 'Giảm'
                ELSE 'Ổn định'
            END as trend
        FROM dim_products p
        JOIN fact_product_prices pp ON p.product_id = pp.product_id
        JOIN dim_time t ON pp.time_id = t.time_id
        WHERE t.date_actual >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY p.category, DATE_TRUNC('month', t.date_actual)
        ORDER BY total_products DESC
        """

        rows = await conn.fetch(query)

        results = []
        for row in rows:
            results.append(MarketAnalysisResponse(
                category=row['category'],
                total_products=row['total_products'],
                average_price=float(row['average_price']),
                price_range_min=float(row['price_range_min']),
                price_range_max=float(row['price_range_max']),
                top_platforms=row['platforms'].split(', '),
                market_trend=row['trend']
            ))

        return results
    finally:
        await conn.close()

@app.get("/api/v1/analytics/vietnam-regions", response_model=List[VietnameseRegionAnalysis])
async def get_vietnam_regional_analysis():
    """
    Phân tích theo vùng miền Việt Nam (Bắc - Trung - Nam)
    """
    conn = await get_db_connection()
    try:
        query = """
        SELECT
            l.region,
            STRING_AGG(DISTINCT p.category ORDER BY COUNT(*) DESC, ', ') as popular_categories,
            AVG(sm.revenue_vnd / sm.daily_sales) as average_order_value,
            STRING_AGG(DISTINCT p.platform ORDER BY COUNT(*) DESC, ', ') as preferred_platforms
        FROM dim_locations l
        JOIN fact_sales_metrics sm ON l.location_id = sm.location_id
        JOIN dim_products p ON sm.product_id = p.product_id
        JOIN dim_time t ON sm.time_id = t.time_id
        WHERE t.date_actual >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY l.region
        ORDER BY l.region
        """

        rows = await conn.fetch(query)

        results = []
        for row in rows:
            # Get seasonal trends for each region
            seasonal_trends = await get_seasonal_trends_for_region(conn, row['region'])

            results.append(VietnameseRegionAnalysis(
                region=row['region'],
                popular_categories=row['popular_categories'].split(', ')[:5],
                average_order_value=float(row['average_order_value']),
                seasonal_trends=seasonal_trends,
                preferred_platforms=row['preferred_platforms'].split(', ')[:3]
            ))

        return results
    finally:
        await conn.close()

async def get_seasonal_trends_for_region(conn, region: str) -> dict:
    """
    Lấy xu hướng theo mùa cho từng vùng miền
    """
    query = """
    SELECT
        EXTRACT(MONTH FROM t.date_actual) as month,
        AVG(sm.revenue_vnd) as avg_revenue
    FROM dim_locations l
    JOIN fact_sales_metrics sm ON l.location_id = sm.location_id
    JOIN dim_time t ON sm.time_id = t.time_id
    WHERE l.region = $1
    AND t.date_actual >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY EXTRACT(MONTH FROM t.date_actual)
    ORDER BY month
    """

    rows = await conn.fetch(query, region)

    # Mapping Vietnamese seasonal patterns
    trends = {}
    for row in rows:
        month = int(row['month'])
        revenue = float(row['avg_revenue'])

        if month in [1, 2]:  # Tết season
            trends['Tết'] = revenue
        elif month in [8, 9]:  # Back to school
            trends['Mùa tựu trường'] = revenue
        elif month in [11, 12]:  # Christmas/New Year
            trends['Cuối năm'] = revenue
        else:
            trends.setdefault('Bình thường', []).append(revenue)

    # Average normal months
    if 'Bình thường' in trends:
        trends['Bình thường'] = sum(trends['Bình thường']) / len(trends['Bình thường'])

    return trends

@app.get("/api/v1/crawling/status")
async def get_crawling_status():
    """
    Trạng thái crawling các platform Việt Nam
    """
    return {
        "platforms": {
            "Lazada": {
                "last_crawl": "2024-10-16 02:30:00",
                "status": "success",
                "products_crawled": 15420,
                "errors": 0
            },
            "Tiki": {
                "last_crawl": "2024-10-16 03:15:00",
                "status": "success",
                "products_crawled": 12350,
                "errors": 2
            },
            "FPTShop": {
                "last_crawl": "2024-10-16 04:00:00",
                "status": "partial",
                "products_crawled": 8900,
                "errors": 15
            }
        },
        "next_scheduled_crawl": "2024-10-17 02:00:00",
        "data_freshness": "2-4 hours"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

## 🐳 Docker Configuration

### Docker Compose Setup
```yaml
# docker-compose.yml
version: '3.8'

services:
  # PostgreSQL Data Warehouse
  postgres:
    image: postgres:15-alpine
    container_name: vietnam_ecommerce_postgres
    environment:
      POSTGRES_DB: vietnam_ecommerce_dw
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: secure_postgres_password
      POSTGRES_MULTIPLE_DATABASES: analytics,staging,logs
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./database/init:/docker-entrypoint-initdb.d
      - ./database/schemas:/schemas
    ports:
      - "5432:5432"
    networks:
      - ecommerce_network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Redis for Caching
  redis:
    image: redis:7-alpine
    container_name: vietnam_ecommerce_redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    networks:
      - ecommerce_network
    command: redis-server --appendonly yes

  # Apache Spark Master
  spark-master:
    image: bitnami/spark:3.4
    container_name: vietnam_spark_master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - ./spark/apps:/opt/spark-apps
      - ./spark/jars:/opt/spark/jars/extra
    networks:
      - ecommerce_network

  # Apache Spark Worker
  spark-worker:
    image: bitnami/spark:3.4
    container_name: vietnam_spark_worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - ./spark/apps:/opt/spark-apps
      - ./spark/jars:/opt/spark/jars/extra
    networks:
      - ecommerce_network
    depends_on:
      - spark-master

  # Apache Airflow
  airflow-webserver:
    image: apache/airflow:2.7.0-python3.9
    container_name: vietnam_airflow_webserver
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow_password@postgres/airflow
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
      - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    ports:
      - "8081:8080"
    networks:
      - ecommerce_network
    depends_on:
      - postgres
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.7.0-python3.9
    container_name: vietnam_airflow_scheduler
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow_password@postgres/airflow
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    networks:
      - ecommerce_network
    depends_on:
      - postgres
    command: scheduler

  # FastAPI Backend
  fastapi-backend:
    build:
      context: ./backend
      dockerfile: Dockerfile
    container_name: vietnam_fastapi_backend
    environment:
      - DATABASE_URL=postgresql://postgres:secure_postgres_password@postgres:5432/vietnam_ecommerce_dw
      - REDIS_URL=redis://redis:6379/0
      - SPARK_MASTER_URL=spark://spark-master:7077
    ports:
      - "8000:8000"
    volumes:
      - ./backend:/app
      - ./data:/app/data
    networks:
      - ecommerce_network
    depends_on:
      - postgres
      - redis
      - spark-master
    restart: unless-stopped

  # Selenium Crawler Service
  crawler-service:
    build:
      context: ./crawlers
      dockerfile: Dockerfile
    container_name: vietnam_crawler_service
    environment:
      - DATABASE_URL=postgresql://postgres:secure_postgres_password@postgres:5432/vietnam_ecommerce_dw
      - REDIS_URL=redis://redis:6379/1
      - SELENIUM_HUB_URL=http://selenium-hub:4444/wd/hub
    volumes:
      - ./crawlers:/app
      - ./data:/app/data
    networks:
      - ecommerce_network
    depends_on:
      - postgres
      - redis
      - selenium-hub

  # Selenium Hub
  selenium-hub:
    image: selenium/hub:4.15.0
    container_name: vietnam_selenium_hub
    ports:
      - "4444:4444"
      - "4442:4442"
      - "4443:4443"
    networks:
      - ecommerce_network

  # Chrome Selenium Node
  selenium-chrome:
    image: selenium/node-chrome:4.15.0
    container_name: vietnam_selenium_chrome
    shm_size: 2gb
    environment:
      - HUB_HOST=selenium-hub
      - HUB_PORT=4444
    networks:
      - ecommerce_network
    depends_on:
      - selenium-hub

  # Monitoring - Grafana
  grafana:
    image: grafana/grafana:10.1.0
    container_name: vietnam_grafana
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin_password
      - GF_INSTALL_PLUGINS=grafana-postgresql-datasource
    ports:
      - "3000:3000"
    volumes:
      - grafana_data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./monitoring/grafana/datasources:/etc/grafana/provisioning/datasources
    networks:
      - ecommerce_network
    depends_on:
      - postgres

volumes:
  postgres_data:
  redis_data:
  grafana_data:

networks:
  ecommerce_network:
    driver: bridge
```

### Environment Configuration
```bash
# .env
# Database Configuration
POSTGRES_HOST=postgres
POSTGRES_DB=vietnam_ecommerce_dw
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secure_postgres_password
POSTGRES_PORT=5432

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g

# Airflow Configuration
AIRFLOW_DB_HOST=postgres
AIRFLOW_DB_NAME=airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow_password

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Crawler Configuration
SELENIUM_HUB_URL=http://selenium-hub:4444/wd/hub
CRAWLER_DELAY_MIN=2
CRAWLER_DELAY_MAX=5
USER_AGENT_ROTATION=true

# Vietnamese Platform URLs
LAZADA_BASE_URL=https://www.lazada.vn
TIKI_BASE_URL=https://tiki.vn
FPTSHOP_BASE_URL=https://fptshop.com.vn
CELLPHONES_BASE_URL=https://cellphones.com.vn
```

## 🚀 Implementation Phases

### Phase 1: Infrastructure Setup (1 tuần)
```yaml
Week 1: Core Infrastructure
  Day 1-2: Docker environment setup
    - PostgreSQL cluster configuration
    - Redis setup for caching
    - Network and volume configuration

  Day 3-4: Spark cluster setup
    - Master/Worker configuration
    - Resource allocation
    - JAR dependencies

  Day 5-7: Airflow installation
    - Scheduler and webserver setup
    - Database initialization
    - Basic DAG testing
```

### Phase 2: Data Collection (2 tuần)
```yaml
Week 2: Crawler Development
  Day 8-10: Vietnamese platform crawlers
    - Lazada crawler enhancement
    - Tiki crawler development
    - Anti-detection mechanisms

  Day 11-14: Data pipeline setup
    - Airflow DAGs for daily crawling
    - Error handling and retry logic
    - Data validation and cleaning

Week 3: Data Storage
  Day 15-17: PostgreSQL schema design
    - Star schema implementation
    - Indexing strategy
    - Partitioning setup

  Day 18-21: ETL pipeline development
    - Spark jobs for data transformation
    - Vietnamese text processing
    - Data quality checks
```

### Phase 3: Analytics Development (2 tuần)
```yaml
Week 4: Core Analytics
  Day 22-25: Business intelligence queries
    - Price trend analysis
    - Market comparison
    - Platform performance metrics

  Day 26-28: Vietnamese-specific features
    - Regional analysis (North/Central/South)
    - Seasonal trend detection (Tết, school season)
    - Currency and cultural considerations

Week 5: ML Pipeline
  Day 29-32: Machine learning models
    - Vietnamese sentiment analysis
    - Price prediction models
    - Market basket analysis

  Day 33-35: Model deployment
    - Spark ML pipeline
    - Model versioning
    - Batch prediction jobs
```

### Phase 4: API & Dashboard (1 tuần)
```yaml
Week 6: API Development
  Day 36-38: FastAPI backend
    - RESTful endpoints
    - Authentication & authorization
    - Rate limiting and caching

  Day 39-42: Dashboard development
    - Grafana dashboards
    - Real-time monitoring
    - Business KPI visualization
```

### Phase 5: Production Optimization (1 tuần)
```yaml
Week 7: Production Readiness
  Day 43-45: Performance optimization
    - Query optimization
    - Caching strategies
    - Resource scaling

  Day 46-49: Monitoring & Alerting
    - Health checks
    - Error monitoring
    - Performance metrics
    - Backup strategies
```

## 📊 Expected Outputs

### Daily Data Volume
```yaml
Data Sources:
  Lazada: ~50,000 products/day
  Tiki: ~30,000 products/day
  FPTShop: ~15,000 products/day
  CellphoneS: ~10,000 products/day

Total Daily: ~105,000 products
Monthly: ~3.15M products
Annual: ~38M products

Storage Estimates:
  PostgreSQL: ~500GB/year
  Logs: ~100GB/year
  Processed Data: ~200GB/year
```

### Analytics Capabilities
```yaml
Real-time Analytics:
  - Price monitoring and alerts
  - Stock availability tracking
  - New product detection
  - Flash sale monitoring

Daily Reports:
  - Market price comparison
  - Platform performance analysis
  - Category trend analysis
  - Regional preference analysis

Weekly Analysis:
  - Competitive intelligence
  - Market share analysis
  - Customer behavior patterns
  - Seasonal trend detection

Monthly Intelligence:
  - Strategic market insights
  - Long-term trend analysis
  - Predictive analytics
  - Business recommendations
```

## 🎯 Timeline Tổng Thể: 7 tuần

Kiến trúc này được thiết kế đặc biệt cho thị trường e-commerce Việt Nam, tập trung vào PostgreSQL làm data warehouse chính, phù hợp với quy mô dữ liệu và yêu cầu phân tích thời gian thực của dự án.

<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"content": "Create simplified PostgreSQL-based DSS architecture", "status": "completed", "activeForm": "Creating simplified PostgreSQL-based DSS architecture"}, {"content": "Focus on Vietnamese e-commerce crawling sources", "status": "completed", "activeForm": "Focusing on Vietnamese e-commerce crawling sources"}, {"content": "Design technology stack with Docker, Spark, Airflow, FastAPI", "status": "completed", "activeForm": "Designing technology stack with Docker, Spark, Airflow, FastAPI"}, {"content": "Write comprehensive markdown documentation", "status": "completed", "activeForm": "Writing comprehensive markdown documentation"}]