# Kiến Trúc Data Warehouse cho Hệ Thống DSS E-commerce Việt Nam

## 🎯 Tổng Quan Hệ Thống

Hệ thống Data Warehouse được thiết kế dành riêng cho thị trường e-commerce Việt Nam, hỗ trợ ra quyết định kinh doanh với dữ liệu thời gian thực và phân tích sâu.

## 📊 Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA WAREHOUSE ARCHITECTURE                  │
├─────────────────────────────────────────────────────────────────┤
│  Data Sources → Staging → Data Warehouse → Data Marts → BI     │
└─────────────────────────────────────────────────────────────────┘

   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────┐
   │ Operational │    │   Staging   │    │ Data        │    │   BI     │
   │ Systems     │───▶│   Layer     │───▶│ Warehouse   │───▶│ Reports  │
   │             │    │             │    │             │    │          │
   └─────────────┘    └─────────────┘    └─────────────┘    └──────────┘
```

## 🏗️ Kiến Trúc Đa Tầng (Multi-Tier Architecture)

### Tầng 1: Nguồn Dữ Liệu (Data Sources)
- **E-commerce Platforms**: Lazada, Shopee, Tiki, Sendo, FPTShop, CellphoneS
- **Transactional Systems**: PostgreSQL Database
- **External APIs**: Payment gateways, shipping providers
- **Streaming Data**: Kafka real-time events
- **Market Data**: Vietnamese e-commerce statistics

### Tầng 2: Tầng Staging (Staging Layer)
- **Raw Data Store**: MongoDB for unstructured data
- **Staging Tables**: Temporary data processing
- **Data Quality**: Validation and cleansing
- **Change Data Capture**: Track data changes

### Tầng 3: Data Warehouse Core
- **Fact Tables**: Sales, Orders, Inventory
- **Dimension Tables**: Products, Customers, Time, Geography
- **Slowly Changing Dimensions**: Historical data tracking
- **Aggregated Tables**: Pre-calculated metrics

### Tầng 4: Data Marts
- **Sales Mart**: Sales performance analysis
- **Customer Mart**: Customer behavior analysis
- **Product Mart**: Product performance analysis
- **Marketing Mart**: Campaign effectiveness

### Tầng 5: Presentation Layer
- **OLAP Cubes**: Multi-dimensional analysis
- **Reports**: Standard business reports
- **Dashboards**: Real-time monitoring
- **APIs**: Data access endpoints

## 📋 Mô Hình Dữ Liệu Dimensional (Star Schema)

### Fact Table: FACT_SALES
```sql
CREATE TABLE fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    geography_key INTEGER REFERENCES dim_geography(geography_key),
    platform_key INTEGER REFERENCES dim_platform(platform_key),

    -- Metrics (Measures)
    quantity INTEGER NOT NULL,
    unit_price_usd DECIMAL(10,2),
    unit_price_vnd DECIMAL(15,0),
    total_amount_usd DECIMAL(12,2),
    total_amount_vnd DECIMAL(18,0),
    discount_amount_usd DECIMAL(10,2),
    tax_amount_usd DECIMAL(10,2),
    shipping_cost_usd DECIMAL(8,2),
    profit_margin_usd DECIMAL(10,2),

    -- Vietnamese Specific
    vietnam_promotion_applied BOOLEAN DEFAULT FALSE,
    payment_method_local VARCHAR(50),

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Dimension Tables

#### DIM_CUSTOMER (Thông Tin Khách Hàng)
```sql
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,

    -- Basic Info
    full_name VARCHAR(200),
    email VARCHAR(150),
    phone VARCHAR(20),
    date_of_birth DATE,
    age INTEGER,
    gender VARCHAR(10),
    registration_date DATE,

    -- Geographic Info
    city VARCHAR(100),
    state VARCHAR(100),
    region VARCHAR(50), -- North, Central, South
    country VARCHAR(50) DEFAULT 'Vietnam',
    postal_code VARCHAR(20),

    -- Business Attributes
    customer_segment VARCHAR(50), -- Regular, High_Value, Premium
    income_level VARCHAR(30), -- Low, Middle, High
    monthly_spending_usd DECIMAL(10,2),
    preferred_device VARCHAR(30),
    preferred_payment_method VARCHAR(50),
    shopping_frequency VARCHAR(30),

    -- Vietnamese Market Specific
    vietnam_region_preference VARCHAR(20),

    -- SCD Type 2 Support
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### DIM_PRODUCT (Thông Tin Sản Phẩm)
```sql
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,

    -- Product Info
    product_name VARCHAR(300),
    brand VARCHAR(100),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    description TEXT,

    -- Pricing
    price_usd DECIMAL(10,2),
    price_vnd DECIMAL(15,0),
    original_price_usd DECIMAL(10,2),
    discount_percentage DECIMAL(5,2),

    -- Quality Metrics
    rating DECIMAL(3,2),
    review_count INTEGER,

    -- Physical Attributes
    weight_kg DECIMAL(8,3),
    warranty_months INTEGER,
    is_featured BOOLEAN DEFAULT FALSE,
    launch_date DATE,

    -- Vietnamese Market Specific
    vietnam_popularity_score DECIMAL(3,2),
    market_size_usd DECIMAL(15,0),
    market_growth_rate DECIMAL(5,2),
    mobile_commerce_penetration DECIMAL(5,2),

    -- Platform Availability
    platform_availability JSONB,
    payment_methods_supported JSONB,

    -- SCD Type 2 Support
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### DIM_TIME (Chiều Thời Gian)
```sql
CREATE TABLE dim_time (
    time_key SERIAL PRIMARY KEY,
    date_value DATE UNIQUE NOT NULL,

    -- Date Hierarchy
    day_of_week INTEGER,
    day_name VARCHAR(20),
    day_of_month INTEGER,
    day_of_year INTEGER,

    -- Week
    week_of_year INTEGER,
    week_start_date DATE,
    week_end_date DATE,

    -- Month
    month_number INTEGER,
    month_name VARCHAR(20),
    month_short_name VARCHAR(10),
    month_start_date DATE,
    month_end_date DATE,

    -- Quarter
    quarter_number INTEGER,
    quarter_name VARCHAR(10),
    quarter_start_date DATE,
    quarter_end_date DATE,

    -- Year
    year_number INTEGER,
    year_start_date DATE,
    year_end_date DATE,

    -- Vietnamese Specific
    is_vietnamese_holiday BOOLEAN DEFAULT FALSE,
    vietnamese_holiday_name VARCHAR(100),
    is_tet_season BOOLEAN DEFAULT FALSE,
    is_shopping_season BOOLEAN DEFAULT FALSE,

    -- Business Flags
    is_weekend BOOLEAN DEFAULT FALSE,
    is_business_day BOOLEAN DEFAULT TRUE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### DIM_GEOGRAPHY (Thông Tin Địa Lý)
```sql
CREATE TABLE dim_geography (
    geography_key SERIAL PRIMARY KEY,

    -- Address Hierarchy
    country VARCHAR(50) DEFAULT 'Vietnam',
    region VARCHAR(50), -- North, Central, South
    state_province VARCHAR(100),
    city VARCHAR(100),
    district VARCHAR(100),
    ward VARCHAR(100),
    postal_code VARCHAR(20),

    -- Coordinates
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),

    -- Demographics
    population INTEGER,
    population_density DECIMAL(10,2),
    urban_rural_classification VARCHAR(20),

    -- Economic Indicators
    gdp_per_capita_usd DECIMAL(12,2),
    average_income_usd DECIMAL(10,2),
    ecommerce_penetration_rate DECIMAL(5,2),
    internet_penetration_rate DECIMAL(5,2),
    mobile_penetration_rate DECIMAL(5,2),

    -- Vietnamese Specific
    administrative_level VARCHAR(30), -- Province, City, District
    vietnam_region_code VARCHAR(10),
    major_ecommerce_hubs BOOLEAN DEFAULT FALSE,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### DIM_PLATFORM (Thông Tin Nền Tảng)
```sql
CREATE TABLE dim_platform (
    platform_key SERIAL PRIMARY KEY,
    platform_id VARCHAR(50) UNIQUE NOT NULL,

    -- Platform Info
    platform_name VARCHAR(100), -- Lazada, Shopee, Tiki, etc.
    platform_type VARCHAR(50), -- Marketplace, Direct, Social
    platform_category VARCHAR(50),

    -- Business Model
    commission_rate DECIMAL(5,2),
    payment_processing_fee DECIMAL(5,2),
    listing_fee DECIMAL(8,2),

    -- Vietnamese Market Specific
    market_share_vietnam DECIMAL(5,2),
    user_base_vietnam INTEGER,
    mobile_app_downloads INTEGER,

    -- Support Features
    supports_cod BOOLEAN DEFAULT FALSE,
    supports_installment BOOLEAN DEFAULT FALSE,
    supports_crypto BOOLEAN DEFAULT FALSE,
    supported_payment_methods JSONB,

    -- Performance Metrics
    average_delivery_time_days INTEGER,
    customer_satisfaction_score DECIMAL(3,2),

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🔄 ETL Processes và Data Flow

### ETL Pipeline Architecture
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Extract   │───▶│ Transform   │───▶│    Load     │───▶│  Validate   │
│             │    │             │    │             │    │             │
│ • API Calls │    │ • Cleansing │    │ • Fact      │    │ • Quality   │
│ • File Read │    │ • Mapping   │    │ • Dimension │    │ • Integrity │
│ • Streaming │    │ • Business  │    │ • Aggregate │    │ • Completeness│
│ • Database  │    │   Rules     │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### 1. Extraction (Trích Xuất)
```python
# Data Sources Configuration
DATA_SOURCES = {
    'ecommerce_platforms': {
        'lazada': {'api_endpoint': 'https://api.lazada.vn', 'type': 'REST'},
        'shopee': {'api_endpoint': 'https://api.shopee.vn', 'type': 'REST'},
        'tiki': {'api_endpoint': 'https://api.tiki.vn', 'type': 'REST'},
        'sendo': {'api_endpoint': 'https://api.sendo.vn', 'type': 'REST'}
    },
    'database': {
        'postgresql': 'postgresql://user:pass@postgres:5432/ecommerce_dss',
        'mongodb': 'mongodb://user:pass@mongodb:27017/ecommerce_raw'
    },
    'streaming': {
        'kafka': 'kafka:29092'
    }
}
```

### 2. Transformation (Biến Đổi)
```python
# Vietnamese Currency Conversion
def convert_usd_to_vnd(amount_usd, exchange_rate=24000):
    return amount_usd * exchange_rate

# Vietnamese Address Standardization
def standardize_vietnam_address(address):
    regions = {
        'Hanoi': 'North', 'Hai Phong': 'North',
        'Da Nang': 'Central', 'Hue': 'Central',
        'Ho Chi Minh City': 'South', 'Can Tho': 'South'
    }
    return regions.get(address.get('city'), 'Unknown')

# Vietnamese Payment Method Mapping
def map_payment_method(method):
    mappings = {
        'momo': 'E_Wallet_MoMo',
        'zalopay': 'E_Wallet_ZaloPay',
        'cod': 'COD',
        'bank_transfer': 'Bank_Transfer',
        'credit_card': 'Credit_Card'
    }
    return mappings.get(method.lower(), method)
```

### 3. Loading Strategy (Chiến Lược Tải)

#### Incremental Loading
```sql
-- Load new and updated records
INSERT INTO fact_sales (
    order_id, customer_key, product_key, time_key,
    geography_key, platform_key, quantity,
    unit_price_usd, total_amount_usd, created_at
)
SELECT
    s.order_id,
    dc.customer_key,
    dp.product_key,
    dt.time_key,
    dg.geography_key,
    dpl.platform_key,
    s.quantity,
    s.unit_price_usd,
    s.total_amount_usd,
    CURRENT_TIMESTAMP
FROM staging.sales s
JOIN dim_customer dc ON s.customer_id = dc.customer_id AND dc.is_current = TRUE
JOIN dim_product dp ON s.product_id = dp.product_id AND dp.is_current = TRUE
JOIN dim_time dt ON DATE(s.order_date) = dt.date_value
JOIN dim_geography dg ON s.city = dg.city
JOIN dim_platform dpl ON s.platform = dpl.platform_name
WHERE s.last_modified >= (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM fact_sales);
```

## 📈 Data Marts Specification

### 1. Sales Mart (Mart Bán Hàng)
```sql
CREATE VIEW sales_mart AS
SELECT
    fs.sale_id,
    dt.date_value,
    dt.month_name,
    dt.quarter_name,
    dt.year_number,
    dc.customer_segment,
    dc.region,
    dp.category,
    dp.brand,
    dpl.platform_name,

    -- Metrics
    fs.quantity,
    fs.total_amount_usd,
    fs.total_amount_vnd,
    fs.profit_margin_usd,

    -- Vietnamese Specific
    fs.vietnam_promotion_applied,
    fs.payment_method_local

FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_time dt ON fs.time_key = dt.time_key
JOIN dim_platform dpl ON fs.platform_key = dpl.platform_key
WHERE dc.is_current = TRUE AND dp.is_current = TRUE;
```

### 2. Customer Analytics Mart
```sql
CREATE VIEW customer_analytics_mart AS
SELECT
    dc.customer_id,
    dc.customer_segment,
    dc.region,
    dc.income_level,
    dc.preferred_payment_method,

    -- Aggregated Metrics
    COUNT(fs.sale_id) as total_orders,
    SUM(fs.total_amount_usd) as lifetime_value_usd,
    AVG(fs.total_amount_usd) as avg_order_value_usd,

    -- Behavioral Metrics
    COUNT(DISTINCT dp.category) as categories_purchased,
    COUNT(DISTINCT dpl.platform_name) as platforms_used,

    -- Recency, Frequency, Monetary (RFM)
    MAX(dt.date_value) as last_purchase_date,
    COUNT(DISTINCT dt.date_value) as purchase_frequency,
    SUM(fs.total_amount_usd) as monetary_value

FROM dim_customer dc
LEFT JOIN fact_sales fs ON dc.customer_key = fs.customer_key
LEFT JOIN dim_product dp ON fs.product_key = dp.product_key
LEFT JOIN dim_time dt ON fs.time_key = dt.time_key
LEFT JOIN dim_platform dpl ON fs.platform_key = dpl.platform_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_id, dc.customer_segment, dc.region,
         dc.income_level, dc.preferred_payment_method;
```

## 🔧 Data Quality Framework

### Data Quality Dimensions
1. **Completeness**: Dữ liệu đầy đủ
2. **Accuracy**: Tính chính xác
3. **Consistency**: Tính nhất quán
4. **Timeliness**: Tính kịp thời
5. **Validity**: Tính hợp lệ
6. **Uniqueness**: Tính duy nhất

### Quality Checks
```sql
-- Completeness Check
SELECT
    'fact_sales' as table_name,
    'customer_key' as column_name,
    COUNT(*) as total_records,
    COUNT(customer_key) as non_null_records,
    (COUNT(customer_key) * 100.0 / COUNT(*)) as completeness_percentage
FROM fact_sales;

-- Accuracy Check (Vietnamese phone number format)
SELECT
    customer_id,
    phone,
    CASE
        WHEN phone ~ '^\+84[0-9]{9,10}$' THEN 'Valid'
        ELSE 'Invalid'
    END as phone_validity
FROM dim_customer
WHERE is_current = TRUE;

-- Consistency Check (Region mapping)
SELECT
    city,
    region,
    COUNT(*) as records
FROM dim_geography
GROUP BY city, region
HAVING COUNT(DISTINCT region) > 1;
```

## 🚀 Performance Optimization

### 1. Indexing Strategy
```sql
-- Fact Table Indexes
CREATE INDEX idx_fact_sales_customer_key ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product_key ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_time_key ON fact_sales(time_key);
CREATE INDEX idx_fact_sales_date ON fact_sales(created_at);

-- Dimension Table Indexes
CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_time_date ON dim_time(date_value);

-- Composite Indexes for common queries
CREATE INDEX idx_fact_sales_composite ON fact_sales(time_key, customer_key, product_key);
```

### 2. Partitioning Strategy
```sql
-- Partition fact table by time (monthly)
CREATE TABLE fact_sales (
    sale_id BIGSERIAL,
    -- other columns...
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (created_at);

-- Create monthly partitions
CREATE TABLE fact_sales_202410 PARTITION OF fact_sales
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE fact_sales_202411 PARTITION OF fact_sales
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
```

### 3. Materialized Views for Aggregations
```sql
-- Monthly sales summary
CREATE MATERIALIZED VIEW mv_monthly_sales AS
SELECT
    dt.year_number,
    dt.month_number,
    dt.month_name,
    dc.region,
    dp.category,
    dpl.platform_name,

    COUNT(*) as total_orders,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount_usd) as total_revenue_usd,
    SUM(fs.total_amount_vnd) as total_revenue_vnd,
    AVG(fs.total_amount_usd) as avg_order_value

FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_time dt ON fs.time_key = dt.time_key
JOIN dim_platform dpl ON fs.platform_key = dpl.platform_key
WHERE dc.is_current = TRUE AND dp.is_current = TRUE
GROUP BY dt.year_number, dt.month_number, dt.month_name,
         dc.region, dp.category, dpl.platform_name;

-- Refresh schedule
CREATE INDEX ON mv_monthly_sales (year_number, month_number, region, category);
```

## 📊 Business Intelligence Views

### Vietnamese E-commerce KPIs
```sql
-- Revenue by Vietnamese regions
CREATE VIEW vw_revenue_by_vietnam_region AS
SELECT
    dc.region,
    dt.year_number,
    dt.quarter_name,
    SUM(fs.total_amount_vnd) as total_revenue_vnd,
    COUNT(DISTINCT dc.customer_id) as unique_customers,
    COUNT(*) as total_orders,
    AVG(fs.total_amount_vnd) as avg_order_value_vnd
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
JOIN dim_time dt ON fs.time_key = dt.time_key
WHERE dc.is_current = TRUE
GROUP BY dc.region, dt.year_number, dt.quarter_name
ORDER BY dt.year_number, dt.quarter_name, total_revenue_vnd DESC;

-- Platform performance in Vietnam
CREATE VIEW vw_platform_performance_vietnam AS
SELECT
    dpl.platform_name,
    dt.month_name,
    dt.year_number,
    SUM(fs.total_amount_vnd) as revenue_vnd,
    COUNT(*) as total_orders,
    COUNT(DISTINCT fs.customer_key) as unique_customers,
    AVG(fs.total_amount_vnd) as avg_order_value_vnd,
    SUM(CASE WHEN fs.payment_method_local IN ('E_Wallet_MoMo', 'E_Wallet_ZaloPay')
             THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as ewallet_adoption_rate
FROM fact_sales fs
JOIN dim_platform dpl ON fs.platform_key = dpl.platform_key
JOIN dim_time dt ON fs.time_key = dt.time_key
GROUP BY dpl.platform_name, dt.month_name, dt.year_number
ORDER BY dt.year_number, dt.month_name, revenue_vnd DESC;
```

## 🔄 Real-time Streaming Integration

### Kafka Topics Structure
```yaml
kafka_topics:
  sales_events:
    partitions: 6
    replication_factor: 2
    retention_ms: 604800000  # 7 days

  customer_events:
    partitions: 3
    replication_factor: 2
    retention_ms: 2592000000  # 30 days

  product_events:
    partitions: 4
    replication_factor: 2
    retention_ms: 1209600000  # 14 days
```

### Spark Streaming Processing
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def process_sales_stream():
    spark = SparkSession.builder \
        .appName("Vietnam_Ecommerce_Sales_Stream") \
        .getOrCreate()

    # Read from Kafka
    sales_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "sales_events") \
        .load()

    # Process Vietnamese specific transformations
    processed_stream = sales_stream \
        .select(from_json(col("value").cast("string"), sales_schema).alias("data")) \
        .select("data.*") \
        .withColumn("total_amount_vnd", col("total_amount_usd") * 24000) \
        .withColumn("region",
                   when(col("city").isin(["Hanoi", "Hai Phong"]), "North")
                   .when(col("city").isin(["Da Nang", "Hue"]), "Central")
                   .when(col("city").isin(["Ho Chi Minh City", "Can Tho"]), "South")
                   .otherwise("Unknown"))

    # Write to warehouse
    query = processed_stream \
        .writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/ecommerce_dss") \
        .option("dbtable", "fact_sales_realtime") \
        .option("user", "dss_user") \
        .option("password", "dss_password_123") \
        .trigger(processingTime='30 seconds') \
        .start()

    query.awaitTermination()
```

## 🏷️ Metadata Management

### Data Catalog Structure
```json
{
  "tables": {
    "fact_sales": {
      "description": "Bảng fact chính chứa dữ liệu bán hàng e-commerce Việt Nam",
      "source_systems": ["Lazada", "Shopee", "Tiki", "Sendo"],
      "update_frequency": "Real-time via Kafka",
      "data_owner": "Sales Analytics Team",
      "last_updated": "2025-10-04",
      "row_count": 15000000,
      "size_gb": 2.5,
      "columns": {
        "sale_id": {
          "description": "Unique identifier for each sale",
          "data_type": "BIGSERIAL",
          "nullable": false,
          "primary_key": true
        },
        "total_amount_vnd": {
          "description": "Tổng giá trị đơn hàng tính bằng VND",
          "data_type": "DECIMAL(18,0)",
          "nullable": false,
          "business_rules": ["Must be positive", "Converted from USD using daily exchange rate"]
        }
      }
    }
  }
}
```

## 🔐 Security và Compliance

### Data Security Measures
1. **Encryption**: AES-256 for data at rest
2. **Access Control**: Role-based access (RBAC)
3. **Audit Trail**: All data access logged
4. **Data Masking**: PII protection in non-prod environments
5. **Backup**: Daily automated backups

### Vietnamese Data Protection Compliance
- **Personal Data Protection**: Tuân thủ luật bảo vệ dữ liệu cá nhân Việt Nam
- **Data Residency**: Dữ liệu lưu trữ trong nước
- **Consent Management**: Theo dõi sự đồng ý của khách hàng

## 📋 Deployment Architecture

### Infrastructure Requirements
```yaml
hardware_requirements:
  database_servers:
    postgresql:
      cpu: 16 cores
      memory: 64GB RAM
      storage: 2TB SSD
      replication: Master-Slave

    mongodb:
      cpu: 8 cores
      memory: 32GB RAM
      storage: 1TB SSD

  streaming_cluster:
    kafka:
      nodes: 3
      cpu: 8 cores each
      memory: 16GB RAM each
      storage: 500GB SSD each

    spark:
      master: 8 cores, 16GB RAM
      workers: 3 nodes, 16 cores, 32GB RAM each

software_requirements:
  - PostgreSQL 13+
  - MongoDB 5.0+
  - Apache Kafka 2.8+
  - Apache Spark 3.2+
  - Apache Airflow 2.5+
  - Redis 7.0+
```

## 📈 Monitoring và Alerting

### Key Performance Indicators (KPIs)
```yaml
data_warehouse_kpis:
  data_freshness:
    target: "< 5 minutes for streaming data"
    current: "2.3 minutes average"

  data_quality:
    target: "> 99.5% accuracy"
    current: "99.7% accuracy"

  query_performance:
    target: "< 30 seconds for standard reports"
    current: "18 seconds average"

  system_availability:
    target: "> 99.9% uptime"
    current: "99.95% uptime"

business_kpis:
  vietnamese_market_coverage:
    target: "All 63 provinces"
    current: "58 provinces active"

  platform_integration:
    target: "Top 5 Vietnamese platforms"
    current: "6 platforms integrated"
```

### Alert Configuration
```yaml
alerts:
  data_quality:
    - metric: "completeness_percentage"
      threshold: "< 95%"
      action: "notify_data_team"

    - metric: "duplicate_records"
      threshold: "> 1000 per hour"
      action: "stop_etl_pipeline"

  performance:
    - metric: "query_response_time"
      threshold: "> 60 seconds"
      action: "notify_dba_team"

    - metric: "disk_usage"
      threshold: "> 85%"
      action: "archive_old_data"
```

## 🚀 Future Enhancements

### Phase 1: Current Implementation
- ✅ Core dimensional model
- ✅ ETL pipelines
- ✅ Basic reporting

### Phase 2: Advanced Analytics (Q1 2025)
- 🔄 Machine Learning integration
- 🔄 Predictive analytics
- 🔄 Customer segmentation
- 🔄 Demand forecasting

### Phase 3: AI-Powered Insights (Q2 2025)
- 📋 Natural language queries
- 📋 Automated insights generation
- 📋 Anomaly detection
- 📋 Recommendation engine

### Phase 4: Real-time Personalization (Q3 2025)
- 📋 Real-time customer profiles
- 📋 Dynamic pricing optimization
- 📋 Live campaign optimization
- 📋 Instant fraud detection

---

## 📚 References và Documentation

1. **Technical Documentation**: `/docs/technical/`
2. **Business Glossary**: `/docs/business_glossary.md`
3. **API Documentation**: `/docs/api/`
4. **Deployment Guide**: `/docs/deployment/`
5. **User Manual**: `/docs/user_manual_vi.md`

---

**Ghi chú**: Tài liệu này được thiết kế đặc biệt cho thị trường e-commerce Việt Nam với các yêu cầu cụ thể về địa lý, thanh toán, và hành vi khách hàng Việt Nam.