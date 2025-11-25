# Hệ thống ELT tự động cho Tiki & Lazada

## 🎯 Tổng quan

Đây là hệ thống ELT (Extract-Load-Transform) tự động hoàn chỉnh để xử lý dữ liệu từ các crawler Tiki và Lazada thành Data Warehouse chuẩn.

## 🏗️ Kiến trúc hệ thống

```
Crawlers (JSON/CSV) → Staging → ODS → DWH → Data Marts
     ↓                  ↓        ↓      ↓        ↓
  Raw Data         Clean Data   Star   Aggregated
                               Schema   Analytics
```

### Các thành phần chính:

1. **Schema Database** (`schemas/ecommerce_schema.sql`)
   - Star schema design với dimensions và facts
   - Staging, ODS, DWH, Data marts layers
   - Indexes và constraints tối ưu

2. **ELT Pipeline** (`src/elt/tiki_lazada_elt.py`)
   - Extract từ crawler outputs
   - Load vào staging tables
   - Transform theo schema chuẩn
   - Data validation và quality checks

3. **Data Validator** (`src/quality/data_validator.py`)
   - 20+ validation rules tự động
   - Quality scoring và reporting
   - Business rule validation

4. **Airflow DAG** (`airflow/dags/tiki_lazada_elt_dag.py`)
   - Orchestration tự động hàng ngày
   - Crawlers → ELT → Validation → Reporting
   - Email notifications và monitoring

## 📊 Schema thiết kế

### Staging Layer
- `staging.raw_products`: Raw JSON data từ crawlers
- Deduplication với data hashing
- File tracking và metadata

### ODS Layer (Operational Data Store)
- `ods.products`: Cleaned, standardized product data
- `ods.shops`: Shop information
- SCD Type 1 cho current state

### DWH Layer (Star Schema)
**Dimensions:**
- `dim_date`: Date dimension với business calendar
- `dim_platform`: Tiki, Lazada platform info
- `dim_category`: Product categories hierarchy
- `dim_brand`: Brand master data
- `dim_shop`: Shop dimension
- `dim_product`: Product dimension với SCD Type 2

**Facts:**
- `fact_product_daily`: Daily product snapshots
- `fact_price_changes`: Price change tracking

### Data Marts
- `marts.price_analytics`: Aggregated price analytics
- Materialized views cho performance

## 🔄 Pipeline workflow

### 1. Data Collection
```bash
# Tự động chạy crawlers
python tiki_crawler.py --mode auto --max-pages 5
python lazada_crawler.py --mode auto --max-pages 5
```

### 2. ELT Processing
```bash
# Chạy ELT pipeline
python src/elt/tiki_lazada_elt.py --data-path /path/to/crawler/outputs
```

### 3. Data Validation
```bash
# Validate quality
python src/quality/data_validator.py --report-path quality_report.json
```

## 🚀 Cài đặt và chạy

### 1. Database Setup
```sql
-- Tạo database
CREATE DATABASE ecommerce_dss_1;

-- Chạy schema setup
\i schemas/ecommerce_schema.sql
```

### 2. Python Dependencies
```bash
pip install psycopg2-binary pandas numpy airflow
```

### 3. Configuration
Update database config trong các files:
```python
ELT_CONFIG = {
    'db_host': 'localhost',
    'db_name': 'ecommerce_dss_1',
    'db_user': 'dss_user',
    'db_password': 'dss_password_123'
}
```

### 4. Chạy thử nghiệm
```bash
# Test ELT pipeline
python src/elt/tiki_lazada_elt.py

# Test validation
python src/quality/data_validator.py
```

### 5. Production với Airflow
```bash
# Deploy DAG
cp airflow/dags/tiki_lazada_elt_dag.py $AIRFLOW_HOME/dags/

# Enable DAG trong Airflow UI
# Schedule: Daily at 3 AM
```

## 📈 Data Quality Features

### Validation Rules (20+ rules):
- **Schema validation**: NOT NULL, data types, constraints
- **Data integrity**: Price consistency, URL format
- **Business rules**: Platform-URL matching, reasonable ranges
- **Freshness checks**: Daily update requirements
- **Deduplication**: Hash-based duplicate detection

### Quality Scoring:
- Overall quality score (0-100%)
- Platform breakdown (Tiki vs Lazada)
- Table-level quality metrics
- Automatic recommendations

### Monitoring:
- Email alerts on failures
- Quality trend tracking
- Airflow dashboard monitoring

## 🗂️ Cấu trúc dữ liệu chuẩn

### Product Schema:
```json
{
  "source_platform": "tiki|lazada",
  "external_product_id": "unique_id",
  "title": "Product name",
  "category": "Standardized category",
  "brand": "Brand name",
  "current_price": 2500000,
  "original_price": 3000000,
  "discount_percentage": 16.67,
  "rating": 4.5,
  "review_count": 150,
  "sold_count": 50,
  "shop_name": "Shop name",
  "product_url": "https://..."
}
```

## 📊 Analytics & Reporting

### Available Data Marts:
1. **Price Analytics**: Theo platform, category, brand
2. **Product Performance**: Rating, reviews, sales trends
3. **Market Comparison**: Tiki vs Lazada competitive analysis
4. **Category Insights**: Top categories, price ranges

### Sample Analytics Queries:
```sql
-- Average price by platform and category
SELECT platform_name, category_name, AVG(current_price) as avg_price
FROM marts.price_analytics
GROUP BY platform_name, category_name;

-- Top selling products
SELECT title, platform_name, sold_count, rating
FROM dwh.fact_product_daily f
JOIN dwh.dim_product p ON f.product_sk = p.product_sk
JOIN dwh.dim_platform pl ON f.platform_sk = pl.platform_sk
ORDER BY sold_count DESC LIMIT 10;
```

## 🔧 Maintenance

### Daily Tasks (Automated):
- Crawler execution
- ELT pipeline processing
- Data quality validation
- Report generation
- Old file cleanup

### Weekly Tasks:
- Schema optimization review
- Index performance analysis
- Quality trend analysis

### Monthly Tasks:
- Data archiving (old staging data)
- Performance tuning
- Schema evolution planning

## 🎛️ Configuration Options

### ELT Pipeline Config:
```python
ELTConfig(
    db_host='localhost',
    db_port=5432,
    db_name='ecommerce_dss_1',
    crawler_data_path='/data/crawlers/outputs',
    batch_size=1000,
    max_retries=3
)
```

### Validation Config:
- Custom validation rules
- Severity levels (ERROR, WARNING, INFO)
- Quality thresholds
- Alert notifications

## 📞 Troubleshooting

### Common Issues:

1. **No crawler data found**
   - Check crawler_data_path setting
   - Verify crawler execution
   - Check file permissions

2. **Database connection errors**
   - Verify database credentials
   - Check PostgreSQL service status
   - Test connection manually

3. **Data quality failures**
   - Review validation report
   - Check source data quality
   - Update validation rules if needed

4. **Airflow DAG issues**
   - Check DAG import errors
   - Verify Python dependencies
   - Review Airflow logs

### Support:
- Check logs in `airflow/logs/`
- Review quality reports
- Monitor database performance
- Contact ELT team for issues

## 🔄 Future Enhancements

1. **Real-time streaming** với Kafka integration
2. **Machine learning** price prediction models
3. **API endpoints** cho real-time analytics
4. **Dashboard integration** với Grafana/Superset
5. **Multi-source expansion** (Shopee, Sendo, etc.)

---

**Version**: 1.0.0
**Last Updated**: November 2024
**Team**: ELT Development Team