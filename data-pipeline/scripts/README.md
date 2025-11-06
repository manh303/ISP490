# Tiki & Lazada ETL Scripts

Simple ETL scripts được thiết kế để chạy độc lập hoặc thông qua Airflow orchestration.

## 🏗️ Architecture

```
Raw JSON Files → Staging → ODS → DWH → Data Mart
```

## 📁 Scripts Overview

### 1. `01_staging_ingestion.py`
**Staging Data Ingestion**
- Load raw JSON files vào staging tables
- Support Tiki và Lazada data
- Handle deduplication và data validation

**Usage:**
```bash
python 01_staging_ingestion.py --date 2025-11-06
python 01_staging_ingestion.py --data-dirs /path/to/data1 /path/to/data2
```

### 2. `02_ods_standardization.py`
**ODS Data Standardization**
- Clean và standardize product data
- Normalize prices và ratings
- Map categories và platforms

**Usage:**
```bash
python 02_ods_standardization.py --date 2025-11-06
```

### 3. `03_dwh_transformation.py`
**DWH Star Schema Transformation**
- Build dimensions (date, platform, brand, category, product)
- Create fact tables (product daily, review summary)
- Generate data mart (price analytics)

**Usage:**
```bash
python 03_dwh_transformation.py --date 2025-11-06
```

## 🚀 Running the Pipeline

### Standalone Execution
Chạy từng script riêng biệt:

```bash
# Step 1: Ingest raw data
python 01_staging_ingestion.py --date 2025-11-06

# Step 2: Standardize data
python 02_ods_standardization.py --date 2025-11-06

# Step 3: Transform to DWH
python 03_dwh_transformation.py --date 2025-11-06
```

### Complete Pipeline
Chạy toàn bộ pipeline:

```bash
#!/bin/bash
DATE=$(date +%Y-%m-%d)

echo "🚀 Starting ETL pipeline for $DATE"

# Run staging ingestion
python 01_staging_ingestion.py --date $DATE
if [ $? -ne 0 ]; then
    echo "❌ Staging ingestion failed"
    exit 1
fi

# Run ODS standardization
python 02_ods_standardization.py --date $DATE
if [ $? -ne 0 ]; then
    echo "❌ ODS standardization failed"
    exit 1
fi

# Run DWH transformation
python 03_dwh_transformation.py --date $DATE
if [ $? -ne 0 ]; then
    echo "❌ DWH transformation failed"
    exit 1
fi

echo "✅ ETL pipeline completed successfully"
```

### Airflow Integration
Copy `tiki_lazada_etl_dag.py` vào Airflow DAGs folder:

```bash
cp ../airflow/dags/tiki_lazada_etl_dag.py $AIRFLOW_HOME/dags/
```

## ⚙️ Configuration

### Database Configuration
Update database settings trong mỗi script:

```python
db_config = {
    'host': 'localhost',
    'port': 5433,
    'database': 'ecommerce_dss',
    'user': 'dss_user',
    'password': 'dss_password_123'
}
```

### Data Directories
Staging script sẽ scan các directories này:

```python
data_directories = [
    'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/data',
    'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/crawlers/outputs'
]
```

## 📊 Data Processing Flow

### Staging Layer
- **Input**: Raw JSON files từ crawlers
- **Processing**: Load với checksum validation
- **Output**: `stg_raw_products`, `stg_raw_reviews`

### ODS Layer
- **Input**: Staging tables
- **Processing**: Clean, standardize, deduplicate
- **Output**: `ods_product_clean`, `ods_price_point`, `ods_rating_snapshot`

### DWH Layer
- **Input**: ODS tables
- **Processing**: Star schema transformation
- **Output**: Dimension và fact tables

### Data Mart Layer
- **Input**: DWH fact tables
- **Processing**: Aggregation và analytics
- **Output**: `dm_price_analytics`

## 🔍 Platform Detection

Scripts tự động detect platform từ file names:

| Platform | File Patterns |
|----------|---------------|
| **Tiki** | `*tiki*.json`, `*working_tiki*.json` |
| **Lazada** | `*lazada*.json`, `*enhanced_lazada*.json` |

## 📈 Monitoring & Logging

### Log Output
Mỗi script tạo detailed logs:

```
2025-11-06 10:00:00 - INFO - ✅ Connected to database
2025-11-06 10:00:01 - INFO - 📁 Processed tiki_products.json: 150 products (tiki)
2025-11-06 10:00:02 - INFO - 📦 Standardized 145 products
2025-11-06 10:00:03 - INFO - ✅ Pipeline completed successfully
```

### Database Monitoring
Check processing stats:

```sql
-- View processing summary
SELECT * FROM v_batch_processing_stats
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date DESC;

-- Check data volumes
SELECT
    source_platform,
    COUNT(*) as product_count,
    DATE(created_at) as process_date
FROM stg_raw_products
WHERE source_platform IN ('tiki', 'lazada')
GROUP BY source_platform, DATE(created_at)
ORDER BY process_date DESC;
```

## 🛠️ Dependencies

Install required packages:

```bash
pip install psycopg2-binary
```

## 🎯 Features

- ✅ **Independent Scripts**: Có thể chạy standalone
- ✅ **Date Filtering**: Process specific dates
- ✅ **Error Handling**: Graceful failure handling
- ✅ **Platform Detection**: Auto-detect Tiki/Lazada files
- ✅ **Data Validation**: Checksum và deduplication
- ✅ **Logging**: Detailed processing logs
- ✅ **Airflow Ready**: Compatible với Airflow orchestration

## 📞 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   ```bash
   # Test connection
   psql -h localhost -p 5433 -U dss_user -d ecommerce_dss
   ```

2. **No Data Files Found**
   ```bash
   # Check data directories
   ls -la /path/to/data/collection/data/
   # Verify file naming patterns
   ```

3. **Script Import Errors**
   ```bash
   # Ensure Python path
   export PYTHONPATH=/path/to/scripts:$PYTHONPATH
   ```

### Performance Optimization

- Run scripts during off-peak hours
- Monitor database connection pools
- Use indexed columns for date filtering
- Consider partitioning large fact tables

## 🔄 Scheduling Options

### Cron Job (Linux)
```bash
# Add to crontab for daily 2 AM execution
0 2 * * * /path/to/run_etl_pipeline.sh >> /var/log/etl_pipeline.log 2>&1
```

### Windows Task Scheduler
```cmd
# Create scheduled task
schtasks /create /tn "TikiLazadaETL" /tr "python C:\path\to\scripts\01_staging_ingestion.py" /sc daily /st 02:00
```

### Airflow (Recommended)
Use provided DAG file cho advanced scheduling và monitoring capabilities.