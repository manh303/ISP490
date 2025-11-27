# Chạy Pipeline Manual (Theo Main DAG)

## Bước 1: Create ODS Tables
```bash
pip install psycopg2-binary
python -c "import psycopg2; conn=psycopg2.connect(host='dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com',port=5432,database='ecommerce_dss_1',user='dss_user',password='6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G'); cur=conn.cursor(); cur.execute(open('data-pipeline/src/spark_jobs/create_ods_tables.sql').read()); conn.commit(); conn.close(); print('✅ ODS tables created')"
```

## Bước 2: Truncate ODS (Clean today's data)
```bash
python -c "import psycopg2; from datetime import date; conn=psycopg2.connect(host='dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com',port=5432,database='ecommerce_dss_1',user='dss_user',password='6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G'); cur=conn.cursor(); today=date.today(); cur.execute('DELETE FROM ods_product_clean WHERE DATE(crawled_at) = %s', (today,)); cur.execute('DELETE FROM ods_review_clean WHERE DATE(crawled_at) = %s', (today,)); conn.commit(); conn.close(); print('✅ Today partition cleaned')"
```

## Bước 3: Transform to ODS
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/ods_transformation.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
```

## Bước 4: Data Quality Check
```bash
python data-pipeline/src/standardization/data_quality.py
```

## Bước 5: Category Mapping (Parallel)
```bash
python data-pipeline/src/standardization/category_mapping.py
```

## Bước 6: Identifier Sync (Parallel)
```bash
python data-pipeline/src/standardization/identifier_sync.py
```

## Bước 7: Technical Metadata (Parallel)
```bash
python data-pipeline/src/standardization/technical_metadata.py
```

## Bước 8: Build DWH
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/dwh_build.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
```

## Bước 9: Build Datamart
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/datamart_build.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
```

## Bước 10: ML Models (Parallel)

### Product Recommendation
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/product_recommendation.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
```

### Price Optimization
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/price_optimization.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
```

### Demand Forecasting
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/demand_forecasting.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
```

### Sales Forecasting
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/sales_forecasting.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
```

## Chạy tất cả một lần (Full Pipeline Script)
```bash
#!/bin/bash
set -e

echo "=== FULL PIPELINE (Main DAG) ==="

echo "Step 1: Create ODS Tables..."
python -c "import psycopg2; conn=psycopg2.connect(host='dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com',port=5432,database='ecommerce_dss_1',user='dss_user',password='6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G'); cur=conn.cursor(); cur.execute(open('data-pipeline/src/spark_jobs/create_ods_tables.sql').read()); conn.commit(); conn.close(); print('✅ Done')"

echo "Step 2: Truncate ODS..."
python -c "import psycopg2; from datetime import date; conn=psycopg2.connect(host='dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com',port=5432,database='ecommerce_dss_1',user='dss_user',password='6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G'); cur=conn.cursor(); today=date.today(); cur.execute('DELETE FROM ods_product_clean WHERE DATE(crawled_at) = %s', (today,)); cur.execute('DELETE FROM ods_review_clean WHERE DATE(crawled_at) = %s', (today,)); conn.commit(); conn.close(); print('✅ Done')"

echo "Step 3: Transform to ODS..."
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-cores 4 \
  --executor-memory 4g \
  --driver-memory 2g \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/ods_transformation.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G

echo "Step 4: Data Quality Check..."
python data-pipeline/src/standardization/data_quality.py

echo "Step 5-7: Standardization (Parallel)..."
python data-pipeline/src/standardization/category_mapping.py &
python data-pipeline/src/standardization/identifier_sync.py &
python data-pipeline/src/standardization/technical_metadata.py &
wait

echo "Step 8: Build DWH..."
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/dwh_build.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G

echo "Step 9: Build Datamart..."
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/datamart_build.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G

echo "Step 10: ML Models (Parallel)..."
docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.7.1.jar /app/src/ml_models/product_recommendation.py --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 --pg-user dss_user --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G &
docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.7.1.jar /app/src/ml_models/price_optimization.py --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 --pg-user dss_user --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G &
wait

echo "✅ Full Pipeline Completed!"
```

## Lưu script
```bash
# Tạo file
nano run_pipeline.sh

# Paste script ở trên, save

# Chmod
chmod +x run_pipeline.sh

# Chạy
./run_pipeline.sh
```

## Kiểm tra kết quả
```bash
# Vào database
docker exec -it postgres psql -U dss_user -d ecommerce_dss_1

# Check ODS
SELECT COUNT(*) FROM ods_product_clean;
SELECT COUNT(*) FROM ods_review_clean;

# Check DWH
SELECT COUNT(*) FROM dim_product;
SELECT COUNT(*) FROM fact_sales;

# Check Datamart
SELECT COUNT(*) FROM dm_product_summary;
```
