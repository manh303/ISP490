# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {"owner": "data_eng", "retries": 1, "retry_delay": timedelta(minutes=5)}

def upload_to_minio(**context):
    from minio import Minio
    from pathlib import Path
    
    client = Minio("minio:9000", "minioadmin", "minioadmin123", secure=False)
    bucket = "crawler-data"
    
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    
    output_dir = Path("/app/data/outputs")
    date = context['ds']
    uploaded = 0
    
    for jsonl_file in output_dir.rglob(f"**/date={date}/*.jsonl"):
        relative = jsonl_file.relative_to(output_dir)
        client.fput_object(bucket, str(relative).replace("\\", "/"), str(jsonl_file))
        uploaded += 1
    
    print(f"Uploaded {uploaded} files to MinIO")

with DAG(
    dag_id="minio_ecommerce_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["minio", "s3", "dss"]
) as dag:

    start = EmptyOperator(task_id="start")

    # Data Sources - Crawlers
    crawl_tiki = BashOperator(
        task_id="crawl_tiki",
        bash_command="python /app/crawlers/tiki/tiki_crawler.py"
    )

    crawl_lazada = BashOperator(
        task_id="crawl_lazada",
        bash_command="python /app/crawlers/lazada/runners/lazada_with_cookies.py"
    )

    # Upload to MinIO (Staging - Raw Data)
    upload_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio
    )

    # Data Standardization Tool
    data_cleaning = BashOperator(
        task_id="data_cleaning",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/standardization/data_cleaning.py \
  --source s3a://crawler-data/*/date={{ ds }}/*.jsonl
"""
    )

    data_standardization = BashOperator(
        task_id="data_standardization",
        bash_command="python /app/src/standardization/data_quality.py"
    )

    quality_dedup = BashOperator(
        task_id="quality_dedup",
        bash_command="python /app/src/standardization/deduplication.py"
    )

    sync_identifier = BashOperator(
        task_id="sync_identifier",
        bash_command="python /app/src/standardization/identifier_sync.py"
    )

    category_mapping = BashOperator(
        task_id="category_mapping",
        bash_command="python /app/src/standardization/category_mapping.py"
    )

    technical_metadata = BashOperator(
        task_id="technical_metadata",
        bash_command="python /app/src/standardization/technical_metadata.py"
    )

    # Analytical Infrastructure
    build_dwh = BashOperator(
        task_id="build_data_warehouse",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/dwh_build.py
"""
    )

    build_datamart = BashOperator(
        task_id="build_datamart",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/datamart_build.py
"""
    )

    # Intelligence DSS System - ML Models
    ml_price_optimization = BashOperator(
        task_id="ml_price_optimization",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  /app/src/ml_models/price_optimization.py
"""
    )

    ml_inventory_recommendation = BashOperator(
        task_id="ml_inventory_recommendation",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  /app/src/ml_models/demand_forecasting.py
"""
    )

    ml_customer_segment = BashOperator(
        task_id="ml_customer_segment",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  /app/src/ml_models/customer_segmentation.py
"""
    )

    end = EmptyOperator(task_id="end")

    # Pipeline Flow
    start >> [crawl_tiki, crawl_lazada] >> upload_minio
    upload_minio >> data_cleaning >> data_standardization >> quality_dedup
    quality_dedup >> [sync_identifier, category_mapping, technical_metadata]
    [sync_identifier, category_mapping, technical_metadata] >> build_dwh >> build_datamart
    build_datamart >> [ml_price_optimization, ml_inventory_recommendation, ml_customer_segment] >> end
