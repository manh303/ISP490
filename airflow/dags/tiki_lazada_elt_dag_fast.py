# Fast pipeline for testing (10-30 minutes)
import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
default_args = {"owner": "data_eng", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="tiki_lazada_pipeline_fast",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fast", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    PREAMBLE = rf"""
export CRAWLER_OUTPUT_DIR="{CRAWLER_OUTPUT_DIR}"
export LAZADA_PAGES_PER_RUN="2"
export TIKI_MAX_PRODUCTS="50"
export MAX_REVIEWS_PER_PRODUCT="10"
"""

    # Fast crawl (2 pages only)
    crawl_lazada = BashOperator(
        task_id="crawl_lazada",
        bash_command=PREAMBLE + r"""
python /app/crawlers/lazada/runners/lazada_with_cookies.py
"""
    )

    crawl_tiki = BashOperator(
        task_id="crawl_tiki",
        bash_command=PREAMBLE + r"""
python /app/crawlers/tiki/tiki_crawler.py
"""
    )

    # Skip reviews for fast mode
    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=r"""
python /app/src/staging/load_raw_data.py
"""
    )

    transform_to_ods = BashOperator(
        task_id="transform_to_ods",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/ods_transformation.py \
  --pg-url jdbc:postgresql://localhost:5432/ecommerce_dss \
  --pg-user dss_user \
  --pg-pass dss_password_123
"""
    )

    build_dwh = BashOperator(
        task_id="build_dwh",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/dwh_build.py \
  --pg-url jdbc:postgresql://localhost:5432/ecommerce_dss \
  --pg-user dss_user \
  --pg-pass dss_password_123
"""
    )

    end = EmptyOperator(task_id="end")

    start >> [crawl_lazada, crawl_tiki] >> load_to_stg >> transform_to_ods >> build_dwh >> end
