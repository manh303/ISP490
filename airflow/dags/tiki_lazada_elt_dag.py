# -*- coding: utf-8 -*-
import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

# Import metadata collection helpers
from helpers.collect_metrics_after_etl import collect_table_stats_after_etl, collect_db_health

CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs")

default_args = {"owner": "data_eng", "retries": 2, "retry_delay": timedelta(minutes=10)}

SKIP_ML = os.getenv("SKIP_ML_MODELS", "false").lower() == "true"

with DAG(
    dag_id="tiki_lazada_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["elt", "spark", "retail", "ml"],
) as dag:

    start = EmptyOperator(task_id="start")

    PREAMBLE = rf"""
set -euo pipefail
echo "Node: $(hostname)"
echo "Python path: $(command -v python)"; python -V 2>&1 || true
mkdir -p "{CRAWLER_OUTPUT_DIR}" "/app/data/logs" "/tmp/crawler_logs"
export CRAWLER_OUTPUT_DIR="{CRAWLER_OUTPUT_DIR}"
export CRAWLER_LOG_DIR="/app/data/logs"
export FORCE_CRAWLER_LOG_DIR="/tmp/crawler_logs"
export CHROME_BIN="${{CHROME_BIN:-/usr/bin/chromium-browser}}"
export CHROMEDRIVER_PATH="${{CHROMEDRIVER_PATH:-/usr/bin/chromedriver}}"
export DISPLAY="${{DISPLAY:-:99}}"
export LAZADA_PROFILE_DIR="${{LAZADA_PROFILE_DIR:-/app/data/.profiles/lazada}}"
export TIKI_PROFILE_DIR="${{TIKI_PROFILE_DIR:-/app/data/.profiles/tiki}}"
export LAZADA_HEADLESS="${{LAZADA_HEADLESS:-1}}"
"""

    crawl_lazada = BashOperator(
        task_id="crawl_lazada",
        bash_command=PREAMBLE + r"""
SCRIPT="/app/crawlers/lazada/runners/lazada_with_cookies.py"
RUN_DATE=$(date +%F)
OUT_DIR="${CRAWLER_OUTPUT_DIR}/lazada/date=${RUN_DATE}"
if [ -d "$OUT_DIR" ] && find "$OUT_DIR" -type f -name '*.jsonl' -print -quit | grep -q .; then
  echo "Lazada raw already exists for ${RUN_DATE}, skipping crawl."
  exit 0
fi
pip install -q playwright 2>/dev/null || true
playwright install chromium 2>/dev/null || true
[ -f "$SCRIPT" ] || { echo "❌ Không tìm thấy $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
"""
    )

    crawl_lazada_reviews = BashOperator(
        task_id="crawl_lazada_reviews",
        bash_command=PREAMBLE + r"""
SCRIPT="/app/crawlers/lazada/runners/lazada_reviews_from_products.py"
RUN_DATE=$(date +%F)
OUT_DIR="${CRAWLER_OUTPUT_DIR}/lazada_reviews/date=${RUN_DATE}"
if [ -d "$OUT_DIR" ] && find "$OUT_DIR" -type f -name '*.jsonl' -print -quit | grep -q .; then
  echo "Lazada reviews already exist for ${RUN_DATE}, skipping crawl."
  exit 0
fi
pip install -q playwright 2>/dev/null || true
playwright install chromium 2>/dev/null || true
[ -f "$SCRIPT" ] || { echo "❌ Không tìm thấy $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
"""
    )

    crawl_tiki = BashOperator(
        task_id="crawl_tiki",
        bash_command=PREAMBLE + r"""
SCRIPT="/app/crawlers/tiki/tiki_crawler.py"
RUN_DATE=$(date +%F)
OUT_DIR="${CRAWLER_OUTPUT_DIR}/tiki/date=${RUN_DATE}"
if [ -d "$OUT_DIR" ] && find "$OUT_DIR" -type f -name '*.jsonl' -print -quit | grep -q .; then
  echo "Tiki raw already exists for ${RUN_DATE}, skipping crawl."
  exit 0
fi
[ -f "$SCRIPT" ] || { echo "? Kh?ng t?m th?y $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
"""
    )

    crawl_tiki_reviews = BashOperator(
        task_id="crawl_tiki_reviews",
        bash_command=PREAMBLE + r"""
SCRIPT="/app/crawlers/tiki/tiki_review_crawler.py"
RUN_DATE=$(date +%F)
OUT_DIR="${CRAWLER_OUTPUT_DIR}/tiki_reviews/date=${RUN_DATE}"
if [ -d "$OUT_DIR" ] && find "$OUT_DIR" -type f -name '*.jsonl' -print -quit | grep -q .; then
  echo "Tiki reviews already exist for ${RUN_DATE}, skipping crawl."
  exit 0
fi
[ -f "$SCRIPT" ] || { echo "? Kh?ng t?m th?y $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
"""
    )

    def _raw_ready(**context):
        from datetime import datetime as dt
        run_date = dt.now().strftime("%Y-%m-%d")
        need = [
            Path(CRAWLER_OUTPUT_DIR) / "lazada" / f"date={run_date}",
            Path(CRAWLER_OUTPUT_DIR) / "tiki" / f"date={run_date}",
        ]
        for base in need:
            if not base.exists():
                return False
            if not any(p.suffix == ".jsonl" for p in base.rglob("*.jsonl")):
                return False
        return True

    def _reviews_ready(**context):
        from datetime import datetime as dt
        run_date = dt.now().strftime("%Y-%m-%d")
        lazada_reviews = Path(CRAWLER_OUTPUT_DIR) / "lazada_reviews" / f"date={run_date}"
        tiki_reviews = Path(CRAWLER_OUTPUT_DIR) / "tiki_reviews" / f"date={run_date}"
        
        has_lazada = lazada_reviews.exists() and any(p.suffix == ".jsonl" for p in lazada_reviews.rglob("*.jsonl"))
        has_tiki = tiki_reviews.exists() and any(p.suffix == ".jsonl" for p in tiki_reviews.rglob("*.jsonl"))
        
        return has_lazada or has_tiki

    wait_raw_ready = PythonSensor(
        task_id="wait_raw_ready",
        python_callable=_raw_ready,
        poke_interval=30,
        timeout=1800,
        mode="reschedule",
    )

    wait_reviews_ready = PythonSensor(
        task_id="wait_reviews_ready",
        python_callable=_reviews_ready,
        poke_interval=30,
        timeout=1800,
        mode="reschedule",
    )

    load_to_stg = BashOperator(
        task_id="load_to_stg",
        bash_command=rf"""
pip install -q psycopg2-binary 2>/dev/null || true
export INCREMENTAL_LOAD=true
python /app/src/staging/load_raw_data.py
"""
    )

    create_ods_tables = BashOperator(
        task_id="create_ods_tables",
        bash_command=rf"""
pip install -q psycopg2-binary 2>/dev/null || true
psql postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 -f /app/src/spark_jobs/create_ods_tables.sql 2>/dev/null || \
python -c "import psycopg2; conn=psycopg2.connect(host='dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com',port=5432,database='ecommerce_dss_1',user='dss_user',password='6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G'); cur=conn.cursor(); cur.execute(open('/app/src/spark_jobs/create_ods_tables.sql').read()); conn.commit(); conn.close(); print('✅ ODS tables created')"
"""
    )

    truncate_ods = BashOperator(
        task_id="truncate_ods",
        bash_command=r"""
pip install -q psycopg2-binary 2>/dev/null || true
python -c "import psycopg2; from datetime import date; conn=psycopg2.connect(host='dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com',port=5432,database='ecommerce_dss_1',user='dss_user',password='6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G'); cur=conn.cursor(); today=date.today(); cur.execute('DELETE FROM ods_product_clean WHERE crawl_date = %s', (today,)); cur.execute('DELETE FROM ods_review_clean WHERE crawl_date = %s', (today,)); conn.commit(); conn.close(); print('✅ Today partition cleaned')"
"""
    )

    transform_to_ods = BashOperator(
        task_id="transform_to_ods",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --executor-cores 4 \
  --executor-memory 4g \
  --driver-memory 2g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=200 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/ods_transformation.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
"""
    )

    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=rf"""
python /app/src/standardization/data_quality.py
"""
    )

    identifier_sync = BashOperator(
        task_id="identifier_sync",
        bash_command=rf"""
python /app/src/standardization/identifier_sync.py
"""
    )

    category_mapping = BashOperator(
        task_id="category_mapping",
        bash_command=rf"""
python /app/src/standardization/category_mapping.py
"""
    )

    technical_metadata = BashOperator(
        task_id="technical_metadata",
        bash_command=rf"""
python /app/src/standardization/technical_metadata.py
"""
    )

    build_dwh = BashOperator(
        task_id="build_dwh",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.sql.session.timeZone=UTC \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/dwh_build.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
"""
    )

    build_datamart = BashOperator(
        task_id="build_datamart",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.sql.session.timeZone=UTC \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/datamart_build.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
"""
    )

    ml_product_recommendation = BashOperator(
        task_id="ml_product_recommendation",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.sql.session.timeZone=UTC \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/product_recommendation.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
"""
    )

    ml_price_optimization = BashOperator(
        task_id="ml_price_optimization",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.sql.session.timeZone=UTC \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/price_optimization.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
"""
    )

    ml_demand_forecasting = BashOperator(
        task_id="ml_demand_forecasting",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.sql.session.timeZone=UTC \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/demand_forecasting.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
"""
    )

    ml_sales_forecasting = BashOperator(
        task_id="ml_sales_forecasting",
        bash_command=r"""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.sql.session.timeZone=UTC \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/ml_models/sales_forecasting.py \
  --pg-url jdbc:postgresql://dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com:5432/ecommerce_dss_1 \
  --pg-user dss_user \
  --pg-pass 6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G
"""
    )

    # Metadata collection task - runs after all ETL/ML completes
    def collect_metadata_wrapper():
        """Wrapper to call both metadata collection functions"""
        print("📊 Collecting table statistics...")
        collect_table_stats_after_etl()
        print("💚 Collecting database health...")
        collect_db_health()
        print("✅ Metadata collection completed!")
    
    collect_metadata = PythonOperator(
        task_id="collect_metadata",
        python_callable=collect_metadata_wrapper,
    )

    end = EmptyOperator(task_id="end")

    # Parallel crawling

    start >> [crawl_lazada, crawl_tiki]
    crawl_lazada >> crawl_lazada_reviews
    crawl_tiki >> crawl_tiki_reviews
    [crawl_lazada, crawl_tiki] >> wait_raw_ready
    [crawl_lazada_reviews, crawl_tiki_reviews] >> wait_reviews_ready
    
    # Parallel processing
    [wait_raw_ready, wait_reviews_ready] >> load_to_stg >> create_ods_tables >> truncate_ods >> transform_to_ods >> data_quality_check
    
    # Parallel standardization
    data_quality_check >> [category_mapping, identifier_sync, technical_metadata]
    [category_mapping, identifier_sync, technical_metadata] >> build_dwh >> build_datamart
    
    # Parallel ML (all independent)
    if SKIP_ML:
        build_datamart >> collect_metadata >> end
    else:
        build_datamart >> [ml_product_recommendation, ml_price_optimization, ml_demand_forecasting, ml_sales_forecasting] >> collect_metadata >> end
