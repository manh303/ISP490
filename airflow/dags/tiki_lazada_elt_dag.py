# -*- coding: utf-8 -*-
# airflow/dags/tiki_lazada_pipeline.py
import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_JOB_PATH = "/app/src/spark_jobs/retail_etl.py"

default_args = {"owner": "data_eng", "retries": 2, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="tiki_lazada_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["elt", "spark", "retail"],
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
SCRIPT="/app/crawlers/lazada/runners/lazada_reviews_crawler_airflow.py"
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
[ -f "$SCRIPT" ] || { echo "❌ Không tìm thấy $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
"""
    )

    crawl_tiki_reviews = BashOperator(
        task_id="crawl_tiki_reviews",
        bash_command=PREAMBLE + r"""
SCRIPT="/app/crawlers/tiki/tiki_review_crawler.py"
[ -f "$SCRIPT" ] || { echo "❌ Không tìm thấy $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
"""
    )

    def _raw_ready(**context):
        # Use today's date instead of execution_date for manual runs
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
        # Check if reviews data is ready
        from datetime import datetime as dt
        run_date = dt.now().strftime("%Y-%m-%d")
        lazada_reviews = Path(CRAWLER_OUTPUT_DIR) / "lazada_reviews" / f"date={run_date}"
        tiki_reviews = Path(CRAWLER_OUTPUT_DIR) / "tiki_reviews" / f"date={run_date}"
        
        # Check if at least one reviews directory exists with data
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
python /app/src/staging/load_raw_data.py
"""
    )

    transform_to_ods = BashOperator(
        task_id="transform_to_ods",
        bash_command=rf"""
python /app/src/standardization/data_cleaning.py
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
        bash_command=rf"""
python /app/src/warehouse_build.py
"""
    )

    build_datamart = BashOperator(
        task_id="build_datamart",
        bash_command=rf"""
python /app/src/datamart_build.py
"""
    )

    end = EmptyOperator(task_id="end")

    start >> [crawl_lazada, crawl_tiki] >> wait_raw_ready
    crawl_lazada >> crawl_lazada_reviews >> wait_reviews_ready
    crawl_tiki >> crawl_tiki_reviews >> wait_reviews_ready
    [wait_raw_ready, wait_reviews_ready] >> load_to_stg >> transform_to_ods >> data_quality_check
    data_quality_check >> [identifier_sync, category_mapping, technical_metadata]
    [identifier_sync, category_mapping, technical_metadata] >> build_dwh >> build_datamart >> end
