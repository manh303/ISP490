# -*- coding: utf-8 -*-
import os
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
CHECKPOINT_DIR = os.getenv("CRAWLER_CHECKPOINT_DIR", "/tmp/crawler_checkpoints")

default_args = {
    "owner": "data_eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def upload_to_minio(**context):
    from pathlib import Path
    try:
        from minio import Minio
    except ImportError:
        # Try to install at runtime to avoid task failure when dependency missing
        import subprocess, sys
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "minio", "--quiet"], check=True)
            from minio import Minio
            print("Installed 'minio' package at runtime.")
        except Exception as e:
            print(f"Failed to import/install minio: {e}. Skipping upload_to_minio.")
            return

    client = Minio("minio:9000", "minioadmin", "minioadmin123", secure=False)
    bucket = "crawler-data"

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    output_dir = Path(CRAWLER_OUTPUT_DIR)
    date = context["ds"]
    uploaded = 0

    # upload tất cả jsonl trong các folder có pattern date=YYYY-MM-DD (products + reviews)
    for jsonl_file in output_dir.rglob(f"**/date={date}/*.jsonl"):
        relative = jsonl_file.relative_to(output_dir)
        client.fput_object(bucket, str(relative).replace("\\", "/"), str(jsonl_file))
        uploaded += 1

    print(f"Uploaded {uploaded} files to MinIO")


def _raw_ready(**context):
    run_date = context["ds"]
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
    run_date = context["ds"]
    lazada_reviews = Path(CRAWLER_OUTPUT_DIR) / "lazada_reviews" / f"date={run_date}"
    tiki_reviews = Path(CRAWLER_OUTPUT_DIR) / "tiki_reviews" / f"date={run_date}"

    has_lazada = lazada_reviews.exists() and any(
        p.suffix == ".jsonl" for p in lazada_reviews.rglob("*.jsonl")
    )
    has_tiki = tiki_reviews.exists() and any(
        p.suffix == ".jsonl" for p in tiki_reviews.rglob("*.jsonl")
    )

    # chỉ cần có review của 1 trong 2 là cho qua để không chặn pipeline
    return has_lazada or has_tiki


with DAG(
    dag_id="minio_ecommerce_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["minio", "s3", "dss"],
) as dag:

    start = EmptyOperator(task_id="start")

    PREAMBLE = rf"""
set -euo pipefail
echo "Node: $(hostname)"
echo "Python path: $(command -v python)"; python -V 2>&1 || true
mkdir -p "{CRAWLER_OUTPUT_DIR}" "{CHECKPOINT_DIR}" "/app/data/logs" "/tmp/crawler_logs"
export CRAWLER_OUTPUT_DIR="{CRAWLER_OUTPUT_DIR}"
export CRAWLER_CHECKPOINT_DIR="{CHECKPOINT_DIR}"
export CRAWLER_LOG_DIR="/app/data/logs"
export FORCE_CRAWLER_LOG_DIR="/tmp/crawler_logs"
export CHROME_BIN="${{CHROME_BIN:-/usr/bin/chromium-browser}}"
export CHROMEDRIVER_PATH="${{CHROMEDRIVER_PATH:-/usr/bin/chromedriver}}"
export DISPLAY="${{DISPLAY:-:99}}"
export LAZADA_PROFILE_DIR="${{LAZADA_PROFILE_DIR:-/app/data/.profiles/lazada}}"
export TIKI_PROFILE_DIR="${{TIKI_PROFILE_DIR:-/app/data/.profiles/tiki}}"
export LAZADA_HEADLESS="${{LAZADA_HEADLESS:-1}}"
"""

    # =========================
    #       CRAWLERS (GIỮ NGUYÊN)
    # =========================
    crawl_lazada = BashOperator(
        task_id="crawl_lazada",
        bash_command=PREAMBLE + r"""
SCRIPT="/app/crawlers/lazada/runners/lazada_with_cookies.py"
RUN_DATE="{{ ds }}"
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
RUN_DATE="{{ ds }}"
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
RUN_DATE="{{ ds }}"
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
RUN_DATE="{{ ds }}"
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

    # Sensors chờ dữ liệu raw + reviews
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

    # Upload to MinIO (Staging - Raw Data)
    upload_minio = PythonOperator(
        task_id="upload_to_minio", python_callable=upload_to_minio
    )

    # =========================
    #   DATA STANDARDIZATION TOOL (MỚI)
    # =========================
    # Job Spark 1: clean + standardize + category mapping + sync id + dedup + technical metadata + cleaned parquet → MinIO
    data_cleaning = BashOperator(
        task_id="data_cleaning",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  /app/src/spark_jobs/clean_standardize_syncid.py
""",
    )

    # Job Spark 2: aggregation → DWH (Postgres) + technical metadata
    build_dwh = BashOperator(
        task_id="build_data_warehouse",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/aggregate_to_dwh.py
""",
        env={"TARGET_DATE": "{{ ds }}"},  # cho script biết ngày chạy để filter aggregation nếu cần
    )

    # Analytical Infrastructure – Data Mart (giữ nguyên, dùng fact trong DWH)
    build_datamart = BashOperator(
        task_id="build_datamart",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/datamart_build.py
""",
    )

    # Intelligence DSS System - ML Models (giữ nguyên)
    ml_price_optimization = BashOperator(
        task_id="ml_price_optimization",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  /app/src/ml_models/price_optimization.py
""",
    )

    ml_inventory_recommendation = BashOperator(
        task_id="ml_inventory_recommendation",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  /app/src/ml_models/demand_forecasting.py
""",
    )

    ml_customer_segment = BashOperator(
        task_id="ml_customer_segment",
        bash_command="""
spark-submit --master spark://spark-master:7077 \
  /app/src/ml_models/customer_segmentation.py
""",
    )

    end = EmptyOperator(task_id="end")

    # =========================
    #       PIPELINE FLOW
    # =========================

    # Crawl song song (GIỮ NGUYÊN)
    start >> [crawl_lazada, crawl_tiki]
    crawl_lazada >> crawl_lazada_reviews
    crawl_tiki >> crawl_tiki_reviews

    [crawl_lazada, crawl_tiki] >> wait_raw_ready
    [crawl_lazada_reviews, crawl_tiki_reviews] >> wait_reviews_ready

    # Chỉ upload lên MinIO khi cả raw + (ít nhất một) reviews đã sẵn sàng
    [wait_raw_ready, wait_reviews_ready] >> upload_minio

    # Sau đó mới tới chuẩn hoá dữ liệu, DWH, Datamart, ML
    upload_minio >> data_cleaning >> build_dwh >> build_datamart
    build_datamart >> [
        ml_price_optimization,
        ml_inventory_recommendation,
        ml_customer_segment,
    ] >> end
