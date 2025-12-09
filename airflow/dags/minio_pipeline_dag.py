# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

# Import metadata collection helpers
from helpers.collect_metrics_after_etl import collect_table_stats_after_etl, collect_db_health

# ============================================================
#  CẤU HÌNH CƠ BẢN
# ============================================================

CRAWLER_OUTPUT_DIR = os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
CHECKPOINT_DIR = os.getenv("CRAWLER_CHECKPOINT_DIR", "/tmp/crawler_checkpoints")

default_args = {
    "owner": "data_eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# ============================================================
#      PHẦN A – ETL META LOGGING (schema metadata.*)
# ============================================================

PIPELINE_JOB_CODE = "MINIO_ECOMMERCE_DWH_PIPELINE"
PIPELINE_JOB_NAME = "Ecommerce DSS - DWH Pipeline (Crawl → MinIO → Spark)"


def _get_pg_conn():
    """
    Kết nối Postgres từ DATABASE_URL.
    Nếu không có hoặc lỗi -> trả về None và bỏ qua logging (không làm fail DAG).
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("[META] DATABASE_URL not set, skip ETL meta logging.")
        return None

    try:
        import psycopg2  # type: ignore
    except ImportError:
        print("[META] psycopg2 not installed, skip ETL meta logging.")
        return None

    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"[META] Failed to connect to Postgres: {e}")
        return None


def _ensure_etl_job(conn):
    """
    Đảm bảo metadata.etl_job có dòng cho PIPELINE_JOB_CODE.
    Trả về job_id hoặc None.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO metadata.etl_job (job_code, job_name, description)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_code) DO NOTHING;
        """,
            (
                PIPELINE_JOB_CODE,
                PIPELINE_JOB_NAME,
                "Full DWH pipeline: crawl → MinIO → Spark build star schema (products + reviews)",
            ),
        )
        conn.commit()

        cur.execute(
            "SELECT job_id FROM metadata.etl_job WHERE job_code = %s;",
            (PIPELINE_JOB_CODE,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            print("[META] Cannot find or create etl_job for pipeline.")
            return None
        return row[0]
    except Exception as e:
        print(f"[META] Error ensuring etl_job: {e}")
        return None


def start_etl_run(job_code, run_date, airflow_run_id=None):
    """
    Tạo 1 dòng metadata.etl_run với status=RUNNING.
    Trả về run_id hoặc None.
    """
    conn = _get_pg_conn()
    if conn is None:
        return None

    try:
        job_id = _ensure_etl_job(conn)
        if job_id is None:
            conn.close()
            return None

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO metadata.etl_run (
                job_id, run_date, started_at, status, airflow_run_id
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING run_id;
        """,
            (job_id, run_date, datetime.utcnow(), "RUNNING", airflow_run_id),
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        print(f"[META] Created etl_run id={run_id} for job_code={job_code}")
        return run_id
    except Exception as e:
        print(f"[META] Error creating etl_run: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return None


def finish_etl_run(run_id, status, rows_read=None, rows_written=None, error_message=None):
    """
    Update metadata.etl_run khi DAG kết thúc.
    """
    if run_id is None:
        print("[META] finish_etl_run called with run_id=None, skip.")
        return

    conn = _get_pg_conn()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE metadata.etl_run
            SET finished_at = %s,
                status = %s,
                rows_read = COALESCE(%s, rows_read),
                rows_written = COALESCE(%s, rows_written),
                error_message = COALESCE(%s, error_message)
            WHERE run_id = %s;
        """,
            (datetime.utcnow(), status, rows_read, rows_written, error_message, run_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[META] Finished etl_run id={run_id} with status={status}")
    except Exception as e:
        print(f"[META] Error updating etl_run: {e}")
        try:
            conn.close()
        except Exception:
            pass


def etl_run_start(**context):
    """
    Task PythonOperator: bắt đầu pipeline run.
    Trả về run_id để lưu vào XCom.
    """
    run_date = context["ds"]  # 'YYYY-MM-DD'
    airflow_run_id = context.get("run_id")
    run_id = start_etl_run(
        job_code=PIPELINE_JOB_CODE, run_date=run_date, airflow_run_id=airflow_run_id
    )
    return run_id


def etl_run_finish(**context):
    """
    Task PythonOperator: kết thúc pipeline run.
    Đọc run_id từ XCom, check trạng thái DagRun để set SUCCESS/FAILED.
    """
    ti = context["ti"]
    dag_run = context.get("dag_run")

    run_id = ti.xcom_pull(task_ids="etl_run_start")
    if not run_id:
        print("[META] No run_id from XCom, skip finish_etl_run.")
        return

    dag_state = None
    try:
        if dag_run is not None:
            dag_state = getattr(dag_run, "state", None)
    except Exception:
        dag_state = None

    status = "SUCCESS"
    if dag_state and str(dag_state).lower() == "failed":
        status = "FAILED"

    finish_etl_run(run_id=run_id, status=status)


# ============================================================
#      PHẦN B – HÀM PHỤ CRAWLER & SENSOR
# ============================================================

def upload_to_minio(**context):
    """
    Upload tất cả file jsonl theo date=ds lên MinIO bucket crawler-data.
    """
    from pathlib import Path

    try:
        from minio import Minio
    except ImportError:
        import subprocess, sys

        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "minio", "--quiet"],
                check=True,
            )
            from minio import Minio  # type: ignore
            print("Installed 'minio' package at runtime.")
        except Exception as e:
            print(f"Failed to import/install minio: {e}. Skipping upload_to_minio.")
            return

    client = Minio(
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False
    )
    bucket = "crawler-data"

    if not client.bucket_exists(bucket_name=bucket):
        client.make_bucket(bucket_name=bucket)

    output_dir = Path(CRAWLER_OUTPUT_DIR)
    date = context["ds"]
    uploaded = 0

    for jsonl_file in output_dir.rglob(f"**/date={date}/*.jsonl"):
        relative = jsonl_file.relative_to(output_dir)
        object_name = str(relative).replace("\\", "/")
        client.fput_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=str(jsonl_file)
        )
        uploaded += 1

    print(f"Uploaded {uploaded} files to MinIO")


def _raw_ready(**context):
    """
    Kiểm tra đã có dữ liệu raw (products) cho cả tiki & lazada hay chưa.
    """
    from pathlib import Path

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
    """
    Kiểm tra đã có dữ liệu reviews cho tiki hoặc lazada chưa.
    (chỉ cần 1 trong 2 để pipeline không bị chặn).
    """
    from pathlib import Path

    run_date = context["ds"]
    lazada_reviews = (
        Path(CRAWLER_OUTPUT_DIR) / "lazada_reviews" / f"date={run_date}"
    )
    tiki_reviews = Path(CRAWLER_OUTPUT_DIR) / "tiki_reviews" / f"date={run_date}"

    has_lazada = lazada_reviews.exists() and any(
        p.suffix == ".jsonl" for p in lazada_reviews.rglob("*.jsonl")
    )
    has_tiki = tiki_reviews.exists() and any(
        p.suffix == ".jsonl" for p in tiki_reviews.rglob("*.jsonl")
    )

    return has_lazada or has_tiki


# ============================================================
#                         DAG
# ============================================================

with DAG(
    dag_id="minio_ecommerce_dwh_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 0 * * *",  # chạy mỗi ngày 2h sáng
    catchup=False,
    default_args=default_args,
    tags=["minio", "dwh", "spark", "dss", "etl"],
) as dag:

    # --------------------------------------------------------
    # PHẦN 0 – START & GHI LOG ETL
    # --------------------------------------------------------
    start = EmptyOperator(task_id="start")

    etl_run_start_task = PythonOperator(
        task_id="etl_run_start",
        python_callable=etl_run_start,
    )

    # --------------------------------------------------------
    # PHẦN 1 – CRAWLERS & RAW → MINIO
    #   1.1 Cấu hình chung
    #   1.2 Crawl Tiki/Lazada products + reviews
    #   1.3 Sensor chờ dữ liệu đầy đủ
    #   1.4 Upload JSONL lên MinIO (bucket crawler-data)
    # --------------------------------------------------------

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

    # 1.2 Crawl products
    crawl_lazada = BashOperator(
        task_id="crawl_lazada",
        bash_command=PREAMBLE
        + r"""
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
""",
    )

    crawl_tiki = BashOperator(
        task_id="crawl_tiki",
        bash_command=PREAMBLE
        + r"""
SCRIPT="/app/crawlers/tiki/tiki_crawler.py"
RUN_DATE="{{ ds }}"
OUT_DIR="${CRAWLER_OUTPUT_DIR}/tiki/date=${RUN_DATE}"
if [ -d "$OUT_DIR" ] && find "$OUT_DIR" -type f -name '*.jsonl' -print -quit | grep -q .; then
  echo "Tiki raw already exists for ${RUN_DATE}, skipping crawl."
  exit 0
fi
[ -f "$SCRIPT" ] || { echo "❌ Không tìm thấy $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
""",
    )

    # 1.2 Crawl reviews
    crawl_lazada_reviews = BashOperator(
        task_id="crawl_lazada_reviews",
        bash_command=PREAMBLE
        + r"""
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
""",
    )

    crawl_tiki_reviews = BashOperator(
        task_id="crawl_tiki_reviews",
        bash_command=PREAMBLE
        + r"""
SCRIPT="/app/crawlers/tiki/tiki_review_crawler.py"
RUN_DATE="{{ ds }}"
OUT_DIR="${CRAWLER_OUTPUT_DIR}/tiki_reviews/date=${RUN_DATE}"
if [ -d "$OUT_DIR" ] && find "$OUT_DIR" -type f -name '*.jsonl' -print -quit | grep -q .; then
  echo "Tiki reviews already exist for ${RUN_DATE}, skipping crawl."
  exit 0
fi
[ -f "$SCRIPT" ] || { echo "❌ Không tìm thấy $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
""",
    )

    # 1.3 Sensors
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

    # 1.4 Upload raw JSONL lên MinIO
    upload_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
    )

    # --------------------------------------------------------
    # PHẦN 2 – SPARK BUILD STAR DWH (products + reviews)
    #
    #  Dựa vào file load_cleaned_from_minio.py đã có:
    #   - STEP 1  : load_raw_data
    #   - STEP 2  : clean_data
    #   - STEP 2.5: map_categories
    #   - STEP 2.8: standardize_data
    #   - STEP 2.9: synchronize_identifiers
    #   - STEP 3  : deduplicate_data
    #   - STEP 4  : validate_data
    #   - STEP 5.x: ensure_star_schema + load_dimensions + fact_product_daily
    #   - STEP 8.x: review pipeline + fact_review + fact_review_daily
    # --------------------------------------------------------

    spark_build_star_dwh = BashOperator(
        task_id="spark_build_star_dwh",
        bash_command="""
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --executor-cores 1 \
  --executor-memory 1g \
  --driver-memory 2g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=200 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=2 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.driver.maxResultSize=512m \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.executor.memoryOverhead=768m \
  --conf spark.driver.memoryOverhead=768m \
  --conf spark.sql.autoBroadcastJoinThreshold=10485760 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.files.maxPartitionBytes=67108864 \
  --conf spark.shuffle.compress=true \
  --conf spark.shuffle.spill.compress=true \
  --conf spark.rdd.compress=true \
  --conf spark.io.compression.codec=snappy \
  --conf spark.shuffle.file.buffer=64k \
  --conf spark.reducer.maxSizeInFlight=48m \
  --conf spark.executor.extraJavaOptions='-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=2' \
  --conf spark.driver.extraJavaOptions='-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35' \
  --conf spark.executorEnv.DB_HOST=postgres \
  --conf spark.executorEnv.DB_PORT=5432 \
  --conf spark.executorEnv.DB_NAME=ecommerce_dss \
  --conf spark.executorEnv.DB_USER=dss_user \
  --conf spark.executorEnv.DB_PASSWORD=dss_password_123 \
  --conf spark.yarn.appMasterEnv.DB_HOST=postgres \
  --conf spark.yarn.appMasterEnv.DB_PORT=5432 \
  --conf spark.yarn.appMasterEnv.DB_NAME=ecommerce_dss \
  --conf spark.yarn.appMasterEnv.DB_USER=dss_user \
  --conf spark.yarn.appMasterEnv.DB_PASSWORD=dss_password_123 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /app/src/spark_jobs/load_cleaned_from_minio.py
""",
        execution_timeout=timedelta(hours=2),  # Tăng timeout cho Spark job
        pool="spark_jobs",  # Sử dụng pool riêng để kiểm soát concurrency
    )

    # ========================================================================
    # NEW: MEMORY-OPTIMIZED SPLIT SPARK PIPELINE (Products + Reviews separate)
    # ========================================================================
    
    spark_build_products_v2 = BashOperator(
        task_id="spark_build_products_v2",
        bash_command="""
docker exec spark-master spark-submit \\
  --master spark://spark-master:7077 \\
  --deploy-mode client \\
  --executor-cores 1 \\
  --executor-memory 768m \\
  --driver-memory 1536m \\
  --conf spark.sql.session.timeZone=UTC \\
  --conf spark.sql.shuffle.partitions=200 \\
  --conf spark.default.parallelism=200 \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.minExecutors=1 \\
  --conf spark.dynamicAllocation.maxExecutors=2 \\
  --conf spark.dynamicAllocation.initialExecutors=1 \\
  --conf spark.driver.maxResultSize=512m \\
  --conf spark.memory.fraction=0.6 \\
  --conf spark.memory.storageFraction=0.3 \\
  --conf spark.executor.memoryOverhead=512m \\
  --conf spark.driver.memoryOverhead=512m \\
  --conf spark.sql.autoBroadcastJoinThreshold=10485760 \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
  --conf spark.sql.files.maxPartitionBytes=67108864 \\
  --conf spark.shuffle.compress=true \\
  --conf spark.shuffle.spill.compress=true \\
  --conf spark.rdd.compress=true \\
  --conf spark.io.compression.codec=snappy \\
  --conf spark.shuffle.file.buffer=64k \\
  --conf spark.reducer.maxSizeInFlight=48m \\
  --conf spark.executor.extraJavaOptions='-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=2' \\
  --conf spark.driver.extraJavaOptions='-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35' \\
  --conf spark.executorEnv.DB_HOST=postgres \\
  --conf spark.executorEnv.DB_PORT=5432 \\
  --conf spark.executorEnv.DB_NAME=ecommerce_dss \\
  --conf spark.executorEnv.DB_USER=dss_user \\
  --conf spark.executorEnv.DB_PASSWORD=dss_password_123 \\
  --jars /opt/spark/jars/postgresql-42.7.1.jar \\
  /app/src/spark_jobs/product_pipeline.py
""",
        execution_timeout=timedelta(hours=1),
        pool="spark_jobs",
    )

    spark_build_reviews_v2 = BashOperator(
        task_id="spark_build_reviews_v2",
        bash_command="""
docker exec spark-master spark-submit \\
  --master spark://spark-master:7077 \\
  --deploy-mode client \\
  --executor-cores 1 \\
  --executor-memory 768m \\
  --driver-memory 1g \\
  --conf spark.sql.session.timeZone=UTC \\
  --conf spark.sql.shuffle.partitions=200 \\
  --conf spark.default.parallelism=200 \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.minExecutors=1 \\
  --conf spark.dynamicAllocation.maxExecutors=2 \\
  --conf spark.dynamicAllocation.initialExecutors=1 \\
  --conf spark.driver.maxResultSize=512m \\
  --conf spark.memory.fraction=0.6 \\
  --conf spark.memory.storageFraction=0.3 \\
  --conf spark.executor.memoryOverhead=512m \\
  --conf spark.driver.memoryOverhead=512m \\
  --conf spark.sql.autoBroadcastJoinThreshold=10485760 \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
  --conf spark.sql.files.maxPartitionBytes=67108864 \\
  --conf spark.shuffle.compress=true \\
  --conf spark.shuffle.spill.compress=true \\
  --conf spark.rdd.compress=true \\
  --conf spark.io.compression.codec=snappy \\
  --conf spark.shuffle.file.buffer=64k \\
  --conf spark.reducer.maxSizeInFlight=48m \\
  --conf spark.executor.extraJavaOptions='-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=2' \\
  --conf spark.driver.extraJavaOptions='-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35' \\
  --conf spark.executorEnv.DB_HOST=postgres \\
  --conf spark.executorEnv.DB_PORT=5432 \\
  --conf spark.executorEnv.DB_NAME=ecommerce_dss \\
  --conf spark.executorEnv.DB_USER=dss_user \\
  --conf spark.executorEnv.DB_PASSWORD=dss_password_123 \\
  --jars /opt/spark/jars/postgresql-42.7.1.jar \\
  /app/src/spark_jobs/review_pipeline.py
""",
        execution_timeout=timedelta(hours=1),
        pool="spark_jobs",
    )

    # --------------------------------------------------------
    # PHẦN 3 – METADATA COLLECTION
    # --------------------------------------------------------
    
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

    # --------------------------------------------------------
    # PHẦN 4 – KẾT THÚC & GHI LOG
    # --------------------------------------------------------

    etl_run_finish_task = PythonOperator(
        task_id="etl_run_finish",
        python_callable=etl_run_finish,
        trigger_rule="all_done",  # chạy dù upstream success hay fail
    )

    end = EmptyOperator(task_id="end")

    # =========================
    #       PIPELINE FLOW
    # =========================

    # START → ghi log START
    start >> etl_run_start_task

    # Crawl song song
    etl_run_start_task >> [crawl_lazada, crawl_tiki]

    # Crawl reviews phụ thuộc từng platform
    crawl_lazada >> crawl_lazada_reviews
    crawl_tiki >> crawl_tiki_reviews

    # Sensor chờ raw + reviews
    [crawl_lazada, crawl_tiki] >> wait_raw_ready
    [crawl_lazada_reviews, crawl_tiki_reviews] >> wait_reviews_ready

    # Khi đủ data → upload MinIO (raw zone)
    [wait_raw_ready, wait_reviews_ready] >> upload_minio

    # # Sau đó Spark job build full star DWH (products + reviews)
    # upload_minio >> spark_build_star_dwh

    # # Thu thập metadata statistics sau khi DWH hoàn thành
    # spark_build_star_dwh >> collect_metadata

    # ========================================================================
    # NEW: Split pipeline workflow (Products → Reviews → Metadata)
    # To use: Disable spark_build_star_dwh line above and enable this
    # ========================================================================
    upload_minio >> spark_build_products_v2 >> spark_build_reviews_v2 >> collect_metadata

    # Khi metadata collection xong → ghi log FINISH → end
    collect_metadata >> etl_run_finish_task >> end
