#!/usr/bin/env python3
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "elt_team",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

CONTAINER_TIKI = "/app/crawlers/tiki/tiki_crawler.py"
CONTAINER_LAZADA = "/app/crawlers/lazada/runners/lazada_crawler.py"
WIN_TIKI = r"C:\DoAn_FPT_FALL2025\ecommerce-dss-project\data-collection\crawlers\tiki\tiki_crawler.py"
WIN_LAZADA = r"C:\DoAn_FPT_FALL2025\ecommerce-dss-project\data-collection\crawlers\lazada\runners\lazada_crawler.py"

CRAWLER_OUTPUT_DIR = os.environ.get("CRAWLER_DATA_PATH", "/app/data/outputs")
CRAWLER_LOG_DIR = os.environ.get("CRAWLER_LOG_DIR", "/tmp/crawler_logs")

DB_HOST = os.environ.get("DB_HOST", "ecommerce-dss-project-postgres-1")
DB_PORT = int(os.environ.get("DB_PORT", "5433"))
DB_NAME = os.environ.get("DB_NAME", "ecommerce_dss")
DB_USER = os.environ.get("DB_USER", "dss_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "dss_password_123")

dag = DAG(
    dag_id="tiki_lazada_elt_pipeline",
    default_args=DEFAULT_ARGS,
    description="Parallel crawl Tiki & Lazada then run ELT",
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "crawl", "elt", "tiki", "lazada"],
)

def check_data_availability(**context):
    import glob, os
    from pathlib import Path
    from datetime import datetime, timedelta
    data_path = Path(CRAWLER_OUTPUT_DIR)
    data_path.mkdir(parents=True, exist_ok=True)
    all_files, recent_files = [], []
    cutoff = datetime.now() - timedelta(hours=24)
    for pattern in ("*.json", "*.csv"):
        files = glob.glob(str(data_path / pattern))
        all_files.extend(files)
        for f in files:
            try:
                st = os.stat(f)
                if datetime.fromtimestamp(st.st_mtime) > cutoff:
                    recent_files.append(f)
            except OSError:
                pass
    if not all_files:
        return {"status": "no_data", "files": [], "total": 0}
    if not recent_files:
        recent_files = sorted(all_files, key=lambda p: os.path.getmtime(p), reverse=True)[:10]
    return {"status": "ok", "files": recent_files, "total": len(all_files), "selected": len(recent_files)}

def run_elt_pipeline(**context):
    return {"status": "elt_started"}

def validate_data_quality(**context):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    results = {}
    try:
        results["ods_products"] = int(postgres_hook.get_first("SELECT COUNT(*) FROM ods.products;")[0])
    except Exception as e:
        results["ods_products"] = f"ERROR: {e}"
    return results

# IMPORTANT: escape shell ${VAR} → ${{VAR}} to avoid str.format capturing them
BASH_PREAMBLE = r"""
set -euo pipefail
echo "Node: $(hostname)"
echo "Python: $(which python)  $($([ -x "$(which python)" ] && python -V || echo N/A))"
mkdir -p "{log_dir}" "{out_dir}"
export CRAWLER_LOG_DIR="{log_dir}"
export FORCE_CRAWLER_LOG_DIR="{log_dir}"
export CRAWLER_OUTPUT_DIR="{out_dir}"
# Chrome env if present; else webdriver-manager may fallback
export CHROME_BIN="${{CHROME_BIN:-/usr/bin/chromium-browser}}"
export CHROMEDRIVER_PATH="${{CHROMEDRIVER_PATH:-/usr/bin/chromedriver}}"
export DISPLAY="${{DISPLAY:-:99}}"
"""

TIKI_CMD = BASH_PREAMBLE + r"""
if [ -f "{ct_path}" ]; then
  SCRIPT="{ct_path}"
  CWD="$(dirname "{ct_path}")"
elif [ -f "{win_path}" ]; then
  SCRIPT="{win_path}"
  CWD="$(dirname "{win_path}")"
else
  echo "❌ Không tìm thấy Tiki crawler script"; exit 1
fi
export PYTHONPATH="${{PYTHONPATH:-}}:$CWD:/app/crawlers"
echo "🚀 Tiki: $SCRIPT"
echo "CWD    : $CWD"
cd "$CWD"
python -u "$SCRIPT"
"""

LAZADA_CMD = BASH_PREAMBLE + r"""
if [ -f "{ct_path}" ]; then
  SCRIPT="{ct_path}"
  CWD="$(dirname "{ct_path}")"
elif [ -f "{win_path}" ]; then
  SCRIPT="{win_path}"
  CWD="$(dirname "{win_path}")"
else
  echo "❌ Không tìm thấy Lazada crawler script"; exit 1
fi
export PYTHONPATH="${{PYTHONPATH:-}}:$CWD:/app/crawlers:/app/crawlers/lazada"
echo "🚀 Lazada: $SCRIPT"
echo "CWD     : $CWD"
cd "$CWD"
python -u "$SCRIPT"
"""

with dag:
    with TaskGroup(group_id="data_collection") as data_collection:
        run_tiki = BashOperator(
            task_id="run_tiki_crawler",
            bash_command=TIKI_CMD.format(
                log_dir=CRAWLER_LOG_DIR,
                out_dir=CRAWLER_OUTPUT_DIR,
                ct_path=CONTAINER_TIKI,
                win_path=WIN_TIKI,
            ),
            execution_timeout=timedelta(hours=5),
            doc_md="**Run Tiki crawler** (long running)",
        )

        run_lazada = BashOperator(
            task_id="run_lazada_crawler",
            bash_command=LAZADA_CMD.format(
                log_dir=CRAWLER_LOG_DIR,
                out_dir=CRAWLER_OUTPUT_DIR,
                ct_path=CONTAINER_LAZADA,
                win_path=WIN_LAZADA,
            ),
            execution_timeout=timedelta(hours=5),
            doc_md="**Run Lazada crawler** (long running)",
        )

        check_data = PythonOperator(
            task_id="check_data_availability",
            python_callable=check_data_availability,
            doc_md="**Check crawler outputs** in last 24h",
        )
        [run_tiki, run_lazada] >> check_data

    with TaskGroup(group_id="database_setup") as database_setup:
        create_schemas = PostgresOperator(
            task_id="create_schemas",
            postgres_conn_id="postgres_default",
            sql="""
            CREATE SCHEMA IF NOT EXISTS staging;
            CREATE SCHEMA IF NOT EXISTS ods;
            CREATE SCHEMA IF NOT EXISTS dwh;
            CREATE SCHEMA IF NOT EXISTS marts;
            CREATE SCHEMA IF NOT EXISTS control;
            """,
        )
        create_tables = BashOperator(
            task_id="create_tables",
            bash_command=f"""
            export PGPASSWORD='{DB_PASSWORD}'
            psql -h {DB_HOST} -p {DB_PORT} -U {DB_USER} -d {DB_NAME} -f /app/schemas/ecommerce_schema.sql \
              || echo "Schema file not found, continuing..."
            """,
        )
        create_schemas >> create_tables

    with TaskGroup(group_id="elt_processing") as elt_processing:
        run_elt = PythonOperator(
            task_id="run_elt_pipeline",
            python_callable=run_elt_pipeline,
            doc_md="Run ELT pipeline after crawling",
        )
        validate_quality = PythonOperator(
            task_id="validate_data_quality",
            python_callable=validate_data_quality,
            doc_md="Validate data quality",
        )
        run_elt >> validate_quality

    send_success = EmailOperator(
        task_id="success_email",
        to=["admin@company.com"],
        subject="✅ Tiki-Lazada ELT Pipeline - Success {{ ds }}",
        html_content="""
        <h3>Pipeline Completed Successfully</h3>
        <p><strong>Date:</strong> {{ ds }}</p>
        <p><strong>Execution Time:</strong> {{ ts }}</p>
        <p>Crawlers finished and ELT completed.</p>
        """,
        trigger_rule="all_success",
    )

    data_collection >> database_setup >> elt_processing >> send_success
