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

    crawl_tiki = BashOperator(
        task_id="crawl_tiki",
        bash_command=PREAMBLE + r"""
SCRIPT="/app/crawlers/tiki/tiki_crawler.py"
[ -f "$SCRIPT" ] || { echo "❌ Không tìm thấy $SCRIPT"; exit 1; }
cd "$(dirname "$SCRIPT")"
python -u "$SCRIPT"
"""
    )

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

    wait_raw_ready = PythonSensor(
        task_id="wait_raw_ready",
        python_callable=_raw_ready,
        poke_interval=30,
        timeout=1800,
        mode="reschedule",
    )

    spark_etl = BashOperator(
        task_id="spark_etl",
        bash_command=rf"""
{SPARK_SUBMIT} --master {SPARK_MASTER} \
  --packages org.postgresql:postgresql:42.7.3 \
  {SPARK_JOB_PATH} \
  --date "{{{{ ds }}}}" \
  --input "{CRAWLER_OUTPUT_DIR}" \
  --bronze "/app/data/bronze" \
  --silver "/app/data/silver" \
  --pg-url "jdbc:postgresql://postgres:5432/ecommerce_dss" \
  --pg-user "dss_user" \
  --pg-pass "dss_password_123"
"""
    )

    dwh_ddl = PostgresOperator(
        task_id="dwh_ddl",
        postgres_conn_id="postgres_default",
        sql=r"""
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS ods.stg_products (
    snapshot_date date,
    source text,
    product_key text,
    title text,
    brand text,
    model text,
    canonical_category text,
    price bigint,
    currency text,
    rating double precision,
    review_count bigint,
    seller text,
    url text,
    image_url text,
    collected_at timestamptz
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key text PRIMARY KEY,
    brand text,
    model text,
    canonical_category text,
    first_seen date,
    last_seen date,
    is_active boolean default true
);

CREATE TABLE IF NOT EXISTS dwh.fct_product_snapshot (
    snapshot_date date,
    product_key text,
    source text,
    price bigint,
    rating double precision,
    review_count bigint,
    seller text,
    PRIMARY KEY (snapshot_date, product_key, source)
);

CREATE TABLE IF NOT EXISTS mart.mart_price_daily (
    snapshot_date date,
    product_key text,
    min_price bigint,
    max_price bigint,
    avg_price numeric,
    price_volatility numeric,
    PRIMARY KEY (snapshot_date, product_key)
);

CREATE TABLE IF NOT EXISTS mart.mart_popularity_daily (
    snapshot_date date,
    product_key text,
    rating double precision,
    review_count bigint,
    rank_popularity bigint,
    PRIMARY KEY (snapshot_date, product_key)
);
""",
    )

    dwh_merge = PostgresOperator(
        task_id="dwh_merge",
        postgres_conn_id="postgres_default",
        sql=r"""
INSERT INTO dwh.dim_product(product_key, brand, model, canonical_category, first_seen, last_seen, is_active)
SELECT
    s.product_key,
    NULLIF(TRIM(s.brand),'') AS brand,
    NULLIF(TRIM(s.model),'') AS model,
    NULLIF(TRIM(s.canonical_category),'') AS canonical_category,
    MIN(s.snapshot_date) AS first_seen,
    MAX(s.snapshot_date) AS last_seen,
    true
FROM ods.stg_products s
GROUP BY s.product_key, NULLIF(TRIM(s.brand),''), NULLIF(TRIM(s.model),''), NULLIF(TRIM(s.canonical_category),'')
ON CONFLICT (product_key) DO UPDATE
SET
    brand = COALESCE(EXCLUDED.brand, dwh.dim_product.brand),
    model = COALESCE(EXCLUDED.model, dwh.dim_product.model),
    canonical_category = COALESCE(EXCLUDED.canonical_category, dwh.dim_product.canonical_category),
    last_seen = GREATEST(dwh.dim_product.last_seen, EXCLUDED.last_seen),
    is_active = true;

INSERT INTO dwh.fct_product_snapshot(snapshot_date, product_key, source, price, rating, review_count, seller)
SELECT DISTINCT s.snapshot_date, s.product_key, s.source, s.price, s.rating, s.review_count, s.seller
FROM ods.stg_products s
WHERE s.snapshot_date = '{{ ds }}';
""",
    )

    build_mart_price = PostgresOperator(
        task_id="build_mart_price",
        postgres_conn_id="postgres_default",
        sql=r"""
DELETE FROM mart.mart_price_daily WHERE snapshot_date = '{{ ds }}';
INSERT INTO mart.mart_price_daily(snapshot_date, product_key, min_price, max_price, avg_price, price_volatility)
SELECT '{{ ds }}'::date, product_key,
       MIN(price) AS min_price,
       MAX(price) AS max_price,
       AVG(price)::numeric(18,2) AS avg_price,
       (STDDEV_POP(price)/NULLIF(AVG(price),0))::numeric(18,4) AS price_volatility
FROM dwh.fct_product_snapshot
WHERE snapshot_date = '{{ ds }}'
GROUP BY product_key;
""",
    )

    build_mart_popularity = PostgresOperator(
        task_id="build_mart_popularity",
        postgres_conn_id="postgres_default",
        sql=r"""
DELETE FROM mart.mart_popularity_daily WHERE snapshot_date = '{{ ds }}';
WITH base AS (
  SELECT product_key, MAX(rating) AS rating, MAX(review_count) AS review_count
  FROM dwh.fct_product_snapshot
  WHERE snapshot_date = '{{ ds }}'
  GROUP BY product_key
)
INSERT INTO mart.mart_popularity_daily(snapshot_date, product_key, rating, review_count, rank_popularity)
SELECT '{{ ds }}'::date, product_key, rating, review_count,
       DENSE_RANK() OVER (ORDER BY review_count DESC NULLS LAST, rating DESC NULLS LAST)
FROM base;
""",
    )

    end = EmptyOperator(task_id="end")

    start >> [crawl_lazada, crawl_tiki] >> wait_raw_ready
    wait_raw_ready >> spark_etl >> dwh_ddl >> dwh_merge >> [build_mart_price, build_mart_popularity] >> end
