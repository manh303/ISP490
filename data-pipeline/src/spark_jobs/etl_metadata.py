# spark_jobs/etl_metadata.py
import os
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_dss")
DB_USER = os.getenv("DB_USER", "dss_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


def _get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def log_etl(
    job_name: str,
    stage: str,
    status: str,
    start_time: datetime,
    records_processed: int = 0,
    records_failed: int = 0,
    error_message: str | None = None,
    load_id: str | None = None,
):
    """
    Ghi 1 dòng metadata vào meta_etl_log
    - job_name: tên job spark / airflow task_id
    - stage: CLEAN_STD_SYNC / AGG_DWH / ...
    - status: SUCCESS / FAILED
    - start_time: thời điểm job bắt đầu
    - end_time: tự set NOW() khi insert
    - load_id: có thể là cleaned_prefix, target_date,...
    """
    try:
        conn = _get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO meta_etl_log
                (job_name, stage, status,
                 start_time, end_time,
                 records_processed, records_failed,
                 error_message, load_id)
            VALUES (%s, %s, %s,
                    %s, NOW(),
                    %s, %s,
                    %s, %s)
            """,
            (
                job_name,
                stage,
                status,
                start_time,
                records_processed,
                records_failed,
                error_message,
                load_id,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[META] Logged ETL metadata for job={job_name}, stage={stage}, status={status}")
    except Exception as e:
        # Không để lỗi log làm fail cả job
        print(f"[META] Failed to log ETL metadata: {e}")
