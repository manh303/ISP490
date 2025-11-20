# quick_check_models_storage.py
import psycopg2
from psycopg2.extras import DictCursor

DB_CONFIG = {
    "host": "dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com",
    "port": 5432,
    "database": "ecommerce_dss",
    "user": "dss_user",
    "password": "IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4",
}

def main():
    query = """
        SELECT model_id, model_name, model_type, version, status, is_production, created_at
        FROM ml.models_storage
        ORDER BY created_at DESC
        LIMIT 20;
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()

            if not rows:
                print("⚠️ Bảng ml.models_storage hiện đang TRỐNG (không có model nào).")
                return

            cols = rows[0].keys()
            print(" | ".join(cols))
            print("-" * 80)
            for row in rows:
                print(" | ".join(str(row[c]) for c in cols))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
