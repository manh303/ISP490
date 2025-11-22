# inspect_dwh_schema.py
import psycopg2
from psycopg2.extras import DictCursor

DB_CONFIG = {
    "host": "dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com",
    "port": 5432,
    "database": "ecommerce_dss",
    "user": "dss_user",
    "password": "IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4",
    # Nếu connect lỗi vì SSL, thử mở dòng dưới:
    # "sslmode": "require",
}


def get_dwh_columns(conn):
    """
    Lấy danh sách các cột trong schema dwh từ information_schema.
    """
    query = """
        SELECT
            table_name,
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'dwh'
        ORDER BY table_name, ordinal_position;
    """
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return rows


def print_schema_summary(rows):
    """
    In ra cấu trúc schema dwh: từng bảng + các cột bên trong.
    """
    if not rows:
        print("⚠️ Không tìm thấy cột nào trong schema 'dwh'. Có thể schema trống hoặc tên schema sai.")
        return

    current_table = None
    for r in rows:
        table_name = r["table_name"]
        if table_name != current_table:
            # Đổi sang bảng mới
            current_table = table_name
            print("\n" + "=" * 80)
            print(f"TABLE: dwh.{current_table}")
            print("-" * 80)
            print(f"{'Column':30} {'Type':20} {'Null?':6} {'Len':6} {'Prec':6} {'Scale':6}")
            print("-" * 80)

        col = r["column_name"]
        dtype = r["data_type"]
        is_null = r["is_nullable"]
        char_len = r["character_maximum_length"]
        num_prec = r["numeric_precision"]
        num_scale = r["numeric_scale"]

        print(f"{col:30} {dtype:20} {is_null:6} {str(char_len or ''):6} {str(num_prec or ''):6} {str(num_scale or ''):6}")


def main():
    print("🔌 Đang kết nối tới database...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        rows = get_dwh_columns(conn)
        print_schema_summary(rows)
    finally:
        conn.close()
        print("\n✅ Đã đóng kết nối database.")


if __name__ == "__main__":
    main()
