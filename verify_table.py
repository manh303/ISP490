# inspect_iam_schema.py
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import sql

DB_CONFIG = {
    "host": "dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com",
    "port": 5432,
    "database": "ecommerce_dss_1",
    "user": "dss_user",
    "password": "6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G",
    # Nếu connect lỗi vì SSL, thử mở dòng dưới:
    # "sslmode": "require",
}


def get_iam_columns(conn):
    """
    Lấy danh sách các cột trong schema iam từ information_schema.
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
        WHERE table_schema = 'iam'
        ORDER BY table_name, ordinal_position;
    """
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return rows


def print_schema_summary(rows):
    """
    In ra cấu trúc schema iam: từng bảng + các cột bên trong.
    """
    if not rows:
        print("⚠️ Không tìm thấy cột nào trong schema 'iam'. Có thể schema trống hoặc tên schema sai.")
        return

    current_table = None
    for r in rows:
        table_name = r["table_name"]
        if table_name != current_table:
            # Đổi sang bảng mới
            current_table = table_name
            print("\n" + "=" * 80)
            print(f"TABLE: iam.{current_table}")
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


def get_table_names(rows):
    """Lấy danh sách tên bảng duy nhất từ rows information_schema."""
    return sorted({r["table_name"] for r in rows})


def print_sample_data(conn, table_names, limit=5):
    """
    In ra một vài dòng dữ liệu mẫu cho từng bảng trong schema iam.
    """
    if not table_names:
        return

    print("\n" + "#" * 80)
    print(f"# DỮ LIỆU MẪU (tối đa {limit} dòng mỗi bảng) TRONG SCHEMA iam")
    print("#" * 80)

    with conn.cursor(cursor_factory=DictCursor) as cur:
        for table_name in table_names:
            print("\n" + "-" * 80)
            print(f"Sample data: iam.{table_name} (tối đa {limit} dòng)")
            print("-" * 80)

            query = sql.SQL("SELECT * FROM iam.{table} LIMIT {limit};").format(
                table=sql.Identifier(table_name),
                limit=sql.Literal(limit)
            )

            cur.execute(query)
            rows = cur.fetchall()

            if not rows:
                print("  (Bảng không có dữ liệu)")
                continue

            # In header
            cols = rows[0].keys()
            print(" | ".join(cols))
            print("-" * 80)

            for row in rows:
                values = [str(row[c]) for c in cols]
                print(" | ".join(values))


def main():
    print("🔌 Đang kết nối tới database...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        rows = get_iam_columns(conn)
        # In cấu trúc bảng + cột
        print_schema_summary(rows)

        # Lấy danh sách bảng rồi in dữ liệu mẫu
        table_names = get_table_names(rows)
        print_sample_data(conn, table_names, limit=5)
    finally:
        conn.close()
        print("\n✅ Đã đóng kết nối database.")


if __name__ == "__main__":
    main()
