"""
Script kiểm tra schema & sample data cho các bảng dùng trong DSS:
- Price DSS
- Recommendation DSS
- Sentiment DSS

Cách chạy:
    # Cách 1: dùng env
    export DATABASE_URL="postgresql://user:pass@host:port/dbname"
    python check_dss_tables.py

    # Cách 2: sửa DB_DSN ngay trong file (nếu không dùng env)
"""

import os
import asyncio
from typing import List, Tuple

import asyncpg

# ==========================
# 1. Cấu hình kết nối DB
# ==========================

# Ưu tiên lấy từ env DATABASE_URL cho giống backend
DB_DSN = os.getenv("DATABASE_URL", "postgresql://dss_user:dss_password_123@localhost/ecommerce_dss").strip()

# Nếu không dùng env thì anh có thể gán cứng DSN ở đây:
# DB_DSN = "postgresql://dss_user:password@host:5432/ecommerce_dss"

if not DB_DSN:
    raise RuntimeError(
        "Chưa cấu hình DATABASE_URL.\n"
        "Hãy export biến môi trường DATABASE_URL hoặc sửa DB_DSN trong file."
    )

# ==========================
# 2. Danh sách bảng cần kiểm tra
# ==========================

# Các bảng DSS dùng cho price / reco / sentiment
# (danh sách này anh có thể bổ sung nếu sau này DSS dùng thêm bảng khác)
TABLES: List[Tuple[str, str]] = [
    # DWH dimensions & facts
    ("dwh", "dim_date"),
    ("dwh", "dim_platform"),
    ("dwh", "dim_category"),
    ("dwh", "dim_product"),
    ("dwh", "fact_product_daily"),
    ("dwh", "product_metrics_global"),
    ("dwh", "fact_review"),

    # ML tables
    ("ml", "fact_price_prediction"),
    ("ml", "fact_product_recommendation"),
    ("ml", "fact_review_sentiment"),

    # DSS session (để link decision sau này)
    ("dss", "dss_analysis_session"),
    # Nếu anh muốn kiểm tra luôn decision:
    ("dss", "dss_decision"),
    ("dss", "dss_decision_action"),
]


# ==========================
# 3. Hàm helper
# ==========================

async def print_table_schema(conn: asyncpg.Connection, schema: str, table: str) -> None:
    """In ra danh sách cột + kiểu dữ liệu của 1 bảng."""
    print("=" * 80)
    print(f"TABLE: {schema}.{table}")
    print("-" * 80)

    columns = await conn.fetch(
        """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = $2
        ORDER BY ordinal_position
        """,
        schema,
        table,
    )

    if not columns:
        print(f"⚠️  Không tìm thấy bảng {schema}.{table} trong database.")
        return

    print("COLUMNS:")
    for col in columns:
        col_name = col["column_name"]
        data_type = col["data_type"]
        is_nullable = col["is_nullable"]
        default = col["column_default"]
        print(f"  - {col_name:<32} {data_type:<20} NULLABLE={is_nullable} DEFAULT={default}")
    print()


async def print_table_sample_rows(conn: asyncpg.Connection, schema: str, table: str, limit: int = 5) -> None:
    """In ra 5–10 dòng sample data từ bảng."""
    print(f"SAMPLE DATA ({schema}.{table}, tối đa {limit} dòng):")

    # Dùng f-string vì schema/table cố định trong danh sách TABLES (không từ input người dùng)
    query = f'SELECT * FROM "{schema}"."{table}" LIMIT {limit};'

    try:
        rows = await conn.fetch(query)
    except Exception as e:
        print(f"⚠️  Lỗi khi SELECT từ {schema}.{table}: {e}")
        print()
        return

    if not rows:
        print("  (Bảng không có dữ liệu hoặc không trả dòng nào)")
        print()
        return

    # In từng dòng ở dạng dict cho dễ đọc
    for idx, row in enumerate(rows, start=1):
        as_dict = dict(row)
        print(f"  Row {idx}:")
        for k, v in as_dict.items():
            print(f"    {k}: {v}")
        print()

    print()


# ==========================
# 4. Main
# ==========================

async def main() -> None:
    print("🔌 Connecting to Postgres...")
    conn = await asyncpg.connect(DB_DSN)
    try:
        db_ver = await conn.fetchval("SELECT version();")
        print("✅ Connected.")
        print(db_ver)
        print()

        for schema, table in TABLES:
            await print_table_schema(conn, schema, table)
            await print_table_sample_rows(conn, schema, table, limit=5)

    finally:
        await conn.close()
        print("🔌 Connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
