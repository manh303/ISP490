# src/spark_jobs/etl/dwh_loader.py

"""
Load dữ liệu đã transform/aggregate vào Data Warehouse (schema dwh).
"""

import re
from datetime import datetime
from typing import Dict, Any, Tuple

import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from unidecode import unidecode

# Import with fallback for both relative and absolute imports
try:
    from spark_jobs.etl.config import (
        DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
        DWH_SCHEMA, STAR_SCHEMA_SQL_TEMPLATE, ML_SCHEMA
    )
except ImportError:
    from .config import (
        DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
        DWH_SCHEMA, STAR_SCHEMA_SQL_TEMPLATE, ML_SCHEMA
    )


def get_db_connection():
    """Get a PostgreSQL connection."""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def ensure_star_schema(conn):
    """Tạo đầy đủ schema/bảng DWH + ML nếu chưa có."""
    ddl = STAR_SCHEMA_SQL_TEMPLATE.format(dwh=DWH_SCHEMA, ml=ML_SCHEMA)
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print("[INFO] DWH star schema ensured.")


def make_slug(name: str) -> str:
    """ 
    Tạo slug đơn giản từ product_name: bỏ dấu, thường hóa,
    thay chuỗi không phải [a-z0-9] bằng dấu gạch ngang.
    """
    if not name:
        return None
    s = unidecode(str(name))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or None


def truncate_str(value, max_len):
    """
    Cắt chuỗi về tối đa max_len ký tự để tránh lỗi varchar(n).
    Nếu None thì trả về None.
    """
    if value is None:
        return None
    s = str(value)
    return s[:max_len]


def load_dimensions(df_dedup: DataFrame, conn) -> Dict[str, Dict]:
    """
    WHAT:
        Load dim_date, dim_platform, dim_category, dim_brand, dim_product.

    WHY:
        Tạo metadata tables (dimension) làm trục phân tích cho fact.

    HOW:
        - Lấy distinct từ df_dedup
        - Insert/Upsert vào dwh.dim_*
        - Đọc lại dim để build mapping: date_map, platform_map, category_map, brand_map, product_map
    """
    cur = conn.cursor()

    # ========== DIM_DATE ==========
    print("[INFO] Loading dim_date...")
    date_pdf = (
        df_dedup.select("snapshot_date")
        .where(F.col("snapshot_date").isNotNull())
        .distinct()
        .toPandas()
    )
    insert_date_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_date (
            date_value,
            year,
            month,
            day,
            quarter,
            week_of_year,
            day_of_week,
            day_name,
            is_weekend
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date_value) DO NOTHING
    """

    date_rows = []
    for _, r in date_pdf.iterrows():
        d = r["snapshot_date"]
        if isinstance(d, str):
            d = datetime.strptime(d, "%Y-%m-%d").date()
        year_ = d.year
        month_ = d.month
        day_ = d.day
        quarter_ = (month_ - 1) // 3 + 1
        week_of_year = d.isocalendar()[1]
        day_of_week = d.isoweekday()
        day_name = d.strftime("%a")
        is_weekend = day_of_week >= 6
        date_rows.append(
            (d, year_, month_, day_, quarter_, week_of_year, day_of_week, day_name, is_weekend)
        )

    if date_rows:
        execute_batch(cur, insert_date_sql, date_rows, page_size=500)
        conn.commit()
        print(f"  ✅ Loaded/ensured {len(date_rows)} dates")

    cur.execute(f"SELECT date_sk, date_value FROM {DWH_SCHEMA}.dim_date")
    date_map = {row[1]: row[0] for row in cur.fetchall()}

    # ========== DIM_PLATFORM ==========
    print("[INFO] Loading dim_platform...")
    plat_pdf = (
        df_dedup.select("source_platform_std")
        .where(F.col("source_platform_std").isNotNull())
        .distinct()
        .toPandas()
    )

    insert_platform_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_platform (platform_code, platform_name, country_code)
        VALUES (%s, %s, %s)
        ON CONFLICT (platform_code) DO NOTHING
    """

    plat_rows = []
    for _, r in plat_pdf.iterrows():
        code = str(r["source_platform_std"]).strip()
        plat_rows.append((code, code.upper(), "VN"))

    if plat_rows:
        execute_batch(cur, insert_platform_sql, plat_rows, page_size=100)
        conn.commit()
        print(f"  ✅ Loaded/ensured {len(plat_rows)} platforms")

    cur.execute(f"SELECT platform_sk, platform_code FROM {DWH_SCHEMA}.dim_platform")
    platform_map = {row[1]: row[0] for row in cur.fetchall()}

    # ========== DIM_CATEGORY ==========
    print("[INFO] Loading dim_category...")
    cat_pdf = (
        df_dedup.select("category_std", "category_lvl1", "category_lvl2", "category_lvl3")
        .where(F.col("category_std").isNotNull())
        .distinct()
        .toPandas()
    )

    insert_category_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_category (
            category_std_key, category_lvl1, category_lvl2, category_lvl3, full_path
        )
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (category_std_key) DO NOTHING
    """

    cat_rows = []
    for _, r in cat_pdf.iterrows():
        key = str(r["category_std"]).strip()
        l1 = r.get("category_lvl1")
        l2 = r.get("category_lvl2")
        l3 = r.get("category_lvl3")
        parts = [str(x) for x in [l1, l2, l3] if x]
        full_path = " > ".join(parts) if parts else None
        cat_rows.append((key, l1, l2, l3, full_path))

    if cat_rows:
        execute_batch(cur, insert_category_sql, cat_rows, page_size=500)
        conn.commit()
        print(f"  ✅ Loaded/ensured {len(cat_rows)} categories")

    cur.execute(f"SELECT category_sk, category_std_key FROM {DWH_SCHEMA}.dim_category")
    category_map = {row[1]: row[0] for row in cur.fetchall()}

    # ========== DIM_BRAND ==========
    print("[INFO] Loading dim_brand...")
    brand_pdf = (
        df_dedup.select("brand_std")
        .where(F.col("brand_std").isNotNull())
        .distinct()
        .toPandas()
    )

    insert_brand_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_brand (brand_name, brand_normalized)
        VALUES (%s, %s)
        ON CONFLICT (brand_name) DO NOTHING
    """

    brand_rows = []
    for _, r in brand_pdf.iterrows():
        name = str(r["brand_std"]).strip()
        norm = name.upper()
        brand_rows.append((name, norm))

    if brand_rows:
        execute_batch(cur, insert_brand_sql, brand_rows, page_size=500)
        conn.commit()
        print(f"  ✅ Loaded/ensured {len(brand_rows)} brands")

    cur.execute(f"SELECT brand_sk, brand_name FROM {DWH_SCHEMA}.dim_brand")
    brand_map = {row[1]: row[0] for row in cur.fetchall()}
    

    # ========== DIM_PRODUCT ==========
    print("[INFO] Loading dim_product...")
    
    prod_df = (
        df_dedup.select(
            "global_product_id_synced",
            "product_master_id",
            "product_name_std",
            "brand_std",
            "category_std",
        )
        .where(F.col("global_product_id_synced").isNotNull())
        .distinct()
    )
    
    total_products = prod_df.count()
    print(f"[INFO] Processing {total_products} products in batches...")

    insert_product_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_product (
            product_key,
            product_master_id,
            product_name,
            product_slug,
            brand_sk,
            category_sk
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (product_key) DO UPDATE SET
            product_master_id = COALESCE(EXCLUDED.product_master_id, {DWH_SCHEMA}.dim_product.product_master_id),
            product_name      = COALESCE(EXCLUDED.product_name,      {DWH_SCHEMA}.dim_product.product_name),
            product_slug      = COALESCE(EXCLUDED.product_slug,      {DWH_SCHEMA}.dim_product.product_slug),
            brand_sk          = COALESCE(EXCLUDED.brand_sk,          {DWH_SCHEMA}.dim_product.brand_sk),
            category_sk       = COALESCE(EXCLUDED.category_sk,       {DWH_SCHEMA}.dim_product.category_sk)
    """

    prod_rows_all = prod_df.collect()
    
    print(f"[INFO] Collected {len(prod_rows_all)} product rows, now inserting in batches...")
    
    BATCH_SIZE = 5000
    total_loaded = 0
    
    for i in range(0, len(prod_rows_all), BATCH_SIZE):
        batch = prod_rows_all[i:i+BATCH_SIZE]
        
        prod_rows = []
        for r in batch:
            product_key = str(r["global_product_id_synced"])[:100]

            master_id_raw = r["product_master_id"] if "product_master_id" in r else None
            product_name_raw = r["product_name_std"] if "product_name_std" in r else None
            brand_name = r["brand_std"] if "brand_std" in r else None
            cat_key = r["category_std"] if "category_std" in r else None

            brand_sk = brand_map.get(brand_name)
            category_sk = category_map.get(cat_key)

            product_slug_raw = make_slug(product_name_raw)

            master_id = truncate_str(master_id_raw, 256)
            product_name = truncate_str(product_name_raw, 500)
            product_slug = truncate_str(product_slug_raw, 500)

            prod_rows.append(
                (
                    product_key,
                    master_id,
                    product_name,
                    product_slug,
                    brand_sk,
                    category_sk,
                )
            )

        if prod_rows:
            execute_batch(cur, insert_product_sql, prod_rows, page_size=1000)
            conn.commit()
            total_loaded += len(prod_rows)
            print(f"  [PROGRESS] Loaded {total_loaded}/{len(prod_rows_all)} products ({total_loaded/len(prod_rows_all)*100:.1f}%)")

    print(f"  ✅ Loaded/ensured {total_loaded} products total")

    cur.execute(f"SELECT product_sk, product_key FROM {DWH_SCHEMA}.dim_product")
    product_map = {row[1]: row[0] for row in cur.fetchall()}

    cur.close()

    return {
        "date_map": date_map,
        "platform_map": platform_map,
        "category_map": category_map,
        "brand_map": brand_map,
        "product_map": product_map,
    }


def load_fact_product_daily(df_dedup: DataFrame, conn, mappings: Dict[str, Dict]) -> None:
    """
    WHAT:
        Load dữ liệu aggregate daily vào dwh.fact_product_daily.
        
    OPTIMIZED FOR MEMORY - NO toPandas():
        - Sử dụng foreachPartition để insert trực tiếp từ Spark
        - Không collect() toàn bộ data về driver
        - Mỗi partition tự insert vào DB độc lập

    WHY:
        Đây là fact chính để Analyst / DSS query.

    HOW:
        - Pre-aggregate trong Spark
        - foreachPartition: mỗi partition mở connection riêng và insert
    """
    print("[INFO] Loading fact_product_daily (foreachPartition - NO toPandas)...")

    from pyspark.sql.types import DoubleType

    # Broadcast mappings to all executors
    date_map = mappings["date_map"]
    platform_map = mappings["platform_map"]
    product_map = mappings["product_map"]

    df = df_dedup
    if "price" not in df.columns:
        df = df.withColumn("price", F.col("price_current_vnd"))
    if "review_count" not in df.columns:
        df = df.withColumn("review_count", F.lit(0).cast("long"))
    if "rating" not in df.columns:
        df = df.withColumn("rating", F.lit(None).cast(DoubleType()))

    # Aggregate ALL data in Spark (distributed)
    agg_df = (
        df.where(
            F.col("snapshot_date").isNotNull()
            & F.col("global_product_id_synced").isNotNull()
            & F.col("source_platform_std").isNotNull()
        )
        .groupBy("snapshot_date", "global_product_id_synced", "source_platform_std")
        .agg(
            F.count("*").alias("snapshot_count"),
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.expr("percentile_approx(price, 0.5)").alias("median_price"),
            F.stddev("price").alias("price_stddev"),
            F.sum(F.col("review_count")).cast("long").alias("total_review_count"),
            F.avg("rating").alias("avg_rating"),
        )
    )
    
    # Repartition to balance load across executors
    agg_df = agg_df.repartition(20)
    
    total_count = agg_df.count()
    print(f"  📦 Aggregated {total_count:,} rows, now inserting via foreachPartition...")

    # DB config for workers (they don't have access to main connection)
    db_host = DB_HOST
    db_port = DB_PORT
    db_name = DB_NAME
    db_user = DB_USER
    db_password = DB_PASSWORD
    dwh_schema = DWH_SCHEMA
    
    # Broadcast lookup maps to all workers
    spark = df_dedup.sparkSession
    bc_date_map = spark.sparkContext.broadcast(date_map)
    bc_platform_map = spark.sparkContext.broadcast(platform_map)
    bc_product_map = spark.sparkContext.broadcast(product_map)

    def write_partition_to_db(rows):
        """
        Insert a partition of rows to PostgreSQL.
        Each executor opens its own DB connection.
        """
        import psycopg2
        from psycopg2.extras import execute_batch
        from datetime import datetime as dt
        
        date_map_local = bc_date_map.value
        platform_map_local = bc_platform_map.value
        product_map_local = bc_product_map.value
        
        BIGINT_MAX = 9223372036854775807
        
        def safe_num(v):
            if v is None:
                return None
            try:
                if v != v:  # NaN check
                    return None
                if abs(v) > BIGINT_MAX:
                    return None
                return v
            except:
                return None
        
        try:
            conn_local = psycopg2.connect(
                host=db_host, port=db_port, database=db_name,
                user=db_user, password=db_password
            )
            cur = conn_local.cursor()
            
            insert_sql = f"""
                INSERT INTO {dwh_schema}.fact_product_daily (
                    date_sk, product_sk, platform_sk,
                    currency_code,
                    min_price, max_price, avg_price, median_price, price_stddev,
                    total_review_count, avg_rating, snapshot_count
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (date_sk, product_sk, platform_sk)
                DO UPDATE SET
                    min_price         = EXCLUDED.min_price,
                    max_price         = EXCLUDED.max_price,
                    avg_price         = EXCLUDED.avg_price,
                    median_price      = EXCLUDED.median_price,
                    price_stddev      = EXCLUDED.price_stddev,
                    total_review_count= EXCLUDED.total_review_count,
                    avg_rating        = EXCLUDED.avg_rating,
                    snapshot_count    = EXCLUDED.snapshot_count
            """
            
            buffer = []
            BATCH_SIZE = 5000
            
            for r in rows:
                # Extract values from Row
                snapshot_date = r["snapshot_date"]
                product_key = str(r["global_product_id_synced"])[:100]
                platform_code = str(r["source_platform_std"]).strip()
                
                # Convert date string to date object if needed
                if isinstance(snapshot_date, str):
                    try:
                        snapshot_date = dt.strptime(snapshot_date, "%Y-%m-%d").date()
                    except:
                        continue
                
                # Lookup surrogate keys
                date_sk = date_map_local.get(snapshot_date)
                product_sk = product_map_local.get(product_key)
                platform_sk = platform_map_local.get(platform_code)
                
                if date_sk is None or product_sk is None or platform_sk is None:
                    continue
                
                total_review_count = int(r["total_review_count"]) if r["total_review_count"] else 0
                snapshot_count = int(r["snapshot_count"]) if r["snapshot_count"] else 0
                
                buffer.append((
                    date_sk,
                    product_sk,
                    platform_sk,
                    "VND",
                    safe_num(r["min_price"]),
                    safe_num(r["max_price"]),
                    safe_num(r["avg_price"]),
                    safe_num(r["median_price"]),
                    safe_num(r["price_stddev"]),
                    total_review_count,
                    safe_num(r["avg_rating"]),
                    snapshot_count,
                ))
                
                if len(buffer) >= BATCH_SIZE:
                    execute_batch(cur, insert_sql, buffer, page_size=1000)
                    conn_local.commit()
                    buffer = []
            
            # Insert remaining rows
            if buffer:
                execute_batch(cur, insert_sql, buffer, page_size=1000)
                conn_local.commit()
            
            cur.close()
            conn_local.close()
            
        except Exception as e:
            print(f"[ERROR] Partition insert failed: {e}")
            import traceback
            traceback.print_exc()

    # Execute: each partition writes to DB independently
    agg_df.foreachPartition(write_partition_to_db)
    
    # Cleanup broadcast variables
    bc_date_map.unpersist()
    bc_platform_map.unpersist()
    bc_product_map.unpersist()

    print(f"  ✅ Loaded fact_product_daily via foreachPartition (no toPandas)")


def _ensure_dates_in_dim(conn, date_map, date_values):
    """
    Bổ sung thêm các ngày mới vào dim_date nếu chưa có.
    Trả về date_map mới (date_value -> date_sk).
    """
    normalized = set()
    for d in date_values:
        if d is None:
            continue
        s = str(d).strip()
        if len(s) >= 10:
            s = s[:10]
        if len(s) == 10 and s.count("-") == 2:
            normalized.add(s)

    if not normalized:
        return date_map

    cur = conn.cursor()
    insert_sql = f"""
        INSERT INTO {DWH_SCHEMA}.dim_date (
            date_value, year, month, day, quarter,
            week_of_year, day_of_week, day_name, is_weekend
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date_value) DO NOTHING
    """

    rows = []
    for s in normalized:
        try:
            d = datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            continue

        if d in date_map:
            continue

        year_ = d.year
        month_ = d.month
        day_ = d.day
        quarter_ = (month_ - 1) // 3 + 1
        week_of_year = d.isocalendar()[1]
        day_of_week = d.isoweekday()
        day_name = d.strftime("%a")
        is_weekend = day_of_week >= 6

        rows.append(
            (d, year_, month_, day_, quarter_, week_of_year, day_of_week, day_name, is_weekend)
        )

    if rows:
        execute_batch(cur, insert_sql, rows, page_size=200)
        conn.commit()
        cur.execute(f"SELECT date_sk, date_value FROM {DWH_SCHEMA}.dim_date")
        date_map = {row[1]: row[0] for row in cur.fetchall()}

    cur.close()
    return date_map
