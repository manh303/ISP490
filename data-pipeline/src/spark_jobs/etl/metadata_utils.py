# src/spark_jobs/etl/metadata_utils.py

"""
Utility functions for review dimension loading and metadata management.
"""

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType
import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import functions as F

# Import with fallback for both relative and absolute imports
try:
    from spark_jobs.etl.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DWH_SCHEMA
    from spark_jobs.etl.dwh_loader import _ensure_dates_in_dim
except ImportError:
    from .config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DWH_SCHEMA
    from .dwh_loader import _ensure_dates_in_dim


def load_review_dimensions_to_dwh(df: DataFrame) -> None:
    """
    WHAT: Load dim_reviewer vào DWH.
    
    WHY: Tạo dimension table cho reviewer.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.4: LOADING REVIEW DIMENSIONS TO DWH")
    print("=" * 60)

    if df is None:
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        cur = conn.cursor()

        reviewer_df = (
            df.select(
                col("reviewer_name_std").alias("reviewer_name"),
                col("source_platform_std"),
            )
            .distinct()
            .limit(100000)
        ).toPandas()

        if not reviewer_df.empty:
            dim_reviewer_table = f"{DWH_SCHEMA}.dim_reviewer"
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {dim_reviewer_table} (
                    reviewer_id SERIAL PRIMARY KEY,
                    reviewer_name VARCHAR(500),
                    source_platform VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            cur.execute(create_table_sql)
            conn.commit()
            
            # Ensure UNIQUE constraint exists
            alter_constraint_sql = f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'dim_reviewer_unique_name_platform'
                    ) THEN
                        ALTER TABLE {dim_reviewer_table}
                        ADD CONSTRAINT dim_reviewer_unique_name_platform
                        UNIQUE (reviewer_name, source_platform);
                    END IF;
                END $$;
            """
            cur.execute(alter_constraint_sql)
            conn.commit()

            insert_sql = f"""
                INSERT INTO {dim_reviewer_table} (reviewer_name, source_platform)
                VALUES (%s, %s)
                ON CONFLICT (reviewer_name, source_platform) DO NOTHING
            """
            rows = [
                (row["reviewer_name"], row["source_platform_std"])
                for _, row in reviewer_df.iterrows()
            ]
            execute_batch(cur, insert_sql, rows, page_size=1000)
            conn.commit()
            print(f" ✓ Loaded {len(rows)} reviewers to {dim_reviewer_table}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f" Error loading review dimensions: {e}")
        import traceback
        traceback.print_exc()


def load_fact_review_star(df_reviews_time: DataFrame, conn, mappings: dict) -> None:
    """
    WHAT: Load dữ liệu review chi tiết vào dwh.fact_review.
    
    WHY: Tạo fact table cho review detail.
    
    OPTIMIZED: Sử dụng foreachPartition thay vì toPandas()
    """
    print("[INFO] Loading dwh.fact_review (foreachPartition - NO toPandas)...")

    date_map = mappings["date_map"]
    platform_map = mappings["platform_map"]
    product_map = mappings["product_map"]

    needed_cols = [
        "review_id_std",
        "global_review_id",
        "global_product_id",
        "source_platform_std",
        "review_date_fmt",
        "rating_std",
        "helpful_count",
        "sentiment_score",
        "review_text_std",
        "reviewer_name_std",
        "verified_purchase",
        "review_date",
    ]
    available = [c for c in needed_cols if c in df_reviews_time.columns]
    if not available:
        print("  ⚠ Không tìm thấy cột review nào phù hợp để load fact_review")
        return

    df_sel = df_reviews_time.select(*available)

    # Đảm bảo dim_date có đủ ngày review - dùng collect() nhỏ cho dates
    date_values = (
        df_sel.select("review_date_fmt")
        .where(F.col("review_date_fmt").isNotNull())
        .distinct()
        .limit(10000)  # Limit to avoid collecting too much
        .rdd.flatMap(lambda x: x).collect()
    )
    mappings["date_map"] = _ensure_dates_in_dim(conn, date_map, date_values)
    date_map = mappings["date_map"]

    # Repartition for balanced load
    df_sel = df_sel.repartition(10)
    
    total_count = df_sel.count()
    print(f"  📦 Processing {total_count:,} reviews via foreachPartition...")

    # DB config and broadcast maps
    db_host, db_port, db_name = DB_HOST, DB_PORT, DB_NAME
    db_user, db_password, dwh_schema = DB_USER, DB_PASSWORD, DWH_SCHEMA
    
    spark = df_reviews_time.sparkSession
    bc_date_map = spark.sparkContext.broadcast(date_map)
    bc_platform_map = spark.sparkContext.broadcast(platform_map)
    bc_product_map = spark.sparkContext.broadcast(product_map)

    def write_reviews_partition(rows):
        import psycopg2
        from psycopg2.extras import execute_batch
        from datetime import datetime as dt
        
        date_map_local = bc_date_map.value
        platform_map_local = bc_platform_map.value
        product_map_local = bc_product_map.value
        
        try:
            conn_local = psycopg2.connect(
                host=db_host, port=db_port, database=db_name,
                user=db_user, password=db_password
            )
            cur = conn_local.cursor()
            
            insert_sql = f"""
                INSERT INTO {dwh_schema}.fact_review (
                    review_id_nk, product_sk, platform_sk, date_sk,
                    rating, helpful_votes, sentiment_score,
                    review_title, review_body, reviewer_name,
                    is_verified_purchase, raw_review_date
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (review_id_nk, platform_sk) DO UPDATE SET
                    rating = EXCLUDED.rating,
                    helpful_votes = EXCLUDED.helpful_votes,
                    sentiment_score = EXCLUDED.sentiment_score,
                    review_body = EXCLUDED.review_body,
                    reviewer_name = EXCLUDED.reviewer_name
            """
            
            buffer = []
            BATCH_SIZE = 2000
            
            for r in rows:
                product_key = r["global_product_id"] if "global_product_id" in r else None
                platform_code = r["source_platform_std"] if "source_platform_std" in r else None
                review_date_fmt = r["review_date_fmt"] if "review_date_fmt" in r else None
                
                if not product_key or not platform_code or not review_date_fmt:
                    continue
                
                try:
                    d = dt.strptime(str(review_date_fmt)[:10], "%Y-%m-%d").date()
                except:
                    continue
                
                date_sk = date_map_local.get(d)
                product_sk = product_map_local.get(str(product_key)[:100])
                platform_sk = platform_map_local.get(str(platform_code).strip())
                
                if date_sk is None or product_sk is None or platform_sk is None:
                    continue
                
                review_id_nk = r.get("review_id_std") or r.get("global_review_id")
                if not review_id_nk:
                    continue
                
                try:
                    rating_val = int(r.get("rating_std")) if r.get("rating_std") else None
                except:
                    rating_val = None
                
                try:
                    helpful_val = int(r.get("helpful_count")) if r.get("helpful_count") else 0
                except:
                    helpful_val = 0
                
                try:
                    sentiment_val = float(r.get("sentiment_score")) if r.get("sentiment_score") else None
                except:
                    sentiment_val = None
                
                buffer.append((
                    str(review_id_nk)[:255],
                    int(product_sk),
                    int(platform_sk),
                    int(date_sk),
                    rating_val,
                    helpful_val,
                    sentiment_val,
                    None,  # review_title
                    r.get("review_text_std"),
                    r.get("reviewer_name_std"),
                    bool(r.get("verified_purchase")) if r.get("verified_purchase") else False,
                    d,
                ))
                
                if len(buffer) >= BATCH_SIZE:
                    execute_batch(cur, insert_sql, buffer, page_size=500)
                    conn_local.commit()
                    buffer = []
            
            if buffer:
                execute_batch(cur, insert_sql, buffer, page_size=500)
                conn_local.commit()
            
            cur.close()
            conn_local.close()
        except Exception as e:
            print(f"[ERROR] Review partition insert failed: {e}")

    df_sel.foreachPartition(write_reviews_partition)
    
    bc_date_map.unpersist()
    bc_platform_map.unpersist()
    bc_product_map.unpersist()

    print(f"  ✅ Loaded fact_review via foreachPartition (no toPandas)")


def load_fact_review_daily_star(df_reviews_agg: DataFrame, conn, mappings: dict) -> None:
    """
    WHAT: Load dữ liệu aggregate review vào dwh.fact_review_daily.
    
    WHY: Tạo aggregate fact table cho daily review analysis.
    
    OPTIMIZED: Sử dụng foreachPartition thay vì toPandas()
    """
    print("[INFO] Loading dwh.fact_review_daily (foreachPartition - NO toPandas)...")

    if df_reviews_agg is None:
        print("  ⚠ Không có aggregate review để load")
        return

    date_map = mappings["date_map"]
    platform_map = mappings["platform_map"]
    product_map = mappings["product_map"]

    # Ensure dates exist - small collect
    date_values = (
        df_reviews_agg.select("agg_date")
        .where(F.col("agg_date").isNotNull())
        .distinct()
        .limit(1000)
        .rdd.flatMap(lambda x: x).collect()
    )
    mappings["date_map"] = _ensure_dates_in_dim(conn, date_map, date_values)
    date_map = mappings["date_map"]

    # Repartition
    df_reviews_agg = df_reviews_agg.repartition(10)
    
    total_count = df_reviews_agg.count()
    print(f"  📦 Processing {total_count:,} aggregated review rows...")

    # DB config and broadcast
    db_host, db_port, db_name = DB_HOST, DB_PORT, DB_NAME
    db_user, db_password, dwh_schema = DB_USER, DB_PASSWORD, DWH_SCHEMA
    
    spark = df_reviews_agg.sparkSession
    bc_date_map = spark.sparkContext.broadcast(date_map)
    bc_platform_map = spark.sparkContext.broadcast(platform_map)
    bc_product_map = spark.sparkContext.broadcast(product_map)

    def write_review_daily_partition(rows):
        import psycopg2
        from psycopg2.extras import execute_batch
        from datetime import datetime as dt
        
        date_map_local = bc_date_map.value
        platform_map_local = bc_platform_map.value
        product_map_local = bc_product_map.value
        
        def safe_int(v):
            try:
                return int(v) if v is not None else 0
            except:
                return 0
        
        def safe_float(v):
            try:
                return float(v) if v is not None else None
            except:
                return None
        
        try:
            conn_local = psycopg2.connect(
                host=db_host, port=db_port, database=db_name,
                user=db_user, password=db_password
            )
            cur = conn_local.cursor()
            
            insert_sql = f"""
                INSERT INTO {dwh_schema}.fact_review_daily (
                    date_sk, product_sk, platform_sk,
                    review_count, avg_rating,
                    rating_1_count, rating_2_count, rating_3_count,
                    rating_4_count, rating_5_count, avg_sentiment
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (date_sk, product_sk, platform_sk) DO UPDATE SET
                    review_count = EXCLUDED.review_count,
                    avg_rating = EXCLUDED.avg_rating,
                    rating_1_count = EXCLUDED.rating_1_count,
                    rating_2_count = EXCLUDED.rating_2_count,
                    rating_3_count = EXCLUDED.rating_3_count,
                    rating_4_count = EXCLUDED.rating_4_count,
                    rating_5_count = EXCLUDED.rating_5_count,
                    avg_sentiment = EXCLUDED.avg_sentiment
            """
            
            buffer = []
            BATCH_SIZE = 2000
            
            for r in rows:
                agg_date = r.get("agg_date")
                product_key = r.get("global_product_id")
                platform_code = r.get("source_platform_std")
                
                if not agg_date or not product_key or not platform_code:
                    continue
                
                try:
                    d = dt.strptime(str(agg_date)[:10], "%Y-%m-%d").date()
                except:
                    continue
                
                date_sk = date_map_local.get(d)
                product_sk = product_map_local.get(str(product_key)[:100])
                platform_sk = platform_map_local.get(str(platform_code).strip())
                
                if date_sk is None or product_sk is None or platform_sk is None:
                    continue
                
                buffer.append((
                    int(date_sk),
                    int(product_sk),
                    int(platform_sk),
                    safe_int(r.get("total_reviews")),
                    safe_float(r.get("avg_rating")),
                    safe_int(r.get("one_star_count")),
                    safe_int(r.get("two_star_count")),
                    safe_int(r.get("three_star_count")),
                    safe_int(r.get("four_star_count")),
                    safe_int(r.get("five_star_count")),
                    safe_float(r.get("avg_sentiment_score")),
                ))
                
                if len(buffer) >= BATCH_SIZE:
                    execute_batch(cur, insert_sql, buffer, page_size=500)
                    conn_local.commit()
                    buffer = []
            
            if buffer:
                execute_batch(cur, insert_sql, buffer, page_size=500)
                conn_local.commit()
            
            cur.close()
            conn_local.close()
        except Exception as e:
            print(f"[ERROR] Review daily partition insert failed: {e}")

    df_reviews_agg.foreachPartition(write_review_daily_partition)
    
    bc_date_map.unpersist()
    bc_platform_map.unpersist()
    bc_product_map.unpersist()

    print(f"  ✅ Loaded fact_review_daily via foreachPartition (no toPandas)")
