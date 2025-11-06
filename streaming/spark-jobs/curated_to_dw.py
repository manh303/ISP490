#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
curated_to_dw.py
Nạp curated (Parquet) → Postgres DWH theo ddl_dw.sql (dim_platform, dim_brand, dim_product, fact_*)

Yêu cầu:
- Postgres JDBC jar (ví dụ: /opt/bitnami/spark/jars/postgresql-42.7.4.jar)
- pip install psycopg2-binary
"""

import argparse, os, datetime, random, string
from pyspark.sql import SparkSession, functions as F, types as T
import psycopg2

def init_spark():
    spark = (
        SparkSession.builder.appName("CuratedToDW")
        .config("spark.sql.session.timeZone","UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def rand_suffix(n=6):
    import random, string
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

def write_tmp(df, url, user, password, table):
    props = {"user": user, "password": password, "driver":"org.postgresql.Driver"}
    (df.write
       .mode("overwrite")
       .option("truncate","true")
       .jdbc(url=url, table=table, properties=props))

def run_sql(url, user, password, sql, params=None):
    conn = psycopg2.connect(url, user=user, password=password)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
    conn.close()

def main(curated_path, jdbc_url, db_user, db_pass, ingest_dt):
    spark = init_spark()
    date_obj = datetime.date.fromisoformat(ingest_dt)
    date_sk = int(date_obj.strftime("%Y%m%d"))

    cur_products = spark.read.parquet(os.path.join(curated_path,"products")).where(F.col("ingest_dt")==ingest_dt)
    cur_price    = spark.read.parquet(os.path.join(curated_path,"price_snapshots")).where(F.col("ingest_dt")==ingest_dt)
    cur_ratings  = spark.read.parquet(os.path.join(curated_path,"ratings")).where(F.col("ingest_dt")==ingest_dt)

    # 1) TMP tables names
    sfx = rand_suffix()
    tmp_plat   = f"tmp_dim_platform_{sfx}"
    tmp_brand  = f"tmp_dim_brand_{sfx}"
    tmp_prod   = f"tmp_dim_product_{sfx}"
    tmp_fps    = f"tmp_fact_price_snapshot_{sfx}"
    tmp_frday  = f"tmp_fact_rating_daily_{sfx}"

    # 2) Build small DataFrames for tmp tables
    df_plat = (cur_products.select(F.col("platform").alias("platform_code"))
               .distinct()
               .withColumn("platform_name", F.initcap("platform_code")))
    df_brand = (cur_products.select(F.col("brand_std").alias("brand_name"))
                .where(F.col("brand_name").isNotNull() & (F.col("brand_name")!=""))
                .distinct()
                .withColumn("brand_code", F.col("brand_name")))
    # product core
    df_prod = cur_products.select("global_product_id","name","brand_std").distinct() \
                .withColumnRenamed("name","product_name")

    # facts
    df_fps = (cur_price
              .select("global_product_id","platform","price_current","price_original","discount_pct","snapshot_ts")
              .withColumn("date_sk", F.lit(date_sk)))
    df_frday = (cur_ratings
                .groupBy("global_product_id","platform")
                .agg(F.avg("rating_avg").alias("rating_avg"),
                     F.sum("review_count").alias("rating_count"))
                .withColumn("date_sk", F.lit(date_sk))
                .withColumn("rating_5p", F.lit(None).cast(T.IntegerType()))
                .withColumn("rating_4p", F.lit(None).cast(T.IntegerType()))
                .withColumn("rating_3p", F.lit(None).cast(T.IntegerType()))
                .withColumn("rating_2p", F.lit(None).cast(T.IntegerType()))
                .withColumn("rating_1p", F.lit(None).cast(T.IntegerType())))

    # 3) Write TMP tables via JDBC
    write_tmp(df_plat, jdbc_url, db_user, db_pass, tmp_plat)
    write_tmp(df_brand, jdbc_url, db_user, db_pass, tmp_brand)
    write_tmp(df_prod, jdbc_url, db_user, db_pass, tmp_prod)
    write_tmp(df_fps,  jdbc_url, db_user, db_pass, tmp_fps)
    write_tmp(df_frday,jdbc_url, db_user, db_pass, tmp_frday)

    # 4) Merge into DWH by SQL (Postgres)
    # 4.1 dim_platform
    run_sql(jdbc_url, db_user, db_pass, f"""
        INSERT INTO dwh.dim_platform(platform_code, platform_name)
        SELECT DISTINCT platform_code, platform_name FROM {tmp_plat}
        ON CONFLICT (platform_code) DO NOTHING;
    """)

    # 4.2 dim_brand
    run_sql(jdbc_url, db_user, db_pass, f"""
        INSERT INTO dwh.dim_brand(brand_code, brand_name)
        SELECT DISTINCT brand_code, brand_name FROM {tmp_brand}
        ON CONFLICT (brand_code) DO NOTHING;
    """)

    # 4.3 dim_product (MVP: insert **chỉ** sản phẩm mới, SCD2 nâng cấp sau)
    run_sql(jdbc_url, db_user, db_pass, f"""
        INSERT INTO dwh.dim_product(global_product_id, product_name, brand_sk, category_sk,
                                    seller_name, seller_type, effective_from, effective_to, is_current)
        SELECT p.global_product_id, p.product_name, b.brand_sk, NULL, NULL, NULL,
               DATE %(ingest)s, DATE '9999-12-31', TRUE
        FROM {tmp_prod} p
        LEFT JOIN dwh.dim_brand b ON b.brand_name = p.brand_std
        LEFT JOIN dwh.dim_product d ON d.global_product_id = p.global_product_id AND d.is_current = TRUE
        WHERE d.global_product_id IS NULL;
    """, {"ingest": ingest_dt})

    # 4.4 fact_price_snapshot (xóa partition ngày rồi nạp lại)
    run_sql(jdbc_url, db_user, db_pass, """
        DELETE FROM dwh.fact_price_snapshot WHERE date_sk = %(date_sk)s;
    """, {"date_sk": date_sk})

    run_sql(jdbc_url, db_user, db_pass, f"""
        INSERT INTO dwh.fact_price_snapshot(date_sk, product_sk, platform_sk,
                                            price_current, price_original, discount_pct, is_available, captured_at)
        SELECT t.date_sk,
               dp.product_sk,
               pl.platform_sk,
               t.price_current, t.price_original, t.discount_pct,
               TRUE, t.snapshot_ts
        FROM {tmp_fps} t
        JOIN dwh.dim_platform pl ON pl.platform_code = t.platform
        JOIN dwh.dim_product  dp ON dp.global_product_id = t.global_product_id AND dp.is_current = TRUE;
    """)

    # 4.5 fact_rating_daily (xóa partition ngày rồi nạp lại)
    run_sql(jdbc_url, db_user, db_pass, """
        DELETE FROM dwh.fact_rating_daily WHERE date_sk = %(date_sk)s;
    """, {"date_sk": date_sk})

    run_sql(jdbc_url, db_user, db_pass, f"""
        INSERT INTO dwh.fact_rating_daily(date_sk, product_sk, platform_sk,
                                          rating_avg, rating_count, rating_5p, rating_4p, rating_3p, rating_2p, rating_1p)
        SELECT t.date_sk,
               dp.product_sk,
               pl.platform_sk,
               t.rating_avg, t.rating_count, t.rating_5p, t.rating_4p, t.rating_3p, t.rating_2p, t.rating_1p
        FROM {tmp_frday} t
        JOIN dwh.dim_platform pl ON pl.platform_code = t.platform
        JOIN dwh.dim_product  dp ON dp.global_product_id = t.global_product_id AND dp.is_current = TRUE;
    """)

    print(f"[DONE] Loaded to DWH for date_sk={date_sk}")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--curated-path", required=True)
    parser.add_argument("--jdbc-url", required=True, help="e.g. postgresql://host:5432/dbname")
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--ingest-dt", default=datetime.date.today().isoformat())
    args = parser.parse_args()
    main(args.curated_path, args.jdbc_url, args.db_user, args.db_pass, args.ingest_dt)
