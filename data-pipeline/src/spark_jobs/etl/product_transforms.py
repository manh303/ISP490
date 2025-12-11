# src/spark_jobs/etl/product_transforms.py

"""
Các bước TRANSFORM cho dữ liệu PRODUCT:
- Data Cleaning
- Category Mapping
- Data Standardization
- Synchronize Identifiers
- Deduplicate
- Validate

Mỗi hàm tương ứng với 1 step trong sơ đồ "Data Standardization Tool".
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, upper, to_timestamp, to_date, 
    concat, concat_ws, lit, lower, coalesce, sha2, split, element_at,
    udf
)
from pyspark.sql.types import DoubleType, LongType, StringType
from pyspark.sql import functions as F
from unidecode import unidecode

# Import with fallback for both relative and absolute imports
try:
    from spark_jobs.etl.config import CATEGORY_MAPPINGS
except ImportError:
    from .config import CATEGORY_MAPPINGS


# ============================================================
#  STEP 2 – Data Cleaning
# ============================================================
def clean_products(df_raw: DataFrame) -> DataFrame:
    """
    WHAT:
        Làm sạch các field cơ bản từ dữ liệu crawler:
        - đảm bảo có product_id, product_name, brand_name
        - convert giá về dạng số
        - tính data_quality_score

    WHY:
        Dữ liệu raw nhiều null, sai kiểu; nếu không clean
        thì mapping/standardize và ML/DWH sẽ bị sai.

    HOW:
        Áp dụng các rule y hệt trong clean_data() cũ, nhưng thêm comment rõ ràng.
    """
    print("\n" + "=" * 60)
    print(" STEP 2: CLEANING & TRANSFORMING DATA")
    print("=" * 60)
    
    try:
        df = df_raw
        
        # ===== Đảm bảo product_id tồn tại dưới dạng string =====
        missing_id_count = df.filter(col("product_id").isNull() | (trim(col("product_id")) == "")).count()
        if missing_id_count > 0:
            print(f"[WARN] Found {missing_id_count} records with missing product_id. Generating from URL hash...")
            
            df = df.withColumn(
                "product_id",
                when(
                    (col("product_id").isNotNull()) & (trim(col("product_id")) != ""),
                    col("product_id").cast(StringType())
                )
                .when(
                    col("url").isNotNull() & (trim(col("url")) != ""),
                    F.md5(col("url"))
                )
                .otherwise(lit(None).cast(StringType()))
            )
        else:
            df = df.withColumn("product_id", col("product_id").cast(StringType()))

        # ===== Tạo global_product_id & chuẩn hóa platform =====
        df_cleaned = (
            df
            .withColumn(
                "global_product_id",
                concat(col("source"), lit("_"), col("product_id"))
            )
            .withColumn("source_platform", col("source"))
        )

        # ===== Chuẩn hóa product_name =====
        df_cleaned = df_cleaned.withColumn(
            "product_name",
            when(
                (col("product_name").isNotNull()) & (trim(col("product_name")) != ""),
                trim(col("product_name")),
            ).otherwise(lit("Unknown"))
        )

        # ======================================================
        #  brand_name CHỈ XUẤT HIỆN NẾU CÓ
        # ======================================================
        if "brand_name" in df_cleaned.columns and "brand" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(
                "brand_name",
                when(
                    (col("brand").isNotNull()) & (trim(col("brand")) != ""),
                    trim(col("brand"))
                )
                .when(
                    (col("brand_name").isNotNull()) & (trim(col("brand_name")) != ""),
                    trim(col("brand_name"))
                )
                .otherwise(lit("Unknown"))
            )
        elif "brand" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(
                "brand_name",
                when(
                    (col("brand").isNotNull()) & (trim(col("brand")) != ""),
                    trim(col("brand"))
                ).otherwise(lit("Unknown"))
            )
        elif "brand_name" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(
                "brand_name",
                when(
                    (col("brand_name").isNotNull()) & (trim(col("brand_name")) != ""),
                    trim(col("brand_name"))
                ).otherwise(lit("Unknown"))
            )
        else:
            df_cleaned = df_cleaned.withColumn("brand_name", lit("Unknown"))

        # Chuẩn hóa brand_std
        df_cleaned = df_cleaned.withColumn(
            "brand_std",
            upper(trim(col("brand_name")))
        )

        # ===== Chuẩn hóa giá =====
        df_cleaned = df_cleaned.withColumn(
            "price_current",
            when(
                col("price_current").isNotNull(),
                regexp_replace(col("price_current"), "[^0-9]", "").cast(LongType())
            ).otherwise(lit(0))
        )

        df_cleaned = df_cleaned.withColumn(
            "price_original",
            when(
                col("price_original").isNotNull(),
                regexp_replace(col("price_original"), "[^0-9]", "").cast(LongType())
            ).otherwise(lit(0))
        )

        # ===== Chuẩn hóa discount_percent =====
        if "discount_percent" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(
                "discount_percent",
                when(
                    col("discount_percent").isNotNull(),
                    regexp_replace(col("discount_percent"), "[^0-9.]", "").cast(DoubleType())
                ).otherwise(lit(0.0))
            )
        else:
            df_cleaned = df_cleaned.withColumn("discount_percent", lit(0.0))

        # ===== data_quality_score =====
        df_cleaned = df_cleaned.withColumn(
            "data_quality_score",
            when(
                (col("product_name") != "Unknown") & (col("price_current") > 0),
                lit(1.0)
            ).otherwise(lit(0.0))
        )

        # ===== Select các cột thực sự tồn tại =====
        available_cols = df_cleaned.columns
        candidate_cols = [
            "global_product_id",
            "source_platform",
            "product_id",
            "product_name",
            "brand_name",
            "brand_std",
            "category",
            "price_current",
            "price_original",
            "discount_percent",
            "review_count",
            "rating",
            "seller_name",
            "url",
            "crawl_date",
            "data_quality_score",
            "raw_category_path",
        ]
        select_cols = [c for c in candidate_cols if c in available_cols]

        df_cleaned = df_cleaned.select(*select_cols)

        print(f" Cleaned {df_cleaned.count():,} records")
        print(f" Columns used: {select_cols}")

        return df_cleaned

    except Exception as e:
        print(f" Error during cleaning: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================
#  STEP 2.5 – Category Mapping
# ============================================================
def map_product_categories(df: DataFrame) -> DataFrame:
    """
    WHAT:
        Map category thô từ Tiki/Lazada sang category_std chuẩn của DWH.

    WHY:
        Để dim_category và các phân tích theo category dùng chung 1 taxonomy.

    HOW:
        - Chuẩn hóa text category
        - Dùng CATEGORY_MAPPINGS (keyword -> category_path)
        - Ưu tiên match trong product_name, sau đó mới đến category_text
    """
    print("\n" + "=" * 60)
    print("✓ STEP 2.5: CATEGORY MAPPING (using mapping table)")
    print("=" * 60)

    # Get Spark session from DataFrame
    spark = df.sql_ctx.sparkSession

    # ✅ OPTIMIZATION: Broadcast mapping dict to all executors
    print(f"\n[INFO] Total category mappings configured: {len(CATEGORY_MAPPINGS)}")
    mapping_dict = {k.lower(): v for (k, v) in CATEGORY_MAPPINGS}
    print(f"[INFO] Unique category patterns: {len(mapping_dict)}")
    
    # Broadcast for efficient distribution
    mapping_broadcast = spark.sparkContext.broadcast(mapping_dict)
    print("✓ Category mappings broadcasted to executors")
    
    # Count plural variants
    plural_count = sum(1 for k in mapping_dict.keys() if k.endswith('s') and k not in ['access', 'wireless'])
    print(f"[INFO] Including {plural_count} plural variants for Lazada compatibility")

    # ✅ OPTIMIZATION: Reduce sample size to avoid driver OOM
    print("\n[DEBUG] Sample raw categories (first 10):")
    if "category" in df.columns:
        sample_cats = df.select("category", "product_name").distinct().limit(10).collect()
        for i, row in enumerate(sample_cats, 1):
            cat = row["category"] if row["category"] else "NULL"
            name = row["product_name"][:50] if row["product_name"] else "NULL"
            print(f"  {i}. Category: '{cat}' | Product: '{name}'")
    else:
        print("[WARN] 'category' column missing in DataFrame")

    # ✅ UDF to map category
    def _map_category_enhanced(category_text, product_name):
        if not category_text and not product_name:
            return None

        mappings = mapping_broadcast.value
        sorted_mappings = sorted(mappings.items(), key=lambda x: len(x[0]), reverse=True)

        # 1. Ưu tiên product_name
        if product_name:
            p = product_name.lower().strip()
            for key, path in sorted_mappings:
                if key in p:
                    return path

        # 2. Sau đó mới tới category_text
        if category_text:
            t = category_text.lower().strip()
            for key, path in sorted_mappings:
                if key in t:
                    return path

        return None

    map_category_udf = udf(_map_category_enhanced, StringType())

    # Prepare both category and product_name for mapping
    raw_cat_col = col("raw_category_path") if "raw_category_path" in df.columns else lit(None)
    cat_col = col("category") if "category" in df.columns else lit(None)

    df_mapped = df.withColumn(
        "category_text",
        lower(trim(coalesce(raw_cat_col, cat_col, lit(""))))
    ).withColumn(
        "product_name_lower",
        lower(trim(col("product_name")))
    )

    # Define UDF for unidecode
    def safe_unidecode(text):
        return unidecode(text) if text else None
    
    unidecode_udf = udf(safe_unidecode, StringType())

    # Chuẩn hoá: bỏ dấu, sửa typos phổ biến
    df_mapped = df_mapped.withColumn(
        "category_norm",
        regexp_replace(unidecode_udf(col("category_text")), r"[-_]", " ")
    )

    df_mapped = (
        df_mapped
        .withColumn("category_norm",
            regexp_replace(col("category_norm"), "d?ong ho thong minh", "dong ho thong minh")
        )
        .withColumn("category_norm",
            regexp_replace(col("category_norm"), "destops computers", "desktop computers")
        )
        .withColumn("category_norm",
            regexp_replace(col("category_norm"), r"\btvs\b", "tv")
        )
    )

    # ✅ OPTIMIZATION: Repartition for better parallelism
    df_mapped = df_mapped.repartition(200)
    print("✓ DataFrame repartitioned for parallel processing")

    # Map using both category and product_name
    df_mapped = df_mapped.withColumn(
        "category_path",
        map_category_udf(col("category_text"), col("product_name_lower")),
    ).drop("product_name_lower")

    # Tách thành các level
    df_mapped = df_mapped.withColumn(
        "category_array", split(col("category_path"), r"\|")
    )

    df_mapped = (
        df_mapped.withColumn("category_lvl1", col("category_array").getItem(0))
        .withColumn("category_lvl2", col("category_array").getItem(1))
        .withColumn("category_lvl3", col("category_array").getItem(2))
        .withColumn("category_std", element_at(col("category_array"), -1))
    )

    # Ép về OTHER nếu rỗng / null
    df_mapped = (
        df_mapped.withColumn(
            "category_lvl1",
            when(
                (col("category_lvl1").isNull()) | (col("category_lvl1") == ""),
                lit("OTHER"),
            ).otherwise(col("category_lvl1")),
        )
        .withColumn(
            "category_std",
            when(
                (col("category_std").isNull()) | (col("category_std") == ""),
                lit("OTHER"),
            ).otherwise(col("category_std")),
        )
    )

    df_mapped = df_mapped.drop("category_array", "category_text")

    print("\n Category Mapping Summary:")
    dist = df_mapped.groupBy("category_std").count().orderBy(col("count").desc()).collect()
    
    total = sum(row['count'] for row in dist)
    other_count = next((row['count'] for row in dist if row['category_std'] == 'OTHER'), 0)
    other_pct = (other_count / total * 100) if total > 0 else 0
    
    print(f"  Total products: {total:,}")
    print(f"  Categories mapped: {len(dist)}")
    print(f"\n  Top categories:")
    for row in dist[:15]:
        count = row['count']
        pct = (count / total * 100) if total > 0 else 0
        marker = "⚠️ " if row['category_std'] == 'OTHER' else "  "
        print(f"  {marker}{row['category_std']}: {count:,} ({pct:.1f}%)")

    return df_mapped


# ============================================================
#  STEP 2.8 – Data Standardization
# ============================================================
def standardize_products(df: DataFrame) -> DataFrame:
    """
    WHAT:
        Chuẩn hóa platform, brand, product_name, giá, thời gian snapshot.

    WHY:
        Tạo canonical form để:
        - hash ID chuẩn
        - join dim/fact ổn định

    HOW:
        - source_platform_std: tiki/tiki_mass_crawl -> 'tiki', lazada/... -> 'lazada'
        - brand_std: UPPER(TRIM(brand_name))
        - product_name_std: trim + gộp space, null -> 'Unknown'
        - price_current_vnd, price_original_vnd: cast double
        - crawl_ts: to_timestamp(crawl_date), snapshot_date: to_date(crawl_ts)
    """
    print("\n" + "=" * 60)
    print(" STEP 2.8: DATA STANDARDIZATION")
    print("=" * 60)

    df_std = (
        df.withColumn("platform_raw",
            when(col("source_platform").isNotNull(),
                lower(trim(col("source_platform")))
            ).otherwise(lit("unknown"))
        )
        .withColumn(
            "source_platform_std",
            when(col("platform_raw").isin("tiki", "tiki_mass_crawl"), lit("tiki"))
            .when(col("platform_raw").isin("lazada", "lazada_mass_crawl"), lit("lazada"))
            .otherwise(col("platform_raw"))
        )
        .drop("platform_raw")
        .withColumn(
            "brand_std",
            when(col("brand_name").isNotNull(),
                upper(trim(col("brand_name")))).otherwise(lit("UNKNOWN")),
        )
        .withColumn(
            "product_name_std",
            when(
                col("product_name").isNotNull(),
                regexp_replace(trim(col("product_name")), r"\s+", " "),
            ).otherwise(lit("Unknown")),
        )
        .withColumn("price_current_vnd", col("price_current").cast(DoubleType()))
        .withColumn("price_original_vnd", col("price_original").cast(DoubleType()))
    )

    df_std = df_std.withColumn(
        "crawl_ts",
        when(
            col("crawl_date").rlike(r"^\d{4}-\d{2}-\d{2}T"),
            to_timestamp(col("crawl_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        ).otherwise(
            to_timestamp(col("crawl_date"), "yyyy-MM-dd"),
        ),
    )
    
    # ✅ Compute snapshot_date IMMEDIATELY for deduplication
    df_std = df_std.withColumn(
        "snapshot_date",
        to_date(col("crawl_ts"))
    )

    print("\n Standardization Summary:")
    if "source_platform_std" in df_std.columns:
        src_dist = df_std.groupBy("source_platform_std").count().collect()
        print("  By source_platform_std:")
        for row in src_dist:
            print(f"    {row['source_platform_std']}: {row['count']:,}")
    return df_std


# ============================================================
#  STEP 2.9 – Identifier Synchronization
# ============================================================
def sync_product_identifiers(df: DataFrame) -> DataFrame:
    """
    WHAT:
        Sinh & đồng bộ các ID chuẩn:
        - product_id_std
        - global_product_id_synced
        - product_master_id
        - sku_id

    WHY:
        Đảm bảo mỗi sản phẩm & SKU có khóa ổn định để:
        - dedup
        - join dim/fact
        - phân tích cross-platform

    HOW:
        - product_id_std = trim(product_id)
        - global_product_id_synced = source_platform_std + '_' + product_id_std
        - product_master_id = sha2(brand_std || product_name_std || category_std)
        - sku_id = sha2(source_platform_std || seller_name || product_id_std)
    """
    print("\n" + "=" * 60)
    print(" STEP 2.9: IDENTIFIER SYNCHRONIZATION")
    print("=" * 60)

    df_id = df.withColumn(
        "product_id_std",
        when(col("product_id").isNotNull(), trim(col("product_id").cast("string"))).otherwise(
            lit(None)
        ),
    )

    df_id = df_id.withColumn(
        "global_product_id_synced",
        when(
            col("product_id_std").isNotNull() & (col("product_id_std") != ""),
            concat(col("source_platform_std"), lit("_"), col("product_id_std")),
        ).otherwise(trim(col("global_product_id"))),
    )

    df_id = df_id.withColumn(
        "product_master_id",
        sha2(
            concat_ws(
                "||",
                lower(coalesce(col("brand_std"), lit(""))),
                lower(coalesce(col("product_name_std"), lit(""))),
                lower(coalesce(col("category_std"), lit(""))),
            ),
            256,
        ),
    )

    df_id = df_id.withColumn(
        "sku_id",
        sha2(
            concat_ws(
                "||",
                lower(coalesce(col("source_platform_std"), lit(""))),
                lower(coalesce(col("seller_name"), lit(""))),
                lower(coalesce(col("product_id_std"), lit(""))),
            ),
            256,
        ),
    )

    print("\n Identifier Sync Summary:")
    distinct_sync = df_id.select("global_product_id_synced").distinct().count()
    print(f"  Distinct global_product_id_synced: {distinct_sync:,}")
    return df_id


# ============================================================
#  STEP 3 – Deduplicate
# ============================================================
def deduplicate_products(df: DataFrame) -> DataFrame:
    """
    WHAT:
        Bỏ duplicate theo (global_product_id_synced, snapshot_date).

    WHY:
        Đảm bảo grain = 1 sản phẩm / platform / ngày.

    HOW:
        - Nếu có snapshot_date: dropDuplicates(["global_product_id_synced", "snapshot_date"])
        - Nếu không: cảnh báo & dropDuplicates theo global_product_id_synced
    """
    print("\n" + "=" * 60)
    print(" STEP 3: DEDUPLICATION")
    print("=" * 60)

    dedup_cols = []
    if "snapshot_date" in df.columns:
        dedup_cols = ["global_product_id_synced", "snapshot_date"]
        print(f"[INFO] Deduplicating by {dedup_cols} (Preserving History)")
    else:
        dedup_cols = ["global_product_id_synced"]
        print(f"[WARN] 'snapshot_date' missing! Deduplicating by {dedup_cols} (Potential Data Loss)")

    try:
        df_deduplicated = df.dropDuplicates(dedup_cols)
        original_count = df.count()
        deduplicated_count = df_deduplicated.count()
        duplicates_removed = original_count - deduplicated_count

        print(" Deduplicated data:")
        print(f"   Key columns: {dedup_cols}")
        print(f"   Original: {original_count:,} records")
        print(f"   After dedup: {deduplicated_count:,} records")
        print(f"   Removed: {duplicates_removed:,} duplicates")
        return df_deduplicated

    except Exception as e:
        print(f" Error during deduplication: {e}")
        return None


# ============================================================
#  STEP 4 – Validate
# ============================================================
def validate_products(df: DataFrame) -> bool:
    """
    WHAT:
        Kiểm tra chất lượng dữ liệu sau khi dedup.

    WHY:
        Đảm bảo tỉ lệ record 'tốt' (có tên + giá > 0) đủ cao trước khi load DWH.

    HOW:
        - Đếm total_records
        - Đếm valid_records (product_name != 'Unknown' & price_current > 0)
        - Đếm missing_name, missing_price, missing_brand
        - In ra log (và có thể ghi vào bảng metadata sau này)
    """
    print("\n" + "=" * 60)
    print(" STEP 4: DATA VALIDATION")
    print("=" * 60)

    try:
        total_records = df.count()

        valid_records = df.filter(
            (col("product_name").isNotNull()) & (col("price_current") > 0)
        ).count()

        missing_product_name = df.filter(col("product_name").isNull()).count()
        missing_price = df.filter(col("price_current") <= 0).count()
        missing_brand = df.filter(col("brand_name").isNull()).count()

        print(f"\n Data Quality Report:")
        print(f"  Total records: {total_records:,}")
        print(
            f"  Valid records: {valid_records:,} ({valid_records/total_records*100:.1f}%)"
        )
        print(f"  Missing product_name: {missing_product_name:,}")
        print(f"  Missing/invalid price: {missing_price:,}")
        print(f"  Missing brand: {missing_brand:,}")
        return True

    except Exception as e:
        print(f"  Validation error: {e}")
        return True
