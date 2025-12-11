# src/spark_jobs/etl/review_transforms.py

"""
Các bước TRANSFORM cho dữ liệu REVIEW:
- Data Cleaning
- Data Standardization
- Synchronize Identifiers
- Deduplicate
- Validate
- Sentiment Analysis
- Add Time Features
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, lower, concat, lit, coalesce,
    to_timestamp, to_date, year, month, dayofmonth, dayofweek, row_number, avg, count, udf
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as spark_sum

try:
    from textblob import TextBlob
except ImportError:
    TextBlob = None


def clean_review_data(df_reviews: DataFrame) -> DataFrame:
    """
    WHAT: Làm sạch các field cơ bản từ dữ liệu review.
    
    WHY: Dữ liệu raw có nhiều null, cần chuẩn hóa trước khi xử lý tiếp.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.1: CLEANING REVIEW DATA")
    print("=" * 60)

    if df_reviews is None:
        return None

    df_clean = (
        df_reviews.withColumn(
            "review_id",
            when(col("review_id").isNotNull(), trim(col("review_id").cast("string"))).otherwise(
                lit(None)
            ),
        )
        .withColumn(
            "product_id",
            when(col("product_id").isNotNull(), trim(col("product_id").cast("string"))).otherwise(
                lit(None)
            ),
        )
        .withColumn(
            "reviewer_name",
            when(col("reviewer_name").isNotNull(), trim(col("reviewer_name"))).otherwise(
                "Anonymous"
            ),
        )
        .withColumn(
            "rating",
            when(col("rating").isNotNull(), col("rating").cast(DoubleType())).otherwise(0.0),
        )
        .withColumn(
            "review_text",
            when(col("review_text").isNotNull(), trim(col("review_text"))).otherwise(""),
        )
        .withColumn(
            "review_date",
            when(col("review_date").isNotNull(), col("review_date")).otherwise(
                col("crawl_date")
            ),
        )
        .withColumn(
            "helpful_count",
            when(col("helpful_count").isNotNull(), col("helpful_count").cast(LongType())).otherwise(
                0
            ),
        )
        .withColumn(
            "verified_purchase",
            when(col("verified_purchase").isNotNull(), col("verified_purchase")).otherwise(False),
        )
    )

    print(f" ✓ Cleaned {df_clean.count():,} reviews")
    return df_clean


def standardize_review_data(df: DataFrame) -> DataFrame:
    """
    WHAT: Chuẩn hóa các field của review data.
    
    WHY: Tạo format nhất quán cho các bước xử lý tiếp theo.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.1.5: STANDARDIZING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return None

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
            "reviewer_name_std",
            when(col("reviewer_name").isNotNull(), trim(col("reviewer_name"))).otherwise(
                "Anonymous"
            ),
        )
        .withColumn(
            "review_text_std",
            when(col("review_text").isNotNull(),
                regexp_replace(trim(col("review_text")), r"\s+", " ")).otherwise(""),
        )
        .withColumn("rating_std", col("rating").cast(DoubleType()))
    )

    print(f"\n ✓ Standardized {df_std.count():,} reviews")
    return df_std


def synchronize_review_identifiers(df: DataFrame) -> DataFrame:
    """
    WHAT: Đồng bộ các ID cho review data.
    
    WHY: Tạo global_review_id và global_product_id để link với dim_product.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.1.7: SYNCHRONIZING REVIEW IDENTIFIERS")
    print("=" * 60)

    if df is None:
        return None

    df_id = (
        df.withColumn(
            "review_id_std",
            when(col("review_id").isNotNull(), trim(col("review_id").cast("string"))).otherwise(
                lit(None)
            ),
        )
        .withColumn(
            "product_id_std",
            when(col("product_id").isNotNull(), trim(col("product_id").cast("string"))).otherwise(
                lit(None)
            ),
        )
        .withColumn(
            "global_review_id",
            when(
                col("review_id_std").isNotNull(),
                concat(col("source_platform_std"), lit("_"), col("review_id_std")),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "global_product_id",
            when(
                col("product_id_std").isNotNull(),
                concat(col("source_platform_std"), lit("_"), col("product_id_std")),
            ).otherwise(lit(None)),
        )
    )

    print(f"\n ✓ Synchronized identifiers for {df_id.count():,} reviews")
    return df_id


def deduplicate_review_data(df: DataFrame) -> DataFrame:
    """
    WHAT: Bỏ duplicate review theo global_review_id.
    
    WHY: Đảm bảo mỗi review chỉ xuất hiện một lần.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.1.8: DEDUPLICATING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return None

    before_count = df.count()

    df_dedup = (
        df.withColumn("review_date_parsed", to_timestamp(col("review_date")))
        .withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("global_review_id").orderBy(
                    col("review_date_parsed").desc_nulls_last()
                )
            ),
        )
        .filter(col("row_num") == 1)
        .drop("row_num", "review_date_parsed")
    )

    after_count = df_dedup.count()
    duplicates = before_count - after_count

    print(f"\n ✓ Deduplication Summary:")
    print(f"   Before: {before_count:,}")
    print(f"   After: {after_count:,}")
    print(f"   Duplicates removed: {duplicates:,} ({100*duplicates/before_count:.2f}%)")

    return df_dedup


def validate_review_data(df: DataFrame) -> None:
    """
    WHAT: Kiểm tra chất lượng dữ liệu review.
    
    WHY: Đảm bảo review data hợp lệ trước khi load vào DWH.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.1.9: VALIDATING REVIEW DATA")
    print("=" * 60)

    if df is None:
        return

    total = df.count()
    valid_reviews = df.filter(
        col("review_id_std").isNotNull()
        & (col("rating_std") >= 1.0)
        & (col("rating_std") <= 5.0)
    ).count()

    print(f"\n ✓ Validation Summary:")
    print(f"   Total reviews: {total:,}")
    print(f"   Valid reviews: {valid_reviews:,} ({100*valid_reviews/total:.2f}%)")
    print(f"   Invalid reviews: {total - valid_reviews:,}")


def analyze_sentiment(df: DataFrame) -> DataFrame:
    """
    WHAT: Phân tích sentiment cho review text.
    
    WHY: Cung cấp thông tin sentiment để phân tích và DSS.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.2: SENTIMENT ANALYSIS")
    print("=" * 60)

    if df is None:
        return df

    if TextBlob is None:
        print(" ⚠ TextBlob not available, using default sentiment values")
        df_sentiment = (
            df.withColumn("sentiment_score", lit(0.0))
            .withColumn("sentiment_label", lit("neutral"))
            .withColumn("is_positive_review", lit(0))
            .withColumn("is_negative_review", lit(0))
            .withColumn("is_neutral_review", lit(1))
        )
        print(" ✓ Added default sentiment columns")
        return df_sentiment

    def _get_sentiment_score(text: str):
        if not text or len(str(text).strip()) == 0:
            return 0.0
        try:
            blob = TextBlob(str(text))
            return float(blob.sentiment.polarity)
        except Exception:
            return 0.0

    def _get_sentiment_label(score: float):
        if score < -0.1:
            return "negative"
        elif score > 0.1:
            return "positive"
        else:
            return "neutral"

    sentiment_udf = udf(_get_sentiment_score, DoubleType())
    label_udf = udf(_get_sentiment_label, StringType())

    df_sentiment = (
        df.withColumn("sentiment_score", sentiment_udf(col("review_text")))
        .withColumn("sentiment_label", label_udf(col("sentiment_score")))
        .withColumn(
            "is_positive_review",
            when(col("sentiment_score") > 0.1, 1).otherwise(0),
        )
        .withColumn(
            "is_negative_review",
            when(col("sentiment_score") < -0.1, 1).otherwise(0),
        )
        .withColumn(
            "is_neutral_review",
            when(
                (col("sentiment_score") >= -0.1) & (col("sentiment_score") <= 0.1),
                1,
            ).otherwise(0),
        )
    )

    print(" ✓ Sentiment Distribution:")
    for row in (
        df_sentiment.groupBy("sentiment_label")
        .count()
        .orderBy("sentiment_label")
        .collect()
    ):
        print(f"   {row['sentiment_label'].upper():10s}: {row['count']:>10,}")

    return df_sentiment


def add_review_time_features(df: DataFrame) -> DataFrame:
    """
    WHAT: Thêm các features thời gian cho review.
    
    WHY: Cung cấp thông tin phân tích theo thời gian.
    """
    print("\n" + "=" * 60)
    print(" STEP 8.3: ADDING TIME FEATURES")
    print("=" * 60)

    def _parse_relative_date(date_str: str):
        from datetime import datetime as dt, timedelta
        import re as re_module

        if not date_str:
            return None

        s = str(date_str).strip().lower()

        if len(s) == 10 and s.count("-") == 2:
            return s

        if "T" in s:
            return s[:10]

        try:
            match = re_module.search(r"(\d+)\s+(week|day|month|year)s?\s+ago", s)
            if match:
                num = int(match.group(1))
                unit = match.group(2)
                if unit == "week":
                    delta = timedelta(weeks=num)
                elif unit == "day":
                    delta = timedelta(days=num)
                elif unit == "month":
                    delta = timedelta(days=num * 30)
                elif unit == "year":
                    delta = timedelta(days=num * 365)
                else:
                    delta = timedelta(days=0)
                result_date = dt.now() - delta
                return result_date.strftime("%Y-%m-%d")
        except Exception:
            pass

        return None

    parse_relative_udf = udf(_parse_relative_date, StringType())

    df_with_parsed = df.withColumn(
        "review_date_parsed", parse_relative_udf(col("review_date"))
    )

    def _safe_to_date(date_str: str):
        from datetime import datetime as dt

        if not date_str:
            return None
        s = str(date_str).strip()
        if len(s) == 10 and s.count("-") == 2:
            try:
                dt.strptime(s, "%Y-%m-%d")
                return s
            except Exception:
                return None
        if "T" in s:
            return s[:10]
        return None

    safe_to_date_udf = udf(_safe_to_date, StringType())

    df_time = (
        df_with_parsed.withColumn(
            "review_date_fmt",
            coalesce(
                safe_to_date_udf(col("review_date_parsed")),
                safe_to_date_udf(col("crawl_date")),
                safe_to_date_udf(col("review_date")),
                lit(None),
            ),
        )
        .withColumn("review_year", year(to_date(col("review_date_fmt"))))
        .withColumn("review_month", month(to_date(col("review_date_fmt"))))
        .withColumn("review_day", dayofmonth(to_date(col("review_date_fmt"))))
        .withColumn("review_dow", dayofweek(to_date(col("review_date_fmt"))))
        .drop("review_date_parsed")
    )

    print(" ✓ Added time features")
    return df_time


def run_review_transform_pipeline(spark, mappings):
    """
    WHAT: Orchestrate toàn bộ review transform pipeline.
    
    WHY: Gọi tất cả các bước transform cho review trong một hàm.
    
    Returns:
        (df_reviews_dedup, df_reviews_agg) tuple
    """
    # Import with fallback for both relative and absolute imports
    try:
        from spark_jobs.etl.extract import load_raw_reviews
        from spark_jobs.etl.review_aggregation import aggregate_reviews_daily
    except ImportError:
        from .extract import load_raw_reviews
        from .review_aggregation import aggregate_reviews_daily
    
    df_reviews_raw = load_raw_reviews(spark)
    if df_reviews_raw is None:
        return None, None
    
    df_reviews_clean = clean_review_data(df_reviews_raw)
    df_reviews_std = standardize_review_data(df_reviews_clean)
    df_reviews_synced = synchronize_review_identifiers(df_reviews_std)
    df_reviews_dedup = deduplicate_review_data(df_reviews_synced)
    validate_review_data(df_reviews_dedup)
    df_reviews_sentiment = analyze_sentiment(df_reviews_dedup)
    df_reviews_time = add_review_time_features(df_reviews_sentiment)
    df_reviews_agg = aggregate_reviews_daily(df_reviews_time)
    
    return df_reviews_time, df_reviews_agg
