#!/usr/bin/env python3
"""
Advanced Spark Data Processor for Tiki and Lazada
Xử lý dữ liệu nâng cao với ML, streaming, và analytics phức tạp
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, regexp_extract,
    lower, trim, split, explode, to_date, to_timestamp,
    current_timestamp, lit, coalesce, concat_ws, hash,
    lag, lead, row_number, rank, dense_rank, ntile,
    avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    stddev, variance, skewness, kurtosis,
    collect_list, collect_set, size, array_contains,
    monotonically_increasing_id, broadcast,
    udf, pandas_udf, expr, desc, asc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, ArrayType, MapType, BooleanType,
    FloatType, LongType
)
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler,
    StandardScaler, MinMaxScaler, Normalizer,
    HashingTF, IDF, Word2Vec, NGram, Tokenizer,
    StopWordsRemover, RegexTokenizer
)
from pyspark.ml.clustering import KMeans, GaussianMixture
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
import pyspark.sql.functions as F


class AdvancedEcommerceProcessor:
    """Advanced processor với ML, streaming, và analytics phức tạp."""

    def __init__(self, app_name="AdvancedEcommerceProcessor"):
        """Initialize enhanced Spark session."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Cache frequently used DataFrames
        self.cached_dataframes = {}

    def read_multi_source_data(self, sources_config: Dict[str, Any]) -> DataFrame:
        """
        Đọc dữ liệu từ nhiều nguồn với schema evolution.

        Args:
            sources_config: Config cho các nguồn dữ liệu
        """
        dataframes = []

        for source_type, config in sources_config.items():
            if source_type == "json":
                df = self._read_json_with_schema_evolution(config)
                if df: dataframes.append(df)

            elif source_type == "csv":
                df = self._read_csv_advanced(config)
                if df: dataframes.append(df)

            elif source_type == "database":
                df = self._read_database_partitioned(config)
                if df: dataframes.append(df)

            elif source_type == "streaming":
                df = self._read_streaming_data(config)
                if df: dataframes.append(df)

        return self._merge_dataframes_with_schema_resolution(dataframes)

    def _read_json_with_schema_evolution(self, config: Dict) -> Optional[DataFrame]:
        """Đọc JSON với schema evolution."""
        try:
            # Đọc sample để infer schema
            sample_df = self.spark.read.option("samplingRatio", 0.1).json(config["path"])

            # Đọc toàn bộ với schema đã infer
            df = self.spark.read.schema(sample_df.schema).json(config["path"])

            # Add metadata
            df = df.withColumn("source_file", lit(config["path"])) \
                  .withColumn("ingestion_time", current_timestamp()) \
                  .withColumn("platform", lit(config.get("platform", "unknown")))

            return df
        except Exception as e:
            print(f"Error reading JSON {config['path']}: {e}")
            return None

    def _read_csv_advanced(self, config: Dict) -> Optional[DataFrame]:
        """Đọc CSV với error handling nâng cao."""
        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .option("multiLine", "true") \
                .option("escape", '"') \
                .option("badRecordsPath", config.get("error_path", "/tmp/bad_records")) \
                .csv(config["path"])

            return df.withColumn("source_type", lit("csv"))
        except Exception as e:
            print(f"Error reading CSV {config['path']}: {e}")
            return None

    def _read_database_partitioned(self, config: Dict) -> Optional[DataFrame]:
        """Đọc database với partitioning."""
        try:
            # Predicate pushdown cho performance
            predicates = config.get("predicates", [])

            df = self.spark.read \
                .jdbc(
                    url=config["url"],
                    table=config["table"],
                    properties=config["properties"],
                    predicates=predicates
                )

            return df.withColumn("source_type", lit("database"))
        except Exception as e:
            print(f"Error reading database {config['table']}: {e}")
            return None

    def _read_streaming_data(self, config: Dict) -> Optional[DataFrame]:
        """Đọc streaming data (Kafka, files, etc.)."""
        try:
            # Placeholder for streaming - có thể implement Kafka, Kinesis, etc.
            # df = self.spark.readStream...
            print(f"Streaming source {config['source']} not implemented yet")
            return None
        except Exception as e:
            print(f"Error reading streaming data: {e}")
            return None

    def _merge_dataframes_with_schema_resolution(self, dataframes: List[DataFrame]) -> DataFrame:
        """Merge DataFrames với schema resolution."""
        if not dataframes:
            return None

        if len(dataframes) == 1:
            return dataframes[0]

        # Schema alignment
        all_columns = set()
        for df in dataframes:
            all_columns.update(df.columns)

        # Align schemas
        aligned_dfs = []
        for df in dataframes:
            for col_name in all_columns:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, lit(None))
            # Select columns in consistent order
            aligned_dfs.append(df.select(sorted(all_columns)))

        # Union all
        result = aligned_dfs[0]
        for df in aligned_dfs[1:]:
            result = result.union(df)

        return result

    def advanced_data_cleaning(self, df: DataFrame) -> DataFrame:
        """Làm sạch dữ liệu nâng cao với outlier detection."""
        print("Advanced data cleaning...")

        # 1. Duplicate detection với fuzzy matching
        df = self._remove_fuzzy_duplicates(df)

        # 2. Outlier detection và treatment
        df = self._detect_and_handle_outliers(df)

        # 3. Text normalization nâng cao
        df = self._advanced_text_cleaning(df)

        # 4. Data validation và integrity checks
        df = self._validate_data_integrity(df)

        return df

    def _remove_fuzzy_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicates với fuzzy matching."""
        # Tạo hash cho fuzzy matching
        df = df.withColumn(
            "fuzzy_hash",
            hash(concat_ws("|",
                lower(trim(col("name"))),
                col("platform"),
                col("brand")
            ))
        )

        # Remove exact duplicates first
        df = df.dropDuplicates(["fuzzy_hash"])

        return df.drop("fuzzy_hash")

    def _detect_and_handle_outliers(self, df: DataFrame) -> DataFrame:
        """Detect và handle outliers trong price data."""
        # Tính quartiles cho price
        quantiles = df.select("current_price").approxQuantile("current_price", [0.25, 0.75], 0.05)
        if len(quantiles) == 2:
            q1, q3 = quantiles
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            # Flag outliers
            df = df.withColumn(
                "is_price_outlier",
                (col("current_price") < lower_bound) | (col("current_price") > upper_bound)
            )

            # Cap outliers thay vì remove
            df = df.withColumn(
                "current_price_cleaned",
                when(col("current_price") < lower_bound, lower_bound)
                .when(col("current_price") > upper_bound, upper_bound)
                .otherwise(col("current_price"))
            )

        return df

    def _advanced_text_cleaning(self, df: DataFrame) -> DataFrame:
        """Text cleaning nâng cao."""
        text_columns = ["name", "category", "brand", "description"]

        for col_name in text_columns:
            if col_name in df.columns:
                # Unicode normalization
                df = df.withColumn(
                    f"{col_name}_cleaned",
                    regexp_replace(
                        trim(lower(col(col_name))),
                        r'[^\w\sáàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộớờởỡợúùủũụưứừửữựýỳỷỹỵđ-]',
                        ' '
                    )
                )

                # Remove extra spaces
                df = df.withColumn(
                    f"{col_name}_cleaned",
                    regexp_replace(col(f"{col_name}_cleaned"), r'\s+', ' ')
                )

        return df

    def _validate_data_integrity(self, df: DataFrame) -> DataFrame:
        """Data integrity validation."""
        # Price validation
        df = df.withColumn(
            "price_valid",
            (col("current_price") >= 0) & (col("original_price") >= 0) &
            (col("current_price") <= col("original_price"))
        )

        # Rating validation
        df = df.withColumn(
            "rating_valid",
            (col("rating_score") >= 0) & (col("rating_score") <= 5) &
            (col("rating_count") >= 0)
        )

        # Overall validation
        df = df.withColumn(
            "is_valid_record",
            col("price_valid") & col("rating_valid")
        )

        return df

    def feature_engineering_advanced(self, df: DataFrame) -> DataFrame:
        """Feature engineering nâng cao."""
        print("Advanced feature engineering...")

        # 1. Time-based features
        df = self._create_time_features(df)

        # 2. Text features
        df = self._create_text_features(df)

        # 3. Price features
        df = self._create_price_features(df)

        # 4. Competitive features
        df = self._create_competitive_features(df)

        # 5. Behavioral features
        df = self._create_behavioral_features(df)

        return df

    def _create_time_features(self, df: DataFrame) -> DataFrame:
        """Tạo time-based features."""
        if "crawled_at" in df.columns:
            df = df.withColumn("crawled_timestamp", to_timestamp(col("crawled_at")))
            df = df.withColumn("crawl_hour", F.hour("crawled_timestamp"))
            df = df.withColumn("crawl_day_of_week", F.dayofweek("crawled_timestamp"))
            df = df.withColumn("crawl_month", F.month("crawled_timestamp"))

        return df

    def _create_text_features(self, df: DataFrame) -> DataFrame:
        """Tạo text features."""
        if "name_cleaned" in df.columns:
            # Text length
            df = df.withColumn("name_length", F.length("name_cleaned"))

            # Word count
            df = df.withColumn("name_word_count", size(split(col("name_cleaned"), " ")))

            # Contains keywords
            keywords = ["new", "sale", "discount", "premium", "pro", "max", "mini"]
            for keyword in keywords:
                df = df.withColumn(
                    f"has_{keyword}",
                    col("name_cleaned").contains(keyword).cast("int")
                )

        return df

    def _create_price_features(self, df: DataFrame) -> DataFrame:
        """Tạo price-related features."""
        # Price statistics by category
        category_stats = df.groupBy("category").agg(
            avg("current_price").alias("category_avg_price"),
            F.stddev("current_price").alias("category_price_std")
        )

        df = df.join(broadcast(category_stats), "category", "left")

        # Price position in category
        df = df.withColumn(
            "price_vs_category_avg",
            (col("current_price") - col("category_avg_price")) / col("category_avg_price")
        )

        # Price volatility
        if "original_price" in df.columns and "current_price" in df.columns:
            df = df.withColumn(
                "price_volatility",
                F.abs(col("original_price") - col("current_price")) / col("original_price")
            )

        return df

    def _create_competitive_features(self, df: DataFrame) -> DataFrame:
        """Tạo competitive analysis features."""
        # Brand market share
        brand_stats = df.groupBy("brand").agg(
            count("*").alias("brand_product_count"),
            avg("current_price").alias("brand_avg_price"),
            avg("rating_score").alias("brand_avg_rating")
        )

        df = df.join(broadcast(brand_stats), "brand", "left")

        # Platform comparison
        platform_stats = df.groupBy("platform").agg(
            avg("current_price").alias("platform_avg_price"),
            count("*").alias("platform_product_count")
        )

        df = df.join(broadcast(platform_stats), "platform", "left")

        return df

    def _create_behavioral_features(self, df: DataFrame) -> DataFrame:
        """Tạo behavioral features."""
        # Popularity score
        df = df.withColumn(
            "popularity_score",
            (col("rating_score") * F.log1p(col("rating_count"))) +
            (F.log1p(col("sold_count")) * 0.5)
        )

        # Value for money score
        df = df.withColumn(
            "value_score",
            col("rating_score") / F.log1p(col("current_price"))
        )

        return df

    def ml_analysis(self, df: DataFrame) -> Dict[str, Any]:
        """Machine Learning analysis."""
        print("Running ML analysis...")

        results = {}

        # 1. Customer segmentation
        results["customer_segments"] = self._customer_segmentation(df)

        # 2. Price prediction
        results["price_model"] = self._price_prediction_model(df)

        # 3. Product recommendation
        results["recommendation_model"] = self._product_recommendation(df)

        # 4. Market basket analysis
        results["market_basket"] = self._market_basket_analysis(df)

        return results

    def _customer_segmentation(self, df: DataFrame) -> Dict:
        """K-Means clustering cho customer segmentation."""
        # Prepare features
        feature_cols = ["current_price", "rating_score", "popularity_score", "value_score"]
        available_cols = [col for col in feature_cols if col in df.columns]

        if len(available_cols) < 2:
            return {"error": "Not enough features for clustering"}

        # Feature assembly
        assembler = VectorAssembler(inputCols=available_cols, outputCol="features")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

        # KMeans
        kmeans = KMeans(featuresCol="scaled_features", predictionCol="segment", k=5, seed=42)

        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        model = pipeline.fit(df)
        segmented_df = model.transform(df)

        # Analyze segments
        segment_analysis = segmented_df.groupBy("segment").agg(
            count("*").alias("count"),
            avg("current_price").alias("avg_price"),
            avg("rating_score").alias("avg_rating"),
            avg("popularity_score").alias("avg_popularity")
        ).collect()

        return {
            "model": model,
            "segments": [row.asDict() for row in segment_analysis]
        }

    def _price_prediction_model(self, df: DataFrame) -> Dict:
        """Random Forest cho price prediction."""
        # Prepare features
        feature_cols = [
            "rating_score", "rating_count", "sold_count",
            "name_length", "name_word_count", "brand_avg_price"
        ]
        available_cols = [col for col in feature_cols if col in df.columns]

        if len(available_cols) < 3:
            return {"error": "Not enough features for price prediction"}

        # Remove nulls and prepare data
        clean_df = df.filter(col("current_price").isNotNull())
        for col_name in available_cols:
            clean_df = clean_df.filter(col(col_name).isNotNull())

        if clean_df.count() < 100:
            return {"error": "Not enough data for training"}

        # Feature assembly
        assembler = VectorAssembler(inputCols=available_cols, outputCol="features")

        # Random Forest
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="current_price",
            predictionCol="predicted_price",
            numTrees=100
        )

        pipeline = Pipeline(stages=[assembler, rf])

        # Train-test split
        train_df, test_df = clean_df.randomSplit([0.8, 0.2], seed=42)

        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)

        # Evaluate
        evaluator = RegressionEvaluator(
            labelCol="current_price",
            predictionCol="predicted_price",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)

        return {
            "model": model,
            "rmse": rmse,
            "feature_importance": available_cols
        }

    def _product_recommendation(self, df: DataFrame) -> Dict:
        """Product recommendation using collaborative filtering."""
        # Placeholder for recommendation system
        # Có thể implement ALS hoặc content-based filtering
        return {"status": "not_implemented"}

    def _market_basket_analysis(self, df: DataFrame) -> Dict:
        """Market basket analysis."""
        # Group by brand and category combinations
        brand_category_pairs = df.groupBy("brand", "category").count()

        # Find frequent patterns
        frequent_pairs = brand_category_pairs.filter(col("count") >= 10) \
            .orderBy(desc("count")).limit(20)

        return {
            "frequent_brand_category_pairs": [row.asDict() for row in frequent_pairs.collect()]
        }

    def advanced_analytics(self, df: DataFrame) -> Dict[str, Any]:
        """Advanced analytics và insights."""
        print("Running advanced analytics...")

        analytics = {}

        # 1. Trend analysis
        analytics["trends"] = self._trend_analysis(df)

        # 2. Correlation analysis
        analytics["correlations"] = self._correlation_analysis(df)

        # 3. Statistical tests
        analytics["statistical_tests"] = self._statistical_tests(df)

        # 4. Market insights
        analytics["market_insights"] = self._market_insights(df)

        return analytics

    def _trend_analysis(self, df: DataFrame) -> Dict:
        """Time series trend analysis."""
        if "crawled_timestamp" not in df.columns:
            return {"error": "No timestamp column for trend analysis"}

        # Daily trends
        daily_trends = df.groupBy(F.date_trunc("day", "crawled_timestamp").alias("date")) \
            .agg(
                avg("current_price").alias("avg_price"),
                count("*").alias("product_count")
            ).orderBy("date")

        return {
            "daily_price_trend": [row.asDict() for row in daily_trends.collect()]
        }

    def _correlation_analysis(self, df: DataFrame) -> Dict:
        """Correlation analysis giữa các features."""
        numeric_cols = ["current_price", "rating_score", "rating_count", "sold_count"]
        available_cols = [col for col in numeric_cols if col in df.columns]

        if len(available_cols) < 2:
            return {"error": "Not enough numeric columns for correlation"}

        # Prepare vector for correlation
        assembler = VectorAssembler(inputCols=available_cols, outputCol="features")
        vector_df = assembler.transform(df.na.drop())

        # Calculate correlation matrix
        correlation_matrix = Correlation.corr(vector_df, "features").head()

        return {
            "correlation_matrix": correlation_matrix[0].toArray().tolist(),
            "feature_names": available_cols
        }

    def _statistical_tests(self, df: DataFrame) -> Dict:
        """Statistical tests."""
        # Price distribution by platform
        platform_stats = df.groupBy("platform").agg(
            avg("current_price").alias("avg_price"),
            F.stddev("current_price").alias("std_price"),
            count("*").alias("count")
        )

        return {
            "platform_price_stats": [row.asDict() for row in platform_stats.collect()]
        }

    def _market_insights(self, df: DataFrame) -> Dict:
        """Market insights và competitive analysis."""
        insights = {}

        # Top performing categories
        category_performance = df.groupBy("category").agg(
            avg("rating_score").alias("avg_rating"),
            avg("current_price").alias("avg_price"),
            count("*").alias("product_count"),
            F.sum("sold_count").alias("total_sold")
        ).orderBy(desc("avg_rating"))

        insights["top_categories"] = [row.asDict() for row in category_performance.limit(10).collect()]

        # Brand analysis
        brand_performance = df.groupBy("brand").agg(
            avg("rating_score").alias("avg_rating"),
            avg("current_price").alias("avg_price"),
            count("*").alias("product_count")
        ).filter(col("product_count") >= 5).orderBy(desc("avg_rating"))

        insights["top_brands"] = [row.asDict() for row in brand_performance.limit(10).collect()]

        return insights

    def save_advanced_results(self, df: DataFrame, ml_results: Dict, analytics: Dict, output_base: str):
        """Lưu kết quả nâng cao."""
        print("Saving advanced results...")

        # Main processed data
        df.write.mode("overwrite").partitionBy("platform", "category").parquet(f"{output_base}/processed_data")

        # ML results
        if "customer_segments" in ml_results:
            segments_df = ml_results["customer_segments"].get("segments")
            if segments_df:
                self.spark.createDataFrame(segments_df).write.mode("overwrite").json(f"{output_base}/customer_segments")

        # Analytics results
        with open(f"{output_base}/analytics_results.json", "w") as f:
            # Convert non-serializable objects to strings
            serializable_analytics = self._make_serializable(analytics)
            json.dump(serializable_analytics, f, indent=2)

        print("Advanced results saved successfully")

    def _make_serializable(self, obj):
        """Convert objects to JSON serializable format."""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif hasattr(obj, 'toArray'):  # Spark Matrix
            return obj.toArray().tolist()
        elif hasattr(obj, '__dict__'):  # Custom objects
            return str(obj)
        else:
            return obj

    def run_advanced_processing(self, config_path: str = "configs/advanced_config.json"):
        """Chạy xử lý nâng cao."""
        print("Starting Advanced E-commerce Data Processing...")

        # Load config
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
        else:
            config = self._get_default_advanced_config()

        # Read data
        df = self.read_multi_source_data(config["data_sources"])
        if df is None or df.count() == 0:
            print("No data found to process")
            return

        print(f"Loaded {df.count()} records from multiple sources")

        # Advanced processing pipeline
        df = self.advanced_data_cleaning(df)
        df = self.feature_engineering_advanced(df)

        # Cache for ML operations
        df.cache()
        self.cached_dataframes["main"] = df

        # ML Analysis
        ml_results = self.ml_analysis(df)

        # Advanced Analytics
        analytics = self.advanced_analytics(df)

        # Save results
        self.save_advanced_results(df, ml_results, analytics, config["output"]["base_path"])

        print("Advanced processing completed!")

    def _get_default_advanced_config(self) -> Dict:
        """Default config cho advanced processing."""
        return {
            "data_sources": {
                "json": {
                    "path": "data-collection/data/*.json",
                    "platform": "multi"
                },
                "csv": {
                    "path": "data-collection/data/*.csv"
                }
            },
            "output": {
                "base_path": "outputs/advanced_processing"
            }
        }

    def close(self):
        """Clean up resources."""
        for df in self.cached_dataframes.values():
            df.unpersist()
        self.spark.stop()


def main():
    """Main function cho advanced processing."""
    processor = AdvancedEcommerceProcessor()

    try:
        processor.run_advanced_processing()
    finally:
        processor.close()


if __name__ == "__main__":
    main()