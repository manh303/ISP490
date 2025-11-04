"""
Spark Utilities for E-commerce Data Processing
Common utilities and helper functions
"""

import os
import json
import yaml
from typing import Dict, Any, List, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType


class SparkSessionManager:
    """Manages Spark session lifecycle."""

    @staticmethod
    def create_session(app_name: str, configs: Dict[str, str] = None) -> SparkSession:
        """Create optimized Spark session."""
        builder = SparkSession.builder.appName(app_name)

        # Default configs
        default_configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        }

        # Apply configs
        if configs:
            default_configs.update(configs)

        for key, value in default_configs.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        return spark


class DataLoader:
    """Data loading utilities."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_json_with_fallback(self, path: str, schema: StructType = None) -> Optional[DataFrame]:
        """Load JSON with schema fallback."""
        try:
            if schema:
                return self.spark.read.schema(schema).json(path)
            else:
                # Infer schema from sample
                sample_df = self.spark.read.option("samplingRatio", 0.1).json(path)
                return self.spark.read.schema(sample_df.schema).json(path)
        except Exception as e:
            print(f"Error loading JSON from {path}: {e}")
            return None

    def load_csv_with_error_handling(self, path: str, error_path: str = "/tmp/bad_records") -> Optional[DataFrame]:
        """Load CSV with comprehensive error handling."""
        try:
            return self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .option("multiLine", "true") \
                .option("escape", '"') \
                .option("badRecordsPath", error_path) \
                .csv(path)
        except Exception as e:
            print(f"Error loading CSV from {path}: {e}")
            return None

    def load_database_table(self, jdbc_url: str, table: str, properties: Dict[str, str],
                          predicates: List[str] = None) -> Optional[DataFrame]:
        """Load database table with optional predicates."""
        try:
            if predicates:
                return self.spark.read.jdbc(
                    url=jdbc_url,
                    table=table,
                    properties=properties,
                    predicates=predicates
                )
            else:
                return self.spark.read.jdbc(
                    url=jdbc_url,
                    table=table,
                    properties=properties
                )
        except Exception as e:
            print(f"Error loading table {table}: {e}")
            return None


class DataValidator:
    """Data validation utilities."""

    @staticmethod
    def validate_schema(df: DataFrame, required_columns: List[str]) -> bool:
        """Validate DataFrame has required columns."""
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            print(f"Missing required columns: {missing_cols}")
            return False
        return True

    @staticmethod
    def validate_data_quality(df: DataFrame, quality_threshold: float = 0.95) -> Dict[str, Any]:
        """Validate data quality metrics."""
        total_rows = df.count()
        if total_rows == 0:
            return {"valid": False, "reason": "Empty DataFrame"}

        quality_metrics = {}

        # Check for nulls in critical columns
        critical_columns = ["id", "name", "platform"]
        for col_name in critical_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_ratio = null_count / total_rows
                quality_metrics[f"{col_name}_null_ratio"] = null_ratio

        # Overall quality score
        avg_null_ratio = sum(quality_metrics.values()) / len(quality_metrics) if quality_metrics else 0
        quality_score = 1 - avg_null_ratio

        return {
            "valid": quality_score >= quality_threshold,
            "quality_score": quality_score,
            "metrics": quality_metrics,
            "total_rows": total_rows
        }


class ConfigManager:
    """Configuration management."""

    @staticmethod
    def load_yaml_config(config_path: str) -> Dict[str, Any]:
        """Load YAML configuration."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading YAML config {config_path}: {e}")
            return {}

    @staticmethod
    def load_json_config(config_path: str) -> Dict[str, Any]:
        """Load JSON configuration."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading JSON config {config_path}: {e}")
            return {}

    @staticmethod
    def merge_configs(base_config: Dict, override_config: Dict) -> Dict:
        """Merge two configuration dictionaries."""
        merged = base_config.copy()
        for key, value in override_config.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = ConfigManager.merge_configs(merged[key], value)
            else:
                merged[key] = value
        return merged


class OutputManager:
    """Output management utilities."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def save_with_metadata(self, df: DataFrame, path: str, format: str = "parquet",
                          partitions: List[str] = None, metadata: Dict = None):
        """Save DataFrame with metadata."""
        # Add processing metadata
        df_with_metadata = df.withColumn("processing_timestamp", current_timestamp())

        if metadata:
            for key, value in metadata.items():
                df_with_metadata = df_with_metadata.withColumn(f"meta_{key}", lit(str(value)))

        # Save data
        writer = df_with_metadata.write.mode("overwrite")

        if partitions:
            writer = writer.partitionBy(partitions)

        if format == "parquet":
            writer.option("compression", "snappy").parquet(path)
        elif format == "json":
            writer.json(path)
        elif format == "csv":
            writer.option("header", "true").csv(path)

        # Save metadata separately
        if metadata:
            metadata_path = f"{path}_metadata.json"
            with open(metadata_path, 'w') as f:
                json.dump({
                    "processing_time": datetime.now().isoformat(),
                    "record_count": df.count(),
                    "schema": df.schema.json(),
                    **metadata
                }, f, indent=2)

    def save_summary_stats(self, df: DataFrame, path: str):
        """Save summary statistics."""
        numeric_columns = [field.name for field in df.schema.fields
                          if field.dataType.typeName() in ['double', 'integer', 'long', 'float']]

        if numeric_columns:
            summary_df = df.select(numeric_columns).describe()
            summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{path}/summary_stats")


class PerformanceMonitor:
    """Performance monitoring utilities."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.start_time = None

    def start_monitoring(self):
        """Start performance monitoring."""
        self.start_time = datetime.now()
        print(f"Performance monitoring started at: {self.start_time}")

    def log_stage_completion(self, stage_name: str):
        """Log stage completion time."""
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            print(f"Stage '{stage_name}' completed in: {elapsed}")

    def get_memory_usage(self) -> Dict[str, Any]:
        """Get Spark memory usage."""
        sc = self.spark.sparkContext
        status = sc.statusTracker()

        executor_infos = status.getExecutorInfos()
        total_memory = sum([exec.maxMemory for exec in executor_infos])
        memory_used = sum([exec.memoryUsed for exec in executor_infos])

        return {
            "total_memory_mb": total_memory / (1024 * 1024),
            "memory_used_mb": memory_used / (1024 * 1024),
            "memory_usage_percent": (memory_used / total_memory * 100) if total_memory > 0 else 0
        }

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        sc = self.spark.sparkContext
        status = sc.statusTracker()

        return {
            "application_id": sc.applicationId,
            "active_jobs": len(status.getActiveJobIds()),
            "active_stages": len(status.getActiveStageIds()),
            "memory_usage": self.get_memory_usage()
        }


class DataQualityChecker:
    """Data quality checking utilities."""

    @staticmethod
    def check_duplicates(df: DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """Check for duplicate records."""
        total_count = df.count()
        unique_count = df.dropDuplicates(key_columns).count()
        duplicate_count = total_count - unique_count

        return {
            "total_records": total_count,
            "unique_records": unique_count,
            "duplicate_records": duplicate_count,
            "duplicate_percentage": (duplicate_count / total_count * 100) if total_count > 0 else 0
        }

    @staticmethod
    def check_null_values(df: DataFrame) -> Dict[str, Any]:
        """Check null values across all columns."""
        total_rows = df.count()
        null_stats = {}

        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_stats[column] = {
                "null_count": null_count,
                "null_percentage": (null_count / total_rows * 100) if total_rows > 0 else 0
            }

        return null_stats

    @staticmethod
    def check_data_types(df: DataFrame) -> Dict[str, str]:
        """Check data types of all columns."""
        return {field.name: field.dataType.typeName() for field in df.schema.fields}


# Common schemas
ECOMMERCE_PRODUCT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("current_price", StringType(), True),
    StructField("original_price", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("rating_score", StringType(), True),
    StructField("rating_count", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("crawled_at", StringType(), True)
])