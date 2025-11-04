#!/usr/bin/env python3
"""
Simple Spark Data Processor for Tiki and Lazada
Xử lý dữ liệu đơn giản từ JSON, CSV và Database
"""

import os
import sys
import yaml
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace,
    lower, trim, split, explode, to_date,
    current_timestamp, lit, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType, ArrayType
)


class SimpleEcommerceProcessor:
    """Simple processor for Tiki and Lazada data."""

    def __init__(self, app_name="SimpleEcommerceProcessor"):
        """Initialize Spark session."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

    def read_json_data(self, json_path, platform):
        """
        Đọc dữ liệu JSON từ Tiki hoặc Lazada với schema inference.

        Args:
            json_path: Đường dẫn file JSON
            platform: 'tiki' hoặc 'lazada'
        """
        print(f"Reading {platform} JSON data from: {json_path}")

        try:
            # Đọc với schema inference để linh hoạt hơn
            df = self.spark.read.option("multiLine", "true").json(json_path)
            df = df.withColumn("platform", lit(platform))

            print(f"Successfully loaded {df.count()} records from {platform}")
            print(f"Schema: {df.columns}")

            return df
        except Exception as e:
            print(f"Error reading JSON: {e}")
            # Thử đọc từng dòng
            try:
                print("Trying to read as single-line JSON...")
                df = self.spark.read.json(json_path)
                df = df.withColumn("platform", lit(platform))
                print(f"Successfully loaded {df.count()} records from {platform}")
                return df
            except Exception as e2:
                print(f"Error reading single-line JSON: {e2}")
                return None

    def read_csv_data(self, csv_path):
        """Đọc dữ liệu CSV."""
        print(f"Reading CSV data from: {csv_path}")

        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", "UTF-8") \
                .csv(csv_path)
            return df
        except Exception as e:
            print(f"Error reading CSV: {e}")
            return None

    def read_database_data(self, jdbc_url, table_name, properties):
        """Đọc dữ liệu từ database."""
        print(f"Reading database data from table: {table_name}")

        try:
            df = self.spark.read \
                .jdbc(url=jdbc_url, table=table_name, properties=properties)
            return df
        except Exception as e:
            print(f"Error reading database: {e}")
            return None

    def clean_data(self, df):
        """Làm sạch dữ liệu cơ bản với flexible schema."""
        print("Cleaning data...")
        print(f"Columns available: {df.columns}")

        # Loại bỏ duplicates với safe approach
        id_cols = []
        if "id" in df.columns:
            id_cols.append("id")
        if "platform" in df.columns:
            id_cols.append("platform")
        if id_cols:
            df = df.dropDuplicates(id_cols)

        # Đồng nhất column names cho Tiki
        if "product_name" in df.columns and "name" not in df.columns:
            df = df.withColumn("name", col("product_name"))
        if "product_id" in df.columns and "id" not in df.columns:
            df = df.withColumn("id", col("product_id"))

        # Làm sạch text fields
        text_cols = ["name", "category", "brand"]
        for col_name in text_cols:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    trim(regexp_replace(col(col_name), r'[^\w\sáàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộớờởỡợúùủũụưứừửữựýỳỷỹỵđ]', ''))
                )

        # Xử lý giá trị null với safe fillna
        fill_dict = {}
        if "name" in df.columns:
            fill_dict["name"] = "Unknown Product"
        if "category" in df.columns:
            fill_dict["category"] = "Unknown Category"
        if "brand" in df.columns:
            fill_dict["brand"] = "Unknown Brand"
        if "sold_count" in df.columns:
            fill_dict["sold_count"] = 0

        if fill_dict:
            df = df.fillna(fill_dict)

        # Xử lý price với flexible structure
        try:
            if "price" in df.columns:
                # Nested price structure
                df = df.withColumn(
                    "current_price",
                    coalesce(col("price.current"), col("price"), lit(0.0))
                ).withColumn(
                    "original_price",
                    coalesce(col("price.original"), col("price.current"), col("price"), lit(0.0))
                )
            elif "priceShow" in df.columns:
                # Lazada structure
                df = df.withColumn("current_price", col("priceShow").cast("double"))
                df = df.withColumn("original_price", coalesce(col("originalPrice").cast("double"), col("current_price")))
            elif "price_current" in df.columns:
                # Tiki structure
                df = df.withColumn("current_price", coalesce(col("price_current").cast("double"), lit(0.0)))
                df = df.withColumn("original_price", coalesce(col("price_original").cast("double"), col("current_price")))
            else:
                # Create default price columns
                df = df.withColumn("current_price", lit(0.0))
                df = df.withColumn("original_price", lit(0.0))
        except Exception as e:
            print(f"Error processing price columns: {e}")
            df = df.withColumn("current_price", lit(0.0))
            df = df.withColumn("original_price", lit(0.0))

        # Xử lý rating với flexible structure
        try:
            if "rating" in df.columns:
                # Nested rating structure
                df = df.withColumn(
                    "rating_score",
                    coalesce(col("rating.score"), col("rating.average"), lit(0.0))
                ).withColumn(
                    "rating_count",
                    coalesce(col("rating.count"), col("rating.review"), lit(0))
                )
            elif "ratingScore" in df.columns:
                # Lazada structure
                df = df.withColumn("rating_score", coalesce(col("ratingScore"), lit(0.0)))
                df = df.withColumn("rating_count", coalesce(col("review"), lit(0)))
            elif "rating_avg" in df.columns:
                # Tiki structure
                df = df.withColumn("rating_score", coalesce(col("rating_avg").cast("double"), lit(0.0)))
                df = df.withColumn("rating_count", coalesce(col("review_count").cast("int"), lit(0)))
            else:
                # Create default rating columns
                df = df.withColumn("rating_score", lit(0.0))
                df = df.withColumn("rating_count", lit(0))
        except Exception as e:
            print(f"Error processing rating columns: {e}")
            df = df.withColumn("rating_score", lit(0.0))
            df = df.withColumn("rating_count", lit(0))

        return df

    def add_derived_fields(self, df):
        """Thêm các trường tính toán."""
        print("Adding derived fields...")

        # Tính discount percentage
        df = df.withColumn(
            "discount_percentage",
            when(col("original_price") > 0,
                 ((col("original_price") - col("current_price")) / col("original_price") * 100))
            .otherwise(0.0)
        )

        # Phân loại price range
        df = df.withColumn(
            "price_range",
            when(col("current_price") < 500000, "Low")
            .when(col("current_price") < 2000000, "Medium")
            .when(col("current_price") < 5000000, "High")
            .otherwise("Premium")
        )

        # Phân loại rating
        df = df.withColumn(
            "rating_category",
            when(col("rating_score") >= 4.5, "Excellent")
            .when(col("rating_score") >= 4.0, "Good")
            .when(col("rating_score") >= 3.5, "Average")
            .when(col("rating_score") >= 3.0, "Below Average")
            .otherwise("Poor")
        )

        # Thêm processing timestamp
        df = df.withColumn("processed_at", current_timestamp())

        return df

    def process_platform_data(self, json_path=None, csv_path=None,
                            jdbc_config=None, platform="tiki"):
        """Xử lý dữ liệu cho một platform."""
        print(f"\n=== Processing {platform.upper()} Data ===")

        dataframes = []

        # Đọc JSON
        if json_path and os.path.exists(json_path):
            json_df = self.read_json_data(json_path, platform)
            if json_df:
                dataframes.append(json_df)

        # Đọc CSV
        if csv_path and os.path.exists(csv_path):
            csv_df = self.read_csv_data(csv_path)
            if csv_df:
                csv_df = csv_df.withColumn("platform", lit(platform))
                dataframes.append(csv_df)

        # Đọc Database
        if jdbc_config:
            db_df = self.read_database_data(
                jdbc_config["url"],
                jdbc_config["table"],
                jdbc_config["properties"]
            )
            if db_df:
                db_df = db_df.withColumn("platform", lit(platform))
                dataframes.append(db_df)

        if not dataframes:
            print(f"No data found for {platform}")
            return None

        # Union all dataframes
        if len(dataframes) == 1:
            combined_df = dataframes[0]
        else:
            # Get common columns
            common_cols = set(dataframes[0].columns)
            for df in dataframes[1:]:
                common_cols = common_cols.intersection(set(df.columns))

            # Select common columns and union
            common_cols = list(common_cols)
            aligned_dfs = [df.select(common_cols) for df in dataframes]
            combined_df = aligned_dfs[0]
            for df in aligned_dfs[1:]:
                combined_df = combined_df.union(df)

        # Process data
        combined_df = self.clean_data(combined_df)
        combined_df = self.add_derived_fields(combined_df)

        return combined_df

    def save_results(self, df, output_path, format="parquet"):
        """Lưu kết quả."""
        print(f"Saving results to: {output_path}")

        if format == "parquet":
            df.write.mode("overwrite").parquet(output_path)
        elif format == "json":
            df.write.mode("overwrite").json(output_path)
        elif format == "csv":
            df.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .csv(output_path)

        print(f"Data saved successfully in {format} format")

    def show_summary(self, df, platform):
        """Hiển thị tóm tắt dữ liệu."""
        print(f"\n=== {platform.upper()} DATA SUMMARY ===")
        print(f"Total records: {df.count()}")

        print("\n--- Price Statistics ---")
        df.select("current_price").describe().show()

        print("\n--- Price Range Distribution ---")
        df.groupBy("price_range").count().orderBy("count", ascending=False).show()

        print("\n--- Rating Distribution ---")
        df.groupBy("rating_category").count().orderBy("count", ascending=False).show()

        print("\n--- Top 10 Categories ---")
        df.groupBy("category").count().orderBy("count", ascending=False).limit(10).show()

        print("\n--- Top 10 Brands ---")
        df.groupBy("brand").count().orderBy("count", ascending=False).limit(10).show()

    def load_config(self, config_path):
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return None

    def run_simple_processing(self, config_path="../configs/simple_config.yaml"):
        """Chạy xử lý đơn giản."""
        print("Starting Simple E-commerce Data Processing...")

        # Load config
        config = self.load_config(config_path)
        if not config:
            print("Failed to load configuration. Using defaults.")
            return

        data_sources = config.get('data_sources', {})

        # Cấu hình từ file
        tiki_config = data_sources.get('tiki', {})
        lazada_config = data_sources.get('lazada', {})

        processed_data = []

        # Xử lý Tiki
        if tiki_config.get('json_path'):
            print(f"\n=== Processing Tiki data ===")
            tiki_df = self.process_platform_data(
                json_path=tiki_config.get('json_path'),
                csv_path=tiki_config.get('csv_path'),
                platform=tiki_config.get('platform', 'tiki')
            )
            if tiki_df:
                self.show_summary(tiki_df, "tiki")
                self.save_results(tiki_df, "outputs/processed_tiki_simple", "json")
                processed_data.append(tiki_df)

        # Xử lý Lazada
        if lazada_config.get('json_path'):
            print(f"\n=== Processing Lazada data ===")
            lazada_df = self.process_platform_data(
                json_path=lazada_config.get('json_path'),
                csv_path=lazada_config.get('csv_path'),
                platform=lazada_config.get('platform', 'lazada')
            )
            if lazada_df:
                self.show_summary(lazada_df, "lazada")
                self.save_results(lazada_df, "outputs/processed_lazada_simple", "json")
                processed_data.append(lazada_df)

        # Combine all data
        if processed_data:
            print("\n=== COMBINING ALL DATA ===")
            combined_df = processed_data[0]
            for df in processed_data[1:]:
                combined_df = combined_df.union(df)

            self.show_summary(combined_df, "combined")
            self.save_results(combined_df, "outputs/processed_combined_simple", "json")

        print("\nSimple processing completed!")

    def close(self):
        """Đóng Spark session."""
        self.spark.stop()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Simple E-commerce Data Processor')
    parser.add_argument('--config', default='../configs/simple_config.yaml',
                        help='Path to configuration file')

    args = parser.parse_args()

    processor = SimpleEcommerceProcessor()

    try:
        processor.run_simple_processing(args.config)
    finally:
        processor.close()


if __name__ == "__main__":
    main()