#!/usr/bin/env python3
"""
E-commerce DSS Data Standardization Pipeline
Implements the Data Standardization Tool phase from the architecture diagram
"""

import os
import json
import logging
import hashlib
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, lower, upper, coalesce,
    lit, udf, split, regexp_extract, current_timestamp,
    monotonically_increasing_id, hash, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataStandardizationPipeline:
    """
    Main data standardization pipeline following the DSS architecture:
    1. Data Cleaning
    2. Data Standardization
    3. Data Quality & Dedup
    4. Synchronize Identifier
    5. Category Mapping
    6. Technical Metadata
    """

    def __init__(self, spark_master_url: str = "spark://spark-master:7077"):
        self.spark = self._create_spark_session(spark_master_url)
        self.raw_data_path = '/app/raw_data'
        self.output_path = '/app/processed_data'

        # Category mapping configuration
        self.category_mapping = self._load_category_mapping()

        # Quality thresholds (relaxed for better coverage)
        self.quality_thresholds = {
            'min_price': 100,        # Giảm từ 1000 xuống 100
            'max_price': 1000000000, # Tăng từ 500M lên 1B VND
            'min_title_length': 5,   # Giảm từ 10 xuống 5
            'required_fields': ['title', 'source']  # Bỏ requirement 'price'
        }

    def _create_spark_session(self, master_url: str) -> SparkSession:
        """Create and configure Spark session"""
        try:
            spark = SparkSession.builder \
                .appName("EcommerceDSS-DataStandardization") \
                .master(master_url) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()

            spark.sparkContext.setLogLevel("WARN")
            logger.info(f"✅ Spark session created successfully")
            logger.info(f"🎯 Spark Master: {master_url}")
            logger.info(f"📊 Available cores: {spark.sparkContext.defaultParallelism}")

            return spark
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session: {e}")
            raise

    def _load_category_mapping(self) -> Dict[str, str]:
        """Load category mapping rules"""
        return {
            # Điện thoại & Smartphone
            r'(điện thoại|smartphone|phone|iphone|samsung|xiaomi|oppo|vivo|realme)': 'Điện thoại di động',

            # Laptop & Máy tính
            r'(laptop|máy tính xách tay|notebook|macbook)': 'Laptop',
            r'(máy tính bảng|tablet|ipad)': 'Máy tính bảng',
            r'(máy tính để bàn|desktop|pc)': 'Máy tính để bàn',

            # Phụ kiện
            r'(tai nghe|headphone|earphone|airpods)': 'Tai nghe',
            r'(loa|speaker|bluetooth)': 'Loa',
            r'(sạc|charger|cáp|cable)': 'Phụ kiện sạc',
            r'(ốp lưng|case|bao da)': 'Phụ kiện bảo vệ',

            # Thiết bị khác
            r'(đồng hồ thông minh|smartwatch|watch)': 'Đồng hồ thông minh',
            r'(tivi|tv|smart tv)': 'Tivi',
            r'(máy ảnh|camera)': 'Máy ảnh',
            r'(man hình|monitor|màn hình)': 'Màn hình máy tính'
        }

    def run_pipeline(self) -> bool:
        """Execute complete data standardization pipeline"""
        try:
            logger.info("🚀 Starting Data Standardization Pipeline...")

            # Step 1: Load and combine raw data
            logger.info("📥 Step 1: Loading raw data sources...")
            raw_data = self.load_raw_data()
            if raw_data.count() == 0:
                logger.warning("⚠️  No data found to process")
                return False

            logger.info(f"📊 Loaded {raw_data.count():,} raw records")

            # Step 2: Data Cleaning
            logger.info("🧹 Step 2: Data Cleaning...")
            cleaned_data = self._data_cleaning(raw_data)
            logger.info(f"✨ Cleaned data: {cleaned_data.count():,} records")

            # Step 3: Data Standardization
            logger.info("📏 Step 3: Data Standardization...")
            standardized_data = self._data_standardization(cleaned_data)
            logger.info(f"📏 Standardized: {standardized_data.count():,} records")

            # Step 4: Data Quality & Dedup
            logger.info("🔍 Step 4: Data Quality & Deduplication...")
            quality_data = self._data_quality_and_dedup(standardized_data)
            logger.info(f"🎯 Quality filtered: {quality_data.count():,} records")

            # Step 5: Synchronize Identifier
            logger.info("🔗 Step 5: Synchronize Identifier...")
            identified_data = self._synchronize_identifier(quality_data)
            logger.info(f"🆔 Identifiers assigned: {identified_data.count():,} records")

            # Step 6: Category Mapping
            logger.info("📂 Step 6: Category Mapping...")
            categorized_data = self._category_mapping(identified_data)
            logger.info(f"📁 Categories mapped: {categorized_data.count():,} records")

            # Step 7: Technical Metadata
            logger.info("📋 Step 7: Technical Metadata...")
            final_data = self._technical_metadata(categorized_data)
            logger.info(f"📄 Metadata added: {final_data.count():,} records")

            # Step 8: Save processed data
            logger.info("💾 Step 8: Saving processed data...")
            self._save_processed_data(final_data)

            # Step 9: Generate summary report
            logger.info("📊 Step 9: Generating summary report...")
            self._generate_summary_report(raw_data, final_data)

            logger.info("✅ Data Standardization Pipeline completed successfully!")
            return True

        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return False
        finally:
            if self.spark:
                self.spark.stop()

    def load_raw_data(self) -> DataFrame:
        """Load raw data with FIXED source detection for Tiki"""
        logger.info(f"Loading raw data from: {self.raw_data_path}")

        all_files = []
        for root, dirs, files in os.walk(self.raw_data_path):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    all_files.append(file_path)

        logger.info(f"Found {len(all_files)} JSON files")

        combined_data = []
        total_records = 0
        source_counts = {}

        for file_path in all_files:
            try:
                file_name = os.path.basename(file_path)
                logger.info(f"Processing file: {file_name}")

                # FIXED: Check specific patterns first! Progress files = TIKI
                if "progress_" in file_name.lower():
                    source = "tiki"
                    logger.info(f"  ✓ TIKI data detected (progress file)")
                elif "fpt" in file_name.lower():
                    source = "fptshop"
                elif "cellphone" in file_name.lower():
                    source = "cellphones"
                elif "mass_crawl" in file_name.lower():
                    source = "multi_source"
                elif "lazada_multi_category" in file_name.lower():
                    source = "lazada_multi"
                elif "lazada" in file_name.lower():
                    source = "lazada"
                else:
                    source = "unknown"

                logger.info(f"  Source assigned: {source}")

                file_records = 0
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            line = line.strip()
                            if line:
                                record = json.loads(line)
                                record['file_source'] = source
                                record['source_file'] = file_name
                                record['line_number'] = line_num
                                combined_data.append(record)
                                file_records += 1
                                total_records += 1
                        except json.JSONDecodeError as e:
                            logger.warning(f"Skipping malformed JSON at line {line_num}")
                            continue
                        except Exception as e:
                            logger.warning(f"Error at line {line_num}: {e}")
                            continue

                source_counts[source] = source_counts.get(source, 0) + file_records
                logger.info(f"  Loaded {file_records} records")

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                continue

        logger.info(f"Total raw records loaded: {total_records}")
        logger.info("Source distribution:")
        for source, count in sorted(source_counts.items()):
            logger.info(f"  {source}: {count} records ({count/total_records*100:.1f}%)")

        if not combined_data:
            logger.error("No data could be loaded!")
            return self.spark.createDataFrame([], schema=StructType([]))

        df = self.spark.createDataFrame(combined_data)
        return df
        # try:
        #     cellphones_files = [
        #         '/app/raw_data/cellphones_test_1762077170.jsonl'
        #     ]
        #     for file_path in cellphones_files:
        #         if os.path.exists(file_path):
        #             cell_df = self.spark.read.json(file_path)
        #             cell_df = cell_df.withColumn("source", lit("cellphones"))
        #             cell_df = cell_df.withColumn("source_file", lit(os.path.basename(file_path)))
        #             all_data.append(cell_df)
        #             logger.info(f"📞 CellphoneS ({os.path.basename(file_path)}): {cell_df.count():,} records")
        # except Exception as e:
        #     logger.warning(f"⚠️  Could not load CellphoneS data: {e}")

        if not all_data:
            logger.error("❌ No data sources could be loaded")
            return self.spark.createDataFrame([], StructType([]))

        # Union all dataframes
        combined_df = all_data[0]
        for df in all_data[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)

        return combined_df

    def _data_cleaning(self, df: DataFrame) -> DataFrame:
        """Step 1: Data Cleaning - Clean and normalize text fields"""

        # Define cleaning UDFs
        def clean_text(text):
            if not text:
                return None
            # Remove extra whitespace and normalize
            cleaned = re.sub(r'\s+', ' ', str(text).strip())
            # Remove special characters but keep Vietnamese
            cleaned = re.sub(r'[^\w\s\-.,()%ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠàáâãèéêìíòóôõùúăđĩũơƯĂẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼỀỀỂưăạảấầẩẫậắằẳẵặẹẻẽềềểếỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪễệỉịọỏốồổỗộớờởỡợụủứừỬỮỰỲỴÝỶỸửữựỳỵýỷỹ]', ' ', cleaned)
            return cleaned.strip() if cleaned.strip() else None

        def clean_price(price_text):
            if not price_text:
                return None
            # Extract numbers from price text
            price_str = str(price_text).replace('.', '').replace(',', '').replace('₫', '').replace('đ', '')
            numbers = re.findall(r'\d+', price_str)
            if numbers:
                return int(''.join(numbers))
            return None

        clean_text_udf = udf(clean_text, StringType())
        clean_price_udf = udf(clean_price, IntegerType())

        # Apply cleaning
        cleaned_df = df \
            .withColumn("title_clean", clean_text_udf(col("title"))) \
            .withColumn("price_clean",
                       when(col("price").isNotNull(), col("price"))
                       .otherwise(clean_price_udf(col("price_text")))) \
            .withColumn("location_clean", clean_text_udf(col("location"))) \
            .withColumn("shop_name_clean", clean_text_udf(col("shop_name"))) \
            .withColumn("discount_clean", clean_text_udf(col("discount")))

        return cleaned_df

    def _data_standardization(self, df: DataFrame) -> DataFrame:
        """Step 2: Data Standardization - Standardize formats and units"""

        # Standardize price to VND
        def standardize_price(price):
            if not price:
                return None
            price_val = float(price)
            # Handle different price ranges (assume USD if < 10000)
            if price_val < 10000:
                price_val *= 25000  # Convert USD to VND approximate
            return int(price_val)

        standardize_price_udf = udf(standardize_price, IntegerType())

        # Standardize location
        def standardize_location(location):
            if not location:
                return "Không xác định"
            location = str(location).strip()
            # Standardize common location names
            location_mapping = {
                'hà nội': 'Hà Nội',
                'ho chi minh': 'Hồ Chí Minh',
                'hcm': 'Hồ Chí Minh',
                'saigon': 'Hồ Chí Minh',
                'hai phong': 'Hải Phòng',
                'hải phòng': 'Hải Phòng',
                'da nang': 'Đà Nẵng',
                'đà nẵng': 'Đà Nẵng'
            }
            location_lower = location.lower()
            for key, value in location_mapping.items():
                if key in location_lower:
                    return value
            return location.title()

        standardize_location_udf = udf(standardize_location, StringType())

        standardized_df = df \
            .withColumn("price_standardized", standardize_price_udf(col("price_clean"))) \
            .withColumn("location_standardized", standardize_location_udf(col("location_clean"))) \
            .withColumn("title_standardized",
                       when(col("title_clean").isNotNull(), col("title_clean"))
                       .otherwise(col("title")))

        return standardized_df

    def _data_quality_and_dedup(self, df: DataFrame) -> DataFrame:
        """Step 3: Data Quality & Deduplication"""

        # Apply quality filters
        quality_df = df.filter(
            (col("title_standardized").isNotNull()) &
            (col("price_standardized").isNotNull()) &
            (col("price_standardized") >= self.quality_thresholds['min_price']) &
            (col("price_standardized") <= self.quality_thresholds['max_price']) &
            (length(col("title_standardized")) >= self.quality_thresholds['min_title_length'])
        )

        # Create deduplication key
        def create_dedup_key(title, price, source):
            if not title:
                return None
            # Normalize title for dedup
            title_normalized = re.sub(r'[^\w\s]', '', str(title).lower())
            title_normalized = re.sub(r'\s+', ' ', title_normalized).strip()
            # Create key combining title and price range
            price_range = (int(price) // 100000) * 100000 if price else 0
            return f"{title_normalized}_{price_range}_{source}"

        create_dedup_key_udf = udf(create_dedup_key, StringType())

        # Apply deduplication
        dedup_df = quality_df \
            .withColumn("dedup_key",
                       create_dedup_key_udf(col("title_standardized"),
                                           col("price_standardized"),
                                           col("source"))) \
            .dropDuplicates(["dedup_key"]) \
            .drop("dedup_key")

        return dedup_df

    def _synchronize_identifier(self, df: DataFrame) -> DataFrame:
        """Step 4: Synchronize Identifier - Create unique product IDs"""

        # Create product hash ID
        def create_product_id(title, price, source, url):
            # Create unique hash from multiple fields
            content = f"{title}_{price}_{source}_{url}".encode('utf-8')
            hash_value = hashlib.md5(content).hexdigest()[:16]
            return f"PROD_{hash_value.upper()}"

        create_product_id_udf = udf(create_product_id, StringType())

        identified_df = df \
            .withColumn("product_id",
                       create_product_id_udf(col("title_standardized"),
                                            col("price_standardized"),
                                            col("source"),
                                            coalesce(col("url"), lit("")))) \
            .withColumn("record_id", monotonically_increasing_id())

        return identified_df

    def _category_mapping(self, df: DataFrame) -> DataFrame:
        """Step 5: Category Mapping - Assign product categories"""

        def classify_category(title):
            if not title:
                return "Khác"

            title_lower = str(title).lower()

            for pattern, category in self.category_mapping.items():
                if re.search(pattern, title_lower):
                    return category

            return "Khác"

        classify_category_udf = udf(classify_category, StringType())

        categorized_df = df \
            .withColumn("category", classify_category_udf(col("title_standardized"))) \
            .withColumn("category_level_1",
                       when(col("category").isin(["Điện thoại di động", "Laptop", "Máy tính bảng", "Máy tính để bàn"]), "Điện tử")
                       .when(col("category").isin(["Tai nghe", "Loa", "Phụ kiện sạc", "Phụ kiện bảo vệ"]), "Phụ kiện")
                       .otherwise("Khác"))

        return categorized_df

    def _technical_metadata(self, df: DataFrame) -> DataFrame:
        """Step 6: Technical Metadata - Add processing metadata"""

        final_df = df \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processing_version", lit("1.0.0")) \
            .withColumn("data_pipeline", lit("ecommerce-dss-standardization")) \
            .withColumn("quality_score",
                       when((col("title_standardized").isNotNull()) &
                            (col("price_standardized").isNotNull()) &
                            (col("category") != "Khác"), 100)
                       .when((col("title_standardized").isNotNull()) &
                             (col("price_standardized").isNotNull()), 80)
                       .otherwise(60))

        return final_df

    def _save_processed_data(self, df: DataFrame):
        """Save processed data to multiple formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save as Parquet (main format)
        parquet_path = f"{self.output_path}/standardized_products_{timestamp}.parquet"
        df.coalesce(4).write.mode("overwrite").parquet(parquet_path)
        logger.info(f"💾 Saved Parquet: {parquet_path}")

        # Save as JSON for compatibility
        json_path = f"{self.output_path}/standardized_products_{timestamp}.json"
        df.coalesce(1).write.mode("overwrite").json(json_path)
        logger.info(f"💾 Saved JSON: {json_path}")

        # Save sample for preview
        sample_path = f"{self.output_path}/sample_standardized_products_{timestamp}.json"
        df.limit(100).coalesce(1).write.mode("overwrite").json(sample_path)
        logger.info(f"📋 Saved sample: {sample_path}")

    def _generate_summary_report(self, raw_df: DataFrame, final_df: DataFrame):
        """Generate processing summary report"""

        # Collect statistics
        raw_count = raw_df.count()
        final_count = final_df.count()

        # Category distribution
        category_dist = final_df.groupBy("category").count().collect()
        source_dist = final_df.groupBy("source").count().collect()

        # Price statistics
        price_stats = final_df.select("price_standardized").describe().collect()

        # Generate report
        report = {
            "pipeline_execution": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "version": "1.0.0",
                "status": "completed"
            },
            "data_summary": {
                "raw_records": raw_count,
                "processed_records": final_count,
                "data_quality_rate": round((final_count / raw_count) * 100, 2) if raw_count > 0 else 0,
                "processing_efficiency": f"{final_count:,} records processed"
            },
            "category_distribution": {row["category"]: row["count"] for row in category_dist},
            "source_distribution": {row["source"]: row["count"] for row in source_dist},
            "price_statistics": {
                "count": int(price_stats[0]["count"]),
                "mean": round(float(price_stats[1]["price_standardized"]), 2),
                "min": int(float(price_stats[3]["price_standardized"])),
                "max": int(float(price_stats[4]["price_standardized"]))
            }
        }

        # Save report
        report_path = f"{self.output_path}/processing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"📊 Processing Summary Report:")
        logger.info(f"   📥 Raw records: {raw_count:,}")
        logger.info(f"   📤 Processed records: {final_count:,}")
        logger.info(f"   📈 Data quality rate: {report['data_summary']['data_quality_rate']}%")
        logger.info(f"   📋 Report saved: {report_path}")

def main():
    """Main execution function"""
    try:
        # Initialize pipeline
        pipeline = DataStandardizationPipeline()

        # Run pipeline
        success = pipeline.run_pipeline()

        if success:
            logger.info("✅ Data standardization completed successfully!")
            exit(0)
        else:
            logger.error("❌ Data standardization failed!")
            exit(1)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        exit(1)

if __name__ == "__main__":
    main()