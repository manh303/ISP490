#!/usr/bin/env python3
"""
E-commerce DSS Data Standardization Pipeline - FIXED VERSION
Với source detection đúng cho Tiki data từ progress files
"""

import os
import json
import logging
import hashlib
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, lower, upper, coalesce,
    lit, udf, split, regexp_extract, current_timestamp,
    monotonically_increasing_id, hash, abs as spark_abs, length
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
    """Data standardization pipeline với source detection cố định cho Tiki"""

    def __init__(self, spark_master_url: str = "spark://spark-master:7077"):
        self.spark = self._create_spark_session(spark_master_url)
        self.raw_data_path = '/app/raw_data'
        self.output_path = '/app/processed_data'

        # Category mapping
        self.category_mapping = self._load_category_mapping()

        # Relaxed quality thresholds để giữ nhiều data hơn
        self.quality_thresholds = {
            'min_price': 100,
            'max_price': 1000000000,
            'min_title_length': 5,
            'required_fields': ['title', 'source']
        }

    def _create_spark_session(self, master_url: str) -> SparkSession:
        """Create Spark session"""
        try:
            spark = SparkSession.builder \
                .appName("EcommerceDSS-FixedTiki") \
                .master(master_url) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.default.parallelism", "2") \
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
        """Load category mapping"""
        return {
            'smartphones': 'electronics_mobile',
            'phone': 'electronics_mobile',
            'mobile': 'electronics_mobile',
            'smartphone': 'electronics_mobile',
            'điện thoại': 'electronics_mobile',
            'laptop': 'electronics_computer',
            'laptops': 'electronics_computer',
            'máy tính': 'electronics_computer',
            'tablet': 'electronics_tablet',
            'ipad': 'electronics_tablet',
            'home': 'home_living',
            'furniture': 'home_living',
            'kitchen': 'home_kitchen',
            'fashion': 'fashion_clothing',
            'clothing': 'fashion_clothing',
            'shoes': 'fashion_footwear',
            'beauty': 'beauty_cosmetics',
            'cosmetics': 'beauty_cosmetics',
            'health': 'health_wellness',
            'books': 'books_media',
            'sports': 'sports_outdoors',
            'automotive': 'automotive_parts',
            'other': 'general_other',
            'unknown': 'general_unknown'
        }

    def _normalize_record_types(self, record: dict) -> dict:
        """Normalize ALL data types to string để tránh schema conflicts trong Spark"""
        normalized = {}

        # Convert tất cả fields thành string để tránh type conflicts
        for key, value in record.items():
            if value is None:
                normalized[key] = None
            elif isinstance(value, (list, dict)):
                # Convert complex types to JSON string
                normalized[key] = json.dumps(value, ensure_ascii=False)
            else:
                # Convert all other types to string
                normalized[key] = str(value)

        return normalized

    def load_raw_data(self) -> DataFrame:
        """Load raw data với source detection ĐÚNG cho Tiki"""
        logger.info(f"📥 Loading raw data from: {self.raw_data_path}")

        all_files = []
        for root, dirs, files in os.walk(self.raw_data_path):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    all_files.append(file_path)

        logger.info(f"📄 Found {len(all_files)} JSON files")

        combined_data = []
        total_records = 0
        source_counts = {}

        for file_path in all_files:
            try:
                file_name = os.path.basename(file_path)
                logger.info(f"🔍 Processing file: {file_name}")

                # ✅ FIXED SOURCE DETECTION - Progress files = TIKI (kiểm tra đầu tiên!)
                if "progress_" in file_name.lower():
                    source = "tiki"
                    logger.info(f"  🎯 TIKI data detected from progress file!")
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

                logger.info(f"  📌 Source assigned: {source}")

                file_records = 0
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        # Đọc toàn bộ file JSON (có thể là array hoặc single object)
                        file_content = f.read().strip()

                        if file_content:
                            data = json.loads(file_content)

                            # Xử lý các trường hợp JSON format khác nhau
                            if isinstance(data, list):
                                # File là JSON array
                                for idx, record in enumerate(data):
                                    if isinstance(record, dict):
                                        # Normalize data types để tránh schema conflicts
                                        record = self._normalize_record_types(record)
                                        record['file_source'] = source
                                        record['source_file'] = file_name
                                        record['record_index'] = idx
                                        combined_data.append(record)
                                        file_records += 1
                                        total_records += 1
                            elif isinstance(data, dict):
                                # File là single JSON object
                                data = self._normalize_record_types(data)
                                data['file_source'] = source
                                data['source_file'] = file_name
                                data['record_index'] = 0
                                combined_data.append(data)
                                file_records += 1
                                total_records += 1

                except json.JSONDecodeError as e:
                    logger.warning(f"  ⚠️  Invalid JSON format in {file_name}: {e}")
                    continue

                source_counts[source] = source_counts.get(source, 0) + file_records
                logger.info(f"  ✅ Loaded {file_records} records")

            except Exception as e:
                logger.error(f"❌ Error processing {file_path}: {e}")
                continue

        logger.info(f"📊 Total raw records loaded: {total_records:,}")

        if total_records > 0:
            logger.info("📈 Source distribution:")
            for source, count in sorted(source_counts.items()):
                logger.info(f"  📋 {source}: {count:,} records ({count/total_records*100:.1f}%)")
        else:
            logger.warning("⚠️  No records loaded from any source!")

        if not combined_data:
            logger.error("❌ No data could be loaded!")
            return self.spark.createDataFrame([], schema=StructType([]))

        # Tạo temporary file để write data ra trước
        import time
        temp_file = f"/tmp/temp_data_{int(time.time())}.json"

        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                for record in combined_data:
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')

            # Read data từ file bằng Spark JSON reader
            df = self.spark.read.option("multiline", "false").json(temp_file)

            # Clean up temporary file
            if os.path.exists(temp_file):
                os.remove(temp_file)

            return df

        except Exception as e:
            logger.error(f"❌ Error creating DataFrame: {e}")
            # Cleanup on error
            if os.path.exists(temp_file):
                os.remove(temp_file)
            return self.spark.createDataFrame([], schema=StructType([]))

    def step1_data_cleaning(self, df: DataFrame) -> DataFrame:
        """Step 1: Data cleaning"""
        logger.info("🧹 Data cleaning...")

        # Fill null values
        df_cleaned = df.fillna({
            'title': 'Unknown Product',
            'description': '',
            'category': 'unknown',
            'brand': 'unknown',
            'price': 0,
            'rating': 0.0,
            'review_count': 0,
            'location': 'unknown',
            'shop_name': 'unknown'
        })

        # Clean text fields
        text_columns = ['title', 'description', 'category', 'brand', 'shop_name', 'location']
        for col_name in text_columns:
            if col_name in df_cleaned.columns:
                df_cleaned = df_cleaned.withColumn(
                    col_name,
                    trim(regexp_replace(col(col_name), r'[\x00-\x1f\x7f-\x9f]', ''))
                )

        # Clean price
        if 'price' in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(
                'price',
                regexp_replace(
                    regexp_replace(col('price').cast('string'), r'[^\d.]', ''),
                    r'\.(?=.*\.)', ''
                ).cast('double')
            )

        logger.info(f"✨ Cleaning complete: {df_cleaned.count():,} records")
        return df_cleaned

    def step2_data_standardization(self, df: DataFrame) -> DataFrame:
        """Step 2: Data standardization"""
        logger.info("📏 Data standardization...")

        category_mapping = self.category_mapping

        def standardize_category(category_str):
            if not category_str:
                return 'general_unknown'
            category_lower = str(category_str).lower().strip()
            if category_lower in category_mapping:
                return category_mapping[category_lower]
            for key, value in category_mapping.items():
                if key in category_lower or category_lower in key:
                    return value
            return 'general_other'

        standardize_category_udf = udf(standardize_category, StringType())

        df_standardized = df.withColumn(
            'standardized_category',
            standardize_category_udf(col('category'))
        )

        # Standardize location
        if 'location' in df_standardized.columns:
            df_standardized = df_standardized.withColumn(
                'standardized_location',
                when(lower(col('location')).contains('hà nội'), 'Hà Nội')
                .when(lower(col('location')).contains('tp.hcm') | lower(col('location')).contains('hồ chí minh'), 'TP.HCM')
                .when(lower(col('location')).contains('đà nẵng'), 'Đà Nẵng')
                .otherwise(upper(col('location')))
            )

        # Standardize price
        if 'price' in df_standardized.columns:
            df_standardized = df_standardized.withColumn(
                'standardized_price',
                when(col('price').isNull() | (col('price') <= 0), None)
                .otherwise(col('price').cast('double'))
            )

        logger.info(f"📐 Standardization complete: {df_standardized.count():,} records")
        return df_standardized

    def step3_quality_and_deduplication(self, df: DataFrame) -> DataFrame:
        """Step 3: Quality assessment and deduplication"""
        logger.info("🔍 Quality assessment and deduplication...")

        def calculate_quality_score(title, price, description, rating, shop_name):
            score = 0
            # Title quality (30 points)
            if title and len(str(title)) >= self.quality_thresholds['min_title_length']:
                score += 30
            elif title and len(str(title)) >= 3:
                score += 15

            # Price validity (25 points)
            if price and self.quality_thresholds['min_price'] <= float(price) <= self.quality_thresholds['max_price']:
                score += 25
            elif price and float(price) > 0:
                score += 10

            # Description quality (20 points)
            if description and len(str(description)) > 10:
                score += 20
            elif description and len(str(description)) > 0:
                score += 10

            # Rating quality (15 points)
            if rating and 0 <= float(rating) <= 5:
                score += 15

            # Shop information (10 points)
            if shop_name and len(str(shop_name)) > 2:
                score += 10

            return min(score, 100)

        quality_udf = udf(calculate_quality_score, IntegerType())

        df_with_quality = df.withColumn(
            'data_quality_score',
            quality_udf(
                col('title'),
                col('standardized_price'),
                col('description'),
                col('rating'),
                col('shop_name')
            )
        )

        # Quality filter với threshold thấp hơn
        min_quality_score = 20  # Relaxed từ 50 xuống 20
        df_quality_filtered = df_with_quality.filter(
            col('data_quality_score') >= min_quality_score
        )

        logger.info(f"🎯 Quality filtering (score >= {min_quality_score}): {df_quality_filtered.count():,} records remain")

        # Simple deduplication based on title và price
        def create_hash_key(title, price, shop_name):
            title_clean = re.sub(r'[^\w\s]', '', str(title or '')).lower().strip()
            price_clean = str(price or '0')
            shop_clean = re.sub(r'[^\w\s]', '', str(shop_name or '')).lower().strip()
            hash_string = f"{title_clean}_{price_clean}_{shop_clean}"
            return hashlib.md5(hash_string.encode()).hexdigest()

        hash_udf = udf(create_hash_key, StringType())

        df_with_hash = df_quality_filtered.withColumn(
            'dedup_hash',
            hash_udf(col('title'), col('standardized_price'), col('shop_name'))
        )

        df_deduplicated = df_with_hash.dropDuplicates(['dedup_hash'])

        logger.info(f"🔗 Deduplication complete: {df_deduplicated.count():,} unique records")
        return df_deduplicated

    def add_final_metadata(self, df: DataFrame) -> DataFrame:
        """Add final metadata"""
        logger.info("📋 Adding final metadata...")

        df_final = df.withColumn(
            'processing_timestamp', current_timestamp()
        ).withColumn(
            'processing_version', lit('v2.0_tiki_fixed')
        ).withColumn(
            'data_lineage', col('source_file')
        ).withColumn(
            'extraction_method', lit('batch_processing')
        )

        return df_final

    def save_processed_data(self, df: DataFrame, output_name: str = 'standardized_data'):
        """Save processed data"""
        logger.info(f"💾 Saving processed data to: {self.output_path}")

        os.makedirs(self.output_path, exist_ok=True)

        # Save as parquet với partitioning theo source
        output_parquet = f"{self.output_path}/{output_name}_parquet"
        df.write.mode('overwrite').partitionBy('file_source').parquet(output_parquet)

        # Save as JSON để compatibility
        output_json = f"{self.output_path}/{output_name}_json"
        df.coalesce(1).write.mode('overwrite').json(output_json)

        # Save simple report
        report_file = f"{self.output_path}/processing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        # Generate report data
        total_records = df.count()
        source_dist = df.groupBy('file_source').count().collect()

        report_data = {
            "pipeline_execution": {
                "timestamp": datetime.now().isoformat(),
                "version": "v2.0_tiki_fixed",
                "status": "completed"
            },
            "data_summary": {
                "total_processed_records": total_records,
                "processing_efficiency": f"{total_records:,} records processed"
            },
            "source_distribution": {row['file_source']: row['count'] for row in source_dist}
        }

        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ Data saved to {output_parquet} and {output_json}")
        logger.info(f"📊 Report saved to {report_file}")

    def generate_processing_report(self, df: DataFrame):
        """Generate comprehensive processing report"""
        logger.info("📊 Generating processing report...")

        total_records = df.count()
        logger.info(f"\n=== 📋 DATA STANDARDIZATION REPORT ===")
        logger.info(f"📊 Total processed records: {total_records:,}")

        # Source distribution
        logger.info(f"\n--- 📈 Source Distribution ---")
        source_dist = df.groupBy('file_source').count().collect()
        for row in sorted(source_dist, key=lambda x: x['count'], reverse=True):
            percentage = (row['count'] / total_records) * 100
            logger.info(f"  📋 {row['file_source']}: {row['count']:,} records ({percentage:.1f}%)")

        # Quality metrics
        logger.info(f"\n--- 🎯 Quality Metrics ---")
        avg_quality = df.select(col('data_quality_score')).rdd.map(lambda x: x[0]).mean()
        logger.info(f"  📈 Average data quality score: {avg_quality:.1f}/100")

        # Show Tiki specifically
        tiki_count = df.filter(col('file_source') == 'tiki').count()
        if tiki_count > 0:
            logger.info(f"\n--- 🎯 TIKI DATA CONFIRMED ---")
            logger.info(f"  ✅ Tiki records found: {tiki_count:,}")
            logger.info(f"  ✅ Percentage of total: {(tiki_count/total_records)*100:.1f}%")

        logger.info(f"\n=== ✅ PROCESSING COMPLETE ===")

    def run_pipeline(self):
        """Execute the complete pipeline"""
        logger.info("🚀 Starting E-commerce DSS Data Standardization Pipeline (TIKI FIXED)")

        try:
            # Step 1: Load raw data
            raw_df = self.load_raw_data()
            if raw_df.count() == 0:
                logger.error("❌ No data loaded. Pipeline aborted.")
                return False

            # Step 2: Data cleaning
            cleaned_df = self.step1_data_cleaning(raw_df)

            # Step 3: Data standardization
            standardized_df = self.step2_data_standardization(cleaned_df)

            # Step 4: Quality and deduplication
            quality_df = self.step3_quality_and_deduplication(standardized_df)

            # Step 5: Add metadata
            final_df = self.add_final_metadata(quality_df)

            # Step 6: Save data
            self.save_processed_data(final_df, 'tiki_fixed_standardized')

            # Step 7: Generate report
            self.generate_processing_report(final_df)

            logger.info("🎉 Pipeline execution completed successfully!")
            return True

        except Exception as e:
            logger.error(f"❌ Pipeline execution failed: {e}")
            raise
        finally:
            if hasattr(self, 'spark'):
                self.spark.stop()

if __name__ == "__main__":
    pipeline = DataStandardizationPipeline()
    pipeline.run_pipeline()