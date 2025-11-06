#!/usr/bin/env python3
"""
E-commerce DSS Data Standardization Pipeline - Fixed Tiki Source Detection
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
    """Data standardization pipeline with fixed Tiki source detection"""

    def __init__(self):
        self.spark = self._create_spark_session()
        self.raw_data_path = '/app/raw_data'
        self.output_path = '/app/processed_data'
        self.category_mapping = self._load_category_mapping()

        # Relaxed quality thresholds
        self.quality_thresholds = {
            'min_price': 100,
            'max_price': 1000000000,
            'min_title_length': 5,
            'required_fields': ['title', 'source']
        }

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session"""
        return SparkSession.builder \
            .appName("EcommerceDSSDataStandardization") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "2") \
            .getOrCreate()

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
            return self.spark.createDataFrame([], schema=StructType([]))

        df = self.spark.createDataFrame(combined_data)
        return df

    def step1_data_cleaning(self, df: DataFrame) -> DataFrame:
        """Data cleaning"""
        logger.info("Step 1: Data Cleaning")

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

        logger.info(f"Cleaning complete: {df_cleaned.count()} records")
        return df_cleaned

    def step2_data_standardization(self, df: DataFrame) -> DataFrame:
        """Data standardization"""
        logger.info("Step 2: Data Standardization")

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

        if 'location' in df_standardized.columns:
            df_standardized = df_standardized.withColumn(
                'standardized_location',
                when(lower(col('location')).contains('hà nội'), 'Hà Nội')
                .when(lower(col('location')).contains('tp.hcm') | lower(col('location')).contains('hồ chí minh'), 'TP.HCM')
                .when(lower(col('location')).contains('đà nẵng'), 'Đà Nẵng')
                .otherwise(upper(col('location')))
            )

        if 'price' in df_standardized.columns:
            df_standardized = df_standardized.withColumn(
                'standardized_price',
                when(col('price').isNull() | (col('price') <= 0), None)
                .otherwise(col('price').cast('double'))
            )

        logger.info(f"Standardization complete: {df_standardized.count()} records")
        return df_standardized

    def step3_quality_and_deduplication(self, df: DataFrame) -> DataFrame:
        """Quality assessment and deduplication"""
        logger.info("Step 3: Quality Assessment and Deduplication")

        def calculate_quality_score(title, price, description, rating, shop_name):
            score = 0
            if title and len(str(title)) >= self.quality_thresholds['min_title_length']:
                score += 30
            elif title and len(str(title)) >= 3:
                score += 15
            if price and self.quality_thresholds['min_price'] <= float(price) <= self.quality_thresholds['max_price']:
                score += 25
            elif price and float(price) > 0:
                score += 10
            if description and len(str(description)) > 10:
                score += 20
            elif description and len(str(description)) > 0:
                score += 10
            if rating and 0 <= float(rating) <= 5:
                score += 15
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

        # More lenient quality filter
        min_quality_score = 20
        df_quality_filtered = df_with_quality.filter(
            col('data_quality_score') >= min_quality_score
        )

        logger.info(f"Quality filtering (score >= {min_quality_score}): {df_quality_filtered.count()} records remain")

        # Simple deduplication
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

        logger.info(f"Deduplication complete: {df_deduplicated.count()} unique records")
        return df_deduplicated

    def add_final_metadata(self, df: DataFrame) -> DataFrame:
        """Add final metadata"""
        logger.info("Adding final metadata")

        df_final = df.withColumn(
            'processing_timestamp', current_timestamp()
        ).withColumn(
            'processing_version', lit('v1.1_tiki_fixed')
        ).withColumn(
            'data_lineage', col('source_file')
        )

        return df_final

    def save_processed_data(self, df: DataFrame, output_name: str = 'standardized_data'):
        """Save processed data"""
        logger.info(f"Saving processed data to: {self.output_path}")

        os.makedirs(self.output_path, exist_ok=True)

        output_parquet = f"{self.output_path}/{output_name}_parquet"
        df.write.mode('overwrite').partitionBy('file_source').parquet(output_parquet)

        output_json = f"{self.output_path}/{output_name}_json"
        df.coalesce(1).write.mode('overwrite').json(output_json)

        logger.info(f"Data saved to {output_parquet} and {output_json}")

    def generate_processing_report(self, df: DataFrame):
        """Generate processing report"""
        logger.info("Generating processing report")

        total_records = df.count()
        logger.info(f"\n=== DATA STANDARDIZATION REPORT ===")
        logger.info(f"Total processed records: {total_records:,}")

        # Source distribution
        logger.info(f"\n--- Source Distribution ---")
        source_dist = df.groupBy('file_source').count().collect()
        for row in sorted(source_dist, key=lambda x: x['count'], reverse=True):
            percentage = (row['count'] / total_records) * 100
            logger.info(f"  {row['file_source']}: {row['count']:,} records ({percentage:.1f}%)")

        # Quality metrics
        logger.info(f"\n--- Quality Metrics ---")
        avg_quality = df.select(col('data_quality_score')).rdd.map(lambda x: x[0]).mean()
        logger.info(f"  Average data quality score: {avg_quality:.1f}/100")

        logger.info(f"\n=== PROCESSING COMPLETE ===")

    def run_pipeline(self):
        """Execute the pipeline"""
        logger.info("Starting E-commerce DSS Data Standardization Pipeline")

        try:
            raw_df = self.load_raw_data()
            if raw_df.count() == 0:
                logger.error("No data loaded. Pipeline aborted.")
                return

            step1_df = self.step1_data_cleaning(raw_df)
            step2_df = self.step2_data_standardization(step1_df)
            step3_df = self.step3_quality_and_deduplication(step2_df)
            final_df = self.add_final_metadata(step3_df)

            self.save_processed_data(final_df)
            self.generate_processing_report(final_df)

            logger.info("Pipeline execution completed successfully")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            if hasattr(self, 'spark'):
                self.spark.stop()

if __name__ == "__main__":
    pipeline = DataStandardizationPipeline()
    pipeline.run_pipeline()