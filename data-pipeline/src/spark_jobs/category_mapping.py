"""
Spark Category Mapping Job
Maps raw categories to standardized taxonomy
"""

import sys
from pyspark.sql import SparkSession, functions as F
import logging
from datetime import datetime
import argparse
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Spark Category Mapping Job')
    parser.add_argument('--input-path', required=True, help='Input data path')
    parser.add_argument('--output-path', required=True, help='Output data path')
    parser.add_argument('--mapping-config', required=True, help='Category mapping config file')
    parser.add_argument('--minio-endpoint', default='minio:9000', help='MinIO endpoint')
    parser.add_argument('--minio-access-key', default='minioadmin', help='MinIO access key')
    parser.add_argument('--minio-secret-key', default='minioadmin', help='MinIO secret key')
    return parser.parse_args()

def create_spark_session(args):
    spark = SparkSession.builder \
        .appName('CategoryMapping') \
        .config('spark.hadoop.fs.s3a.endpoint', f'http://{args.minio_endpoint}') \
        .config('spark.hadoop.fs.s3a.access.key', args.minio_access_key) \
        .config('spark.hadoop.fs.s3a.secret.key', args.minio_secret_key) \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel('INFO')
    return spark

def load_mapping_config(config_path):
    """Load category mapping configuration from JSON file"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f'Loaded mapping config from {config_path}')
        return config
    except FileNotFoundError:
        logger.warning(f'Config file {config_path} not found. Using default mapping.')
        return {
            'mappings': {
                'Electronics': ['điện tử', 'electronics', 'máy tính', 'computer', 'phone'],
                'Fashion': ['thời trang', 'fashion', 'quần áo', 'clothing', 'giày'],
                'Books': ['sách', 'books', 'ebook', 'tạp chí'],
                'Home': ['nhà cửa', 'home', 'đồ dùng gia đình', 'furniture'],
            }
        }

def create_mapping_lookup(spark, config):
    """Create a lookup DataFrame for category mapping"""
    mapping_list = []
    
    for standard_cat, raw_categories in config.get('mappings', {}).items():
        for raw_cat in raw_categories:
            mapping_list.append({
                'raw_category': raw_cat.lower(),
                'standard_category': standard_cat
            })
    
    mapping_df = spark.createDataFrame(mapping_list)
    return mapping_df

def map_categories(df, mapping_config, spark):
    """Map raw categories to standard categories"""
    
    if 'category' not in df.columns:
        logger.warning('Category column not found')
        return df
    
    # Create mapping lookup
    mapping_df = create_mapping_lookup(spark, mapping_config)
    
    # Normalize input categories for matching
    df = df.withColumn('category_normalized', F.lower(F.col('category')))
    
    # Join with mapping
    df = df.join(
        mapping_df,
        F.col('category_normalized') == F.col('raw_category'),
        'left'
    )
    
    # For unmapped categories, use original or assign 'Other'
    df = df.withColumn(
        'category_standardized',
        F.coalesce(F.col('standard_category'), F.lit('Other'))
    )
    
    # Count unmapped categories
    unmapped_count = df.filter(F.col('standard_category').isNull()).count()
    if unmapped_count > 0:
        logger.warning(f'Found {unmapped_count} unmapped categories')
    
    # Drop temporary columns
    df = df.drop('category_normalized', 'raw_category', 'standard_category')
    df = df.withColumnRenamed('category_standardized', 'category')
    
    return df

def add_category_hierarchy(df):
    """Add category hierarchy (level 1, level 2, etc.)"""
    # Define hierarchy mapping
    hierarchy_map = {
        'Electronics': {'level1': 'Technology', 'level2': 'Electronics'},
        'Fashion': {'level1': 'Apparel', 'level2': 'Fashion'},
        'Books': {'level1': 'Media', 'level2': 'Books'},
        'Home': {'level1': 'Home & Living', 'level2': 'Home'},
        'Other': {'level1': 'Uncategorized', 'level2': 'Other'},
    }
    
    # Create hierarchy DataFrame
    hierarchy_list = []
    for category, hierarchy in hierarchy_map.items():
        hierarchy_list.append({
            'category': category,
            'category_level1': hierarchy['level1'],
            'category_level2': hierarchy['level2']
        })
    
    hierarchy_df = spark.createDataFrame(hierarchy_list)
    
    # Join with main data
    df = df.join(hierarchy_df, 'category', 'left')
    
    # Fill nulls
    df = df.fillna({
        'category_level1': 'Uncategorized',
        'category_level2': 'Other'
    })
    
    return df

def add_mapping_metadata(df):
    """Add mapping metadata"""
    df = df.withColumn('category_mapped_at', F.current_timestamp())
    df = df.withColumn('mapping_version', F.lit('1.0'))
    
    return df

def main():
    args = parse_arguments()
    logger.info(f'Starting Category Mapping Job')
    logger.info(f'Input: {args.input_path}')
    logger.info(f'Mapping config: {args.mapping_config}')
    
    spark = create_spark_session(args)
    
    try:
        # Load mapping config
        mapping_config = load_mapping_config(args.mapping_config)
        
        # Read input data
        df = spark.read.option('recursiveFileLookup', 'true') \
            .json(args.input_path) \
            .cache()
        
        before_count = df.count()
        logger.info(f'Input records: {before_count}')
        
        # Apply category mapping
        df = map_categories(df, mapping_config, spark)
        
        # Add category hierarchy
        # Note: hierarchy addition requires spark context passed
        global_spark = spark
        
        # Create hierarchy manually
        hierarchy_map = {
            'Electronics': {'level1': 'Technology', 'level2': 'Electronics'},
            'Fashion': {'level1': 'Apparel', 'level2': 'Fashion'},
            'Books': {'level1': 'Media', 'level2': 'Books'},
            'Home': {'level1': 'Home & Living', 'level2': 'Home'},
            'Other': {'level1': 'Uncategorized', 'level2': 'Other'},
        }
        
        hierarchy_list = []
        for category, hierarchy in hierarchy_map.items():
            hierarchy_list.append({
                'category': category,
                'category_level1': hierarchy['level1'],
                'category_level2': hierarchy['level2']
            })
        
        hierarchy_df = spark.createDataFrame(hierarchy_list)
        df = df.join(hierarchy_df, 'category', 'left')
        df = df.fillna({
            'category_level1': 'Uncategorized',
            'category_level2': 'Other'
        })
        
        # Add metadata
        df = add_mapping_metadata(df)
        
        # Log category statistics
        category_stats = df.groupBy('category').count().collect()
        logger.info('=== Category Distribution ===')
        for row in category_stats:
            logger.info(f'  {row[0]}: {row[1]} records')
        
        final_count = df.count()
        logger.info(f'Output records: {final_count}')
        
        # Write output
        df.write.mode('overwrite').format('json') \
            .option('path', args.output_path) \
            .save()
        
        logger.info(f'Category mapping completed successfully')
        logger.info(f'Output saved to: {args.output_path}')
        
    except Exception as e:
        logger.error(f'Error in category mapping: {str(e)}', exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
