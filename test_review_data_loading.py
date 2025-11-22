#!/usr/bin/env python3
"""
Corrected test script for loading Tiki review data
Loads actual review JSONL files instead of checkpoint file
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, trim, concat, lit
from pyspark.sql.types import DoubleType, LongType, StringType

print('Testing data loading and processing with corrected file path...')

# Initialize Spark session
spark = SparkSession.builder.appName('Test').getOrCreate()

# Load the JSONL file (actual review data, not checkpoint)
json_file = '/app/data/outputs/tiki_reviews/date=2025-11-12/tiki_reviews_20251112_015150.jsonl'
print(f'Loading: {json_file}')

try:
    # Read JSONL file (each line is a JSON object)
    df = spark.read.option('inferSchema', 'true').json(json_file)
    print(f'Loaded {df.count()} records')

    print('Columns:', df.columns)
    print('Schema:')
    df.printSchema()

    print('Sample data:')
    df.show(5, truncate=False)

    # Additional processing test
    print('\n--- Additional Processing ---')

    # Filter valid reviews
    valid_reviews = df.filter(
        col('rating').isNotNull() &
        col('content').isNotNull() &
        (col('content') != '')
    )

    print(f'Valid reviews: {valid_reviews.count()}')

    # Basic statistics
    print('Rating distribution:')
    df.groupBy('rating').count().orderBy('rating').show()

    print('Data processing completed successfully!')

except Exception as e:
    print(f'Error loading data: {e}')
    import traceback
    traceback.print_exc()

finally:

