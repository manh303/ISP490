# Review Data Processing Pipeline - Summary

## Overview
All review processing functionality has been consolidated into the main Spark pipeline file: **`load_cleaned_from_minio.py`**

This unified approach handles both product and review data in a single pipeline execution.

## Architecture

### Data Sources
1. **Local directories**: `/app/data/outputs/tiki_reviews/` and `/app/data/outputs/lazada_reviews/`
   - Format: JSON files
   - Contains: review_id, product_id, rating, review_text, reviewer_name, etc.

2. **MinIO backup**: `s3a://reviews-data/`
   - Used if local directories are unavailable

### Pipeline Steps

#### STEP 8: Load Review Data
- **Function**: `load_review_data(spark)`
- **Input**: JSON files from Tiki and Lazada local directories
- **Output**: Combined Spark DataFrame with all reviews
- **Adds**: `source_platform` column (tiki/lazada)

#### STEP 8.1: Clean Review Data
- **Function**: `clean_review_data(df_reviews)`
- Standardizes fields:
  - `review_id`: String, non-null
  - `product_id`: String, non-null
  - `reviewer_name`: String (defaults to "Anonymous")
  - `rating`: Double (defaults to 0.0)
  - `review_text`: String (defaults to "")
  - `review_date`: Timestamp
  - `helpful_count`: Long (defaults to 0)
  - `verified_purchase`: Boolean (defaults to False)

#### STEP 8.2: Sentiment Analysis
- **Function**: `analyze_sentiment(df)`
- Uses TextBlob for natural language processing
- Outputs:
  - `sentiment_score`: Polarity score (-1 to 1)
  - `sentiment_label`: "negative", "neutral", "positive"
  - `is_positive_review`: Binary flag (1 if score > 0.1)
  - `is_negative_review`: Binary flag (1 if score < -0.1)
  - `is_neutral_review`: Binary flag

#### STEP 8.3: Add Time Features
- **Function**: `add_review_time_features(df)`
- Extracts temporal dimensions:
  - `review_date_fmt`: Date only
  - `review_year`: Year
  - `review_month`: Month (1-12)
  - `review_day`: Day of month (1-31)
  - `review_dow`: Day of week (1=Sunday, 7=Saturday)

#### STEP 8.4: Aggregate Reviews by Product
- **Function**: `aggregate_reviews_by_product(df)`
- Group by: `product_id`, `source_platform`
- Metrics calculated:
  - `total_reviews`: Count of all reviews
  - `avg_rating`: Average star rating
  - `five_star_count`, `four_star_count`, etc.: Distribution
  - `avg_sentiment_score`: Average sentiment polarity
  - `positive_reviews`, `negative_reviews`, `neutral_reviews`: Counts
  - `total_helpful_count`: Sum of helpful votes
  - `verified_reviews`: Count of verified purchases
  - `positive_sentiment_pct`: Percentage of positive reviews
  - `negative_sentiment_pct`: Percentage of negative reviews
  - `verified_purchase_pct`: Percentage verified

#### STEP 8.5: Save Review Results
- **Function**: `save_review_results(df_reviews, df_agg)`
- Outputs:
  - **Local Parquet**: `/tmp/reviews_processed_{timestamp}/`
    - `cleaned_reviews/`: Full cleaned review data
    - `reviews_by_product/`: Aggregated metrics per product
  - **MinIO**: `s3a://processed-reviews/reviews_{timestamp}/`
    - Same structure as local output

## Integration with Main Pipeline

The review pipeline executes **after** the product/DWH pipeline:

```
PRODUCT PIPELINE:
1. Load raw data
2. Clean & transform
3. Map categories
4. Standardize
5. Synchronize IDs
6. Deduplicate
7. Load dimensions to DWH
8. Aggregate to fact table
9. Save cleaned data

REVIEW PIPELINE: (Steps 8.x)
1. Load review data
2. Clean reviews
3. Sentiment analysis
4. Add time features
5. Aggregate by product
6. Save results
```

## Configuration

Set these environment variables (in `.env`):

```bash
# MinIO for processed reviews
MINIO_PROCESSED_REVIEWS_BUCKET=processed-reviews

# Data sources
CRAWLER_OUTPUT_DIR=/app/data/outputs
```

## Dependencies

Core dependencies:
- `pyspark==3.5.0`
- `pandas==2.0.3`
- `textblob==0.17.1` (for sentiment analysis)
- `minio==7.2.0` (for MinIO uploads)
- `psycopg2-binary==2.9.7` (for PostgreSQL)

Install: `pip install -r deployment/spark/requirements.txt`

## Output Structure

### MinIO Storage
```
s3a://processed-reviews/
  reviews_YYYYMMDD_HHMMSS/
    cleaned_reviews/
      part-00000.parquet
      part-00001.parquet
      ...
    reviews_by_product/
      part-00000.parquet
      part-00001.parquet
      ...
```

### Data Schemas

**cleaned_reviews** columns:
- review_id, product_id, source_platform
- reviewer_name, rating, review_text
- review_date, review_date_fmt
- helpful_count, verified_purchase
- sentiment_score, sentiment_label
- is_positive_review, is_negative_review, is_neutral_review
- review_year, review_month, review_day, review_dow

**reviews_by_product** columns:
- product_id, source_platform
- total_reviews, avg_rating
- five_star_count, four_star_count, three_star_count, two_star_count, one_star_count
- avg_sentiment_score
- positive_reviews, negative_reviews, neutral_reviews
- total_helpful_count, verified_reviews
- positive_sentiment_pct, negative_sentiment_pct, verified_purchase_pct

## Running the Pipeline

```bash
# From project root
python data-pipeline/src/spark_jobs/load_cleaned_from_minio.py
```

The unified pipeline will:
1. Process product data → DWH + MinIO
2. Process review data → MinIO (aggregated by product)
3. Generate execution logs

## Monitoring

Check logs for:
- Data quality statistics
- Sentiment distribution
- Product aggregation counts
- MinIO upload confirmations

Example output:
```
============================================================
 STEP 8.2: SENTIMENT ANALYSIS
============================================================
 ✓ Sentiment Distribution:
   NEGATIVE  :      12,345
   NEUTRAL   :      45,678
   POSITIVE  :      89,012

============================================================
 STEP 8.4: AGGREGATING REVIEWS BY PRODUCT
============================================================
 ✓ Generated aggregates for 2,156 products
```

## Troubleshooting

1. **No review data found**: Ensure directories exist
   - `/app/data/outputs/tiki_reviews/`
   - `/app/data/outputs/lazada_reviews/`

2. **TextBlob import error**: Install with `pip install textblob`

3. **MinIO upload fails**: Check bucket exists and credentials are correct

4. **Memory errors**: Reduce coalesce partitions in save functions

## Future Enhancements

- Load aggregated review metrics to DWH
- Review-product join enrichment
- Category-level sentiment analysis
- Temporal sentiment trends
- Review text keyword extraction
