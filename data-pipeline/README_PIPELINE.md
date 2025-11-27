# Data Pipeline - Staging & Standardization

## Pipeline Flow

```
1. Crawler (DONE) → Raw JSONL files
2. Staging → Load to staging.raw_* tables
3. Data Cleaning → Clean to clean.* tables  
4. Data Quality → Dedup & validate
```

## Usage

### Step 1: Load Raw Data to Staging

```bash
cd data-pipeline/staging
pip install psycopg2-binary
python load_raw_data.py
```

**What it does:**
- Reads JSONL files from `/tmp/data/outputs/lazada/` and `/tmp/data/outputs/lazada_reviews/`
- Creates `staging.raw_products` and `staging.raw_reviews` tables
- Loads raw data into staging tables

### Step 2: Clean & Standardize Data

```bash
cd data-pipeline/standardization
python data_cleaning.py
```

**What it does:**
- Creates `clean.products` and `clean.reviews` tables
- Cleans and standardizes data:
  - Trims whitespace
  - Handles NULL values
  - Creates composite keys
  - Standardizes formats

### Step 3: Data Quality Check

```bash
python data_quality.py
```

**What it does:**
- Checks data quality metrics
- Removes duplicates
- Validates data integrity

## Database Schema

### Staging Schema

```sql
staging.raw_products
- id, source, product_id, product_name
- price_current, rating_avg, review_count
- brand, url, category, crawl_date

staging.raw_reviews  
- id, review_id, product_id
- reviewer_name, rating, review_text
- crawl_timestamp
```

### Clean Schema

```sql
clean.products
- product_key (PK), source, product_id
- product_name, price, rating, review_count
- brand, category, url

clean.reviews
- review_key (PK), product_key (FK)
- reviewer_name, rating, review_text
- review_date
```

## Environment Variables

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_dss_1
DB_USER=dss_user
DB_PASSWORD=dss_password_123
CRAWLER_OUTPUT_DIR=/tmp/data/outputs
```

## Run All Steps

```bash
# Step 1: Staging
python data-pipeline/staging/load_raw_data.py

# Step 2: Cleaning
python data-pipeline/standardization/data_cleaning.py

# Step 3: Quality
python data-pipeline/standardization/data_quality.py
```

## Next Steps

After data is cleaned:
- Category Mapping
- Technical Metadata
- Aggregation (Daily)
- Load to Data Warehouse
