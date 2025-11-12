# Data Pipeline - Following datawarehouse.sql Schema

## Architecture

```
Raw JSONL → STG (stg_raw_*) → ODS (ods_*) → DWH (dwh_*) → DM (dm_*)
```

## Layer 1: STAGING (STG)

**Tables:** `stg_raw_products`, `stg_raw_reviews`

**Purpose:** Raw landing zone with full JSONB payload

```bash
python data-pipeline/staging/load_raw_data.py
```

**What it does:**
- Loads JSONL files into `stg_raw_products` and `stg_raw_reviews`
- Stores complete JSON in `raw_data` JSONB column
- Tracks `load_id` for lineage

## Layer 2: ODS (Operational Data Store)

**Tables:** `ods_platform_ref`, `ods_product_clean`, `ods_price_point`, `ods_review_clean`

**Purpose:** Cleaned, standardized data

```bash
python data-pipeline/standardization/data_cleaning.py
```

**What it does:**
- Extracts data from JSONB in STG
- Creates `global_product_id` (platform_productid)
- Normalizes to ODS tables
- Links to `ods_platform_ref`

## Layer 3: Data Quality

```bash
python data-pipeline/standardization/data_quality.py
```

**What it does:**
- Validates ODS data quality
- Removes duplicates
- Reports metrics

## Database Schema

### STG Layer
```sql
stg_raw_products
- raw_data JSONB (full product JSON)
- source_platform, platform_product_id
- crawled_at, load_id

stg_raw_reviews
- raw_data JSONB (full review JSON)
- source_platform, platform_product_id
- crawled_at, load_id
```

### ODS Layer
```sql
ods_platform_ref
- platform_sk, platform_code (lazada)

ods_product_clean
- global_product_id (PK)
- product_name, brand_name, seller_name

ods_price_point
- global_product_id, platform_sk
- price_current, price_original, discount_percent
- captured_at

ods_review_clean
- global_product_id, platform_sk
- reviewer_name, rating, review_content
- review_time, helpful_count
```

## Run Complete Pipeline

```bash
# Step 1: Load raw data to STG
python data-pipeline/staging/load_raw_data.py

# Step 2: Transform STG → ODS
python data-pipeline/standardization/data_cleaning.py

# Step 3: Quality check
python data-pipeline/standardization/data_quality.py
```

## Next Steps

- [ ] Category mapping (ods_category_taxonomy)
- [ ] Product ID synchronization (ods_product_id_map)
- [ ] DWH dimension tables (dwh_dim_*)
- [ ] DWH fact tables (dwh_fact_*)
- [ ] Data Mart (dm_price_analytics)
