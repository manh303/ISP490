# Review Detail Table Implementation

## Overview

Thêm bảng `fact_reviews_detail` vào Data Warehouse để lưu **nội dung đánh giá chi tiết** của từng review.

## Architecture

### Dòng Chảy Dữ Liệu

```
Raw Reviews Data
    ↓
Clean → Standardize → Sync IDs → Dedup
    ↓
Sentiment Analysis (TextBlob)
    ↓
├─→ fact_review_daily_agg (tổng hợp theo ngày)
├─→ fact_reviews_detail (chi tiết từng review) ← NEW
├─→ dim_reviewer (thông tin reviewer)
└─→ MinIO/processed-reviews (parquet files)
```

## Tables

### 1. fact_reviews_detail (NEW)

**Mục đích**: Lưu nội dung đánh giá chi tiết của từng review

**Schema**:
```sql
CREATE TABLE dwh.fact_reviews_detail (
    review_id VARCHAR(100) NOT NULL,
    global_product_id VARCHAR(100) NOT NULL,
    source_platform_std VARCHAR(50),
    reviewer_name VARCHAR(500),
    rating DOUBLE PRECISION,
    review_text TEXT,
    review_date DATE,
    helpful_count BIGINT DEFAULT 0,
    verified_purchase BOOLEAN DEFAULT FALSE,
    sentiment_score DOUBLE PRECISION DEFAULT 0.0,
    sentiment_label VARCHAR(20),
    review_quality_score DOUBLE PRECISION DEFAULT 0.75,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (review_id, source_platform_std)
);
```

**Columns**:
- `review_id`: ID review duy nhất từ platform
- `global_product_id`: ID sản phẩm chuẩn hóa (tiki_XXX hoặc lazada_XXX)
- `source_platform_std`: Nền tảng (tiki, lazada)
- `reviewer_name`: Tên người đánh giá
- `rating`: Điểm 1-5
- `review_text`: **Nội dung đánh giá (text đầy đủ)**
- `review_date`: Ngày đánh giá
- `helpful_count`: Số người thấy hữu ích
- `verified_purchase`: Mua hàng xác thực
- `sentiment_score`: Điểm cảm xúc (-1.0 đến 1.0) từ TextBlob
- `sentiment_label`: positive/neutral/negative
- `review_quality_score`: Chất lượng review (0.5-1.0)

### 2. fact_review_daily_agg (đã có)

**Mục đích**: Tổng hợp review theo ngày + sản phẩm

**Columns**: total_reviews, avg_rating, star counts, sentiment stats, etc.

## Pipeline Changes

### File: `load_cleaned_from_minio.py`

**Hàm mới**: `load_review_details_to_dwh(df)`

```python
def load_review_details_to_dwh(df):
    """Load individual review details to fact_reviews_detail table"""
    # Select relevant columns
    # Insert to PostgreSQL with upsert logic
    # Handle duplicates by review_id + platform
```

**Gọi trong main pipeline**:
```python
# Step 8.6.5 - Load review details
load_review_details_to_dwh(df_reviews_time)

# Step 8.6 - Load review aggregates
df_reviews_agg = aggregate_reviews_daily(df_reviews_time)
load_review_aggregation_to_dwh(df_reviews_agg)
```

## Usage

### 1. Chạy pipeline
```bash
cd /app/data-pipeline
python -m spark_jobs.load_cleaned_from_minio
```

### 2. Kiểm tra dữ liệu
```bash
python check_review_details.py
```

### 3. Query dữ liệu
```bash
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "SELECT * FROM dwh.fact_reviews_detail LIMIT 5;"
```

## Example Queries

### Lấy review chi tiết cho một sản phẩm
```sql
SELECT 
    reviewer_name,
    rating,
    review_text,
    sentiment_label,
    review_date
FROM dwh.fact_reviews_detail
WHERE global_product_id = 'lazada_2274902846'
ORDER BY review_date DESC;
```

### Tìm review âm tính
```sql
SELECT 
    review_id,
    reviewer_name,
    rating,
    review_text,
    sentiment_score
FROM dwh.fact_reviews_detail
WHERE sentiment_label = 'negative'
ORDER BY sentiment_score ASC
LIMIT 20;
```

### Thống kê sentiment
```sql
SELECT 
    sentiment_label,
    COUNT(*) as count,
    AVG(rating) as avg_rating
FROM dwh.fact_reviews_detail
GROUP BY sentiment_label;
```

## Data Flow Diagram

```
┌─────────────────────┐
│  Raw Reviews JSON   │
│  (Tiki/Lazada)      │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Clean Review Data  │
│  - Trim text        │
│  - Standardize      │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│ Sentiment Analysis  │
│ (TextBlob)          │
├─────────────────────┤
│ sentiment_score     │
│ sentiment_label     │
└──────────┬──────────┘
           │
    ┌──────┴──────────┐
    ↓                 ↓
┌──────────────┐  ┌─────────────┐
│ Detail Table │  │ Agg Table   │
│ (per review) │  │ (per day)   │
│              │  │             │
│ review_id    │  │ agg_date    │
│ review_text  │  │ total_revs  │
│ rating       │  │ avg_rating  │
│ sentiment... │  │ sentiment % │
└──────────────┘  └─────────────┘
     │                │
     └────────┬───────┘
              ↓
       PostgreSQL DWH
         (ecommerce_dss)
```

## Comparison: Detail vs Aggregate

| Aspect | fact_reviews_detail | fact_review_daily_agg |
|--------|--------------------|-----------------------|
| **Granularity** | Per review | Per day + product |
| **Records** | Millions | Thousands |
| **Content** | review_text, reviewer_name | N/A |
| **Use Case** | Detailed analysis, sentiment mining | Dashboard, KPIs |
| **Size** | Large (text data) | Small |
| **Query Speed** | Slower (text search) | Fast (aggregate) |

## Performance Notes

- **Insert Batch Size**: 1000 rows (configurable)
- **Primary Key**: (review_id, source_platform_std)
- **Upsert Logic**: Update rating/text/sentiment if duplicate review_id
- **Indexing**: Consider adding index on global_product_id for common queries
  ```sql
  CREATE INDEX idx_reviews_product ON dwh.fact_reviews_detail(global_product_id);
  ```

## Monitoring

### Check load status
```bash
python check_review_details.py
```

### Monitor table growth
```sql
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'dwh'
AND tablename IN ('fact_reviews_detail', 'fact_review_daily_agg');
```

## Next Steps

1. ✅ Add `fact_reviews_detail` table to pipeline
2. ✅ Load review details with sentiment scores
3. 🔲 Add full-text search index on review_text
4. 🔲 Create dashboard views (TOP negative reviews, etc.)
5. 🔲 Add data quality metrics
