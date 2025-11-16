# DSS Drill-Down Analytics Design

## Tổng Quan

Thiết kế backend cho Interactive Drill-Down Analysis theo workflow analyst:

```
Overall Dashboard (Revenue tổng)
    ↓ (Thấy Lazada giảm 20%)
Platform Dashboard (Lazada chi tiết)
    ↓ (Thấy Electronics giảm)
Category Dashboard (Lazada - Electronics)
    ↓ (Mở table top products)
Product Detail (Brand X - Product analysis)
    - Price changes
    - Stock changes
    - Competitor prices
    - Reviews sentiment
```

---

## Database Đủ Điều Kiện Để Support?

### ✅ SỲ CÓ ĐỦ SCHEMA:

#### 1. **Dimension Tables** (Fact là gì)
- `dwh_dim_product` - Product info ✅
- `dwh_dim_brand` - Brand ✅
- `dwh_dim_category` - Category ✅
- `dwh_dim_platform` - Platform (Lazada, Tiki) ✅
- `dwh_dim_date` - Time dimension ✅

#### 2. **Fact Table** (Metrics/KPIs)
- `dwh_fact_product_daily` - Daily metrics:
  - `price_current` - Current price ✅
  - `price_original` - Original price ✅
  - `discount_pct` - Discount % ✅
  - `is_available` - Availability ✅
  - `sold_count` - Sales count ✅
  - `rating_avg` - Rating ✅
  - `rating_count` - Rating count ✅
  - `review_count` - Review count ✅

#### 3. **Price History** (For price trend analysis)
- `ods_price_point` - Có! Track price per platform per date ✅
  - `platform_sk`, `global_product_id`, `captured_at`
  - `price_current`, `price_original`, `discount_percent`

#### 4. **Availability/Stock** (For stock analysis)
- `dwh_fact_product_daily.is_available` - Có ✅
- `ods_price_point.is_available` - Có ✅

#### 5. **Reviews/Sentiment**
- `dwh_fact_review_summary` - Có! ✅
  - `sentiment_score`, `positive_reviews`, `negative_reviews`
- `ods_review_clean` - Có! ✅
  - `sentiment_score`, `sentiment_label`

### ⚠️ CẦN BỔ SUNG:

#### 1. **Campaign/Promotion Data**
```sql
-- MISSING: Campaign/Promotion table
CREATE TABLE dwh_dim_campaign (
    campaign_sk SERIAL PRIMARY KEY,
    campaign_code VARCHAR(100) UNIQUE,
    campaign_name TEXT,
    campaign_type VARCHAR(50),  -- flash_sale, seasonal, brand_focus
    start_date DATE,
    end_date DATE,
    budget DECIMAL(15,2),
    platform_sk INT
);

CREATE TABLE dwh_fact_campaign_product (
    campaign_sk INT,
    product_sk BIGINT,
    date_sk INT,
    discount_amount DECIMAL(15,2),
    expected_revenue DECIMAL(15,2),
    actual_revenue DECIMAL(15,2),
    PRIMARY KEY (campaign_sk, product_sk, date_sk),
    FOREIGN KEY (campaign_sk) REFERENCES dwh_dim_campaign(campaign_sk),
    FOREIGN KEY (product_sk) REFERENCES dwh_dim_product(product_sk),
    FOREIGN KEY (date_sk) REFERENCES dwh_dim_date(date_sk)
);
```

#### 2. **Orders/Transactions** (For order count and AOV)
```sql
-- MISSING: Order fact table
CREATE TABLE dwh_fact_orders (
    order_sk BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    date_sk INT,
    customer_sk BIGINT,
    product_sk BIGINT,
    platform_sk INT,
    quantity INT,
    unit_price DECIMAL(15,2),
    total_amount DECIMAL(15,2),
    discount_amount DECIMAL(15,2),
    status VARCHAR(50),  -- completed, cancelled, returned
    FOREIGN KEY (date_sk) REFERENCES dwh_dim_date(date_sk),
    FOREIGN KEY (product_sk) REFERENCES dwh_dim_product(product_sk),
    FOREIGN KEY (platform_sk) REFERENCES dwh_dim_platform(platform_sk)
);
```

#### 3. **Inventory/Stock Levels** (Detail stock changes)
```sql
-- MISSING: Detailed inventory
CREATE TABLE dwh_fact_inventory (
    date_sk INT,
    product_sk BIGINT,
    platform_sk INT,
    stock_quantity INT,
    reserved_quantity INT,
    available_quantity INT,
    stock_status VARCHAR(50),  -- in_stock, low_stock, out_of_stock
    days_out_of_stock INT,
    PRIMARY KEY (date_sk, product_sk, platform_sk),
    FOREIGN KEY (date_sk) REFERENCES dwh_dim_date(date_sk),
    FOREIGN KEY (product_sk) REFERENCES dwh_dim_product(product_sk),
    FOREIGN KEY (platform_sk) REFERENCES dwh_dim_platform(platform_sk)
);
```

---

## Backend API Design

### Endpoint 1: Overall Dashboard
```
GET /api/v1/dss/drilldown/overall
Query: start_date, end_date

Response:
{
  "period_label": "2024-11-01 to 2024-11-30",
  "revenue_metrics": {
    "total_revenue": 50000000,
    "previous_period_revenue": 62500000,
    "revenue_change_percent": -20.0,
    "revenue_trend": "decreasing",
    "orders_count": 5000,
    "avg_order_value": 10000
  },
  "top_categories": [
    {
      "category_name": "Electronics",
      "revenue": 25000000,
      "revenue_percent": 50.0,
      "revenue_change_percent": -25.0,
      "avg_rating": 4.2
    }
  ],
  "top_platforms": [
    {
      "platform_name": "Lazada",
      "revenue": 30000000,
      "revenue_percent": 60.0,
      "revenue_change_percent": -20.0
    }
  ],
  "key_alerts": [
    {
      "type": "warning",
      "severity": "high",
      "title": "Overall Revenue Declined 20%",
      "action": "Drill down to platform level"
    }
  ]
}
```

### Endpoint 2: Platform Drill-Down
```
GET /api/v1/dss/drilldown/platform/{platform_code}
Query: start_date, end_date

Response:
{
  "platform_name": "Lazada",
  "platform_revenue_metrics": {
    "total_revenue": 30000000,
    "revenue_change_percent": -20.0
  },
  "top_categories": [
    {
      "category_name": "Electronics",
      "revenue": 15000000,
      "revenue_percent": 50.0,
      "revenue_change_percent": -25.0,
      "out_of_stock_count": 45
    },
    {
      "category_name": "Home",
      "revenue": 9000000,
      "revenue_percent": 30.0,
      "revenue_change_percent": -5.0
    }
  ],
  "problematic_categories": [
    {
      "category_name": "Electronics",
      "revenue": 15000000,
      "change_percent": -25.0,
      "out_of_stock_count": 45
    }
  ]
}
```

### Endpoint 3: Category Drill-Down
```
GET /api/v1/dss/drilldown/category/{category_code}
Query: platform_code, start_date, end_date

Response:
{
  "category_name": "Electronics",
  "category_revenue_metrics": {
    "total_revenue": 15000000,
    "revenue_change_percent": -25.0
  },
  "top_brands": [
    {
      "brand_name": "Brand X",
      "revenue": 5000000,
      "revenue_percent": 33.3,
      "product_count": 120,
      "avg_price": 3500000
    }
  ],
  "top_products": [
    {
      "global_product_id": "prod_123",
      "product_name": "Laptop Model A",
      "brand_name": "Brand X",
      "current_price": 4000000,
      "previous_price": 3500000,
      "price_change_percent": 14.3,
      "is_available": false,
      "out_of_stock_reason": "price_increased",
      "sold_count": 500
    }
  ],
  "price_changes": [
    {
      "product_name": "Laptop Model A",
      "price_change_percent": 14.3,
      "previous_price": 3500000,
      "current_price": 4000000
    }
  ],
  "out_of_stock_products": [
    {
      "product_name": "Laptop Model A",
      "reason": "price_increased",
      "sold_count": 0
    }
  ]
}
```

### Endpoint 4: Product Detail
```
GET /api/v1/dss/drilldown/product/{global_product_id}
Query: platform_code, days=30

Response:
{
  "product_info": {
    "global_product_id": "prod_123",
    "product_name": "Laptop Model A",
    "brand_name": "Brand X",
    "current_price": 4000000,
    "price_change_percent": 14.3,
    "is_available": false,
    "sold_count": 500,
    "avg_rating": 4.5
  },
  "price_history": [
    {
      "date": "2024-11-01",
      "price": 3500000,
      "discount_percent": 5.0
    },
    {
      "date": "2024-11-15",
      "price": 4000000,
      "discount_percent": 0.0
    }
  ],
  "availability_history": [
    {
      "date": "2024-11-01",
      "available": true
    },
    {
      "date": "2024-11-16",
      "available": false,
      "reason": "Out of Stock"
    }
  ],
  "competitor_prices": [
    {
      "platform": "Tiki",
      "price": 3800000
    },
    {
      "platform": "FPTShop",
      "price": 3900000
    }
  ],
  "reviews_summary": {
    "avg_rating": 4.5,
    "total_reviews": 1200,
    "positive_reviews": 900,
    "negative_reviews": 100
  }
}
```

---

## Database Views Tạo Ra

### 1. **Revenue Analysis Views**
- `v_daily_revenue_by_platform` - Daily revenue per platform
- `v_daily_revenue_by_category` - Daily revenue per category
- `v_daily_revenue_platform_category` - Cross-dimension (platform + category)

### 2. **Product-Level Views**
- `v_product_daily_metrics` - All product metrics per day
- `v_price_changes` - Price history with lag analysis
- `v_availability_changes` - Stock status changes

### 3. **Brand-Level Views**
- `v_daily_revenue_by_brand` - Brand revenue tracking
- `v_brand_platform_performance` - Brand perf by platform

### 4. **Alert Views** (Auto-detect issues)
- `v_alert_price_increase` - Products with price increases >10%
- `v_alert_out_of_stock` - Out of stock products
- `v_alert_category_decline` - Categories with revenue decline >20%

### 5. **Summary Views**
- `v_monthly_revenue_platform` - Monthly summary
- `v_top_products_revenue` - Top 100 products

---

## Key SQL Queries Required

### 1. Overall Revenue (Current vs Previous Period)
```sql
-- Current period
SELECT SUM(price_current * sold_count) as total_revenue
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
WHERE d.date_value >= :start_date AND d.date_value <= :end_date;

-- Previous period (same duration)
SELECT SUM(price_current * sold_count) as prev_revenue
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
WHERE d.date_value >= :prev_start AND d.date_value <= :prev_end;
```

### 2. Revenue by Platform
```sql
SELECT 
    pl.platform_code,
    pl.platform_name,
    SUM(price_current * sold_count) as platform_revenue
FROM dwh_fact_product_daily f
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
WHERE d.date_value >= :start_date AND d.date_value <= :end_date
GROUP BY pl.platform_code, pl.platform_name
ORDER BY platform_revenue DESC;
```

### 3. Price Change Detection (7 days vs 30 days)
```sql
WITH price_history AS (
    SELECT 
        p.global_product_id,
        p.product_name,
        pl.platform_code,
        d.date_value,
        f.price_current,
        LAG(f.price_current, 7) OVER (
            PARTITION BY p.product_sk, pl.platform_sk 
            ORDER BY d.date_value
        ) as price_7days_ago
    FROM dwh_fact_product_daily f
    JOIN dwh_dim_product p ON f.product_sk = p.product_sk
    JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
    JOIN dwh_dim_date d ON f.date_sk = d.date_sk
)
SELECT 
    global_product_id,
    product_name,
    platform_code,
    date_value,
    price_current,
    price_7days_ago,
    ROUND(((price_current - price_7days_ago) / price_7days_ago * 100)::NUMERIC, 2) as price_change_pct
FROM price_history
WHERE price_7days_ago IS NOT NULL AND price_current > price_7days_ago;
```

### 4. Out of Stock Detection
```sql
SELECT 
    p.global_product_id,
    p.product_name,
    pl.platform_code,
    cat.category_name,
    COUNT(*) as consecutive_oos_days
FROM dwh_fact_product_daily f
JOIN dwh_dim_product p ON f.product_sk = p.product_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
JOIN dwh_dim_category cat ON p.category_sk = cat.category_sk
WHERE f.is_available = FALSE
  AND f.date_sk >= :start_date_sk
GROUP BY p.global_product_id, p.product_name, pl.platform_code, cat.category_name
HAVING COUNT(*) > 3;  -- Out of stock for 3+ days
```

### 5. Competitor Price Comparison
```sql
SELECT 
    p.global_product_id,
    p.product_name,
    pl.platform_code,
    pl.platform_name,
    f.price_current,
    f.rating_avg,
    ROW_NUMBER() OVER (PARTITION BY p.global_product_id ORDER BY f.price_current) as price_rank
FROM dwh_fact_product_daily f
JOIN dwh_dim_product p ON f.product_sk = p.product_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
WHERE p.global_product_id = :global_product_id
  AND f.date_sk = :latest_date_sk;
```

---

## Implementation Roadmap

### Phase 1: Create Views (SQL)
1. Create drill-down views (v_daily_revenue_by_platform, etc.)
2. Create alert views (v_alert_price_increase, v_alert_oos, etc.)
3. Optimize indexes for performance

### Phase 2: Backend API
1. Implement `/overall` endpoint
2. Implement `/platform/{code}` endpoint
3. Implement `/category/{code}` endpoint
4. Implement `/product/{id}` endpoint
5. Add error handling and logging

### Phase 3: Database Schema Extensions (Optional but Recommended)
1. Add `dwh_dim_campaign` for campaign tracking
2. Add `dwh_fact_orders` for order-level metrics
3. Add `dwh_fact_inventory` for detailed stock tracking
4. Create data quality checks

### Phase 4: Frontend Integration
1. Build Overall Dashboard UI
2. Implement drill-down navigation
3. Add filters (date, platform, category)
4. Add real-time alerts notification
5. Add export functionality (PDF, CSV)

---

## Performance Optimization

### 1. Indexes Cần Tạo
```sql
-- Date-based queries
CREATE INDEX idx_product_daily_date ON dwh_fact_product_daily(date_sk DESC, platform_sk, product_sk);

-- Platform-based queries
CREATE INDEX idx_product_daily_platform ON dwh_fact_product_daily(platform_sk, date_sk DESC);

-- Category-based queries
CREATE INDEX idx_product_category ON dwh_dim_product(category_sk, is_current);

-- Price change queries
CREATE INDEX idx_price_point_date ON ods_price_point(captured_at DESC, product_sk, platform_sk);

-- Availability queries
CREATE INDEX idx_price_point_availability ON ods_price_point(is_available, captured_at DESC);
```

### 2. Query Optimization
- Use window functions (LAG, ROW_NUMBER) for trend analysis
- Pre-calculate monthly/weekly aggregates
- Cache frequently accessed views
- Use pagination for large result sets

### 3. Materialized Views (For heavy queries)
```sql
CREATE MATERIALIZED VIEW mv_monthly_revenue_by_platform AS
SELECT 
    d.year,
    d.month,
    pl.platform_code,
    SUM(CAST(f.price_current * f.sold_count AS DECIMAL)) as monthly_revenue
FROM dwh_fact_product_daily f
JOIN dwh_dim_date d ON f.date_sk = d.date_sk
JOIN dwh_dim_platform pl ON f.platform_sk = pl.platform_sk
GROUP BY d.year, d.month, pl.platform_code;

REFRESH MATERIALIZED VIEW mv_monthly_revenue_by_platform;
```

---

## Summary: Database Condition Assessment

| Item | Status | Note |
|------|--------|------|
| Dimension Tables | ✅ Full | All required dimensions exist |
| Fact Table (Daily) | ✅ Adequate | Has price, availability, sales |
| Price History | ✅ Full | ods_price_point tracks history |
| Reviews/Sentiment | ✅ Full | Multiple tables support this |
| Campaign Data | ⚠️ Missing | Need campaign dimension |
| Orders/Transactions | ⚠️ Missing | Need detailed order facts |
| Inventory Levels | ⚠️ Limited | Only availability flag, no quantity |

**Verdict**: Database **CÓ ĐỦ** để implement drill-down analytics. 
Missing items (campaign, orders) là optional enhancements, không bắt buộc cho core functionality.

Files created:
1. `/backend/app/api/v1/dss_drilldown.py` - Complete API endpoints
2. `/database/views/dss_drilldown_views.sql` - Supporting views and indexes
