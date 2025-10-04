# Cấu Trúc Data Warehouse E-commerce Việt Nam

## 🇻🇳 Tổng Quan

Data Warehouse được thiết kế chuyên biệt cho thị trường e-commerce Việt Nam, tập trung 100% vào dữ liệu và đặc thù kinh doanh trong nước.

## 📊 Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────────┐
│                 VIETNAM E-COMMERCE DATA WAREHOUSE              │
│                        PostgreSQL Database                     │
└─────────────────────────────────────────────────────────────────┘

Schema: vietnam_dw
├── 📅 Dimension Tables (5 tables)
│   ├── dim_time                 # Thời gian với lễ tết VN
│   ├── dim_geography_vn         # 63 tỉnh thành Việt Nam
│   ├── dim_customer_vn          # Khách hàng Việt Nam
│   ├── dim_product_vn           # Sản phẩm e-commerce VN
│   └── dim_platform_vn          # Nền tảng VN (Shopee, Lazada...)
│
├── 📈 Fact Tables (3 tables)
│   ├── fact_sales_vn            # Fact bán hàng chính
│   ├── fact_inventory_vn        # Tồn kho
│   └── fact_customer_activity_vn # Hoạt động khách hàng
│
├── 📊 Aggregate Tables (2 tables)
│   ├── agg_daily_sales_vn       # Tổng hợp ngày
│   └── agg_monthly_platform_performance_vn # Hiệu suất platform
│
└── 🔍 Views & Materialized Views
    ├── vw_vietnam_sales_summary
    ├── vw_vietnam_customer_analytics
    └── mv_monthly_sales_summary
```

## 🏗️ Chi Tiết Các Bảng

### 📅 1. DIM_TIME - Chiều Thời Gian Việt Nam

**Mục đích**: Chứa thông tin thời gian với các ngày lễ, tết và mùa mua sắm đặc trưng Việt Nam

**Đặc điểm Việt Nam**:
- Tết Nguyên Đán (mùa cao điểm)
- Lễ 30/4, 1/5, 2/9
- Ngày mua sắm 9/9, 10/10, 11/11, 12/12
- Tháng Cô Hồn (tháng 7 âm lịch)

```sql
CREATE TABLE vietnam_dw.dim_time (
    time_key SERIAL PRIMARY KEY,
    date_value DATE UNIQUE NOT NULL,

    -- Phân cấp thời gian
    day_name_vietnamese VARCHAR(20),     -- Thứ Hai, Thứ Ba...
    month_name_vietnamese VARCHAR(20),   -- Tháng Một, Tháng Hai...
    quarter_name_vietnamese VARCHAR(15), -- Quý 1, Quý 2...

    -- Lễ tết Việt Nam
    is_vietnamese_holiday BOOLEAN,
    vietnamese_holiday_name VARCHAR(100),
    is_tet_season BOOLEAN,              -- Mùa Tết
    is_ghost_month BOOLEAN,             -- Tháng Cô Hồn
    is_shopping_festival BOOLEAN,       -- 9/9, 10/10, 11/11, 12/12
    shopping_festival_name VARCHAR(50)
);
```

**Dữ liệu mẫu**:
- 2020-2030: 4,018 ngày
- 365 ngày lễ Việt Nam
- 44 ngày mua sắm lớn

### 🗺️ 2. DIM_GEOGRAPHY_VN - Địa Lý Việt Nam

**Mục đích**: Phân tích theo 63 tỉnh thành Việt Nam với 3 miền Bắc-Trung-Nam

**Cấu trúc hành chính**:
- **3 miền**: Bắc (MB), Trung (MT), Nam (MN)
- **63 tỉnh thành**: Bao gồm 5 thành phố trực thuộc TW
- **Logistics**: Tích hợp với GHTK, GHN, VNPost, J&T

```sql
CREATE TABLE vietnam_dw.dim_geography_vn (
    geography_key SERIAL PRIMARY KEY,

    -- Phân cấp hành chính VN
    region_code VARCHAR(10),             -- MB, MT, MN
    region_name_vietnamese VARCHAR(50),  -- Miền Bắc, Miền Trung, Miền Nam
    province_code VARCHAR(10),           -- HN, HCM, DN, HP...
    province_name_vietnamese VARCHAR(100), -- Hà Nội, TP.HCM...

    -- Mã bưu chính VN (6 số)
    postal_code VARCHAR(6),

    -- Thương mại điện tử
    ecommerce_penetration_rate DECIMAL(5,2),
    is_major_ecommerce_hub BOOLEAN,
    average_delivery_time_days INTEGER,
    cod_availability BOOLEAN DEFAULT TRUE
);
```

**Dữ liệu tích hợp**:
- 63 tỉnh thành đầy đủ
- Hub e-commerce: HN, HCM, DN, HP, CT
- Thời gian giao hàng trung bình từng vùng

### 👥 3. DIM_CUSTOMER_VN - Khách Hàng Việt Nam

**Mục đích**: Phân tích khách hàng Việt Nam với hành vi mua sắm địa phương

**Đặc điểm Việt Nam**:
- Số điện thoại format +84
- Phương thức thanh toán VN (COD, MoMo, ZaloPay)
- Phân khúc thu nhập VND
- Hành vi văn hóa Việt Nam

```sql
CREATE TABLE vietnam_dw.dim_customer_vn (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE,

    -- Thông tin cá nhân
    full_name_vietnamese VARCHAR(200),
    phone_number VARCHAR(20),           -- +84xxxxxxxxx
    geography_key INTEGER REFERENCES dim_geography_vn,

    -- Phân khúc VN
    customer_segment_vietnamese VARCHAR(50), -- Khách hàng thường/VIP/cao cấp
    income_level_vietnamese VARCHAR(30),     -- Thu nhập thấp/trung bình/cao
    monthly_income_vnd DECIMAL(12,0),        -- VND currency

    -- Thanh toán VN
    preferred_payment_method VARCHAR(50),    -- COD, MoMo, ZaloPay, VNPay
    uses_ewallet BOOLEAN,
    uses_cod BOOLEAN,

    -- Đặc điểm văn hóa VN
    vietnam_region_preference VARCHAR(20),
    celebrates_vietnamese_holidays BOOLEAN,

    -- SCD Type 2
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);
```

### 🛍️ 4. DIM_PRODUCT_VN - Sản Phẩm E-commerce Việt Nam

**Mục đích**: Catalog sản phẩm với đặc thù thị trường Việt Nam

**Tối ưu cho VN**:
- Giá VND làm đơn vị chính
- Phân loại sản phẩm bằng tiếng Việt
- Compliance với tiêu chuẩn VN
- Tích hợp platform VN

```sql
CREATE TABLE vietnam_dw.dim_product_vn (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE,

    -- Tên sản phẩm song ngữ
    product_name_vietnamese VARCHAR(300),
    product_name_english VARCHAR(300),
    brand_vietnamese VARCHAR(100),

    -- Phân loại 3 cấp
    category_level_1_vietnamese VARCHAR(100), -- Điện thoại & Phụ kiện
    category_level_2_vietnamese VARCHAR(100), -- Điện thoại di động
    category_level_3_vietnamese VARCHAR(100), -- Smartphone

    -- Giá VND (chính)
    price_vnd DECIMAL(15,0) NOT NULL,
    price_usd DECIMAL(10,2),                 -- Tham khảo
    price_tier_vietnamese VARCHAR(30),       -- Bình dân/Tầm trung/Cao cấp

    -- Đặc thù VN
    vietnam_popularity_score DECIMAL(3,2),
    made_in_vietnam BOOLEAN,
    vietnamese_language_support BOOLEAN,
    meets_vietnam_standards BOOLEAN,

    -- Platform VN
    available_on_shopee BOOLEAN,
    available_on_lazada BOOLEAN,
    available_on_tiki BOOLEAN,
    available_on_sendo BOOLEAN,

    -- Thanh toán VN
    supports_cod BOOLEAN DEFAULT TRUE,
    supports_momo BOOLEAN,
    supports_zalopay BOOLEAN,
    supports_vnpay BOOLEAN,

    -- SCD Type 2
    effective_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);
```

### 🏪 5. DIM_PLATFORM_VN - Nền Tảng E-commerce Việt Nam

**Mục đích**: Thông tin các platform hoạt động tại Việt Nam

**Platforms chính**:
- Shopee (35.2% thị phần)
- Lazada (28.5% thị phần)
- Tiki (15.8% thị phần)
- Sendo (10.3% thị phần)
- FPT Shop, CellphoneS...

```sql
CREATE TABLE vietnam_dw.dim_platform_vn (
    platform_key SERIAL PRIMARY KEY,
    platform_id VARCHAR(50) UNIQUE,

    -- Thông tin platform
    platform_name VARCHAR(100),            -- Shopee, Lazada...
    platform_name_vietnamese VARCHAR(100), -- Shopee Việt Nam
    vietnam_entity_name VARCHAR(200),      -- Tên công ty VN
    business_license_number VARCHAR(50),   -- Giấy phép kinh doanh

    -- Thị phần VN
    vietnam_market_share_percentage DECIMAL(5,2),
    vietnam_user_base INTEGER,
    vietnam_gmv_vnd DECIMAL(18,0),

    -- Model kinh doanh
    commission_rate_percentage DECIMAL(5,2),
    payment_processing_fee_percentage DECIMAL(5,2),

    -- Tích hợp thanh toán VN
    supports_momo BOOLEAN,
    supports_zalopay BOOLEAN,
    supports_vnpay BOOLEAN,
    supports_cod BOOLEAN DEFAULT TRUE,

    -- Logistics VN
    integrated_with_ghtk BOOLEAN,          -- Giao Hàng Tiết Kiệm
    integrated_with_ghn BOOLEAN,           -- Giao Hàng Nhanh
    integrated_with_vnpost BOOLEAN,        -- Bưu điện VN

    -- Compliance VN
    complies_with_vietnam_ecommerce_law BOOLEAN,
    complies_with_vietnam_tax_law BOOLEAN,

    launch_date_vietnam DATE
);
```

## 📈 Fact Tables

### 💰 FACT_SALES_VN - Fact Bán Hàng Chính

**Mục đích**: Lưu trữ tất cả giao dịch bán hàng e-commerce Việt Nam

**Đặc điểm**:
- VND làm đơn vị tiền tệ chính
- Tích hợp phương thức thanh toán VN
- Tracking delivery với logistics VN
- Phân tích mùa lễ tết VN

```sql
CREATE TABLE vietnam_dw.fact_sales_vn (
    sale_key BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,

    -- Foreign keys
    customer_key INTEGER REFERENCES dim_customer_vn,
    product_key INTEGER REFERENCES dim_product_vn,
    time_key INTEGER REFERENCES dim_time,
    geography_key INTEGER REFERENCES dim_geography_vn,
    platform_key INTEGER REFERENCES dim_platform_vn,

    -- Measures chính (VND)
    quantity_ordered INTEGER,
    unit_price_vnd DECIMAL(15,0),
    gross_sales_amount_vnd DECIMAL(18,0),
    net_sales_amount_vnd DECIMAL(18,0),
    tax_amount_vnd DECIMAL(15,0),          -- VAT 10%
    shipping_fee_vnd DECIMAL(12,0),
    gross_profit_vnd DECIMAL(18,0),

    -- Platform fees (VND)
    platform_commission_vnd DECIMAL(12,0),
    payment_processing_fee_vnd DECIMAL(10,0),

    -- Thanh toán VN
    payment_method_vietnamese VARCHAR(50),  -- Thanh toán khi nhận hàng
    payment_method_english VARCHAR(50),     -- COD
    is_cod_order BOOLEAN,
    is_installment_order BOOLEAN,

    -- Giao hàng VN
    shipping_method_vietnamese VARCHAR(50), -- Giao hàng tiêu chuẩn
    shipping_provider VARCHAR(50),          -- GHTK, GHN, VNPost

    -- Status
    order_status_vietnamese VARCHAR(50),    -- Đã giao, Đã hủy
    order_status_english VARCHAR(50),       -- Delivered, Cancelled

    -- VN specific flags
    applied_vietnam_promotion BOOLEAN,
    is_tet_order BOOLEAN,
    is_holiday_order BOOLEAN,
    is_festival_order BOOLEAN,

    -- Timestamps (UTC+7)
    order_timestamp TIMESTAMP,
    delivered_timestamp TIMESTAMP,

    -- Conversion reference
    exchange_rate_vnd_usd DECIMAL(8,4) DEFAULT 24000,
    net_sales_amount_usd DECIMAL(12,2)
);
```

**Partitioning**: Phân vùng theo tháng để tối ưu performance

```sql
-- Ví dụ partitions
fact_sales_vn_202410  -- Oct 2024
fact_sales_vn_202411  -- Nov 2024
fact_sales_vn_202412  -- Dec 2024
```

## 📊 Views cho Business Intelligence

### 1. Vietnam Sales Summary
```sql
CREATE VIEW vw_vietnam_sales_summary AS
SELECT
    dt.month_name_vietnamese,
    dg.region_name_vietnamese,
    dp.platform_name,
    COUNT(*) as total_orders,
    SUM(fs.net_sales_amount_vnd) as total_revenue_vnd,
    AVG(fs.net_sales_amount_vnd) as avg_order_value_vnd,

    -- Payment method breakdown
    SUM(CASE WHEN fs.is_cod_order THEN 1 ELSE 0 END) as cod_orders,
    SUM(CASE WHEN fs.payment_method_english LIKE '%MoMo%' THEN 1 ELSE 0 END) as momo_orders,

    -- Cultural events impact
    SUM(CASE WHEN fs.is_tet_order THEN 1 ELSE 0 END) as tet_orders
FROM fact_sales_vn fs
JOIN dim_time dt ON fs.time_key = dt.time_key
JOIN dim_geography_vn dg ON fs.geography_key = dg.geography_key
JOIN dim_platform_vn dp ON fs.platform_key = dp.platform_key;
```

### 2. Vietnam Customer Analytics
```sql
CREATE VIEW vw_vietnam_customer_analytics AS
SELECT
    dc.customer_segment_vietnamese,
    dg.region_name_vietnamese,
    COUNT(fs.sale_key) as total_orders_lifetime,
    SUM(fs.net_sales_amount_vnd) as lifetime_value_vnd,
    dc.preferred_payment_method,
    SUM(CASE WHEN fs.is_cod_order THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as cod_usage_rate
FROM dim_customer_vn dc
LEFT JOIN fact_sales_vn fs ON dc.customer_key = fs.customer_key
LEFT JOIN dim_geography_vn dg ON dc.geography_key = dg.geography_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_segment_vietnamese, dg.region_name_vietnamese, dc.preferred_payment_method;
```

## 🚀 Performance Optimization

### 1. Indexing Strategy
```sql
-- Time-based queries
CREATE INDEX idx_fact_sales_time ON fact_sales_vn(time_key);
CREATE INDEX idx_fact_sales_order_timestamp ON fact_sales_vn(order_timestamp);

-- Vietnam-specific queries
CREATE INDEX idx_fact_sales_cod_orders ON fact_sales_vn(is_cod_order) WHERE is_cod_order = TRUE;
CREATE INDEX idx_fact_sales_tet_orders ON fact_sales_vn(is_tet_order) WHERE is_tet_order = TRUE;

-- Composite indexes
CREATE INDEX idx_fact_sales_composite_time_platform ON fact_sales_vn(time_key, platform_key);
CREATE INDEX idx_fact_sales_composite_customer_product ON fact_sales_vn(customer_key, product_key);
```

### 2. Materialized Views
```sql
-- Monthly aggregation for fast reporting
CREATE MATERIALIZED VIEW mv_monthly_sales_summary AS
SELECT
    dt.year_number,
    dt.month_number,
    dg.region_name_vietnamese,
    dp.platform_name,
    COUNT(*) as total_orders,
    SUM(fs.net_sales_amount_vnd) as total_revenue_vnd
FROM fact_sales_vn fs
JOIN dim_time dt ON fs.time_key = dt.time_key
JOIN dim_geography_vn dg ON fs.geography_key = dg.geography_key
JOIN dim_platform_vn dp ON fs.platform_key = dp.platform_key
GROUP BY dt.year_number, dt.month_number, dg.region_name_vietnamese, dp.platform_name;

-- Refresh monthly
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_sales_summary;
```

## 📋 Deployment Instructions

### 1. Deploy Schema
```bash
# Connect to PostgreSQL
psql -h localhost -U dss_user -d ecommerce_dss

# Run deployment script
\i database/scripts/deploy_vietnam_datawarehouse.sql
```

### 2. Verify Deployment
```sql
-- Check tables
SELECT tablename, schemaname FROM pg_tables WHERE schemaname = 'vietnam_dw';

-- Check data
SELECT
    'dim_time' as table_name, count(*) as rows FROM vietnam_dw.dim_time
UNION ALL
SELECT 'dim_geography_vn', count(*) FROM vietnam_dw.dim_geography_vn
UNION ALL
SELECT 'dim_platform_vn', count(*) FROM vietnam_dw.dim_platform_vn;
```

## 📊 Sample Analytics Queries

### 1. Revenue by Vietnamese Regions
```sql
SELECT
    dg.region_name_vietnamese,
    SUM(fs.net_sales_amount_vnd) as total_revenue_vnd,
    COUNT(*) as total_orders,
    AVG(fs.net_sales_amount_vnd) as avg_order_value_vnd
FROM vietnam_dw.fact_sales_vn fs
JOIN vietnam_dw.dim_geography_vn dg ON fs.geography_key = dg.geography_key
JOIN vietnam_dw.dim_time dt ON fs.time_key = dt.time_key
WHERE dt.year_number = 2024
GROUP BY dg.region_name_vietnamese
ORDER BY total_revenue_vnd DESC;
```

### 2. Tet Season Impact Analysis
```sql
SELECT
    CASE WHEN dt.is_tet_season THEN 'Mùa Tết' ELSE 'Bình thường' END as period_type,
    COUNT(*) as total_orders,
    SUM(fs.net_sales_amount_vnd) as total_revenue_vnd,
    AVG(fs.net_sales_amount_vnd) as avg_order_value_vnd
FROM vietnam_dw.fact_sales_vn fs
JOIN vietnam_dw.dim_time dt ON fs.time_key = dt.time_key
WHERE dt.year_number = 2024
GROUP BY dt.is_tet_season
ORDER BY total_revenue_vnd DESC;
```

### 3. Payment Method Adoption by Region
```sql
SELECT
    dg.region_name_vietnamese,
    fs.payment_method_vietnamese,
    COUNT(*) as order_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY dg.region_name_vietnamese) as percentage
FROM vietnam_dw.fact_sales_vn fs
JOIN vietnam_dw.dim_geography_vn dg ON fs.geography_key = dg.geography_key
GROUP BY dg.region_name_vietnamese, fs.payment_method_vietnamese
ORDER BY dg.region_name_vietnamese, order_count DESC;
```

### 4. Platform Market Share in Vietnam
```sql
SELECT
    dp.platform_name,
    dp.vietnam_market_share_percentage as reported_share,
    COUNT(*) as actual_orders,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as actual_order_share,
    SUM(fs.net_sales_amount_vnd) as gmv_vnd,
    SUM(fs.net_sales_amount_vnd) * 100.0 / SUM(SUM(fs.net_sales_amount_vnd)) OVER () as actual_gmv_share
FROM vietnam_dw.fact_sales_vn fs
JOIN vietnam_dw.dim_platform_vn dp ON fs.platform_key = dp.platform_key
JOIN vietnam_dw.dim_time dt ON fs.time_key = dt.time_key
WHERE dt.year_number = 2024
GROUP BY dp.platform_name, dp.vietnam_market_share_percentage
ORDER BY gmv_vnd DESC;
```

## 🎯 Key Performance Indicators (KPIs)

### 1. Revenue KPIs
- **Total GMV VND**: Tổng giá trị hàng hóa
- **Revenue by Region**: Doanh thu theo 3 miền
- **Average Order Value**: Giá trị đơn hàng trung bình
- **Revenue Growth Rate**: Tăng trưởng theo tháng/quý

### 2. Customer KPIs
- **Customer Acquisition**: Khách hàng mới theo tháng
- **Customer Retention**: Tỷ lệ khách quay lại
- **Customer Lifetime Value**: Giá trị trọn đời khách hàng
- **Customer Segmentation**: Phân khúc khách hàng VN

### 3. Platform KPIs
- **Market Share**: Thị phần từng platform
- **Commission Revenue**: Doanh thu hoa hồng
- **Delivery Performance**: Hiệu suất giao hàng
- **Payment Method Mix**: Tỷ lệ phương thức thanh toán

### 4. Cultural & Seasonal KPIs
- **Tet Season Performance**: Hiệu suất mùa Tết
- **Holiday Impact**: Ảnh hưởng ngày lễ
- **Shopping Festival ROI**: ROI các ngày mua sắm lớn
- **Regional Preferences**: Sở thích theo vùng miền

## 🔧 Maintenance

### 1. Daily Tasks
- Load fact data từ operational systems
- Refresh aggregate tables
- Monitor data quality

### 2. Monthly Tasks
- Refresh materialized views
- Create new partitions
- Archive old data
- Update dimension changes

### 3. Quarterly Tasks
- Review and optimize indexes
- Update Vietnamese holidays calendar
- Performance tuning
- Capacity planning

---

## 📞 Support & Documentation

**Schema Owner**: DSS Team
**Database**: PostgreSQL 13+
**Schema**: `vietnam_dw`
**Focus**: 100% Vietnam E-commerce Market

**Key Features**:
✅ Vietnamese language support
✅ VND currency primary
✅ 63 provinces coverage
✅ Cultural holidays & festivals
✅ Local payment methods
✅ Vietnam-specific business rules
✅ Compliance with Vietnam laws