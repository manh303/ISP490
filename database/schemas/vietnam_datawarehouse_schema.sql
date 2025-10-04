-- ====================================================================
-- DATA WAREHOUSE SCHEMA CHO THỊ TRƯỜNG E-COMMERCE VIỆT NAM
-- ====================================================================
-- Description: Comprehensive data warehouse schema designed specifically
--              for Vietnamese e-commerce market analysis
-- Created: 2025-10-04
-- Focus: Vietnam-only market with local business requirements
-- ====================================================================

-- Create Data Warehouse Schema
CREATE SCHEMA IF NOT EXISTS vietnam_dw;
GRANT ALL PRIVILEGES ON SCHEMA vietnam_dw TO dss_user;

-- Set search path to include our DW schema
SET search_path TO vietnam_dw, public;

-- ====================================================================
-- DIMENSION TABLES (Các Bảng Chiều)
-- ====================================================================

-- 1. DIM_TIME - Chiều Thời Gian (Có lễ tết Việt Nam)
-- ====================================================================
CREATE TABLE vietnam_dw.dim_time (
    time_key SERIAL PRIMARY KEY,
    date_value DATE UNIQUE NOT NULL,

    -- Phân cấp ngày
    day_of_week INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name_vietnamese VARCHAR(20) NOT NULL, -- Thứ Hai, Thứ Ba, ...
    day_name_english VARCHAR(20) NOT NULL,
    day_of_month INTEGER NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_year INTEGER NOT NULL CHECK (day_of_year BETWEEN 1 AND 366),

    -- Phân cấp tuần
    week_of_year INTEGER NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    week_start_date DATE NOT NULL,
    week_end_date DATE NOT NULL,

    -- Phân cấp tháng
    month_number INTEGER NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name_vietnamese VARCHAR(20) NOT NULL, -- Tháng Một, Tháng Hai, ...
    month_name_english VARCHAR(20) NOT NULL,
    month_short_vietnamese VARCHAR(10) NOT NULL, -- T1, T2, T3, ...
    month_start_date DATE NOT NULL,
    month_end_date DATE NOT NULL,

    -- Phân cấp quý
    quarter_number INTEGER NOT NULL CHECK (quarter_number BETWEEN 1 AND 4),
    quarter_name_vietnamese VARCHAR(15) NOT NULL, -- Quý 1, Quý 2, ...
    quarter_name_english VARCHAR(10) NOT NULL, -- Q1, Q2, Q3, Q4
    quarter_start_date DATE NOT NULL,
    quarter_end_date DATE NOT NULL,

    -- Phân cấp năm
    year_number INTEGER NOT NULL,
    year_start_date DATE NOT NULL,
    year_end_date DATE NOT NULL,

    -- Các ngày đặc biệt Việt Nam
    is_vietnamese_holiday BOOLEAN DEFAULT FALSE,
    vietnamese_holiday_name VARCHAR(100),
    holiday_type VARCHAR(50), -- 'National', 'Traditional', 'International'

    -- Mùa lễ và mua sắm Việt Nam
    is_tet_season BOOLEAN DEFAULT FALSE, -- Mùa Tết Nguyên Đán
    is_ghost_month BOOLEAN DEFAULT FALSE, -- Tháng Cô Hồn
    is_mid_autumn BOOLEAN DEFAULT FALSE, -- Tết Trung Thu
    is_christmas_season BOOLEAN DEFAULT FALSE,
    is_shopping_festival BOOLEAN DEFAULT FALSE, -- 9/9, 10/10, 11/11, 12/12
    shopping_festival_name VARCHAR(50),

    -- Phân loại ngày
    is_weekend BOOLEAN DEFAULT FALSE,
    is_business_day BOOLEAN DEFAULT TRUE,
    is_school_day BOOLEAN DEFAULT TRUE,

    -- Năm tài chính (Việt Nam: 1/1 - 31/12)
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER NOT NULL,
    fiscal_month INTEGER NOT NULL,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. DIM_GEOGRAPHY_VN - Chiều Địa Lý Việt Nam (63 Tỉnh Thành)
-- ====================================================================
CREATE TABLE vietnam_dw.dim_geography_vn (
    geography_key SERIAL PRIMARY KEY,

    -- Phân cấp hành chính Việt Nam
    country VARCHAR(20) DEFAULT 'Vietnam' NOT NULL,
    region_code VARCHAR(10) NOT NULL, -- MB (Miền Bắc), MT (Miền Trung), MN (Miền Nam)
    region_name_vietnamese VARCHAR(50) NOT NULL, -- Miền Bắc, Miền Trung, Miền Nam
    region_name_english VARCHAR(50) NOT NULL, -- North, Central, South

    -- Tỉnh/Thành phố trực thuộc TW
    province_code VARCHAR(10) NOT NULL, -- HN, HCM, DN, HP, etc.
    province_name_vietnamese VARCHAR(100) NOT NULL, -- Hà Nội, TP.HCM, Đà Nẵng
    province_name_english VARCHAR(100) NOT NULL,
    province_type VARCHAR(30) NOT NULL, -- 'Thành phố TW', 'Tỉnh'

    -- Quận/Huyện/Thị xã
    district_name_vietnamese VARCHAR(100),
    district_name_english VARCHAR(100),
    district_type VARCHAR(30), -- 'Quận', 'Huyện', 'Thị xã', 'Thành phố'

    -- Phường/Xã/Thị trấn
    ward_name_vietnamese VARCHAR(100),
    ward_name_english VARCHAR(100),
    ward_type VARCHAR(20), -- 'Phường', 'Xã', 'Thị trấn'

    -- Mã bưu chính Việt Nam (6 chữ số)
    postal_code VARCHAR(6),

    -- Tọa độ địa lý
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),

    -- Thông tin nhân khẩu
    population INTEGER,
    population_density DECIMAL(10,2), -- người/km²
    urban_population_percentage DECIMAL(5,2),

    -- Phân loại kinh tế
    economic_classification VARCHAR(30), -- 'Đô thị loại I', 'Đô thị loại II', 'Nông thôn'
    gdp_per_capita_vnd DECIMAL(15,0),
    average_income_vnd DECIMAL(12,0),

    -- Chỉ số thương mại điện tử
    ecommerce_penetration_rate DECIMAL(5,2), -- % dân số sử dụng e-commerce
    internet_penetration_rate DECIMAL(5,2),
    mobile_penetration_rate DECIMAL(5,2),
    smartphone_penetration_rate DECIMAL(5,2),

    -- Đặc điểm thị trường
    is_major_ecommerce_hub BOOLEAN DEFAULT FALSE,
    is_free_trade_zone BOOLEAN DEFAULT FALSE,
    is_border_province BOOLEAN DEFAULT FALSE,
    is_coastal_province BOOLEAN DEFAULT FALSE,

    -- Logistics và vận chuyển
    has_international_airport BOOLEAN DEFAULT FALSE,
    has_seaport BOOLEAN DEFAULT FALSE,
    average_delivery_time_days INTEGER,
    cod_availability BOOLEAN DEFAULT TRUE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. DIM_CUSTOMER_VN - Chiều Khách Hàng Việt Nam
-- ====================================================================
CREATE TABLE vietnam_dw.dim_customer_vn (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,

    -- Thông tin cá nhân
    full_name_vietnamese VARCHAR(200),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone_number VARCHAR(20), -- Format: +84xxxxxxxxx
    date_of_birth DATE,
    age INTEGER,
    gender VARCHAR(10) CHECK (gender IN ('Nam', 'Nữ', 'Khác', 'Male', 'Female', 'Other')),

    -- Địa chỉ (liên kết với dim_geography_vn)
    geography_key INTEGER REFERENCES vietnam_dw.dim_geography_vn(geography_key),
    full_address_vietnamese TEXT,

    -- Thông tin tài chính (VND)
    monthly_income_vnd DECIMAL(12,0),
    monthly_spending_vnd DECIMAL(12,0),
    credit_score INTEGER CHECK (credit_score BETWEEN 300 AND 850),

    -- Phân khúc khách hàng Việt Nam
    customer_segment_vietnamese VARCHAR(50), -- 'Khách hàng thường', 'Khách hàng VIP', 'Khách hàng cao cấp'
    customer_segment_english VARCHAR(50), -- 'Regular', 'High_Value', 'Premium'
    income_level_vietnamese VARCHAR(30), -- 'Thu nhập thấp', 'Thu nhập trung bình', 'Thu nhập cao'
    income_level_english VARCHAR(30), -- 'Low', 'Middle', 'High'

    -- Hành vi mua sắm
    preferred_device VARCHAR(30), -- 'Mobile', 'Desktop', 'Tablet'
    preferred_platform VARCHAR(50), -- 'Shopee', 'Lazada', 'Tiki', 'Sendo'
    shopping_frequency VARCHAR(30), -- 'Daily', 'Weekly', 'Monthly', 'Quarterly'
    average_order_value_vnd DECIMAL(12,0),

    -- Phương thức thanh toán ưa thích (Việt Nam)
    preferred_payment_method VARCHAR(50), -- 'COD', 'MoMo', 'ZaloPay', 'VNPay', 'Banking'
    uses_ewallet BOOLEAN DEFAULT FALSE,
    uses_cod BOOLEAN DEFAULT TRUE,
    uses_installment BOOLEAN DEFAULT FALSE,

    -- Đặc điểm văn hóa Việt Nam
    vietnam_region_preference VARCHAR(20), -- 'North', 'Central', 'South'
    speaks_vietnamese BOOLEAN DEFAULT TRUE,
    prefers_vietnamese_brands BOOLEAN DEFAULT FALSE,
    celebrates_vietnamese_holidays BOOLEAN DEFAULT TRUE,

    -- Thời gian hoạt động
    registration_date DATE NOT NULL,
    first_purchase_date DATE,
    last_purchase_date DATE,
    account_status VARCHAR(20) DEFAULT 'Active', -- 'Active', 'Inactive', 'Suspended'

    -- Slowly Changing Dimension Type 2 Support
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. DIM_PRODUCT_VN - Chiều Sản Phẩm E-commerce Việt Nam
-- ====================================================================
CREATE TABLE vietnam_dw.dim_product_vn (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,

    -- Thông tin sản phẩm
    product_name_vietnamese VARCHAR(300),
    product_name_english VARCHAR(300),
    brand_vietnamese VARCHAR(100),
    brand_english VARCHAR(100),
    manufacturer VARCHAR(100),

    -- Phân loại sản phẩm
    category_level_1_vietnamese VARCHAR(100), -- Điện thoại & Phụ kiện
    category_level_1_english VARCHAR(100), -- Mobile & Accessories
    category_level_2_vietnamese VARCHAR(100), -- Điện thoại di động
    category_level_2_english VARCHAR(100), -- Mobile Phones
    category_level_3_vietnamese VARCHAR(100), -- Smartphone
    category_level_3_english VARCHAR(100), -- Smartphone

    -- Giá cả (VND - đơn vị chính)
    price_vnd DECIMAL(15,0) NOT NULL,
    original_price_vnd DECIMAL(15,0),
    price_usd DECIMAL(10,2), -- Chỉ để tham khảo
    discount_percentage DECIMAL(5,2) DEFAULT 0,
    price_tier_vietnamese VARCHAR(30), -- 'Bình dân', 'Tầm trung', 'Cao cấp', 'Siêu cao cấp'
    price_tier_english VARCHAR(30), -- 'Budget', 'Mid-range', 'Premium', 'Luxury'

    -- Đánh giá và review
    rating DECIMAL(3,2) CHECK (rating >= 0 AND rating <= 5),
    review_count INTEGER DEFAULT 0,
    rating_category_vietnamese VARCHAR(20), -- 'Kém', 'Trung bình', 'Tốt', 'Xuất sắc'
    rating_category_english VARCHAR(20), -- 'Poor', 'Fair', 'Good', 'Excellent'

    -- Thông số kỹ thuật
    weight_kg DECIMAL(8,3),
    dimensions_cm VARCHAR(50), -- "dài x rộng x cao"
    warranty_months INTEGER,
    origin_country VARCHAR(50),
    made_in_vietnam BOOLEAN DEFAULT FALSE,

    -- Đặc điểm thị trường Việt Nam
    vietnam_popularity_score DECIMAL(3,2), -- 0-1 scale
    suitable_for_vietnamese_climate BOOLEAN DEFAULT TRUE,
    vietnamese_language_support BOOLEAN DEFAULT FALSE,
    has_vietnamese_manual BOOLEAN DEFAULT FALSE,

    -- Compliance và chứng nhận
    vietnam_quality_certified BOOLEAN DEFAULT FALSE,
    energy_efficiency_rating VARCHAR(10), -- A+, A, B, C, D
    meets_vietnam_standards BOOLEAN DEFAULT TRUE,

    -- Platform availability (JSON format)
    available_on_shopee BOOLEAN DEFAULT FALSE,
    available_on_lazada BOOLEAN DEFAULT FALSE,
    available_on_tiki BOOLEAN DEFAULT FALSE,
    available_on_sendo BOOLEAN DEFAULT FALSE,
    platform_availability JSONB,

    -- Supported payment methods in Vietnam
    supports_cod BOOLEAN DEFAULT TRUE,
    supports_momo BOOLEAN DEFAULT FALSE,
    supports_zalopay BOOLEAN DEFAULT FALSE,
    supports_vnpay BOOLEAN DEFAULT FALSE,
    supports_installment BOOLEAN DEFAULT FALSE,
    payment_methods_supported JSONB,

    -- Inventory và availability
    stock_quantity INTEGER DEFAULT 0,
    availability_status VARCHAR(30) DEFAULT 'In Stock',
    is_featured BOOLEAN DEFAULT FALSE,
    is_bestseller_vietnam BOOLEAN DEFAULT FALSE,
    is_new_arrival BOOLEAN DEFAULT FALSE,

    -- Thời gian
    launch_date DATE,
    first_available_date DATE,
    last_stock_update TIMESTAMP,

    -- Market sizing (Vietnam specific)
    vietnam_market_size_vnd DECIMAL(18,0),
    vietnam_market_growth_rate DECIMAL(5,2),
    mobile_commerce_penetration DECIMAL(5,2),

    -- SEO và marketing
    seo_title_vietnamese VARCHAR(300),
    seo_description_vietnamese TEXT,
    tags_vietnamese TEXT[], -- Array of Vietnamese tags
    tags_english TEXT[], -- Array of English tags

    -- Slowly Changing Dimension Type 2 Support
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,

    -- Metadata
    data_source VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. DIM_PLATFORM_VN - Chiều Nền Tảng E-commerce Việt Nam
-- ====================================================================
CREATE TABLE vietnam_dw.dim_platform_vn (
    platform_key SERIAL PRIMARY KEY,
    platform_id VARCHAR(50) UNIQUE NOT NULL,

    -- Thông tin nền tảng
    platform_name VARCHAR(100) NOT NULL, -- Shopee, Lazada, Tiki, Sendo, etc.
    platform_name_vietnamese VARCHAR(100),
    platform_type VARCHAR(50), -- 'Marketplace', 'B2C', 'C2C', 'Social Commerce'
    parent_company VARCHAR(100),

    -- Thông tin doanh nghiệp tại Việt Nam
    vietnam_entity_name VARCHAR(200),
    business_license_number VARCHAR(50),
    tax_code VARCHAR(20),
    legal_address_vietnam TEXT,

    -- Thị phần và quy mô tại Việt Nam
    vietnam_market_share_percentage DECIMAL(5,2),
    vietnam_user_base INTEGER,
    vietnam_seller_base INTEGER,
    vietnam_gmv_vnd DECIMAL(18,0), -- Gross Merchandise Value

    -- Model kinh doanh
    commission_rate_percentage DECIMAL(5,2),
    payment_processing_fee_percentage DECIMAL(5,2),
    listing_fee_vnd DECIMAL(10,0),
    advertising_fee_percentage DECIMAL(5,2),

    -- Tích hợp thanh toán Việt Nam
    supports_momo BOOLEAN DEFAULT FALSE,
    supports_zalopay BOOLEAN DEFAULT FALSE,
    supports_vnpay BOOLEAN DEFAULT FALSE,
    supports_vietcombank BOOLEAN DEFAULT FALSE,
    supports_cod BOOLEAN DEFAULT TRUE,
    supports_installment BOOLEAN DEFAULT FALSE,
    supported_payment_methods JSONB,

    -- Tích hợp logistics Việt Nam
    integrated_with_ghtk BOOLEAN DEFAULT FALSE, -- Giao Hàng Tiết Kiệm
    integrated_with_ghn BOOLEAN DEFAULT FALSE, -- Giao Hàng Nhanh
    integrated_with_vnpost BOOLEAN DEFAULT FALSE, -- Bưu điện Việt Nam
    integrated_with_jnt BOOLEAN DEFAULT FALSE, -- J&T Express
    supported_logistics_providers JSONB,

    -- Hiệu suất giao hàng
    average_delivery_time_hanoi INTEGER, -- days
    average_delivery_time_hcm INTEGER, -- days
    average_delivery_time_danang INTEGER, -- days
    cod_success_rate_percentage DECIMAL(5,2),

    -- Đặc điểm kỹ thuật
    has_mobile_app BOOLEAN DEFAULT FALSE,
    mobile_app_downloads INTEGER,
    mobile_app_rating DECIMAL(3,2),
    website_traffic_rank_vietnam INTEGER,

    -- Dịch vụ khách hàng
    supports_vietnamese_language BOOLEAN DEFAULT TRUE,
    customer_service_hours VARCHAR(100),
    has_live_chat BOOLEAN DEFAULT FALSE,
    has_hotline BOOLEAN DEFAULT FALSE,
    hotline_number VARCHAR(20),

    -- Chương trình khuyến mãi đặc trưng
    has_flash_sale BOOLEAN DEFAULT FALSE,
    has_voucher_system BOOLEAN DEFAULT FALSE,
    has_loyalty_program BOOLEAN DEFAULT FALSE,
    has_referral_program BOOLEAN DEFAULT FALSE,

    -- Tuân thủ pháp luật Việt Nam
    complies_with_vietnam_ecommerce_law BOOLEAN DEFAULT TRUE,
    complies_with_vietnam_consumer_protection BOOLEAN DEFAULT TRUE,
    complies_with_vietnam_tax_law BOOLEAN DEFAULT TRUE,

    -- Metrics hiệu suất
    customer_satisfaction_score DECIMAL(3,2),
    seller_satisfaction_score DECIMAL(3,2),
    platform_reliability_percentage DECIMAL(5,2),

    -- Thời gian hoạt động
    launch_date_vietnam DATE,
    last_major_update DATE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- FACT TABLES (Các Bảng Fact)
-- ====================================================================

-- 1. FACT_SALES_VN - Fact Bán Hàng E-commerce Việt Nam
-- ====================================================================
CREATE TABLE vietnam_dw.fact_sales_vn (
    sale_key BIGSERIAL PRIMARY KEY,

    -- Business keys
    order_id VARCHAR(50) NOT NULL,
    order_line_id VARCHAR(50),
    transaction_id VARCHAR(50),

    -- Foreign keys to dimensions
    customer_key INTEGER REFERENCES vietnam_dw.dim_customer_vn(customer_key),
    product_key INTEGER REFERENCES vietnam_dw.dim_product_vn(product_key),
    time_key INTEGER REFERENCES vietnam_dw.dim_time(time_key),
    geography_key INTEGER REFERENCES vietnam_dw.dim_geography_vn(geography_key),
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),

    -- Degenerate dimensions
    invoice_number VARCHAR(50),
    promotion_code VARCHAR(50),
    coupon_code VARCHAR(50),

    -- Quantity measures
    quantity_ordered INTEGER NOT NULL CHECK (quantity_ordered > 0),
    quantity_shipped INTEGER DEFAULT 0,
    quantity_returned INTEGER DEFAULT 0,
    quantity_cancelled INTEGER DEFAULT 0,

    -- Pricing measures (VND - primary currency)
    unit_price_vnd DECIMAL(15,0) NOT NULL,
    unit_cost_vnd DECIMAL(15,0),
    list_price_vnd DECIMAL(15,0),

    -- Line-level totals (VND)
    gross_sales_amount_vnd DECIMAL(18,0) NOT NULL,
    discount_amount_vnd DECIMAL(15,0) DEFAULT 0,
    net_sales_amount_vnd DECIMAL(18,0) NOT NULL,
    tax_amount_vnd DECIMAL(15,0) DEFAULT 0, -- VAT 10%
    shipping_fee_vnd DECIMAL(12,0) DEFAULT 0,

    -- Cost and profitability (VND)
    total_cost_vnd DECIMAL(18,0),
    gross_profit_vnd DECIMAL(18,0),
    gross_profit_margin_percentage DECIMAL(5,2),

    -- Platform fees (VND)
    platform_commission_vnd DECIMAL(12,0),
    payment_processing_fee_vnd DECIMAL(10,0),
    listing_fee_vnd DECIMAL(10,0),
    advertising_fee_vnd DECIMAL(10,0),

    -- Conversion for reference (USD)
    exchange_rate_vnd_usd DECIMAL(8,4) DEFAULT 24000,
    net_sales_amount_usd DECIMAL(12,2),

    -- Payment information (Vietnam specific)
    payment_method_vietnamese VARCHAR(50), -- 'Thanh toán khi nhận hàng', 'Ví MoMo', etc.
    payment_method_english VARCHAR(50), -- 'COD', 'E_Wallet_MoMo', etc.
    payment_status VARCHAR(30), -- 'Pending', 'Paid', 'Failed', 'Refunded'
    payment_date TIMESTAMP,

    -- Shipping information
    shipping_method_vietnamese VARCHAR(50), -- 'Giao hàng tiêu chuẩn', 'Giao hàng nhanh'
    shipping_method_english VARCHAR(50), -- 'Standard', 'Express', 'Same_Day'
    shipping_provider VARCHAR(50), -- 'GHTK', 'GHN', 'VNPost', 'J&T'

    -- Order status
    order_status_vietnamese VARCHAR(50), -- 'Đang xử lý', 'Đã giao', 'Đã hủy'
    order_status_english VARCHAR(50), -- 'Processing', 'Delivered', 'Cancelled'

    -- Vietnam-specific flags
    is_cod_order BOOLEAN DEFAULT FALSE,
    is_installment_order BOOLEAN DEFAULT FALSE,
    is_gift_order BOOLEAN DEFAULT FALSE,
    is_bulk_order BOOLEAN DEFAULT FALSE,
    applied_vietnam_promotion BOOLEAN DEFAULT FALSE,
    promotion_type_vietnamese VARCHAR(50),

    -- Delivery information
    estimated_delivery_date DATE,
    actual_delivery_date DATE,
    delivery_time_days INTEGER,
    delivery_rating INTEGER CHECK (delivery_rating BETWEEN 1 AND 5),

    -- Customer satisfaction
    customer_rating INTEGER CHECK (customer_rating BETWEEN 1 AND 5),
    customer_review_text TEXT,
    customer_review_date DATE,

    -- Seasonal and cultural flags
    is_tet_order BOOLEAN DEFAULT FALSE,
    is_holiday_order BOOLEAN DEFAULT FALSE,
    is_festival_order BOOLEAN DEFAULT FALSE,

    -- Time stamps (Vietnam timezone: UTC+7)
    order_timestamp TIMESTAMP NOT NULL,
    shipped_timestamp TIMESTAMP,
    delivered_timestamp TIMESTAMP,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_quantities CHECK (
        quantity_shipped <= quantity_ordered AND
        quantity_returned <= quantity_shipped AND
        quantity_cancelled <= quantity_ordered
    ),
    CONSTRAINT chk_amounts CHECK (
        gross_sales_amount_vnd >= net_sales_amount_vnd AND
        net_sales_amount_vnd >= 0 AND
        discount_amount_vnd >= 0
    )
);

-- 2. FACT_INVENTORY_VN - Fact Tồn Kho Việt Nam
-- ====================================================================
CREATE TABLE vietnam_dw.fact_inventory_vn (
    inventory_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys
    product_key INTEGER REFERENCES vietnam_dw.dim_product_vn(product_key),
    geography_key INTEGER REFERENCES vietnam_dw.dim_geography_vn(geography_key),
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),
    time_key INTEGER REFERENCES vietnam_dw.dim_time(time_key),

    -- Stock measures
    beginning_inventory INTEGER DEFAULT 0,
    ending_inventory INTEGER DEFAULT 0,
    received_quantity INTEGER DEFAULT 0,
    sold_quantity INTEGER DEFAULT 0,
    returned_quantity INTEGER DEFAULT 0,
    damaged_quantity INTEGER DEFAULT 0,

    -- Stock value (VND)
    inventory_value_vnd DECIMAL(18,0),
    average_cost_vnd DECIMAL(15,0),

    -- Stock status
    is_in_stock BOOLEAN DEFAULT TRUE,
    is_low_stock BOOLEAN DEFAULT FALSE,
    is_out_of_stock BOOLEAN DEFAULT FALSE,
    reorder_point INTEGER DEFAULT 0,

    -- Vietnam specific
    warehouse_location_vietnamese VARCHAR(100),
    supplier_vietnam VARCHAR(100),

    -- Metadata
    snapshot_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. FACT_CUSTOMER_ACTIVITY_VN - Fact Hoạt Động Khách Hàng
-- ====================================================================
CREATE TABLE vietnam_dw.fact_customer_activity_vn (
    activity_key BIGSERIAL PRIMARY KEY,

    -- Foreign keys
    customer_key INTEGER REFERENCES vietnam_dw.dim_customer_vn(customer_key),
    product_key INTEGER REFERENCES vietnam_dw.dim_product_vn(product_key),
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),
    time_key INTEGER REFERENCES vietnam_dw.dim_time(time_key),

    -- Activity measures
    page_views INTEGER DEFAULT 0,
    session_duration_minutes INTEGER DEFAULT 0,
    items_viewed INTEGER DEFAULT 0,
    items_added_to_cart INTEGER DEFAULT 0,
    items_purchased INTEGER DEFAULT 0,
    search_queries INTEGER DEFAULT 0,

    -- Engagement measures
    reviews_written INTEGER DEFAULT 0,
    photos_uploaded INTEGER DEFAULT 0,
    videos_uploaded INTEGER DEFAULT 0,
    shares_on_social INTEGER DEFAULT 0,

    -- Device and access
    device_type VARCHAR(30), -- 'Mobile', 'Desktop', 'Tablet'
    browser_type VARCHAR(50),
    operating_system VARCHAR(50),
    access_location_vietnamese VARCHAR(100),

    -- Metadata
    activity_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- AGGREGATE TABLES (Các Bảng Tổng Hợp)
-- ====================================================================

-- 1. AGG_DAILY_SALES_VN - Tổng Hợp Bán Hàng Hàng Ngày
-- ====================================================================
CREATE TABLE vietnam_dw.agg_daily_sales_vn (
    agg_key SERIAL PRIMARY KEY,

    -- Dimensions
    date_value DATE NOT NULL,
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),
    geography_key INTEGER REFERENCES vietnam_dw.dim_geography_vn(geography_key),

    -- Measures
    total_orders INTEGER DEFAULT 0,
    total_customers INTEGER DEFAULT 0,
    total_products_sold INTEGER DEFAULT 0,
    total_revenue_vnd DECIMAL(18,0) DEFAULT 0,
    total_profit_vnd DECIMAL(18,0) DEFAULT 0,
    average_order_value_vnd DECIMAL(15,0) DEFAULT 0,

    -- Vietnam specific KPIs
    cod_orders_count INTEGER DEFAULT 0,
    ewallet_orders_count INTEGER DEFAULT 0,
    mobile_orders_count INTEGER DEFAULT 0,

    -- Calculated at
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint
    UNIQUE(date_value, platform_key, geography_key)
);

-- 2. AGG_MONTHLY_PLATFORM_PERFORMANCE_VN - Hiệu Suất Platform Hàng Tháng
-- ====================================================================
CREATE TABLE vietnam_dw.agg_monthly_platform_performance_vn (
    agg_key SERIAL PRIMARY KEY,

    -- Time dimension
    year_number INTEGER NOT NULL,
    month_number INTEGER NOT NULL,

    -- Platform dimension
    platform_key INTEGER REFERENCES vietnam_dw.dim_platform_vn(platform_key),

    -- Performance measures
    total_gmv_vnd DECIMAL(20,0) DEFAULT 0, -- Gross Merchandise Value
    total_orders INTEGER DEFAULT 0,
    total_customers INTEGER DEFAULT 0,
    total_sellers INTEGER DEFAULT 0,

    -- Customer metrics
    new_customers INTEGER DEFAULT 0,
    returning_customers INTEGER DEFAULT 0,
    customer_retention_rate DECIMAL(5,2) DEFAULT 0,

    -- Operational metrics
    average_delivery_time_days DECIMAL(4,2) DEFAULT 0,
    cod_success_rate DECIMAL(5,2) DEFAULT 0,
    customer_satisfaction_score DECIMAL(3,2) DEFAULT 0,

    -- Calculated at
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint
    UNIQUE(year_number, month_number, platform_key)
);

-- ====================================================================
-- INDEXES FOR PERFORMANCE (Chỉ Mục Tối Ưu Hiệu Suất)
-- ====================================================================

-- Time dimension indexes
CREATE INDEX idx_dim_time_date ON vietnam_dw.dim_time(date_value);
CREATE INDEX idx_dim_time_year_month ON vietnam_dw.dim_time(year_number, month_number);
CREATE INDEX idx_dim_time_vietnamese_holidays ON vietnam_dw.dim_time(is_vietnamese_holiday) WHERE is_vietnamese_holiday = TRUE;
CREATE INDEX idx_dim_time_tet_season ON vietnam_dw.dim_time(is_tet_season) WHERE is_tet_season = TRUE;

-- Geography dimension indexes
CREATE INDEX idx_dim_geography_region ON vietnam_dw.dim_geography_vn(region_code);
CREATE INDEX idx_dim_geography_province ON vietnam_dw.dim_geography_vn(province_code);
CREATE INDEX idx_dim_geography_postal ON vietnam_dw.dim_geography_vn(postal_code);

-- Customer dimension indexes
CREATE INDEX idx_dim_customer_id ON vietnam_dw.dim_customer_vn(customer_id);
CREATE INDEX idx_dim_customer_current ON vietnam_dw.dim_customer_vn(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_customer_segment ON vietnam_dw.dim_customer_vn(customer_segment_english);
CREATE INDEX idx_dim_customer_geography ON vietnam_dw.dim_customer_vn(geography_key);

-- Product dimension indexes
CREATE INDEX idx_dim_product_id ON vietnam_dw.dim_product_vn(product_id);
CREATE INDEX idx_dim_product_current ON vietnam_dw.dim_product_vn(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_product_category ON vietnam_dw.dim_product_vn(category_level_1_english);
CREATE INDEX idx_dim_product_brand ON vietnam_dw.dim_product_vn(brand_english);
CREATE INDEX idx_dim_product_price_tier ON vietnam_dw.dim_product_vn(price_tier_english);

-- Platform dimension indexes
CREATE INDEX idx_dim_platform_id ON vietnam_dw.dim_platform_vn(platform_id);
CREATE INDEX idx_dim_platform_name ON vietnam_dw.dim_platform_vn(platform_name);

-- Fact sales indexes (partitioned by time for better performance)
CREATE INDEX idx_fact_sales_customer ON vietnam_dw.fact_sales_vn(customer_key);
CREATE INDEX idx_fact_sales_product ON vietnam_dw.fact_sales_vn(product_key);
CREATE INDEX idx_fact_sales_time ON vietnam_dw.fact_sales_vn(time_key);
CREATE INDEX idx_fact_sales_platform ON vietnam_dw.fact_sales_vn(platform_key);
CREATE INDEX idx_fact_sales_geography ON vietnam_dw.fact_sales_vn(geography_key);
CREATE INDEX idx_fact_sales_order_timestamp ON vietnam_dw.fact_sales_vn(order_timestamp);
CREATE INDEX idx_fact_sales_order_id ON vietnam_dw.fact_sales_vn(order_id);

-- Composite indexes for common queries
CREATE INDEX idx_fact_sales_composite_time_platform ON vietnam_dw.fact_sales_vn(time_key, platform_key);
CREATE INDEX idx_fact_sales_composite_customer_product ON vietnam_dw.fact_sales_vn(customer_key, product_key);
CREATE INDEX idx_fact_sales_cod_orders ON vietnam_dw.fact_sales_vn(is_cod_order) WHERE is_cod_order = TRUE;

-- ====================================================================
-- PARTITIONING (Phân Vùng Dữ Liệu)
-- ====================================================================

-- Partition fact_sales_vn by month for better performance
-- Note: PostgreSQL 10+ declarative partitioning
ALTER TABLE vietnam_dw.fact_sales_vn RENAME TO fact_sales_vn_template;

CREATE TABLE vietnam_dw.fact_sales_vn (
    LIKE vietnam_dw.fact_sales_vn_template INCLUDING ALL
) PARTITION BY RANGE (order_timestamp);

-- Create monthly partitions for 2024-2025
CREATE TABLE vietnam_dw.fact_sales_vn_202410 PARTITION OF vietnam_dw.fact_sales_vn
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE vietnam_dw.fact_sales_vn_202411 PARTITION OF vietnam_dw.fact_sales_vn
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

CREATE TABLE vietnam_dw.fact_sales_vn_202412 PARTITION OF vietnam_dw.fact_sales_vn
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE vietnam_dw.fact_sales_vn_202501 PARTITION OF vietnam_dw.fact_sales_vn
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- ====================================================================
-- VIEWS FOR BUSINESS INTELLIGENCE (Views cho BI)
-- ====================================================================

-- 1. Vietnam Sales Summary View
CREATE VIEW vietnam_dw.vw_vietnam_sales_summary AS
SELECT
    dt.date_value,
    dt.month_name_vietnamese,
    dt.quarter_name_vietnamese,
    dt.year_number,
    dg.region_name_vietnamese,
    dg.province_name_vietnamese,
    dp.platform_name,

    -- Sales metrics
    COUNT(*) as total_orders,
    SUM(fs.quantity_ordered) as total_quantity,
    SUM(fs.net_sales_amount_vnd) as total_revenue_vnd,
    AVG(fs.net_sales_amount_vnd) as avg_order_value_vnd,
    SUM(fs.gross_profit_vnd) as total_profit_vnd,
    AVG(fs.gross_profit_margin_percentage) as avg_profit_margin,

    -- Payment method breakdown
    SUM(CASE WHEN fs.is_cod_order THEN 1 ELSE 0 END) as cod_orders,
    SUM(CASE WHEN fs.payment_method_english LIKE '%MoMo%' THEN 1 ELSE 0 END) as momo_orders,
    SUM(CASE WHEN fs.payment_method_english LIKE '%ZaloPay%' THEN 1 ELSE 0 END) as zalopay_orders,

    -- Cultural events impact
    SUM(CASE WHEN fs.is_tet_order THEN 1 ELSE 0 END) as tet_orders,
    SUM(CASE WHEN fs.is_holiday_order THEN 1 ELSE 0 END) as holiday_orders

FROM vietnam_dw.fact_sales_vn fs
JOIN vietnam_dw.dim_time dt ON fs.time_key = dt.time_key
JOIN vietnam_dw.dim_geography_vn dg ON fs.geography_key = dg.geography_key
JOIN vietnam_dw.dim_platform_vn dp ON fs.platform_key = dp.platform_key
GROUP BY
    dt.date_value, dt.month_name_vietnamese, dt.quarter_name_vietnamese, dt.year_number,
    dg.region_name_vietnamese, dg.province_name_vietnamese, dp.platform_name
ORDER BY dt.date_value DESC;

-- 2. Vietnam Customer Analytics View
CREATE VIEW vietnam_dw.vw_vietnam_customer_analytics AS
SELECT
    dc.customer_id,
    dc.customer_segment_vietnamese,
    dc.income_level_vietnamese,
    dg.region_name_vietnamese,
    dg.province_name_vietnamese,

    -- Customer lifetime metrics
    COUNT(fs.sale_key) as total_orders_lifetime,
    SUM(fs.net_sales_amount_vnd) as lifetime_value_vnd,
    AVG(fs.net_sales_amount_vnd) as avg_order_value_vnd,
    MAX(dt.date_value) as last_purchase_date,
    MIN(dt.date_value) as first_purchase_date,

    -- Behavioral patterns
    COUNT(DISTINCT prod.category_level_1_vietnamese) as categories_purchased,
    COUNT(DISTINCT plat.platform_name) as platforms_used,

    -- Payment preferences
    dc.preferred_payment_method,
    SUM(CASE WHEN fs.is_cod_order THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as cod_usage_rate,

    -- Device preferences
    dc.preferred_device

FROM vietnam_dw.dim_customer_vn dc
LEFT JOIN vietnam_dw.fact_sales_vn fs ON dc.customer_key = fs.customer_key
LEFT JOIN vietnam_dw.dim_time dt ON fs.time_key = dt.time_key
LEFT JOIN vietnam_dw.dim_product_vn prod ON fs.product_key = prod.product_key
LEFT JOIN vietnam_dw.dim_platform_vn plat ON fs.platform_key = plat.platform_key
LEFT JOIN vietnam_dw.dim_geography_vn dg ON dc.geography_key = dg.geography_key
WHERE dc.is_current = TRUE
GROUP BY
    dc.customer_id, dc.customer_segment_vietnamese, dc.income_level_vietnamese,
    dg.region_name_vietnamese, dg.province_name_vietnamese,
    dc.preferred_payment_method, dc.preferred_device
ORDER BY lifetime_value_vnd DESC;

-- ====================================================================
-- GRANTS AND PERMISSIONS (Phân Quyền)
-- ====================================================================

-- Grant permissions to DSS user
GRANT USAGE ON SCHEMA vietnam_dw TO dss_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA vietnam_dw TO dss_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA vietnam_dw TO dss_user;

-- Grant read-only access to analytics users
CREATE ROLE vietnam_analytics_readonly;
GRANT USAGE ON SCHEMA vietnam_dw TO vietnam_analytics_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA vietnam_dw TO vietnam_analytics_readonly;

-- ====================================================================
-- COMMENTS FOR DOCUMENTATION (Chú Thích Tài Liệu)
-- ====================================================================

COMMENT ON SCHEMA vietnam_dw IS 'Data Warehouse schema cho thị trường e-commerce Việt Nam';

COMMENT ON TABLE vietnam_dw.dim_time IS 'Dimension table chứa thông tin thời gian với các ngày lễ và mùa đặc biệt của Việt Nam';
COMMENT ON TABLE vietnam_dw.dim_geography_vn IS 'Dimension table chứa thông tin địa lý 63 tỉnh thành Việt Nam';
COMMENT ON TABLE vietnam_dw.dim_customer_vn IS 'Dimension table chứa thông tin khách hàng Việt Nam với SCD Type 2';
COMMENT ON TABLE vietnam_dw.dim_product_vn IS 'Dimension table chứa thông tin sản phẩm e-commerce Việt Nam';
COMMENT ON TABLE vietnam_dw.dim_platform_vn IS 'Dimension table chứa thông tin các nền tảng e-commerce hoạt động tại Việt Nam';
COMMENT ON TABLE vietnam_dw.fact_sales_vn IS 'Fact table chính chứa dữ liệu bán hàng e-commerce Việt Nam';

-- ====================================================================
-- INITIAL DATA SETUP (Thiết Lập Dữ Liệu Ban Đầu)
-- ====================================================================

-- Insert Vietnam administrative divisions (sample)
INSERT INTO vietnam_dw.dim_geography_vn (
    region_code, region_name_vietnamese, region_name_english,
    province_code, province_name_vietnamese, province_name_english, province_type,
    postal_code, is_major_ecommerce_hub
) VALUES
('MB', 'Miền Bắc', 'North', 'HN', 'Hà Nội', 'Hanoi', 'Thành phố TW', '100000', TRUE),
('MB', 'Miền Bắc', 'North', 'HP', 'Hải Phòng', 'Hai Phong', 'Thành phố TW', '180000', TRUE),
('MT', 'Miền Trung', 'Central', 'DN', 'Đà Nẵng', 'Da Nang', 'Thành phố TW', '550000', TRUE),
('MN', 'Miền Nam', 'South', 'HCM', 'TP. Hồ Chí Minh', 'Ho Chi Minh City', 'Thành phố TW', '700000', TRUE),
('MN', 'Miền Nam', 'South', 'CT', 'Cần Thơ', 'Can Tho', 'Thành phố TW', '900000', TRUE);

-- Insert major Vietnamese e-commerce platforms
INSERT INTO vietnam_dw.dim_platform_vn (
    platform_id, platform_name, platform_name_vietnamese, platform_type,
    vietnam_market_share_percentage, supports_cod, supports_momo, supports_zalopay
) VALUES
('SHOPEE_VN', 'Shopee', 'Shopee Việt Nam', 'Marketplace', 35.2, TRUE, TRUE, TRUE),
('LAZADA_VN', 'Lazada', 'Lazada Việt Nam', 'Marketplace', 28.5, TRUE, TRUE, TRUE),
('TIKI_VN', 'Tiki', 'Tiki', 'B2C', 15.8, TRUE, TRUE, FALSE),
('SENDO_VN', 'Sendo', 'Sen Đỏ', 'Marketplace', 10.3, TRUE, TRUE, TRUE),
('FPTSHOP_VN', 'FPT Shop', 'FPT Shop', 'B2C', 5.2, TRUE, FALSE, FALSE);

-- ====================================================================
-- END OF VIETNAM DATA WAREHOUSE SCHEMA
-- ====================================================================