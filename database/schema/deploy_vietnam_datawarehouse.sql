-- ====================================================================
-- Vietnam E-commerce Data Warehouse Deployment Script
-- ====================================================================
-- Sample data population for Vietnam e-commerce data warehouse
-- Creates realistic Vietnamese business data for testing and development
--
-- Author: DSS Team
-- Version: 1.0.0
-- ====================================================================

-- Set search path
SET search_path TO vietnam_dw, public;

-- ====================================================================
-- POPULATE TIME DIMENSION
-- ====================================================================

-- Insert date range for 2022-2025 (4 years)
INSERT INTO vietnam_dw.dim_time_vn (
    time_key, date_value, year, quarter, month, month_name_vn,
    week_of_year, day_of_month, day_of_week, day_name_vn,
    is_weekend, is_holiday_vn, holiday_name_vn,
    is_tet_season, is_mid_autumn, is_womens_day, is_shopping_festival,
    season_vn, fiscal_year, fiscal_quarter
)
SELECT
    EXTRACT(YEAR FROM date_series) * 10000 +
    EXTRACT(MONTH FROM date_series) * 100 +
    EXTRACT(DAY FROM date_series) as time_key,
    date_series as date_value,
    EXTRACT(YEAR FROM date_series) as year,
    EXTRACT(QUARTER FROM date_series) as quarter,
    EXTRACT(MONTH FROM date_series) as month,
    CASE EXTRACT(MONTH FROM date_series)
        WHEN 1 THEN 'Tháng Một'
        WHEN 2 THEN 'Tháng Hai'
        WHEN 3 THEN 'Tháng Ba'
        WHEN 4 THEN 'Tháng Tư'
        WHEN 5 THEN 'Tháng Năm'
        WHEN 6 THEN 'Tháng Sáu'
        WHEN 7 THEN 'Tháng Bảy'
        WHEN 8 THEN 'Tháng Tám'
        WHEN 9 THEN 'Tháng Chín'
        WHEN 10 THEN 'Tháng Mười'
        WHEN 11 THEN 'Tháng Mười Một'
        WHEN 12 THEN 'Tháng Mười Hai'
    END as month_name_vn,
    EXTRACT(WEEK FROM date_series) as week_of_year,
    EXTRACT(DAY FROM date_series) as day_of_month,
    EXTRACT(DOW FROM date_series) as day_of_week,
    CASE EXTRACT(DOW FROM date_series)
        WHEN 0 THEN 'Chủ nhật'
        WHEN 1 THEN 'Thứ hai'
        WHEN 2 THEN 'Thứ ba'
        WHEN 3 THEN 'Thứ tư'
        WHEN 4 THEN 'Thứ năm'
        WHEN 5 THEN 'Thứ sáu'
        WHEN 6 THEN 'Thứ bảy'
    END as day_name_vn,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,

    -- Vietnamese holidays
    CASE
        WHEN EXTRACT(MONTH FROM date_series) = 1 AND EXTRACT(DAY FROM date_series) = 1 THEN TRUE
        WHEN EXTRACT(MONTH FROM date_series) = 4 AND EXTRACT(DAY FROM date_series) = 30 THEN TRUE
        WHEN EXTRACT(MONTH FROM date_series) = 5 AND EXTRACT(DAY FROM date_series) = 1 THEN TRUE
        WHEN EXTRACT(MONTH FROM date_series) = 9 AND EXTRACT(DAY FROM date_series) = 2 THEN TRUE
        WHEN (EXTRACT(MONTH FROM date_series) = 1 AND EXTRACT(DAY FROM date_series) BETWEEN 20 AND 31) OR
             (EXTRACT(MONTH FROM date_series) = 2 AND EXTRACT(DAY FROM date_series) BETWEEN 1 AND 20) THEN TRUE
        ELSE FALSE
    END as is_holiday_vn,

    CASE
        WHEN EXTRACT(MONTH FROM date_series) = 1 AND EXTRACT(DAY FROM date_series) = 1 THEN 'Tết Dương lịch'
        WHEN EXTRACT(MONTH FROM date_series) = 4 AND EXTRACT(DAY FROM date_series) = 30 THEN 'Ngày thống nhất'
        WHEN EXTRACT(MONTH FROM date_series) = 5 AND EXTRACT(DAY FROM date_series) = 1 THEN 'Ngày lao động'
        WHEN EXTRACT(MONTH FROM date_series) = 9 AND EXTRACT(DAY FROM date_series) = 2 THEN 'Quốc khánh'
        WHEN (EXTRACT(MONTH FROM date_series) = 1 AND EXTRACT(DAY FROM date_series) BETWEEN 20 AND 31) OR
             (EXTRACT(MONTH FROM date_series) = 2 AND EXTRACT(DAY FROM date_series) BETWEEN 1 AND 20) THEN 'Tết Nguyên đán'
        ELSE NULL
    END as holiday_name_vn,

    CASE WHEN (EXTRACT(MONTH FROM date_series) = 1 AND EXTRACT(DAY FROM date_series) BETWEEN 20 AND 31) OR
              (EXTRACT(MONTH FROM date_series) = 2 AND EXTRACT(DAY FROM date_series) BETWEEN 1 AND 20) THEN TRUE ELSE FALSE END as is_tet_season,
    CASE WHEN EXTRACT(MONTH FROM date_series) = 9 AND EXTRACT(DAY FROM date_series) = 15 THEN TRUE ELSE FALSE END as is_mid_autumn,
    CASE WHEN EXTRACT(MONTH FROM date_series) = 3 AND EXTRACT(DAY FROM date_series) = 8 THEN TRUE ELSE FALSE END as is_womens_day,
    CASE
        WHEN EXTRACT(MONTH FROM date_series) = 11 AND EXTRACT(DAY FROM date_series) BETWEEN 9 AND 12 THEN TRUE
        WHEN EXTRACT(MONTH FROM date_series) = 12 AND EXTRACT(DAY FROM date_series) = 12 THEN TRUE
        ELSE FALSE
    END as is_shopping_festival,

    CASE
        WHEN EXTRACT(MONTH FROM date_series) IN (12, 1, 2) THEN 'Mùa đông'
        WHEN EXTRACT(MONTH FROM date_series) IN (3, 4, 5) THEN 'Mùa xuân'
        WHEN EXTRACT(MONTH FROM date_series) IN (6, 7, 8) THEN 'Mùa hè'
        WHEN EXTRACT(MONTH FROM date_series) IN (9, 10, 11) THEN 'Mùa thu'
    END as season_vn,

    EXTRACT(YEAR FROM date_series) as fiscal_year,
    EXTRACT(QUARTER FROM date_series) as fiscal_quarter

FROM generate_series('2022-01-01'::date, '2025-12-31'::date, '1 day'::interval) as date_series;

-- ====================================================================
-- POPULATE GEOGRAPHY DIMENSION
-- ====================================================================

INSERT INTO vietnam_dw.dim_geography_vn (
    province_code, province_name_vn, province_name_en, region_vn,
    region_code, area_km2, population, city_tier,
    gdp_per_capita_vnd, economic_development_level, is_metropolitan,
    internet_penetration_percent, smartphone_penetration_percent,
    ecommerce_readiness_score, average_delivery_days, cod_availability
) VALUES
-- Northern Region
('HNI', 'Hà Nội', 'Hanoi', 'North', 'N', 3359, 8435700, 1, 185000000, 'High', TRUE, 95.5, 89.2, 4.8, 1, TRUE),
('HPG', 'Hải Phòng', 'Hai Phong', 'North', 'N', 1523, 2028514, 2, 150000000, 'High', TRUE, 92.1, 85.6, 4.6, 2, TRUE),
('QNI', 'Quảng Ninh', 'Quang Ninh', 'North', 'N', 6102, 1320324, 2, 120000000, 'Medium', FALSE, 88.3, 82.1, 4.2, 2, TRUE),
('HD', 'Hải Dương', 'Hai Duong', 'North', 'N', 1656, 1909755, 3, 95000000, 'Medium', FALSE, 85.7, 78.9, 3.8, 3, TRUE),
('HY', 'Hưng Yên', 'Hung Yen', 'North', 'N', 930, 1269858, 3, 90000000, 'Medium', FALSE, 83.2, 76.4, 3.6, 3, TRUE),

-- Central Region
('DN', 'Đà Nẵng', 'Da Nang', 'Central', 'C', 1285, 1230000, 1, 165000000, 'High', TRUE, 94.8, 88.7, 4.7, 2, TRUE),
('HUE', 'Thừa Thiên Huế', 'Thua Thien Hue', 'Central', 'C', 5033, 1157301, 2, 110000000, 'Medium', FALSE, 87.6, 81.3, 4.1, 3, TRUE),
('QNA', 'Quảng Nam', 'Quang Nam', 'Central', 'C', 10438, 1495000, 3, 85000000, 'Medium', FALSE, 82.4, 75.8, 3.7, 4, TRUE),
('KH', 'Khánh Hòa', 'Khanh Hoa', 'Central', 'C', 5197, 1267000, 2, 125000000, 'Medium', FALSE, 89.1, 83.5, 4.3, 3, TRUE),
('BD', 'Bình Định', 'Binh Dinh', 'Central', 'C', 6051, 1501800, 3, 75000000, 'Medium', FALSE, 80.2, 73.6, 3.5, 4, TRUE),

-- Southern Region
('HCM', 'Hồ Chí Minh', 'Ho Chi Minh City', 'South', 'S', 2061, 9314911, 1, 210000000, 'High', TRUE, 97.2, 92.8, 4.9, 1, TRUE),
('BD2', 'Bình Dương', 'Binh Duong', 'South', 'S', 2695, 2738356, 2, 175000000, 'High', FALSE, 91.3, 86.2, 4.5, 2, TRUE),
('DNA', 'Đồng Nai', 'Dong Nai', 'South', 'S', 5907, 3097107, 2, 155000000, 'High', FALSE, 89.7, 84.1, 4.4, 2, TRUE),
('BR', 'Bà Rịa - Vũng Tàu', 'Ba Ria - Vung Tau', 'South', 'S', 1982, 1183000, 2, 145000000, 'Medium', FALSE, 88.9, 83.7, 4.2, 2, TRUE),
('LA', 'Long An', 'Long An', 'South', 'S', 4494, 1688600, 3, 105000000, 'Medium', FALSE, 85.1, 79.3, 3.9, 3, TRUE),
('CT', 'Cần Thơ', 'Can Tho', 'South', 'S', 1439, 1235171, 2, 135000000, 'Medium', TRUE, 90.4, 85.8, 4.3, 3, TRUE),
('AG', 'An Giang', 'An Giang', 'South', 'S', 3536, 1908348, 3, 80000000, 'Medium', FALSE, 81.6, 74.2, 3.4, 4, TRUE),
('KG', 'Kiên Giang', 'Kien Giang', 'South', 'S', 6346, 1726067, 3, 85000000, 'Medium', FALSE, 83.8, 76.9, 3.6, 4, TRUE);

-- ====================================================================
-- POPULATE PLATFORM DIMENSION
-- ====================================================================

INSERT INTO vietnam_dw.dim_platform_vn (
    platform_id, platform_name_vn, platform_name_en, platform_type,
    market_share_percent, commission_rate_percent, is_international, launch_year_vn,
    supports_cod, supports_momo, supports_zalopay, supports_banking,
    has_logistics_service, has_installment_service, primary_age_group_vn, average_delivery_days
) VALUES
('shopee_vn', 'Shopee Việt Nam', 'Shopee Vietnam', 'Marketplace', 35.2, 3.5, TRUE, 2015, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, '18-35', 3),
('lazada_vn', 'Lazada Việt Nam', 'Lazada Vietnam', 'Marketplace', 18.7, 4.0, TRUE, 2012, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, '25-40', 3),
('tiki_vn', 'Tiki', 'Tiki', 'Marketplace', 12.8, 3.0, FALSE, 2010, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, '25-45', 2),
('sendo_vn', 'Sendo', 'Sendo', 'Marketplace', 8.5, 2.5, FALSE, 2012, TRUE, TRUE, FALSE, TRUE, TRUE, FALSE, '20-35', 4),
('thegioididong', 'Thế Giới Di Động', 'Mobile World', 'Direct', 6.2, 0.0, FALSE, 2004, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, '20-40', 2),
('dienmayxanh', 'Điện Máy Xanh', 'Dien May Xanh', 'Direct', 4.8, 0.0, FALSE, 2009, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, '25-50', 2),
('fptshop', 'FPT Shop', 'FPT Shop', 'Direct', 3.1, 0.0, FALSE, 2012, TRUE, TRUE, TRUE, TRUE, FALSE, TRUE, '20-40', 1),
('viettelecom', 'Viettel Store', 'Viettel Store', 'Direct', 2.8, 0.0, FALSE, 2018, TRUE, TRUE, FALSE, TRUE, FALSE, FALSE, '18-45', 2);

-- ====================================================================
-- POPULATE CUSTOMER DIMENSION
-- ====================================================================

-- Generate 1000 Vietnamese customers
INSERT INTO vietnam_dw.dim_customer_vn (
    customer_id, customer_name, email, phone_vn, gender, age_group, birth_year,
    province_vn, district_vn, region_vn, city_tier, preferred_language,
    preferred_payment_vn, is_cod_preferred, loyalty_tier_vn, income_bracket_vn,
    education_level_vn, occupation_vn, family_status_vn
)
SELECT
    'CUST_VN_' || LPAD(generate_series::text, 6, '0') as customer_id,

    -- Vietnamese names
    CASE (random() * 50)::int
        WHEN 0 THEN 'Nguyễn Văn ' || (ARRAY['An', 'Bình', 'Cường', 'Dũng', 'Hùng', 'Long', 'Nam', 'Phong', 'Quang', 'Tùng'])[ceil(random()*10)]
        WHEN 1 THEN 'Trần Thị ' || (ARRAY['Lan', 'Hoa', 'Mai', 'Linh', 'Thu', 'Hương', 'Ngọc', 'Phương', 'Thảo', 'Xuân'])[ceil(random()*10)]
        WHEN 2 THEN 'Lê Minh ' || (ARRAY['Đức', 'Hiếu', 'Hoàng', 'Khôi', 'Nhật', 'Quân', 'Thành', 'Tuấn', 'Việt', 'Đạt'])[ceil(random()*10)]
        WHEN 3 THEN 'Phạm Thanh ' || (ARRAY['Bình', 'Giang', 'Hải', 'Linh', 'Loan', 'Nga', 'Tâm', 'Thủy', 'Vân', 'Yến'])[ceil(random()*10)]
        WHEN 4 THEN 'Hoàng Đức ' || (ARRAY['Anh', 'Bảo', 'Hải', 'Khánh', 'Minh', 'Phúc', 'Quý', 'Sơn', 'Thắng', 'Toàn'])[ceil(random()*10)]
        ELSE 'Khách hàng ' || generate_series
    END as customer_name,

    'customer' || generate_series || '@' ||
    (ARRAY['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'vnn.vn'])[ceil(random()*5)] as email,

    -- Vietnamese phone format
    CASE (random() * 4)::int
        WHEN 0 THEN '0' || (ARRAY['90', '91', '92', '93', '94', '96', '97', '98', '99'])[ceil(random()*9)] || LPAD((random()*9999999)::int::text, 7, '0')
        WHEN 1 THEN '0' || (ARRAY['70', '76', '77', '78', '79'])[ceil(random()*5)] || LPAD((random()*9999999)::int::text, 7, '0')
        WHEN 2 THEN '0' || (ARRAY['81', '82', '83', '84', '85', '88'])[ceil(random()*6)] || LPAD((random()*9999999)::int::text, 7, '0')
        ELSE '0' || (ARRAY['56', '58', '59'])[ceil(random()*3)] || LPAD((random()*9999999)::int::text, 7, '0')
    END as phone_vn,

    (ARRAY['Nam', 'Nữ', 'Khác'])[ceil(random()*3)] as gender,

    CASE (random() * 6)::int
        WHEN 0 THEN '18-25'
        WHEN 1 THEN '26-35'
        WHEN 2 THEN '36-45'
        WHEN 3 THEN '46-55'
        WHEN 4 THEN '56-65'
        ELSE '65+'
    END as age_group,

    1950 + (random() * 50)::int as birth_year,

    -- Province distribution
    (ARRAY['Hồ Chí Minh', 'Hà Nội', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ', 'Bình Dương', 'Đồng Nai', 'Khánh Hòa', 'Quảng Nam', 'Long An', 'Kiên Giang', 'An Giang'])[ceil(random()*12)] as province_vn,

    'Quận ' || (1 + (random() * 12)::int) as district_vn,

    CASE
        WHEN generate_series % 3 = 0 THEN 'North'
        WHEN generate_series % 3 = 1 THEN 'Central'
        ELSE 'South'
    END as region_vn,

    CASE (random() * 3)::int
        WHEN 0 THEN 1
        WHEN 1 THEN 2
        ELSE 3
    END as city_tier,

    'vi' as preferred_language,

    (ARRAY['COD', 'MoMo', 'ZaloPay', 'Banking', 'Visa/MasterCard'])[ceil(random()*5)] as preferred_payment_vn,

    CASE WHEN random() > 0.6 THEN TRUE ELSE FALSE END as is_cod_preferred,

    (ARRAY['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond'])[ceil(random()*5)] as loyalty_tier_vn,

    CASE (random() * 5)::int
        WHEN 0 THEN 'Dưới 10 triệu'
        WHEN 1 THEN '10-20 triệu'
        WHEN 2 THEN '20-50 triệu'
        WHEN 3 THEN '50-100 triệu'
        ELSE 'Trên 100 triệu'
    END as income_bracket_vn,

    (ARRAY['Trung học', 'Cao đẳng', 'Đại học', 'Thạc sĩ', 'Tiến sĩ'])[ceil(random()*5)] as education_level_vn,

    (ARRAY['Nhân viên văn phòng', 'Giáo viên', 'Kỹ sư', 'Bác sĩ', 'Kinh doanh', 'Sinh viên', 'Nội trợ', 'Công nhân', 'Nông dân', 'Khác'])[ceil(random()*10)] as occupation_vn,

    (ARRAY['Độc thân', 'Đã kết hôn', 'Có con', 'Ly hôn', 'Góa'])[ceil(random()*5)] as family_status_vn

FROM generate_series(1, 1000);

-- ====================================================================
-- POPULATE PRODUCT DIMENSION
-- ====================================================================

-- Generate 500 Vietnamese products
INSERT INTO vietnam_dw.dim_product_vn (
    product_id, product_name_vn, product_name_en, brand_vn,
    category_level_1_vn, category_level_2_vn, category_level_3_vn,
    product_type_vn, is_imported, country_of_origin,
    cost_price_vnd, retail_price_vnd, discount_price_vnd,
    is_available, is_cod_eligible, is_tet_product, target_age_group_vn,
    average_rating, review_count
)
SELECT
    'PROD_VN_' || LPAD(generate_series::text, 6, '0') as product_id,

    -- Vietnamese product names
    CASE (random() * 10)::int
        WHEN 0 THEN 'iPhone ' || (ARRAY['13', '14', '15'])[ceil(random()*3)] || ' ' || (ARRAY['64GB', '128GB', '256GB'])[ceil(random()*3)]
        WHEN 1 THEN 'Samsung Galaxy ' || (ARRAY['A54', 'S23', 'Note 20'])[ceil(random()*3)]
        WHEN 2 THEN 'Áo thun ' || (ARRAY['nam', 'nữ', 'unisex'])[ceil(random()*3)] || ' ' || (ARRAY['cotton', 'polyester', 'bamboo'])[ceil(random()*3)]
        WHEN 3 THEN 'Giày ' || (ARRAY['thể thao', 'công sở', 'dạo phố'])[ceil(random()*3)] || ' ' || (ARRAY['Nike', 'Adidas', 'Converse', 'Bitis'])[ceil(random()*4)]
        WHEN 4 THEN 'Nồi cơm điện ' || (ARRAY['Sunhouse', 'Sharp', 'Panasonic', 'Cuckoo'])[ceil(random()*4)] || ' ' || (ARRAY['1.8L', '2.2L', '3.2L'])[ceil(random()*3)]
        WHEN 5 THEN 'Laptop ' || (ARRAY['Dell', 'HP', 'Asus', 'Acer', 'Lenovo'])[ceil(random()*5)] || ' ' || (ARRAY['i5', 'i7', 'Ryzen 5'])[ceil(random()*3)]
        WHEN 6 THEN 'Bánh kẹo ' || (ARRAY['Tết', 'Trung thu', 'sinh nhật', 'quà tặng'])[ceil(random()*4)]
        WHEN 7 THEN 'Mỹ phẩm ' || (ARRAY['Cocoon', 'Thorakao', 'Sakura', 'LOreal'])[ceil(random()*4)]
        WHEN 8 THEN 'Đồ gia dụng ' || (ARRAY['nhà bếp', 'phòng ngủ', 'phòng khách'])[ceil(random()*3)]
        ELSE 'Sản phẩm ' || generate_series
    END as product_name_vn,

    'Product ' || generate_series as product_name_en,

    (ARRAY['Apple', 'Samsung', 'Xiaomi', 'Nike', 'Adidas', 'Uniqlo', 'Zara', 'Panasonic', 'Sharp', 'Sunhouse', 'Dell', 'HP', 'Asus', 'Sony', 'LG', 'Cocoon', 'Thorakao', 'Bitis', 'Canifa', 'IVY moda'])[ceil(random()*20)] as brand_vn,

    -- Category hierarchy
    CASE (random() * 8)::int
        WHEN 0 THEN 'Điện tử'
        WHEN 1 THEN 'Thời trang'
        WHEN 2 THEN 'Gia dụng'
        WHEN 3 THEN 'Sách & Văn phòng phẩm'
        WHEN 4 THEN 'Thể thao & Du lịch'
        WHEN 5 THEN 'Sức khỏe & Làm đẹp'
        WHEN 6 THEN 'Thực phẩm & Đồ uống'
        ELSE 'Mẹ & Bé'
    END as category_level_1_vn,

    CASE (random() * 8)::int
        WHEN 0 THEN 'Điện thoại & Phụ kiện'
        WHEN 1 THEN 'Quần áo nam'
        WHEN 2 THEN 'Đồ dùng nhà bếp'
        WHEN 3 THEN 'Sách tiếng Việt'
        WHEN 4 THEN 'Giày thể thao'
        WHEN 5 THEN 'Chăm sóc da'
        WHEN 6 THEN 'Thực phẩm khô'
        ELSE 'Đồ chơi trẻ em'
    END as category_level_2_vn,

    CASE (random() * 8)::int
        WHEN 0 THEN 'Smartphone'
        WHEN 1 THEN 'Áo thun'
        WHEN 2 THEN 'Nồi cơm điện'
        WHEN 3 THEN 'Tiểu thuyết'
        WHEN 4 THEN 'Giày chạy bộ'
        WHEN 5 THEN 'Serum'
        WHEN 6 THEN 'Bánh kẹo'
        ELSE 'Đồ chơi giáo dục'
    END as category_level_3_vn,

    (ARRAY['Sản phẩm chính', 'Phụ kiện', 'Dịch vụ', 'Quà tặng'])[ceil(random()*4)] as product_type_vn,

    CASE WHEN random() > 0.4 THEN TRUE ELSE FALSE END as is_imported,

    CASE
        WHEN random() > 0.7 THEN 'Việt Nam'
        WHEN random() > 0.5 THEN 'Trung Quốc'
        WHEN random() > 0.3 THEN 'Hàn Quốc'
        WHEN random() > 0.2 THEN 'Nhật Bản'
        ELSE 'Mỹ'
    END as country_of_origin,

    -- Pricing in VND
    (50000 + random() * 50000000)::int as cost_price_vnd,
    (100000 + random() * 100000000)::int as retail_price_vnd,
    (80000 + random() * 80000000)::int as discount_price_vnd,

    CASE WHEN random() > 0.1 THEN TRUE ELSE FALSE END as is_available,
    CASE WHEN random() > 0.2 THEN TRUE ELSE FALSE END as is_cod_eligible,
    CASE WHEN random() > 0.8 THEN TRUE ELSE FALSE END as is_tet_product,

    (ARRAY['Trẻ em', 'Thanh thiếu niên', 'Người trưởng thành', 'Người cao tuổi', 'Mọi lứa tuổi'])[ceil(random()*5)] as target_age_group_vn,

    (1 + random() * 4)::decimal(3,2) as average_rating,
    (random() * 1000)::int as review_count

FROM generate_series(1, 500);

-- ====================================================================
-- UPDATE KEYS AND RELATIONSHIPS
-- ====================================================================

-- Update customer keys to enable fact table relationships
UPDATE vietnam_dw.dim_customer_vn
SET
    first_purchase_date = CURRENT_DATE - (random() * 365)::int,
    last_purchase_date = CURRENT_DATE - (random() * 30)::int,
    total_orders = (1 + random() * 50)::int,
    total_spent_vnd = (100000 + random() * 50000000)::int;

UPDATE vietnam_dw.dim_customer_vn
SET
    avg_order_value_vnd = CASE WHEN total_orders > 0 THEN total_spent_vnd / total_orders ELSE 0 END,
    customer_lifetime_months = CASE WHEN first_purchase_date IS NOT NULL THEN
        EXTRACT(MONTH FROM AGE(CURRENT_DATE, first_purchase_date))::int ELSE 0 END;

-- Update product pricing consistency
UPDATE vietnam_dw.dim_product_vn
SET
    min_price_vnd = cost_price_vnd,
    max_price_vnd = retail_price_vnd,
    discount_price_vnd = CASE WHEN random() > 0.3 THEN retail_price_vnd * (0.7 + random() * 0.2) ELSE retail_price_vnd END
WHERE discount_price_vnd > retail_price_vnd;

-- ====================================================================
-- SAMPLE FACT DATA
-- ====================================================================

-- Generate sample sales data for the last 30 days
INSERT INTO vietnam_dw.fact_sales_vn (
    time_key, customer_key, product_key, geography_key, platform_key,
    order_id, sale_date, quantity, unit_price_vnd, total_amount_vnd,
    payment_method_vietnamese, is_cod_order, is_tet_order, shipping_method_vn,
    profit_margin_vnd
)
SELECT
    vietnam_dw.get_time_key(CURRENT_DATE - (random() * 30)::int) as time_key,
    (1 + random() * 999)::int as customer_key,
    (1 + random() * 499)::int as product_key,
    (1 + random() * 17)::int as geography_key,
    (1 + random() * 7)::int as platform_key,
    'ORD_VN_' || LPAD(generate_series::text, 8, '0') as order_id,
    CURRENT_DATE - (random() * 30)::int as sale_date,
    (1 + random() * 5)::int as quantity,
    (50000 + random() * 5000000)::int as unit_price_vnd,
    (100000 + random() * 10000000)::int as total_amount_vnd,
    (ARRAY['COD', 'MoMo', 'ZaloPay', 'Banking', 'Visa/MasterCard'])[ceil(random()*5)] as payment_method_vietnamese,
    CASE WHEN random() > 0.4 THEN TRUE ELSE FALSE END as is_cod_order,
    CASE WHEN random() > 0.9 THEN TRUE ELSE FALSE END as is_tet_order,
    (ARRAY['Giao hàng tiêu chuẩn', 'Giao hàng nhanh', 'Giao hàng hỏa tốc', 'Nhận tại cửa hàng'])[ceil(random()*4)] as shipping_method_vn,
    (10000 + random() * 1000000)::int as profit_margin_vnd
FROM generate_series(1, 5000);

-- Update sales with calculated values
UPDATE vietnam_dw.fact_sales_vn
SET
    final_price_vnd = unit_price_vnd,
    total_amount_vnd = quantity * unit_price_vnd,
    profit_margin_percent = CASE WHEN total_amount_vnd > 0 THEN
        (profit_margin_vnd::decimal / total_amount_vnd::decimal * 100) ELSE 0 END;

-- Generate sample customer activity data
INSERT INTO vietnam_dw.fact_customer_activity_vn (
    time_key, customer_key, product_key, platform_key,
    session_id, activity_date, activity_type_vn,
    session_duration_minutes, page_views, device_type_vn, traffic_source_vn
)
SELECT
    vietnam_dw.get_time_key(CURRENT_DATE - (random() * 7)::int) as time_key,
    (1 + random() * 999)::int as customer_key,
    (1 + random() * 499)::int as product_key,
    (1 + random() * 7)::int as platform_key,
    'SESS_' || LPAD(generate_series::text, 10, '0') as session_id,
    CURRENT_DATE - (random() * 7)::int as activity_date,
    (ARRAY['view', 'cart', 'wishlist', 'compare', 'review', 'purchase'])[ceil(random()*6)] as activity_type_vn,
    (1 + random() * 120)::int as session_duration_minutes,
    (1 + random() * 20)::int as page_views,
    (ARRAY['Desktop', 'Mobile', 'Tablet'])[ceil(random()*3)] as device_type_vn,
    (ARRAY['Organic Search', 'Facebook', 'Google Ads', 'Direct', 'Email', 'Affiliate'])[ceil(random()*6)] as traffic_source_vn
FROM generate_series(1, 10000);

-- ====================================================================
-- CREATE SAMPLE AGGREGATES
-- ====================================================================

-- Populate daily sales aggregates for last 30 days
INSERT INTO vietnam_dw.agg_daily_sales_vn (
    sale_date, platform_key, geography_key,
    total_orders, total_customers, total_revenue_vnd,
    avg_order_value_vnd, cod_orders_count, cod_orders_percent
)
SELECT
    s.sale_date,
    s.platform_key,
    s.geography_key,
    COUNT(*) as total_orders,
    COUNT(DISTINCT s.customer_key) as total_customers,
    SUM(s.total_amount_vnd) as total_revenue_vnd,
    AVG(s.total_amount_vnd) as avg_order_value_vnd,
    SUM(CASE WHEN s.is_cod_order THEN 1 ELSE 0 END) as cod_orders_count,
    (SUM(CASE WHEN s.is_cod_order THEN 1 ELSE 0 END)::decimal / COUNT(*)::decimal * 100) as cod_orders_percent
FROM vietnam_dw.fact_sales_vn s
WHERE s.sale_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY s.sale_date, s.platform_key, s.geography_key;

-- ====================================================================
-- DATA QUALITY CHECKS
-- ====================================================================

-- Check record counts
DO $$
DECLARE
    time_count INTEGER;
    customer_count INTEGER;
    product_count INTEGER;
    geography_count INTEGER;
    platform_count INTEGER;
    sales_count INTEGER;
    activity_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO time_count FROM vietnam_dw.dim_time_vn;
    SELECT COUNT(*) INTO customer_count FROM vietnam_dw.dim_customer_vn;
    SELECT COUNT(*) INTO product_count FROM vietnam_dw.dim_product_vn;
    SELECT COUNT(*) INTO geography_count FROM vietnam_dw.dim_geography_vn;
    SELECT COUNT(*) INTO platform_count FROM vietnam_dw.dim_platform_vn;
    SELECT COUNT(*) INTO sales_count FROM vietnam_dw.fact_sales_vn;
    SELECT COUNT(*) INTO activity_count FROM vietnam_dw.fact_customer_activity_vn;

    RAISE NOTICE 'Vietnam Data Warehouse Deployment Summary:';
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Time dimension: % records', time_count;
    RAISE NOTICE 'Customer dimension: % records', customer_count;
    RAISE NOTICE 'Product dimension: % records', product_count;
    RAISE NOTICE 'Geography dimension: % records', geography_count;
    RAISE NOTICE 'Platform dimension: % records', platform_count;
    RAISE NOTICE 'Sales facts: % records', sales_count;
    RAISE NOTICE 'Customer activity facts: % records', activity_count;
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Vietnam Data Warehouse is ready for use!';
    RAISE NOTICE 'Schema: vietnam_dw';
    RAISE NOTICE 'Currency: Vietnamese Dong (VND)';
    RAISE NOTICE 'Time range: 2022-2025 (4 years)';
    RAISE NOTICE 'Sample data: Last 30 days of sales';
END $$;