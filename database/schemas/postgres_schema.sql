-- Ecommerce DSS Database Schema
-- Created: 2024
-- Description: Complete schema for ecommerce decision support system

-- ====================================
-- 1. PRODUCT DATA TABLES
-- ====================================

-- Raw products table (from various sources)
CREATE TABLE raw_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    original_price DECIMAL(10,2),
    discount_percentage DECIMAL(5,2),
    rating DECIMAL(3,1) CHECK (rating >= 0 AND rating <= 5),
    num_reviews INTEGER DEFAULT 0 CHECK (num_reviews >= 0),
    availability VARCHAR(20) DEFAULT 'In Stock',
    
    -- Source tracking
    data_source VARCHAR(50) NOT NULL, -- 'kaggle', 'scraper', 'api'
    source_url TEXT,
    scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cleaned products table (after processing)
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    brand VARCHAR(100) NOT NULL,
    
    -- Pricing
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    original_price DECIMAL(10,2),
    discount_percentage DECIMAL(5,2) DEFAULT 0,
    price_tier VARCHAR(20), -- 'Budget', 'Mid-range', 'Premium', 'Luxury'
    
    -- Ratings & Reviews
    rating DECIMAL(3,1) CHECK (rating >= 0 AND rating <= 5),
    num_reviews INTEGER DEFAULT 0 CHECK (num_reviews >= 0),
    rating_category VARCHAR(20), -- 'Poor', 'Fair', 'Good', 'Excellent'
    
    -- Availability
    availability VARCHAR(20) DEFAULT 'In Stock',
    stock_level INTEGER DEFAULT 0,
    
    -- Features (for ML)
    is_featured BOOLEAN DEFAULT FALSE,
    is_bestseller BOOLEAN DEFAULT FALSE,
    
    -- Time tracking
    first_seen_date DATE,
    last_updated_date DATE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product features table (engineered features)
CREATE TABLE product_features (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) REFERENCES products(product_id),
    
    -- Price features
    price_rank_in_category DECIMAL(5,4), -- Percentile rank
    price_vs_category_mean DECIMAL(8,4),
    price_vs_category_median DECIMAL(8,4),
    
    -- Rating features
    rating_vs_category_avg DECIMAL(8,4),
    reviews_per_rating DECIMAL(10,2),
    
    -- Text features
    name_length INTEGER,
    name_word_count INTEGER,
    description_length INTEGER,
    description_word_count INTEGER,
    
    -- Calculated at
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ====================================
-- 2. CUSTOMER DATA TABLES
-- ====================================

-- Customer profiles (synthetic for demo)
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50)