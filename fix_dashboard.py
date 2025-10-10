#!/usr/bin/env python3
import re

# Read the main.py file
with open('backend/app/main.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the analytics dashboard queries
fixes = [
    # Fix customer count query
    (r'SELECT COUNT\(DISTINCT customer_unique_id\) as total FROM dw_ecommerce_data',
     'SELECT COUNT(DISTINCT customer_id) as total FROM vietnam_customers_large'),

    # Fix order count query
    (r'SELECT COUNT\(DISTINCT order_id\) as total FROM dw_ecommerce_data',
     'SELECT COUNT(DISTINCT order_id) as total FROM vietnam_orders_large'),

    # Fix revenue sum query
    (r'SELECT SUM\(payment_value\) as total FROM dw_ecommerce_data WHERE payment_value IS NOT NULL',
     'SELECT SUM(total_amount_vnd) as total FROM vietnam_orders_large WHERE total_amount_vnd IS NOT NULL'),

    # Fix average order value query
    (r'SELECT AVG\(payment_value\) as avg FROM dw_ecommerce_data WHERE payment_value IS NOT NULL',
     'SELECT AVG(total_amount_vnd) as avg FROM vietnam_orders_large WHERE total_amount_vnd IS NOT NULL'),

    # Fix database record count
    (r'SELECT COUNT\(\*\) as count FROM dw_ecommerce_data',
     'SELECT COUNT(*) as count FROM vietnam_products_large'),
]

# Apply all fixes
for pattern, replacement in fixes:
    content = re.sub(pattern, replacement, content)

# Fix complex queries manually
content = re.sub(
    r'SELECT\s+DATE\(order_purchase_timestamp\) as date,\s+COUNT\(DISTINCT order_id\) as orders,\s+SUM\(payment_value\) as revenue\s+FROM dw_ecommerce_data\s+WHERE order_purchase_timestamp IS NOT NULL\s+AND payment_value IS NOT NULL',
    '''SELECT
                DATE(order_date) as date,
                COUNT(DISTINCT order_id) as orders,
                SUM(total_amount_vnd) as revenue
            FROM vietnam_orders_large
            WHERE order_date IS NOT NULL
            AND total_amount_vnd IS NOT NULL''',
    content,
    flags=re.DOTALL
)

content = re.sub(
    r'SELECT\s+product_category_name_english,\s+COUNT\(\*\) as sales_count,\s+SUM\(payment_value\) as total_revenue,\s+AVG\(review_score\) as avg_rating\s+FROM dw_ecommerce_data\s+WHERE product_category_name_english IS NOT NULL\s+AND payment_value IS NOT NULL',
    '''SELECT
                category_l1,
                COUNT(*) as sales_count,
                SUM(price_vnd) as total_revenue,
                AVG(rating) as avg_rating
            FROM vietnam_products_large
            WHERE category_l1 IS NOT NULL
            AND price_vnd IS NOT NULL''',
    content,
    flags=re.DOTALL
)

# Write back the fixed content
with open('backend/app/main.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Dashboard queries fixed successfully!")