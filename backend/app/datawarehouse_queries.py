#!/usr/bin/env python3
"""
Vietnam E-commerce Datawarehouse Queries
Optimized queries for Medallion Architecture (Bronze→Silver→Gold)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class DatawarehouseQueries:
    """Centralized datawarehouse queries for Vietnam E-commerce DSS"""

    @staticmethod
    async def get_dashboard_metrics(db_manager) -> Dict[str, Any]:
        """Get comprehensive dashboard metrics from datawarehouse"""
        try:
            logger.info("Querying Vietnam datawarehouse for dashboard metrics...")

            # 1. Overview Metrics from DW Core (Star Schema)
            overview_query = """
                SELECT
                    COUNT(DISTINCT fs.customer_key) as total_customers,
                    COUNT(fs.sales_key) as total_orders,
                    COUNT(DISTINCT fs.product_key) as total_products,
                    COALESCE(SUM(fs.revenue_amount), 0) as total_revenue,
                    COALESCE(AVG(fs.revenue_amount), 0) as avg_order_value,
                    COUNT(CASE WHEN dd.date_value >= CURRENT_DATE THEN 1 END) as today_orders
                FROM dw_core.fact_sales fs
                JOIN dw_core.dim_date dd ON fs.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '30 days'
            """
            overview_result = await db_manager.execute_query(overview_query)

            # 2. Daily Performance Trends (Gold Layer)
            daily_trends_query = """
                SELECT
                    date_value,
                    COALESCE(total_orders, 0) as orders,
                    COALESCE(total_revenue, 0) as revenue,
                    COALESCE(unique_customers, 0) as customers,
                    COALESCE(avg_order_value, 0) as avg_order_value
                FROM gold_layer.daily_business_metrics gbm
                JOIN dw_core.dim_date dd ON gbm.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY dd.date_value DESC
                LIMIT 30
            """
            daily_trends_result = await db_manager.execute_query(daily_trends_query)

            # 3. Regional Performance (Vietnam provinces)
            regional_query = """
                SELECT
                    dg.province_name_vn as region,
                    dg.region_vn as area,
                    COUNT(fs.sales_key) as orders,
                    SUM(fs.revenue_amount) as revenue,
                    COUNT(DISTINCT fs.customer_key) as customers,
                    AVG(fs.revenue_amount) as avg_order_value
                FROM dw_core.fact_sales fs
                JOIN dw_core.dim_geography dg ON fs.geography_key = dg.geography_key
                JOIN dw_core.dim_date dd ON fs.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY dg.province_name_vn, dg.region_vn
                ORDER BY revenue DESC
                LIMIT 15
            """
            regional_result = await db_manager.execute_query(regional_query)

            # 4. Top Products Performance
            top_products_query = """
                SELECT
                    dp.product_name_vn as product_name,
                    dp.category_name_vn as category,
                    dp.brand_name as brand,
                    COUNT(fs.sales_key) as sales_count,
                    SUM(fs.revenue_amount) as revenue,
                    SUM(fs.quantity) as quantity_sold,
                    AVG(fs.unit_price) as avg_price
                FROM dw_core.fact_sales fs
                JOIN dw_core.dim_product dp ON fs.product_key = dp.product_key
                JOIN dw_core.dim_date dd ON fs.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '7 days'
                    AND dp.is_active = true
                GROUP BY dp.product_name_vn, dp.category_name_vn, dp.brand_name
                ORDER BY revenue DESC
                LIMIT 10
            """
            top_products_result = await db_manager.execute_query(top_products_query)

            # 5. Customer Segments Analysis
            customer_segments_query = """
                SELECT
                    dc.customer_segment_vn as segment,
                    COUNT(DISTINCT dc.customer_key) as customer_count,
                    AVG(dc.customer_lifetime_value) as avg_clv,
                    SUM(fs.revenue_amount) as segment_revenue,
                    AVG(fs.revenue_amount) as avg_order_value,
                    COUNT(fs.sales_key) as total_orders
                FROM dw_core.dim_customer dc
                LEFT JOIN dw_core.fact_sales fs ON dc.customer_key = fs.customer_key
                LEFT JOIN dw_core.dim_date dd ON fs.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '30 days' OR dd.date_value IS NULL
                GROUP BY dc.customer_segment_vn
                ORDER BY segment_revenue DESC NULLS LAST
            """
            customer_segments_result = await db_manager.execute_query(customer_segments_query)

            # 6. Payment Methods Performance
            payment_methods_query = """
                SELECT
                    dpm.payment_method_name_vn as payment_method,
                    dpm.payment_provider as provider,
                    COUNT(fs.sales_key) as transactions,
                    SUM(fs.revenue_amount) as revenue,
                    AVG(fs.revenue_amount) as avg_transaction_value
                FROM dw_core.fact_sales fs
                JOIN dw_core.dim_payment_method dpm ON fs.payment_method_key = dpm.payment_method_key
                JOIN dw_core.dim_date dd ON fs.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY dpm.payment_method_name_vn, dpm.payment_provider
                ORDER BY revenue DESC
                LIMIT 10
            """
            payment_methods_result = await db_manager.execute_query(payment_methods_query)

            # 7. Data Pipeline Health Check
            pipeline_health_query = """
                SELECT
                    'bronze_layer' as layer,
                    COUNT(*) as record_count,
                    MAX(_ingestion_timestamp) as last_update,
                    COUNT(CASE WHEN _is_processed = true THEN 1 END) as processed_count
                FROM bronze_layer.raw_orders
                WHERE _ingestion_timestamp >= CURRENT_DATE
                UNION ALL
                SELECT
                    'silver_layer' as layer,
                    COUNT(*) as record_count,
                    MAX(processed_timestamp) as last_update,
                    COUNT(CASE WHEN data_quality_score >= 0.8 THEN 1 END) as quality_passed
                FROM silver_layer.cleaned_orders
                WHERE processed_timestamp >= CURRENT_DATE
                UNION ALL
                SELECT
                    'gold_layer' as layer,
                    COUNT(*) as record_count,
                    MAX(created_timestamp) as last_update,
                    COUNT(*) as aggregated_metrics
                FROM gold_layer.daily_business_metrics
                WHERE date_key >= EXTRACT(YEAR FROM CURRENT_DATE) * 10000 + EXTRACT(MONTH FROM CURRENT_DATE) * 100 + EXTRACT(DAY FROM CURRENT_DATE)
            """
            pipeline_health_result = await db_manager.execute_query(pipeline_health_query)

            # 8. Inventory Alerts
            inventory_alerts_query = """
                SELECT
                    dp.product_name_vn as product_name,
                    dp.category_name_vn as category,
                    fi.current_stock,
                    fi.reorder_level,
                    fi.stock_status,
                    CASE
                        WHEN fi.current_stock <= fi.critical_level THEN 'critical'
                        WHEN fi.current_stock <= fi.reorder_level THEN 'low'
                        WHEN fi.current_stock >= fi.overstock_level THEN 'overstock'
                        ELSE 'normal'
                    END as alert_level
                FROM vietnam_dw.fact_inventory_vn fi
                JOIN dw_core.dim_product dp ON fi.product_key = dp.product_key
                WHERE fi.current_stock <= fi.reorder_level
                   OR fi.current_stock >= fi.overstock_level
                ORDER BY
                    CASE
                        WHEN fi.current_stock <= fi.critical_level THEN 1
                        WHEN fi.current_stock <= fi.reorder_level THEN 2
                        ELSE 3
                    END,
                    fi.current_stock ASC
                LIMIT 20
            """
            inventory_alerts_result = await db_manager.execute_query(inventory_alerts_query)

            # Process Results
            results = {
                "overview": overview_result[0] if overview_result else {},
                "daily_trends": daily_trends_result or [],
                "regional_performance": regional_result or [],
                "top_products": top_products_result or [],
                "customer_segments": customer_segments_result or [],
                "payment_methods": payment_methods_result or [],
                "pipeline_health": pipeline_health_result or [],
                "inventory_alerts": inventory_alerts_result or []
            }

            return results

        except Exception as e:
            logger.error(f"Error querying datawarehouse: {e}")
            return {}

    @staticmethod
    async def get_customer_analytics(db_manager) -> Dict[str, Any]:
        """Get detailed customer analytics from datawarehouse"""
        try:
            # RFM Analysis from Customer Dimension
            rfm_query = """
                SELECT
                    customer_segment_vn as segment,
                    rfm_segment,
                    COUNT(*) as customer_count,
                    AVG(customer_lifetime_value) as avg_clv,
                    AVG(recency_score) as avg_recency,
                    AVG(frequency_score) as avg_frequency,
                    AVG(monetary_score) as avg_monetary,
                    SUM(total_orders) as total_segment_orders,
                    SUM(total_spent) as total_segment_revenue
                FROM dw_core.dim_customer
                WHERE is_active = true
                GROUP BY customer_segment_vn, rfm_segment
                ORDER BY total_segment_revenue DESC
            """
            rfm_result = await db_manager.execute_query(rfm_query)

            # Customer Behavior Analysis
            behavior_query = """
                SELECT
                    dc.age_group_vn as age_group,
                    dc.gender_vn as gender,
                    dg.province_name_vn as province,
                    COUNT(DISTINCT dc.customer_key) as customer_count,
                    AVG(fs.revenue_amount) as avg_order_value,
                    COUNT(fs.sales_key) as total_orders,
                    SUM(fs.revenue_amount) as total_revenue
                FROM dw_core.dim_customer dc
                LEFT JOIN dw_core.fact_sales fs ON dc.customer_key = fs.customer_key
                LEFT JOIN dw_core.dim_geography dg ON dc.geography_key = dg.geography_key
                LEFT JOIN dw_core.dim_date dd ON fs.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '90 days' OR dd.date_value IS NULL
                GROUP BY dc.age_group_vn, dc.gender_vn, dg.province_name_vn
                ORDER BY total_revenue DESC NULLS LAST
                LIMIT 50
            """
            behavior_result = await db_manager.execute_query(behavior_query)

            return {
                "rfm_analysis": rfm_result or [],
                "customer_behavior": behavior_result or []
            }

        except Exception as e:
            logger.error(f"Error in customer analytics: {e}")
            return {}

    @staticmethod
    async def get_sales_analytics(db_manager) -> Dict[str, Any]:
        """Get detailed sales analytics from datawarehouse"""
        try:
            # Sales Performance by Time
            time_performance_query = """
                SELECT
                    dd.month_name_vn as month,
                    dd.quarter_name_vn as quarter,
                    dd.year_number as year,
                    COUNT(fs.sales_key) as total_orders,
                    SUM(fs.revenue_amount) as total_revenue,
                    COUNT(DISTINCT fs.customer_key) as unique_customers,
                    AVG(fs.revenue_amount) as avg_order_value
                FROM dw_core.fact_sales fs
                JOIN dw_core.dim_date dd ON fs.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY dd.month_name_vn, dd.quarter_name_vn, dd.year_number, dd.month_number
                ORDER BY dd.year_number DESC, dd.month_number DESC
                LIMIT 12
            """
            time_performance_result = await db_manager.execute_query(time_performance_query)

            # Category Performance
            category_performance_query = """
                SELECT
                    dp.category_name_vn as category,
                    dp.subcategory_name_vn as subcategory,
                    COUNT(fs.sales_key) as total_sales,
                    SUM(fs.revenue_amount) as total_revenue,
                    SUM(fs.quantity) as total_quantity,
                    AVG(fs.unit_price) as avg_price,
                    COUNT(DISTINCT fs.customer_key) as unique_buyers
                FROM dw_core.fact_sales fs
                JOIN dw_core.dim_product dp ON fs.product_key = dp.product_key
                JOIN dw_core.dim_date dd ON fs.date_key = dd.date_key
                WHERE dd.date_value >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY dp.category_name_vn, dp.subcategory_name_vn
                ORDER BY total_revenue DESC
                LIMIT 20
            """
            category_performance_result = await db_manager.execute_query(category_performance_query)

            return {
                "time_performance": time_performance_result or [],
                "category_performance": category_performance_result or []
            }

        except Exception as e:
            logger.error(f"Error in sales analytics: {e}")
            return {}

    @staticmethod
    def format_dashboard_response(query_results: Dict[str, Any], mock_fallback: bool = False) -> Dict[str, Any]:
        """Format datawarehouse query results for dashboard API response"""

        if not query_results or mock_fallback:
            return DatawarehouseQueries._get_mock_dashboard_data()

        # Extract overview metrics
        overview = query_results.get("overview", {})
        total_customers = overview.get('total_customers', 0)
        total_orders = overview.get('total_orders', 0)
        total_products = overview.get('total_products', 0)
        total_revenue = float(overview.get('total_revenue', 0))
        avg_order_value = float(overview.get('avg_order_value', 0))
        today_orders = overview.get('today_orders', 0)

        # Process regional data
        regional_data = [
            {
                "region": row.get('region', 'Unknown'),
                "area": row.get('area', ''),
                "orders": row.get('orders', 0),
                "revenue": float(row.get('revenue', 0)),
                "customers": row.get('customers', 0),
                "avg_order_value": float(row.get('avg_order_value', 0))
            }
            for row in query_results.get("regional_performance", [])
        ]

        # Process top products
        top_products_data = [
            {
                "name": row.get('product_name', 'Unknown Product'),
                "category": row.get('category', 'Unknown'),
                "brand": row.get('brand', 'Unknown'),
                "sales": row.get('sales_count', 0),
                "revenue": float(row.get('revenue', 0)),
                "quantity": row.get('quantity_sold', 0),
                "avg_price": float(row.get('avg_price', 0))
            }
            for row in query_results.get("top_products", [])
        ]

        # Process customer segments
        customer_segments_data = {}
        for row in query_results.get("customer_segments", []):
            segment = row.get('segment', 'Unknown').lower().replace(' ', '_')
            customer_segments_data[segment] = {
                "count": row.get('customer_count', 0),
                "avg_clv": float(row.get('avg_clv', 0)),
                "revenue": float(row.get('segment_revenue', 0)),
                "avg_order_value": float(row.get('avg_order_value', 0)),
                "total_orders": row.get('total_orders', 0)
            }

        # Process payment methods
        payment_methods_data = [
            {
                "method": row.get('payment_method', 'Unknown'),
                "provider": row.get('provider', ''),
                "transactions": row.get('transactions', 0),
                "revenue": float(row.get('revenue', 0)),
                "avg_value": float(row.get('avg_transaction_value', 0))
            }
            for row in query_results.get("payment_methods", [])
        ]

        # Process pipeline health
        pipeline_health = {}
        for row in query_results.get("pipeline_health", []):
            layer = row.get('layer', 'unknown')
            pipeline_health[layer] = {
                "record_count": row.get('record_count', 0),
                "last_update": row.get('last_update').isoformat() if row.get('last_update') else None,
                "processed_count": row.get('processed_count', 0) or row.get('quality_passed', 0) or row.get('aggregated_metrics', 0)
            }

        # Process daily trends
        daily_trends = [
            {
                "date": row.get('date_value').isoformat() if row.get('date_value') else None,
                "orders": row.get('orders', 0),
                "revenue": float(row.get('revenue', 0)),
                "customers": row.get('customers', 0),
                "avg_order_value": float(row.get('avg_order_value', 0))
            }
            for row in query_results.get("daily_trends", [])
        ]

        # Process inventory alerts
        inventory_alerts = [
            {
                "product_name": row.get('product_name', 'Unknown'),
                "category": row.get('category', 'Unknown'),
                "current_stock": row.get('current_stock', 0),
                "reorder_level": row.get('reorder_level', 0),
                "stock_status": row.get('stock_status', 'unknown'),
                "alert_level": row.get('alert_level', 'normal')
            }
            for row in query_results.get("inventory_alerts", [])
        ]

        return {
            "status": "datawarehouse_connected",
            "data_source": "Vietnam E-commerce Datawarehouse (Medallion Architecture)",
            "overview_metrics": {
                "total_customers": total_customers,
                "total_orders": total_orders,
                "total_products": total_products,
                "total_revenue": total_revenue,
                "avg_order_value": avg_order_value,
                "today_orders": today_orders,
                "conversion_rate": round((total_orders / max(total_customers, 1)) * 100, 2) if total_customers > 0 else 0,
                "growth_rate": round(((today_orders / max(total_orders - today_orders, 1)) - 1) * 100, 2) if total_orders > today_orders else 0
            },
            "regional_performance": regional_data,
            "top_products": top_products_data,
            "payment_methods": payment_methods_data,
            "customer_insights": {
                "segments": customer_segments_data,
                "total_segments": len(customer_segments_data)
            },
            "daily_trends": daily_trends,
            "inventory_alerts": inventory_alerts,
            "datawarehouse_info": {
                "pipeline_health": pipeline_health,
                "total_layers": len(pipeline_health),
                "data_freshness": "medallion_architecture",
                "last_update": max([
                    layer_info.get('last_update', '1970-01-01T00:00:00')
                    for layer_info in pipeline_health.values()
                    if layer_info.get('last_update')
                ], default=datetime.now().isoformat())
            },
            "timestamp": datetime.now().isoformat()
        }

    @staticmethod
    def _get_mock_dashboard_data() -> Dict[str, Any]:
        """Fallback mock data when datawarehouse is not available"""
        from random import randint, choice, uniform

        current_time = datetime.now()
        vietnam_provinces = [
            "Hồ Chí Minh", "Hà Nội", "Đà Nẵng", "Cần Thơ", "Hải Phòng",
            "Bình Dương", "Đồng Nai", "Khánh Hòa", "Lâm Đồng", "Quảng Nam"
        ]

        return {
            "status": "mock_data_fallback",
            "data_source": "Mock Vietnam E-commerce Data (Datawarehouse Unavailable)",
            "overview_metrics": {
                "total_customers": 45267,
                "total_orders": 23891,
                "total_products": randint(4800, 5200),
                "total_revenue": 2847563000,  # VND
                "avg_order_value": 119250,   # VND
                "today_orders": 234,
                "conversion_rate": 3.2,
                "growth_rate": 12.5
            },
            "regional_performance": [
                {
                    "region": province,
                    "area": choice(["Miền Nam", "Miền Bắc", "Miền Trung"]),
                    "orders": randint(100, 2000),
                    "revenue": randint(50000000, 500000000),
                    "customers": randint(50, 1000),
                    "avg_order_value": randint(80000, 250000)
                }
                for province in vietnam_provinces
            ],
            "top_products": [
                {
                    "name": "iPhone 15 Pro Max",
                    "category": "Điện tử",
                    "brand": "Apple",
                    "sales": 847,
                    "revenue": 890500000,
                    "quantity": 847,
                    "avg_price": 1051000
                },
                {
                    "name": "Samsung Galaxy S24",
                    "category": "Điện tử",
                    "brand": "Samsung",
                    "sales": 623,
                    "revenue": 467250000,
                    "quantity": 623,
                    "avg_price": 750000
                }
            ],
            "payment_methods": [
                {"method": "VNPay", "provider": "VNPay", "transactions": 8450, "revenue": 892000000, "avg_value": 105562},
                {"method": "MoMo", "provider": "MoMo", "transactions": 6230, "revenue": 678000000, "avg_value": 108825},
                {"method": "ZaloPay", "provider": "ZaloPay", "transactions": 4120, "revenue": 445000000, "avg_value": 108010},
                {"method": "Thẻ ngân hàng", "provider": "Banking", "transactions": 2340, "revenue": 298000000, "avg_value": 127350}
            ],
            "customer_insights": {
                "segments": {
                    "vip": {"count": 1247, "avg_clv": 2850000, "revenue": 856000000, "avg_order_value": 186000, "total_orders": 4602},
                    "premium": {"count": 3421, "avg_clv": 1250000, "revenue": 567000000, "avg_order_value": 145000, "total_orders": 3910},
                    "regular": {"count": 8934, "avg_clv": 480000, "revenue": 789000000, "avg_order_value": 95000, "total_orders": 8305},
                    "new": {"count": 2456, "avg_clv": 180000, "revenue": 234000000, "avg_order_value": 78000, "total_orders": 3000}
                },
                "total_segments": 4
            },
            "daily_trends": [
                {
                    "date": (current_time - timedelta(days=i)).strftime("%Y-%m-%d"),
                    "orders": randint(200, 500),
                    "revenue": randint(20000000, 60000000),
                    "customers": randint(150, 350),
                    "avg_order_value": randint(80000, 150000)
                }
                for i in range(30, 0, -1)
            ],
            "inventory_alerts": [
                {"product_name": "iPhone 15 Pro", "category": "Điện tử", "current_stock": 5, "reorder_level": 20, "stock_status": "low", "alert_level": "critical"},
                {"product_name": "Samsung Galaxy Watch", "category": "Điện tử", "current_stock": 0, "reorder_level": 15, "stock_status": "out_of_stock", "alert_level": "critical"},
                {"product_name": "AirPods Pro", "category": "Điện tử", "current_stock": 8, "reorder_level": 25, "stock_status": "low", "alert_level": "low"}
            ],
            "datawarehouse_info": {
                "pipeline_health": {
                    "bronze_layer": {"record_count": 12450, "last_update": current_time.isoformat(), "processed_count": 12450},
                    "silver_layer": {"record_count": 12380, "last_update": current_time.isoformat(), "processed_count": 12350},
                    "gold_layer": {"record_count": 30, "last_update": current_time.isoformat(), "processed_count": 30}
                },
                "total_layers": 3,
                "data_freshness": "mock_medallion_architecture",
                "last_update": current_time.isoformat()
            },
            "timestamp": current_time.isoformat()
        }

    @staticmethod
    async def get_dashboard_metrics(db):
        """Get real metrics from data warehouse"""
        try:
            # Get current metrics from Gold layer
            daily_metrics = """
                SELECT 
                    total_orders,
                    total_revenue,
                    avg_order_value,
                    unique_customers,
                    total_products,
                    vip_customers,
                    premium_customers
                FROM gold.daily_business_metrics
                WHERE date = CURRENT_DATE
            """
            
            # Get sales trends from Silver layer
            sales_trends = """
                SELECT 
                    date,
                    total_revenue,
                    order_count,
                    customer_count
                FROM silver.daily_sales_trends
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY date DESC
            """
            
            # Get top products from DW Core
            top_products = """
                SELECT 
                    p.product_name,
                    p.category,
                    SUM(f.quantity) as total_quantity,
                    SUM(f.revenue) as total_revenue
                FROM dw_core.fact_sales f
                JOIN dw_core.dim_product p ON f.product_key = p.product_key
                WHERE f.date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY p.product_name, p.category
                ORDER BY total_revenue DESC
                LIMIT 10
            """

            # Execute queries
            metrics_result = await db.execute_query(daily_metrics)
            trends_result = await db.execute_query(sales_trends)
            products_result = await db.execute_query(top_products)

            return {
                "daily_metrics": metrics_result,
                "sales_trends": trends_result,
                "top_products": products_result
            }

        except Exception as e:
            print(f"Error getting dashboard metrics: {e}")
            return None