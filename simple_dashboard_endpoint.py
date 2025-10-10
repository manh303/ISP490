#!/usr/bin/env python3
"""
Simple dashboard endpoint to add to main.py
"""

endpoint_code = '''
@app.get(f"{settings.API_V1_PREFIX}/analytics/dashboard-vietnam")
async def get_vietnam_dashboard(
    db: Database = Depends(get_postgres_session),
    current_user: dict = Depends(lambda: get_auth_service()) if SECURITY_ENABLED else None
):
    """Get dashboard data using correct Vietnam tables"""
    try:
        # Simple metrics from vietnam tables
        metrics = {}

        # Total customers
        result = await db.fetch_one("SELECT COUNT(DISTINCT customer_id) as total FROM vietnam_customers_large")
        metrics['total_customers'] = result['total'] if result else 0

        # Total orders
        result = await db.fetch_one("SELECT COUNT(DISTINCT order_id) as total FROM vietnam_orders_large")
        metrics['total_orders'] = result['total'] if result else 0

        # Total products
        result = await db.fetch_one("SELECT COUNT(DISTINCT product_id) as total FROM vietnam_products_large")
        metrics['total_products'] = result['total'] if result else 0

        # Total revenue
        result = await db.fetch_one("SELECT SUM(total_amount_vnd) as total FROM vietnam_orders_large WHERE total_amount_vnd IS NOT NULL")
        metrics['total_revenue'] = float(result['total']) if result and result['total'] else 0

        # Average order value
        result = await db.fetch_one("SELECT AVG(total_amount_vnd) as avg FROM vietnam_orders_large WHERE total_amount_vnd IS NOT NULL")
        metrics['avg_order_value'] = float(result['avg']) if result and result['avg'] else 0

        # Top categories
        top_categories = await db.fetch_all("""
            SELECT
                category_l1,
                COUNT(*) as product_count,
                AVG(price_vnd) as avg_price
            FROM vietnam_products_large
            WHERE category_l1 IS NOT NULL
            GROUP BY category_l1
            ORDER BY product_count DESC
            LIMIT 5
        """)

        return {
            "success": True,
            "overview_metrics": metrics,
            "top_categories": [dict(row) for row in top_categories],
            "data_summary": {
                "customers": metrics['total_customers'],
                "orders": metrics['total_orders'],
                "products": metrics['total_products'],
                "revenue_vnd": metrics['total_revenue']
            },
            "generated_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Vietnam dashboard error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
'''

print("Add this endpoint to main.py after the existing dashboard endpoint:")
print(endpoint_code)