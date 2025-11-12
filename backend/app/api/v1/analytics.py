"""
Analytics API for chart data
"""
from fastapi import APIRouter, Depends, Query
from typing import Optional
from datetime import datetime, timedelta

router = APIRouter(prefix="/analytics", tags=["Analytics"])

# Dependency to get database manager
async def get_db():
    try:
        from app.main import db_manager
    except ImportError:
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from main import db_manager
    
    # Ensure database is connected
    if not db_manager.is_connected:
        await db_manager.connect()
    
    return db_manager


@router.get("/products/top-rated")
async def get_top_rated_products(
    limit: int = Query(20, ge=1, le=100),
    db = Depends(get_db)
):
    """Top products by rating - Bar Chart"""
    import logging
    logger = logging.getLogger(__name__)
    
    # Use f-string instead of parameter to avoid conversion issues
    query = f"""
        SELECT 
            p.product_name,
            p.rating_avg,
            p.review_count,
            p.price_current as price,
            p.category
        FROM ods_product_clean p
        WHERE p.review_count >= 10
        ORDER BY p.rating_avg DESC, p.review_count DESC
        LIMIT {limit}
    """
    
    logger.info(f"Executing query with limit={limit}")
    logger.info(f"DB connected: {db.is_connected}")
    
    result = await db.execute_query(query)
    
    logger.info(f"Query returned {len(result)} rows")
    if result:
        logger.info(f"First row: {result[0]}")
    
    return {
        "chart_type": "bar",
        "title": f"Top {limit} Products by Rating",
        "x_axis": "product_name",
        "y_axis": "rating_avg",
        "data": result
    }


@router.get("/products/rating-distribution")
async def get_rating_distribution(
    category: Optional[str] = None,
    db = Depends(get_db)
):
    """Rating distribution histogram"""
    if category:
        query = """
            SELECT 
                FLOOR(p.rating_avg) as rating_bucket,
                COUNT(*) as product_count,
                AVG(p.price_current) as avg_price,
                SUM(p.review_count) as total_reviews
            FROM ods_product_clean p
            WHERE p.category = $1
            GROUP BY FLOOR(p.rating_avg)
            ORDER BY rating_bucket
        """
        result = await db.execute_query(query, (category,))
    else:
        query = """
            SELECT 
                FLOOR(p.rating_avg) as rating_bucket,
                COUNT(*) as product_count,
                AVG(p.price_current) as avg_price,
                SUM(p.review_count) as total_reviews
            FROM ods_product_clean p
            GROUP BY FLOOR(p.rating_avg)
            ORDER BY rating_bucket
        """
        result = await db.execute_query(query)
    
    return {
        "chart_type": "histogram",
        "title": f"Rating Distribution{' - ' + category if category else ''}",
        "x_axis": "rating_bucket",
        "y_axis": "product_count",
        "data": result
    }


@router.get("/reviews/trends")
async def get_review_trends(
    days: int = Query(30, ge=7, le=365),
    db = Depends(get_db)
):
    """Review trends over time - Line Chart"""
    query = """
        SELECT 
            DATE(f.captured_at) as date,
            COUNT(DISTINCT f.product_sk) as products_reviewed,
            AVG(f.rating_avg) as avg_rating,
            SUM(f.review_count) as total_reviews
        FROM dwh_fact_product_daily f
        WHERE f.captured_at >= CURRENT_DATE - $1 * INTERVAL '1 day'
        GROUP BY DATE(f.captured_at)
        ORDER BY date
    """
    
    result = await db.execute_query(query, (days,))
    
    return {
        "chart_type": "line",
        "title": f"Review Trends - Last {days} Days",
        "x_axis": "date",
        "y_axis": ["avg_rating", "total_reviews"],
        "data": result
    }


@router.get("/products/price-vs-rating")
async def get_price_vs_rating(
    category: Optional[str] = None,
    db = Depends(get_db)
):
    """Price vs Rating correlation - Scatter Plot"""
    if category:
        query = """
            SELECT 
                p.product_name,
                p.price_current as price,
                p.rating_avg,
                p.review_count,
                p.category
            FROM ods_product_clean p
            WHERE p.category = $1
            ORDER BY p.review_count DESC
            LIMIT 500
        """
        result = await db.execute_query(query, (category,))
    else:
        query = """
            SELECT 
                p.product_name,
                p.price_current as price,
                p.rating_avg,
                p.review_count,
                p.category
            FROM ods_product_clean p
            ORDER BY p.review_count DESC
            LIMIT 500
        """
        result = await db.execute_query(query)
    
    return {
        "chart_type": "scatter",
        "title": f"Price vs Rating{' - ' + category if category else ''}",
        "x_axis": "price",
        "y_axis": "rating_avg",
        "size": "review_count",
        "data": result
    }


@router.get("/products/category-performance")
async def get_category_performance(
    db = Depends(get_db)
):
    """Category performance comparison - Grouped Bar Chart"""
    query = """
        SELECT 
            p.category,
            COUNT(*) as product_count,
            AVG(p.rating_avg) as avg_rating,
            AVG(p.price_current) as avg_price,
            SUM(p.review_count) as total_reviews,
            COUNT(CASE WHEN p.rating_avg >= 4.0 THEN 1 END) as high_rated_count
        FROM ods_product_clean p
        WHERE p.category IS NOT NULL
        GROUP BY p.category
        ORDER BY total_reviews DESC
        LIMIT 15
    """
    
    result = await db.execute_query(query)
    
    return {
        "chart_type": "grouped_bar",
        "title": "Category Performance Analysis",
        "x_axis": "category",
        "y_axes": ["avg_rating", "product_count", "avg_price"],
        "data": result
    }


@router.get("/reviews/sentiment-distribution")
async def get_sentiment_distribution(
    db = Depends(get_db)
):
    """Review sentiment distribution - Pie Chart"""
    query = """
        SELECT 
            CASE 
                WHEN p.rating_avg >= 4.5 THEN 'Excellent'
                WHEN p.rating_avg >= 4.0 THEN 'Good'
                WHEN p.rating_avg >= 3.0 THEN 'Average'
                WHEN p.rating_avg >= 2.0 THEN 'Poor'
                ELSE 'Very Poor'
            END as sentiment,
            COUNT(*) as product_count,
            SUM(p.review_count) as review_count
        FROM ods_product_clean p
        GROUP BY sentiment
        ORDER BY 
            CASE sentiment
                WHEN 'Excellent' THEN 1
                WHEN 'Good' THEN 2
                WHEN 'Average' THEN 3
                WHEN 'Poor' THEN 4
                ELSE 5
            END
    """
    
    result = await db.execute_query(query)
    
    return {
        "chart_type": "pie",
        "title": "Product Sentiment Distribution",
        "label": "sentiment",
        "value": "product_count",
        "data": result
    }


@router.get("/products/price-segments")
async def get_price_segments(
    db = Depends(get_db)
):
    """Price segment analysis - Stacked Bar Chart"""
    query = """
        SELECT 
            CASE 
                WHEN p.price_current < 100000 THEN 'Budget (<100K)'
                WHEN p.price_current < 500000 THEN 'Mid-range (100K-500K)'
                WHEN p.price_current < 1000000 THEN 'Premium (500K-1M)'
                ELSE 'Luxury (>1M)'
            END as price_segment,
            COUNT(*) as product_count,
            AVG(p.rating_avg) as avg_rating,
            SUM(p.review_count) as total_reviews,
            COUNT(CASE WHEN p.rating_avg >= 4.0 THEN 1 END) as high_rated
        FROM ods_product_clean p
        GROUP BY price_segment
        ORDER BY 
            CASE price_segment
                WHEN 'Budget (<100K)' THEN 1
                WHEN 'Mid-range (100K-500K)' THEN 2
                WHEN 'Premium (500K-1M)' THEN 3
                ELSE 4
            END
    """
    
    result = await db.execute_query(query)
    
    return {
        "chart_type": "stacked_bar",
        "title": "Price Segment Analysis",
        "x_axis": "price_segment",
        "y_axes": ["product_count", "high_rated"],
        "data": result
    }


@router.get("/dashboard/summary")
async def get_dashboard_summary(
    db = Depends(get_db)
):
    """Dashboard summary metrics"""
    query = """
        SELECT 
            COUNT(*) as total_products,
            AVG(rating_avg) as overall_avg_rating,
            SUM(review_count) as total_reviews,
            AVG(price_current) as avg_price,
            COUNT(DISTINCT category) as total_categories,
            COUNT(CASE WHEN rating_avg >= 4.0 THEN 1 END) as high_rated_products,
            COUNT(CASE WHEN review_count >= 100 THEN 1 END) as popular_products
        FROM ods_product_clean
    """
    
    result = await db.execute_query(query)
    data = result[0] if result else {}
    
    return {
        "summary": data,
        "timestamp": datetime.now().isoformat()
    }
