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
            COALESCE(p.rating_avg, 0) as rating_avg,
            COALESCE(p.review_count, 0) as review_count,
            COALESCE(p.price_current, 0) as price,
            COALESCE(p.category, 'Unknown') as category
        FROM ods_product_clean p
        WHERE p.review_count IS NOT NULL AND p.review_count >= 10
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
        "data": result or []
    }


@router.get("/products/rating-distribution")
async def get_rating_distribution(
    category: Optional[str] = None,
    db = Depends(get_db)
):
    """Rating distribution histogram"""
    import logging
    logger = logging.getLogger(__name__)

    if category:
        query = f"""
            SELECT 
                FLOOR(COALESCE(p.rating_avg, 0)) as rating_bucket,
                COUNT(*) as product_count,
                COALESCE(AVG(p.price_current), 0) as avg_price,
                COALESCE(SUM(p.review_count), 0) as total_reviews
            FROM ods_product_clean p
            WHERE p.category = '{category}' AND p.rating_avg IS NOT NULL
            GROUP BY FLOOR(COALESCE(p.rating_avg, 0))
            ORDER BY rating_bucket
        """
        logger.info(f"Executing query with category={category}")
        logger.info(f"DB connected: {db.is_connected}")

        result = await db.execute_query(query)
    else:
        query = """
            SELECT 
                FLOOR(COALESCE(p.rating_avg, 0)) as rating_bucket,
                COUNT(*) as product_count,
                COALESCE(AVG(p.price_current), 0) as avg_price,
                COALESCE(SUM(p.review_count), 0) as total_reviews
            FROM ods_product_clean p
            WHERE p.rating_avg IS NOT NULL
            GROUP BY FLOOR(COALESCE(p.rating_avg, 0))
            ORDER BY rating_bucket
        """
        result = await db.execute_query(query)
    
    return {
        "chart_type": "histogram",
        "title": f"Rating Distribution{' - ' + category if category else ''}",
        "x_axis": "rating_bucket",
        "y_axis": "product_count",
        "data": result or []
    }


@router.get("/reviews/trends")
async def get_review_trends(
    days: int = Query(30, ge=7, le=365),
    db = Depends(get_db)
):
    """Review trends over time - Line Chart"""
    query = f"""
        SELECT 
            DATE(pp.captured_at) as date,
            COUNT(DISTINCT pp.global_product_id) as products_reviewed,
            COALESCE(AVG(COALESCE(opc.rating_avg, 3.5)), 0) as avg_rating,
            COALESCE(SUM(COALESCE(opc.review_count, 0)), 0) as total_reviews
        FROM ods_price_point pp
        LEFT JOIN ods_product_clean opc ON pp.global_product_id = opc.global_product_id
        WHERE pp.captured_at >= CURRENT_DATE - {days} * INTERVAL '1 day'
        GROUP BY DATE(pp.captured_at)
        ORDER BY date ASC
    """
    
    result = await db.execute_query(query)
    
    return {
        "chart_type": "line",
        "title": f"Review Trends - Last {days} Days",
        "x_axis": "date",
        "y_axis": ["avg_rating", "total_reviews"],
        "data": result or []
    }


@router.get("/products/price-vs-rating")
async def get_price_vs_rating(
    category: Optional[str] = None,
    db = Depends(get_db)
):
    """Price vs Rating correlation - Scatter Plot"""
    if category:
        query = f"""
            SELECT 
                p.product_name,
                COALESCE(p.price_current, 0) as price,
                COALESCE(p.rating_avg, 0) as rating_avg,
                COALESCE(p.review_count, 0) as review_count,
                COALESCE(p.category, 'Unknown') as category
            FROM ods_product_clean p
            WHERE p.category = '{category}' AND p.review_count IS NOT NULL
            ORDER BY p.review_count DESC
            LIMIT 500
        """
        result = await db.execute_query(query)
    else:
        query = """
            SELECT 
                p.product_name,
                COALESCE(p.price_current, 0) as price,
                COALESCE(p.rating_avg, 0) as rating_avg,
                COALESCE(p.review_count, 0) as review_count,
                COALESCE(p.category, 'Unknown') as category
            FROM ods_product_clean p
            WHERE p.review_count IS NOT NULL
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
        "data": result or []
    }


@router.get("/products/category-performance")
async def get_category_performance(
    db = Depends(get_db)
):
    """Category performance comparison - Grouped Bar Chart"""
    query = """
        SELECT 
            COALESCE(p.category, 'Unknown') as category,
            COUNT(*) as product_count,
            COALESCE(AVG(p.rating_avg), 0) as avg_rating,
            COALESCE(AVG(p.price_current), 0) as avg_price,
            COALESCE(SUM(p.review_count), 0) as total_reviews,
            COUNT(CASE WHEN p.rating_avg >= 4.0 THEN 1 END) as high_rated_count
        FROM ods_product_clean p
        WHERE p.category IS NOT NULL
        GROUP BY COALESCE(p.category, 'Unknown')
        ORDER BY total_reviews DESC
        LIMIT 15
    """
    
    result = await db.execute_query(query)
    
    return {
        "chart_type": "grouped_bar",
        "title": "Category Performance Analysis",
        "x_axis": "category",
        "y_axes": ["avg_rating", "product_count", "avg_price"],
        "data": result or []
    }


@router.get("/reviews/sentiment-distribution")
async def get_sentiment_distribution(
    db = Depends(get_db)
):
    """Review sentiment distribution - Pie Chart"""
    query = """
        WITH sentiment_data AS (
            SELECT 
                CASE 
                    WHEN COALESCE(rating_avg, 0) >= 4.5 THEN 'Excellent'
                    WHEN COALESCE(rating_avg, 0) >= 4.0 THEN 'Good'
                    WHEN COALESCE(rating_avg, 0) >= 3.0 THEN 'Average'
                    WHEN COALESCE(rating_avg, 0) >= 2.0 THEN 'Poor'
                    ELSE 'Very Poor'
                END as sentiment,
                COALESCE(review_count, 0) as review_count
            FROM ods_product_clean
            WHERE rating_avg IS NOT NULL
        )
        SELECT 
            sentiment,
            COUNT(*) as product_count,
            COALESCE(SUM(review_count), 0) as review_count
        FROM sentiment_data
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
        "data": result or []
    }


@router.get("/products/price-segments")
async def get_price_segments(
    db = Depends(get_db)
):
    """Price segment analysis - Stacked Bar Chart"""
    query = """
        WITH price_data AS (
            SELECT 
                CASE 
                    WHEN COALESCE(price_current, 0) < 100000 THEN 'Budget (<100K)'
                    WHEN COALESCE(price_current, 0) < 500000 THEN 'Mid-range (100K-500K)'
                    WHEN COALESCE(price_current, 0) < 1000000 THEN 'Premium (500K-1M)'
                    ELSE 'Luxury (>1M)'
                END as price_segment,
                COALESCE(rating_avg, 0) as rating_avg,
                COALESCE(review_count, 0) as review_count
            FROM ods_product_clean
            WHERE price_current IS NOT NULL
        )
        SELECT 
            price_segment,
            COUNT(*) as product_count,
            COALESCE(AVG(rating_avg), 0) as avg_rating,
            COALESCE(SUM(review_count), 0) as total_reviews,
            COUNT(CASE WHEN rating_avg >= 4.0 THEN 1 END) as high_rated
        FROM price_data
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
        "data": result or []
    }


@router.get("/platforms/comparison")
async def get_platform_comparison(
    db = Depends(get_db)
):
    """Platform comparison - Tiki vs Lazada - Grouped Bar Chart"""
    query = """
        SELECT 
            COALESCE(p.source_platform, 'Unknown') as platform,
            COUNT(*) as product_count,
            COALESCE(AVG(p.rating_avg), 0) as avg_rating,
            COALESCE(AVG(p.price_current), 0) as avg_price,
            COALESCE(SUM(p.review_count), 0) as total_reviews,
            COUNT(CASE WHEN p.rating_avg >= 4.0 THEN 1 END) as high_rated_count
        FROM ods_product_clean p
        WHERE p.source_platform IN ('tiki', 'lazada')
        GROUP BY COALESCE(p.source_platform, 'Unknown')
        ORDER BY product_count DESC
    """
    
    result = await db.execute_query(query)
    
    return {
        "chart_type": "grouped_bar",
        "title": "Platform Comparison: Tiki vs Lazada",
        "x_axis": "platform",
        "y_axes": ["product_count", "avg_rating", "total_reviews"],
        "data": result or []
    }


@router.get("/platforms/price-comparison")
async def get_platform_price_comparison(
    category: Optional[str] = None,
    db = Depends(get_db)
):
    """Platform price comparison by category - Box Plot data"""
    if category:
        query = f"""
            SELECT 
                COALESCE(p.source_platform, 'Unknown') as platform,
                COALESCE(p.category, 'Unknown') as category,
                COALESCE(AVG(p.price_current), 0) as avg_price,
                COALESCE(MIN(p.price_current), 0) as min_price,
                COALESCE(MAX(p.price_current), 0) as max_price,
                COUNT(*) as product_count
            FROM ods_product_clean p
            WHERE p.source_platform IN ('tiki', 'lazada')
            AND p.category = '{category}' AND p.price_current IS NOT NULL
            GROUP BY COALESCE(p.source_platform, 'Unknown'), COALESCE(p.category, 'Unknown')
        """
    else:
        query = """
            SELECT 
                COALESCE(p.source_platform, 'Unknown') as platform,
                COALESCE(p.category, 'Unknown') as category,
                COALESCE(AVG(p.price_current), 0) as avg_price,
                COALESCE(MIN(p.price_current), 0) as min_price,
                COALESCE(MAX(p.price_current), 0) as max_price,
                COUNT(*) as product_count
            FROM ods_product_clean p
            WHERE p.source_platform IN ('tiki', 'lazada') AND p.price_current IS NOT NULL
            GROUP BY COALESCE(p.source_platform, 'Unknown'), COALESCE(p.category, 'Unknown')
            ORDER BY product_count DESC
            LIMIT 20
        """
    
    result = await db.execute_query(query)
    
    return {
        "chart_type": "grouped_bar",
        "title": f"Platform Price Comparison{' - ' + category if category else ''}",
        "x_axis": "category",
        "y_axis": "avg_price",
        "group_by": "platform",
        "data": result or []
    }


@router.get("/dashboard/summary")
async def get_dashboard_summary(
    db = Depends(get_db)
):
    """Dashboard summary metrics"""
    query = """
        SELECT 
            COUNT(*) as total_products,
            COALESCE(AVG(rating_avg), 0) as overall_avg_rating,
            COALESCE(SUM(review_count), 0) as total_reviews,
            COALESCE(AVG(price_current), 0) as avg_price,
            COUNT(DISTINCT category) as total_categories,
            COUNT(CASE WHEN rating_avg >= 4.0 THEN 1 END) as high_rated_products,
            COUNT(CASE WHEN review_count >= 100 THEN 1 END) as popular_products,
            COUNT(DISTINCT source_platform) as total_platforms
        FROM ods_product_clean
        WHERE rating_avg IS NOT NULL AND price_current IS NOT NULL
    """
    
    result = await db.execute_query(query)
    data = result[0] if result else {}
    
    return {
        "summary": data,
        "timestamp": datetime.now().isoformat()
    }
