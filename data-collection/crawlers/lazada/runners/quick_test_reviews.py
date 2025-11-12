#!/usr/bin/env python3
"""Quick test for reviews crawler"""
import sys
sys.path.insert(0, '.')

try:
    from lazada_reviews_crawler_airflow import LazadaReviewsCrawler
    
    print("Testing Lazada Reviews Crawler...")
    print("=" * 60)
    
    crawler = LazadaReviewsCrawler()
    
    # Test with just 1 category, 1 page, 3 products
    crawler.categories = {"smartphones": "https://www.lazada.vn/dien-thoai-di-dong/"}
    crawler.max_products_per_category = 3
    crawler.max_reviews_per_product = 3
    
    crawler.run(max_pages=1)
    
    print("\n" + "=" * 60)
    print("Test completed!")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
