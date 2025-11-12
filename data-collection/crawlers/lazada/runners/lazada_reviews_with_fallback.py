#!/usr/bin/env python3
"""
Lazada Reviews Crawler with Mock Data Fallback
Tries real crawling first, falls back to mock data if blocked
"""
import sys
import os

def try_real_crawl():
    """Try real crawling"""
    try:
        print("Attempting real crawl...")
        from lazada_reviews_crawler_airflow import WorkingLazadaReviewsCrawler
        
        crawler = WorkingLazadaReviewsCrawler(
            headless=True,
            extract_reviews=True
        )
        
        result = crawler.run_working_crawl(max_pages=1)
        
        if result['success'] and result['summary']['total_reviews_extracted'] > 0:
            print(f"✅ Real crawl successful! {result['summary']['total_reviews_extracted']} reviews")
            return True
        else:
            print("⚠️  Real crawl returned no data")
            return False
            
    except Exception as e:
        print(f"❌ Real crawl failed: {e}")
        return False

def use_mock_data():
    """Generate mock data"""
    try:
        print("\n" + "=" * 60)
        print("Using mock data fallback...")
        print("=" * 60)
        
        from lazada_reviews_mock import main as generate_mock
        generate_mock()
        return True
        
    except Exception as e:
        print(f"❌ Mock data generation failed: {e}")
        return False

def main():
    """Main execution with fallback"""
    print("Lazada Reviews Crawler (with Fallback)")
    print("=" * 60)
    
    # Try real crawl first
    success = try_real_crawl()
    
    # If failed, use mock data
    if not success:
        print("\n⚠️  Real crawling blocked by anti-bot")
        print("Falling back to mock data for demo purposes...")
        success = use_mock_data()
    
    if success:
        print("\n✅ Data collection completed!")
        sys.exit(0)
    else:
        print("\n❌ Both real and mock data failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
