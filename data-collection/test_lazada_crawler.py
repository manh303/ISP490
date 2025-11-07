#!/usr/bin/env python3
"""
Test Script for Lazada Electronics Crawler
Script test và validate chức năng crawler
"""

import asyncio
import json
import time
import unittest
from datetime import datetime
from pathlib import Path
import logging

# Import our modules
from lazada_electronics_crawler import LazadaElectronicsCrawler
from lazada_integration import LazadaCrawlerOrchestrator, LazadaDataProcessor

# Configure logging for testing
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestLazadaCrawler(unittest.TestCase):
    """Test cases for Lazada crawler functionality"""

    def setUp(self):
        """Setup test environment"""
        self.crawler = LazadaElectronicsCrawler(headless=True)
        self.processor = LazadaDataProcessor()
        self.test_output_dir = Path("../data/test_lazada")
        self.test_output_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Cleanup after tests"""
        if hasattr(self.crawler, 'driver') and self.crawler.driver:
            self.crawler.driver.quit()

    def test_driver_setup(self):
        """Test WebDriver initialization"""
        logger.info("🧪 Testing WebDriver setup...")

        success = self.crawler.setup_driver()
        self.assertTrue(success, "WebDriver should initialize successfully")

        if self.crawler.driver:
            self.assertIsNotNone(self.crawler.driver.session_id, "Driver should have valid session")
            logger.info("✅ WebDriver setup test passed")

    def test_category_url_access(self):
        """Test accessing category URLs"""
        logger.info("🧪 Testing category URL access...")

        if not self.crawler.setup_driver():
            self.skipTest("WebDriver setup failed")

        test_url = "https://www.lazada.vn/dien-thoai-di-dong/"

        try:
            self.crawler.driver.get(test_url)
            time.sleep(3)

            # Check if page loaded
            page_title = self.crawler.driver.title
            self.assertIsNotNone(page_title, "Page should have a title")
            self.assertNotIn("404", page_title.lower(), "Page should not be 404")

            logger.info(f"✅ Successfully accessed: {test_url}")
            logger.info(f"   Page title: {page_title}")

        except Exception as e:
            self.fail(f"Failed to access category URL: {e}")

    def test_product_extraction(self):
        """Test product data extraction from sample data"""
        logger.info("🧪 Testing product data extraction...")

        # Mock product element data
        sample_product = {
            'product_id': 'test123',
            'name': 'iPhone 15 Pro Max 256GB',
            'price': 29990000,
            'original_price': 32990000,
            'category': 'Smartphones',
            'rating': 4.5,
            'review_count': 150,
            'brand': 'Apple',
            'images': ['https://example.com/image.jpg'],
            'url': 'https://www.lazada.vn/products/test123'
        }

        transformed = self.processor.transform_lazada_product(sample_product)

        self.assertIsNotNone(transformed, "Product transformation should succeed")
        self.assertEqual(transformed['product_id'], 'lazada_test123')
        self.assertEqual(transformed['name'], 'iPhone 15 Pro Max 256GB')
        self.assertEqual(transformed['price'], 29990000)
        self.assertEqual(transformed['category'], 'Smartphones')

        logger.info("✅ Product extraction test passed")

    def test_category_mapping(self):
        """Test category standardization"""
        logger.info("🧪 Testing category mapping...")

        test_cases = [
            ('Điện Thoại Di Động', 'Smartphones'),
            ('Máy Tính Xách Tay', 'Laptops'),
            ('Tai Nghe', 'Audio'),
            ('Unknown Category', 'Electronics')
        ]

        for input_category, expected_output in test_cases:
            result = self.processor.standardize_category(input_category)
            self.assertEqual(result, expected_output,
                           f"Category '{input_category}' should map to '{expected_output}'")

        logger.info("✅ Category mapping test passed")

    def test_sold_count_extraction(self):
        """Test sold count extraction from various formats"""
        logger.info("🧪 Testing sold count extraction...")

        test_cases = [
            ('Đã bán 1.5k', 1500),
            ('2,300 đã bán', 2300),
            ('Sold 500', 500),
            ('', 0),
            (None, 0),
            ('999+ sold', 999)
        ]

        for input_text, expected_count in test_cases:
            result = self.processor.extract_sold_count(input_text)
            self.assertEqual(result, expected_count,
                           f"'{input_text}' should extract to {expected_count}")

        logger.info("✅ Sold count extraction test passed")

class LazadaCrawlerDemo:
    """Demo class to showcase crawler capabilities"""

    def __init__(self):
        self.demo_output_dir = Path("../data/demo_lazada")
        self.demo_output_dir.mkdir(parents=True, exist_ok=True)

    def run_demo_crawl(self, demo_type: str = "quick"):
        """Run demonstration crawl"""
        print(f"""
        🛒 Lazada Electronics Crawler Demo
        =================================

        Demo type: {demo_type}
        Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """)

        if demo_type == "quick":
            return self._run_quick_demo()
        elif demo_type == "full":
            return self._run_full_demo()
        else:
            print("❌ Unknown demo type. Use 'quick' or 'full'")
            return None

    def _run_quick_demo(self):
        """Quick demo with minimal data"""
        print("🚀 Running quick demo (1 category, 1 page)...")

        try:
            orchestrator = LazadaCrawlerOrchestrator(headless=False)

            result = asyncio.run(orchestrator.run_complete_workflow(
                categories=['smartphones'],
                max_pages_per_category=1
            ))

            self._print_demo_results(result, "Quick Demo")
            return result

        except Exception as e:
            print(f"❌ Demo failed: {e}")
            return None

    def _run_full_demo(self):
        """Full demo with multiple categories"""
        print("🚀 Running full demo (multiple categories, more pages)...")

        try:
            orchestrator = LazadaCrawlerOrchestrator(headless=True)

            result = asyncio.run(orchestrator.run_complete_workflow(
                categories=['smartphones', 'laptops', 'headphones'],
                max_pages_per_category=2
            ))

            self._print_demo_results(result, "Full Demo")
            return result

        except Exception as e:
            print(f"❌ Demo failed: {e}")
            return None

    def _print_demo_results(self, result: dict, demo_name: str):
        """Print formatted demo results"""
        if not result:
            return

        print(f"\n📊 {demo_name} Results:")
        print("=" * 50)

        if result.get('workflow', {}).get('status') == 'success':
            workflow = result['workflow']
            crawling = result.get('crawling', {})
            processing = result.get('processing', {})
            storage = result.get('storage', {})
            sample = result.get('sample_data', {})

            print(f"⏱️  Duration: {workflow.get('duration_seconds', 0):.1f} seconds")
            print(f"📦 Categories: {crawling.get('categories_crawled', 0)}")
            print(f"🔍 Products found: {crawling.get('raw_products_found', 0)}")
            print(f"✅ Success rate: {crawling.get('success_rate', 0):.1f}%")
            print(f"🔄 Transformation rate: {processing.get('transformation_rate', 0):.1f}%")

            print(f"\n💾 Storage Status:")
            print(f"   PostgreSQL: {'✅' if storage.get('postgresql_saved') else '❌'}")
            print(f"   MongoDB: {'✅' if storage.get('mongodb_saved') else '❌'}")
            print(f"   Kafka: {'✅' if storage.get('kafka_sent') else '❌'}")
            print(f"   Redis: {'✅' if storage.get('redis_cached') else '❌'}")

            if sample:
                print(f"\n📈 Data Sample:")
                print(f"   Categories: {', '.join(sample.get('categories', []))}")
                price_range = sample.get('price_range', {})
                print(f"   Price range: {price_range.get('min', 0):,.0f} - {price_range.get('max', 0):,.0f} VND")
                print(f"   Average price: {price_range.get('avg', 0):,.0f} VND")
        else:
            print(f"❌ Demo failed: {result.get('workflow', {}).get('error', 'Unknown error')}")

def run_performance_test():
    """Run performance benchmark test"""
    print("""
    🚀 Lazada Crawler Performance Test
    =================================
    """)

    start_time = datetime.now()

    try:
        # Test with small dataset for performance measurement
        orchestrator = LazadaCrawlerOrchestrator(headless=True)

        result = asyncio.run(orchestrator.run_complete_workflow(
            categories=['smartphones'],
            max_pages_per_category=1
        ))

        end_time = datetime.now()
        duration = end_time - start_time

        if result and result.get('workflow', {}).get('status') == 'success':
            products_count = result.get('crawling', {}).get('raw_products_found', 0)

            print(f"⏱️  Total time: {duration.total_seconds():.1f} seconds")
            print(f"📦 Products collected: {products_count}")

            if products_count > 0:
                rate = products_count / duration.total_seconds()
                print(f"🚀 Collection rate: {rate:.2f} products/second")

            print("✅ Performance test completed successfully")
        else:
            print("❌ Performance test failed")

    except Exception as e:
        print(f"❌ Performance test error: {e}")

def main():
    """Main function for testing and demo"""
    import sys

    if len(sys.argv) > 1:
        action = sys.argv[1].lower()

        if action == "test":
            print("🧪 Running unit tests...")
            unittest.main(argv=[''], exit=False, verbosity=2)

        elif action == "demo":
            demo_type = sys.argv[2] if len(sys.argv) > 2 else "quick"
            demo = LazadaCrawlerDemo()
            demo.run_demo_crawl(demo_type)

        elif action == "performance":
            run_performance_test()

        else:
            print(f"❌ Unknown action: {action}")
            print("Available actions: test, demo, performance")
    else:
        print("""
        🛒 Lazada Crawler Test Suite
        ===========================

        Usage:
        python test_lazada_crawler.py test          # Run unit tests
        python test_lazada_crawler.py demo [quick|full]  # Run demo
        python test_lazada_crawler.py performance   # Run performance test
        """)

if __name__ == "__main__":
    main()