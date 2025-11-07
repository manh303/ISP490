#!/usr/bin/env python3
"""
Quick Run Script for Lazada Electronics Crawler
Script chạy nhanh để test crawler ngay lập tức
"""

import os
import sys
import json
import asyncio
from datetime import datetime
from pathlib import Path

# Add current directory to Python path
sys.path.append(str(Path(__file__).parent))

def print_banner():
    """Print crawler banner"""
    print("""
╔════════════════════════════════════════════════════════════════╗
║                🛒 LAZADA ELECTRONICS CRAWLER 🛒               ║
║                                                                ║
║  Thu thập dữ liệu thiết bị điện tử từ Lazada Việt Nam         ║
║  Tích hợp với database và batch processing systems            ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
    """)

def check_requirements():
    """Check if required packages are installed"""
    required_packages = [
        'selenium', 'pandas', 'requests',
        'sqlalchemy', 'psycopg2', 'pymongo'
    ]

    missing_packages = []

    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print("❌ Missing required packages:")
        for package in missing_packages:
            print(f"   - {package}")
        print("\n📦 Install with: pip install -r requirements_lazada.txt")
        return False

    print("✅ All required packages are installed")
    return True

def setup_output_directory():
    """Setup output directory"""
    output_dir = Path("../data/lazada_electronics")
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Output directory: {output_dir.absolute()}")
    return output_dir

def quick_crawl_demo():
    """Run a quick crawl demo"""
    print("\n🚀 Starting Quick Crawl Demo...")
    print("=" * 50)

    try:
        # Import crawler
        from lazada_electronics_crawler import LazadaElectronicsCrawler

        # Initialize crawler (non-headless for demo)
        crawler = LazadaElectronicsCrawler(headless=False)

        print("🔧 Initializing WebDriver...")

        # Run crawler for smartphones only, 1 page
        result = crawler.run_crawler(
            categories=['smartphones'],
            max_pages_per_category=1
        )

        # Display results
        if result['success']:
            print("\n✅ Crawl completed successfully!")
            print(f"📊 Products collected: {result['products_count']}")
            print(f"⏱️  Duration: {result['report']['crawl_session']['duration_formatted']}")
            print(f"📈 Success rate: {result['report']['performance']['success_rate']:.1f}%")

            # Show sample product
            if result['products']:
                sample = result['products'][0]
                print(f"\n🔍 Sample Product:")
                print(f"   Name: {sample.get('name', 'N/A')}")
                print(f"   Price: {sample.get('price', 0):,.0f} VND")
                print(f"   Category: {sample.get('category', 'N/A')}")
                print(f"   Rating: {sample.get('rating', 'N/A')}")
                print(f"   URL: {sample.get('url', 'N/A')}")

            return True
        else:
            print(f"❌ Crawl failed: {result['error']}")
            return False

    except Exception as e:
        print(f"❌ Error during crawl: {e}")
        return False

def integrated_crawl_demo():
    """Run integrated crawl with database"""
    print("\n🚀 Starting Integrated Crawl Demo...")
    print("=" * 50)

    try:
        # Import integration module
        from lazada_integration import crawl_lazada_electronics

        print("🔧 Initializing integrated workflow...")

        # Run complete workflow
        result = crawl_lazada_electronics(
            categories=['smartphones'],
            max_pages=1,
            headless=False
        )

        # Display results
        if result.get('workflow', {}).get('status') == 'success':
            print("\n✅ Integrated crawl completed successfully!")

            workflow = result['workflow']
            crawling = result.get('crawling', {})
            storage = result.get('storage', {})

            print(f"⏱️  Duration: {workflow.get('duration_seconds', 0):.1f} seconds")
            print(f"📦 Products found: {crawling.get('raw_products_found', 0)}")
            print(f"📈 Success rate: {crawling.get('success_rate', 0):.1f}%")

            print(f"\n💾 Storage Results:")
            print(f"   PostgreSQL: {'✅' if storage.get('postgresql_saved') else '❌'}")
            print(f"   MongoDB: {'✅' if storage.get('mongodb_saved') else '❌'}")
            print(f"   Kafka: {'✅' if storage.get('kafka_sent') else '❌'}")
            print(f"   Redis: {'✅' if storage.get('redis_cached') else '❌'}")

            return True
        else:
            error = result.get('workflow', {}).get('error', 'Unknown error')
            print(f"❌ Integrated crawl failed: {error}")
            return False

    except Exception as e:
        print(f"❌ Error during integrated crawl: {e}")
        print("💡 Make sure database services are running")
        return False

def performance_test():
    """Run performance test"""
    print("\n🚀 Starting Performance Test...")
    print("=" * 50)

    start_time = datetime.now()

    try:
        from lazada_electronics_crawler import LazadaElectronicsCrawler

        crawler = LazadaElectronicsCrawler(headless=True)

        result = crawler.run_crawler(
            categories=['smartphones'],
            max_pages_per_category=1
        )

        end_time = datetime.now()
        duration = end_time - start_time

        if result['success']:
            products_count = result['products_count']

            print(f"\n📊 Performance Results:")
            print(f"⏱️  Total time: {duration.total_seconds():.1f} seconds")
            print(f"📦 Products collected: {products_count}")

            if products_count > 0:
                rate = products_count / duration.total_seconds()
                print(f"🚀 Collection rate: {rate:.2f} products/second")

            return True
        else:
            print(f"❌ Performance test failed: {result['error']}")
            return False

    except Exception as e:
        print(f"❌ Performance test error: {e}")
        return False

def main():
    """Main function"""
    print_banner()

    # Check requirements
    if not check_requirements():
        return

    # Setup output directory
    setup_output_directory()

    print("\n🎯 Choose Demo Type:")
    print("1. Quick Crawl Demo (Basic crawler test)")
    print("2. Integrated Crawl Demo (With database integration)")
    print("3. Performance Test (Speed benchmark)")
    print("4. Exit")

    while True:
        try:
            choice = input("\nEnter your choice (1-4): ").strip()

            if choice == '1':
                quick_crawl_demo()
                break
            elif choice == '2':
                integrated_crawl_demo()
                break
            elif choice == '3':
                performance_test()
                break
            elif choice == '4':
                print("👋 Goodbye!")
                return
            else:
                print("❌ Invalid choice. Please enter 1-4.")
                continue

        except KeyboardInterrupt:
            print("\n\n👋 Demo interrupted by user. Goodbye!")
            return
        except Exception as e:
            print(f"❌ Error: {e}")
            return

    print(f"\n📁 Check output files in: ../data/lazada_electronics/")
    print("📖 See LAZADA_CRAWLER_GUIDE.md for detailed usage instructions")
    print("\n🎉 Demo completed! Happy crawling! 🛒")

if __name__ == "__main__":
    main()