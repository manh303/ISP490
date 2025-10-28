#!/usr/bin/env python3
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import argparse

# Import all crawler classes
from lazada_crawler import LazadaCrawler
from tiki_crawler import TikiCrawler
from sendo_crawler import SendoCrawler
from cellphones_crawler import CellphonesCrawler
from fptshop_crawler import FPTShopCrawler
from thegioididong_crawler import TheGioiDiDongCrawler
from hoanghamobile_crawler import HoangHaMobileCrawler

class MultiPlatformOrchestrator:
    def __init__(self):
        self.crawlers = {
            'lazada': LazadaCrawler,
            'tiki': TikiCrawler,
            'sendo': SendoCrawler,
            'cellphones': CellphonesCrawler,
            'fptshop': FPTShopCrawler,
            'thegioididong': TheGioiDiDongCrawler,
            'hoanghamobile': HoangHaMobileCrawler
        }

        self.categories = [
            'dien-thoai',
            'laptop',
            'tai-nghe',
            'dong-ho-thong-minh',
            'phu-kien'
        ]

    def crawl_single_platform(self, platform: str, category: str, max_pages: int = 2, max_products: int = 20) -> List[Dict]:
        """Crawl a single platform for a specific category"""
        if platform not in self.crawlers:
            print(f"Platform {platform} not supported")
            return []

        crawler_class = self.crawlers[platform]

        try:
            with crawler_class() as crawler:
                print(f"Starting {platform} crawl for {category}")
                products = crawler.crawl_category(category, max_pages, max_products)

                if products:
                    filename = f"{platform}_{category}_products_{int(time.time())}.jsonl"
                    crawler.save_data(products, filename)
                    print(f"✅ {platform}: Crawled {len(products)} products from {category}")
                else:
                    print(f"❌ {platform}: No products found for {category}")

                return products

        except Exception as e:
            print(f"❌ Error crawling {platform} for {category}: {e}")
            return []

    def crawl_all_platforms_single_category(self, category: str, max_pages: int = 2, max_products: int = 20, max_workers: int = 3) -> Dict[str, List[Dict]]:
        """Crawl all platforms for a single category using threading"""
        print(f"\n🚀 Starting multi-platform crawl for category: {category}")
        print(f"📊 Settings: max_pages={max_pages}, max_products={max_products}, max_workers={max_workers}")

        results = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each platform
            future_to_platform = {
                executor.submit(self.crawl_single_platform, platform, category, max_pages, max_products): platform
                for platform in self.crawlers.keys()
            }

            # Collect results as they complete
            for future in as_completed(future_to_platform):
                platform = future_to_platform[future]
                try:
                    products = future.result()
                    results[platform] = products
                except Exception as e:
                    print(f"❌ Exception for {platform}: {e}")
                    results[platform] = []

        # Summary
        total_products = sum(len(products) for products in results.values())
        print(f"\n📈 Crawl Summary for {category}:")
        for platform, products in results.items():
            print(f"  {platform}: {len(products)} products")
        print(f"  Total: {total_products} products")

        return results

    def crawl_sequential(self, platforms: List[str], categories: List[str], max_pages: int = 2, max_products: int = 20) -> Dict[str, Dict[str, List[Dict]]]:
        """Crawl platforms sequentially (safer, respects rate limits better)"""
        print(f"\n🔄 Starting sequential crawl")
        print(f"🎯 Platforms: {', '.join(platforms)}")
        print(f"📁 Categories: {', '.join(categories)}")

        all_results = {}

        for platform in platforms:
            if platform not in self.crawlers:
                print(f"❌ Platform {platform} not supported")
                continue

            platform_results = {}

            for category in categories:
                print(f"\n🔄 Crawling {platform} - {category}")
                products = self.crawl_single_platform(platform, category, max_pages, max_products)
                platform_results[category] = products

                # Delay between categories for the same platform
                if category != categories[-1]:  # Not the last category
                    time.sleep(5)

            all_results[platform] = platform_results

            # Longer delay between platforms
            if platform != platforms[-1]:  # Not the last platform
                print(f"⏳ Waiting 10 seconds before next platform...")
                time.sleep(10)

        return all_results

    def save_combined_results(self, results: Dict[str, Dict[str, List[Dict]]], output_file: str = None):
        """Save all results to a combined file"""
        if not output_file:
            output_file = f"combined_crawl_results_{int(time.time())}.jsonl"

        import json
        import os

        os.makedirs('data-collection/data', exist_ok=True)
        filepath = f'data-collection/data/{output_file}'

        all_products = []

        for platform, categories in results.items():
            for category, products in categories.items():
                for product in products:
                    product['crawl_category'] = category
                    all_products.append(product)

        with open(filepath, 'w', encoding='utf-8') as f:
            for product in all_products:
                json.dump(product, f, ensure_ascii=False)
                f.write('\n')

        print(f"\n💾 Saved {len(all_products)} total products to {filepath}")

def main():
    parser = argparse.ArgumentParser(description='Multi-Platform E-commerce Crawler')
    parser.add_argument('--platforms', nargs='+',
                       choices=['lazada', 'tiki', 'sendo', 'cellphones', 'fptshop', 'thegioididong', 'hoanghamobile', 'all'],
                       default=['all'], help='Platforms to crawl')
    parser.add_argument('--categories', nargs='+',
                       choices=['dien-thoai', 'laptop', 'tai-nghe', 'dong-ho-thong-minh', 'phu-kien', 'all'],
                       default=['dien-thoai'], help='Categories to crawl')
    parser.add_argument('--max-pages', type=int, default=2, help='Maximum pages per category')
    parser.add_argument('--max-products', type=int, default=20, help='Maximum products per category')
    parser.add_argument('--mode', choices=['sequential', 'parallel'], default='sequential',
                       help='Crawling mode: sequential (safer) or parallel (faster)')
    parser.add_argument('--output', type=str, help='Output filename')

    args = parser.parse_args()

    orchestrator = MultiPlatformOrchestrator()

    # Handle 'all' options
    if 'all' in args.platforms:
        platforms = list(orchestrator.crawlers.keys())
    else:
        platforms = args.platforms

    if 'all' in args.categories:
        categories = orchestrator.categories
    else:
        categories = args.categories

    print(f"🤖 Multi-Platform E-commerce Crawler")
    print(f"🎯 Platforms: {', '.join(platforms)}")
    print(f"📁 Categories: {', '.join(categories)}")
    print(f"⚙️  Mode: {args.mode}")
    print(f"📄 Max pages: {args.max_pages}")
    print(f"📦 Max products: {args.max_products}")

    try:
        if args.mode == 'sequential':
            results = orchestrator.crawl_sequential(platforms, categories, args.max_pages, args.max_products)
        else:
            # For parallel mode, crawl each category across all platforms
            results = {}
            for platform in platforms:
                results[platform] = {}

            for category in categories:
                category_results = orchestrator.crawl_all_platforms_single_category(
                    category, args.max_pages, args.max_products
                )
                for platform, products in category_results.items():
                    if platform in results:
                        results[platform][category] = products

        # Save combined results
        orchestrator.save_combined_results(results, args.output)

        # Final summary
        total_products = 0
        for platform_data in results.values():
            for products in platform_data.values():
                total_products += len(products)

        print(f"\n🎉 Crawling completed!")
        print(f"📊 Total products crawled: {total_products}")
        print(f"💾 Results saved to data-collection/data/")

    except KeyboardInterrupt:
        print("\n⚠️  Crawling interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during crawling: {e}")

if __name__ == "__main__":
    main()