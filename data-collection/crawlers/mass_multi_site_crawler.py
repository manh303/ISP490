#!/usr/bin/env python3
"""Mass crawler for multiple categories and pages across multiple sites"""

import requests
import json
import time
import random
import os
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class MassMultiSiteCrawler:
    def __init__(self):
        self.session = requests.Session()

        # Browser headers to avoid detection
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

        # Categories for different sites
        self.tiki_categories = {
            'smartphone': 'điện thoại',
            'laptop': 'laptop',
            'tablet': 'máy tính bảng',
            'smartwatch': 'đồng hồ thông minh',
            'headphone': 'tai nghe',
            'camera': 'máy ảnh',
            'speaker': 'loa bluetooth',
            'monitor': 'màn hình máy tính',
            'mouse': 'chuột máy tính',
            'keyboard': 'bàn phím',
            'tv': 'tivi smart',
            'printer': 'máy in'
        }

        self.sendo_categories = {
            'dien-thoai': 'smartphone',
            'laptop': 'laptop',
            'may-tinh-bang': 'tablet',
            'dong-ho-thong-minh': 'smartwatch',
            'tai-nghe': 'headphone',
            'loa': 'speaker'
        }

        self.stats = {
            'total_products': 0,
            'tiki_products': 0,
            'sendo_products': 0,
            'categories_processed': 0,
            'pages_crawled': 0,
            'start_time': None
        }

    def crawl_tiki_category_with_pagination(self, category_query, max_pages=50, products_per_page=40):
        """Crawl Tiki category with multiple pages"""
        print(f"\n🔍 Crawling Tiki: {category_query} (up to {max_pages} pages)")

        all_products = []

        for page in range(1, max_pages + 1):
            print(f"  📄 Page {page}/{max_pages}")

            try:
                url = "https://tiki.vn/api/v2/products"
                params = {
                    'limit': products_per_page,
                    'include': 'advertisement',
                    'aggregations': 2,
                    'q': category_query,
                    'page': page
                }

                response = self.session.get(url, params=params, timeout=15)

                if response.status_code == 200:
                    data = response.json()
                    products = data.get('data', [])

                    if not products:
                        print(f"     ⚠️ No products found on page {page}, stopping")
                        break

                    print(f"     ✅ Found {len(products)} products")

                    # Process products
                    for product in products:
                        processed_product = {
                            "source": "tiki_api",
                            "category": category_query,
                            "product_id": str(product.get('id', '')),
                            "product_name": product.get('name', ''),
                            "price_current": product.get('price', 0),
                            "price_original": product.get('list_price', 0),
                            "discount_percent": product.get('discount_rate', 0),
                            "rating_avg": product.get('rating_average', 0),
                            "review_count": product.get('review_count', 0),
                            "brand": product.get('brand_name', ''),
                            "seller_name": product.get('seller_name', ''),
                            "url": f"https://tiki.vn/{product.get('url_path', '')}",
                            "image_urls": [product.get('thumbnail_url', '')],
                            "crawl_date": datetime.now().isoformat(),
                            "page_number": page
                        }
                        all_products.append(processed_product)

                    self.stats['pages_crawled'] += 1

                    # Random delay between pages
                    time.sleep(random.uniform(2, 4))

                else:
                    print(f"     ❌ Error {response.status_code} on page {page}")
                    break

            except Exception as e:
                print(f"     ❌ Exception on page {page}: {e}")
                break

        print(f"  ✅ Total products from {category_query}: {len(all_products)}")
        return all_products

    def crawl_sendo_category_with_pagination(self, category_path, max_pages=50):
        """Try to crawl Sendo category with pagination"""
        print(f"\n🔍 Trying Sendo: {category_path} (up to {max_pages} pages)")

        all_products = []

        for page in range(1, max_pages + 1):
            print(f"  📄 Page {page}/{max_pages}")

            try:
                url = f"https://www.sendo.vn/{category_path}?p={page}"
                response = self.session.get(url, timeout=15)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Look for product elements
                    products_found = 0
                    product_selectors = [
                        '.d7ed-item',
                        '.item-product',
                        '[data-product-id]',
                        '.product-item'
                    ]

                    for selector in product_selectors:
                        elements = soup.select(selector)
                        if elements:
                            print(f"     Found {len(elements)} elements with {selector}")

                            for elem in elements[:20]:  # Limit per page
                                try:
                                    # Extract basic info
                                    name_elem = elem.find(['h3', 'h2', 'a'])
                                    name = name_elem.get('title') or name_elem.text.strip() if name_elem else ''

                                    price_elem = elem.find(class_=lambda x: x and 'price' in x.lower())
                                    price = price_elem.text.strip() if price_elem else ''

                                    link_elem = elem.find('a', href=True)
                                    product_url = urljoin(url, link_elem['href']) if link_elem else ''

                                    if name and len(name) > 5:
                                        product = {
                                            "source": "sendo_html",
                                            "category": category_path,
                                            "product_name": name,
                                            "price_current": price,
                                            "url": product_url,
                                            "crawl_date": datetime.now().isoformat(),
                                            "page_number": page
                                        }
                                        all_products.append(product)
                                        products_found += 1

                                except Exception as e:
                                    continue

                            if products_found > 0:
                                break

                    if products_found == 0:
                        print(f"     ⚠️ No products found on page {page}")
                        break
                    else:
                        print(f"     ✅ Extracted {products_found} products")

                    time.sleep(random.uniform(2, 4))

                else:
                    print(f"     ❌ Error {response.status_code}")
                    break

            except Exception as e:
                print(f"     ❌ Exception: {e}")
                break

        print(f"  ✅ Total Sendo products from {category_path}: {len(all_products)}")
        return all_products

    def crawl_all_categories_mass(self, max_pages_per_category=50):
        """Mass crawl across all categories and sites"""
        print("🚀 STARTING MASS CRAWL")
        print(f"📊 Target: {len(self.tiki_categories)} Tiki categories + {len(self.sendo_categories)} Sendo categories")
        print(f"📄 Max pages per category: {max_pages_per_category}")
        print(f"🎯 Expected: {len(self.tiki_categories) * max_pages_per_category * 40} + Sendo products")

        self.stats['start_time'] = time.time()
        all_products = []

        # 1. Crawl all Tiki categories
        print(f"\n{'='*60}")
        print("🟡 PHASE 1: TIKI MASS CRAWL")
        print(f"{'='*60}")

        for i, (category_key, category_query) in enumerate(self.tiki_categories.items(), 1):
            print(f"\n[{i}/{len(self.tiki_categories)}] Processing Tiki category: {category_key}")

            try:
                category_products = self.crawl_tiki_category_with_pagination(
                    category_query,
                    max_pages=max_pages_per_category
                )

                all_products.extend(category_products)
                self.stats['tiki_products'] += len(category_products)
                self.stats['categories_processed'] += 1

                # Save progress after each category
                if category_products:
                    self.save_category_progress(category_products, f"tiki_{category_key}")

                print(f"     📈 Category total: {len(category_products)} products")
                print(f"     📊 Overall total: {len(all_products)} products")

                # Break between categories to avoid blocking
                time.sleep(random.uniform(5, 8))

            except Exception as e:
                print(f"     ❌ Error in category {category_key}: {e}")
                continue

        # 2. Try Sendo categories
        print(f"\n{'='*60}")
        print("🟠 PHASE 2: SENDO ATTEMPT")
        print(f"{'='*60}")

        for i, (category_path, category_name) in enumerate(self.sendo_categories.items(), 1):
            print(f"\n[{i}/{len(self.sendo_categories)}] Trying Sendo category: {category_name}")

            try:
                category_products = self.crawl_sendo_category_with_pagination(
                    category_path,
                    max_pages=5  # Lower for Sendo
                )

                if category_products:
                    all_products.extend(category_products)
                    self.stats['sendo_products'] += len(category_products)
                    self.save_category_progress(category_products, f"sendo_{category_path}")

                time.sleep(random.uniform(3, 6))

            except Exception as e:
                print(f"     ❌ Error in Sendo {category_path}: {e}")
                continue

        # Final results
        self.print_final_summary(all_products)
        self.save_final_results(all_products)

        return all_products

    def save_category_progress(self, products, category_name):
        """Save progress after each category"""
        filename = f"progress_{category_name}_{int(time.time())}.json"
        filepath = os.path.join(os.path.dirname(__file__), '..', 'data', 'mass_crawl', filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        print(f"     💾 Progress saved: {filename}")

    def save_final_results(self, all_products):
        """Save final comprehensive results"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Main results file
        main_filename = f"mass_crawl_results_{timestamp}.json"
        main_filepath = os.path.join(os.path.dirname(__file__), '..', 'data', 'mass_crawl', main_filename)
        os.makedirs(os.path.dirname(main_filepath), exist_ok=True)

        with open(main_filepath, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)

        # Summary file
        summary = {
            'crawl_summary': {
                'crawl_date': datetime.now().isoformat(),
                'total_products': len(all_products),
                'tiki_products': self.stats['tiki_products'],
                'sendo_products': self.stats['sendo_products'],
                'categories_processed': self.stats['categories_processed'],
                'pages_crawled': self.stats['pages_crawled'],
                'duration_minutes': (time.time() - self.stats['start_time']) / 60,
                'products_per_minute': len(all_products) / ((time.time() - self.stats['start_time']) / 60)
            },
            'category_breakdown': self.get_category_breakdown(all_products),
            'top_products': all_products[:10]  # Sample products
        }

        summary_filename = f"mass_crawl_summary_{timestamp}.json"
        summary_filepath = os.path.join(os.path.dirname(main_filepath), summary_filename)

        with open(summary_filepath, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print(f"\n💾 RESULTS SAVED:")
        print(f"   📄 Main file: {main_filename}")
        print(f"   📊 Summary: {summary_filename}")

    def get_category_breakdown(self, products):
        """Get breakdown by category"""
        breakdown = {}
        for product in products:
            category = product.get('category', 'unknown')
            source = product.get('source', 'unknown')
            key = f"{source}_{category}"
            breakdown[key] = breakdown.get(key, 0) + 1
        return breakdown

    def print_final_summary(self, all_products):
        """Print comprehensive final summary"""
        duration = time.time() - self.stats['start_time']

        print(f"\n{'='*80}")
        print("🎉 MASS CRAWL COMPLETED!")
        print(f"{'='*80}")
        print(f"🕐 Duration: {duration/60:.1f} minutes ({duration:.0f} seconds)")
        print(f"📦 Total products: {len(all_products):,}")
        print(f"🟡 Tiki products: {self.stats['tiki_products']:,}")
        print(f"🟠 Sendo products: {self.stats['sendo_products']:,}")
        print(f"📂 Categories processed: {self.stats['categories_processed']}")
        print(f"📄 Pages crawled: {self.stats['pages_crawled']}")
        print(f"⚡ Speed: {len(all_products)/(duration/60):.1f} products/minute")

        # Category breakdown
        breakdown = self.get_category_breakdown(all_products)
        print(f"\n📊 CATEGORY BREAKDOWN:")
        for category, count in sorted(breakdown.items(), key=lambda x: x[1], reverse=True):
            print(f"   {category}: {count:,} products")

        # Sample products
        if all_products:
            print(f"\n🔍 SAMPLE PRODUCTS:")
            for i, product in enumerate(all_products[:5], 1):
                name = product.get('product_name', 'N/A')[:60]
                price = product.get('price_current', 'N/A')
                source = product.get('source', 'N/A')
                print(f"   {i}. [{source}] {name}... | {price}")

def main():
    """Run mass crawl"""
    crawler = MassMultiSiteCrawler()

    print("🚀 MASS MULTI-SITE CRAWLER")
    print("This will crawl multiple categories with pagination across multiple sites")
    print("Expected to collect thousands of products")

    confirm = input("\nStart mass crawl? (y/N): ")
    if confirm.lower() != 'y':
        print("Mass crawl cancelled")
        return

    try:
        products = crawler.crawl_all_categories_mass(max_pages_per_category=20)

        print(f"\n🎊 SUCCESS! Collected {len(products):,} total products")
        print("Check the data/mass_crawl/ folder for results")

    except KeyboardInterrupt:
        print("\n⏹️ Crawl interrupted by user")
    except Exception as e:
        print(f"\n❌ Crawl error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()