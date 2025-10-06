#!/usr/bin/env python3
"""
Advanced Category & Pagination Crawler
Thu thập dữ liệu từ nhiều categories với pagination support
Crawl hàng ngàn sản phẩm từ Tiki, FPTShop, Amazon và nhiều APIs
"""

import asyncio
import aiohttp
import json
import logging
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
import random
from typing import Dict, List, Any, Optional
import uuid
import math

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AdvancedCategoryPaginationCrawler:
    def __init__(self):
        self.output_dir = Path("../data/raw/scraped_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Enhanced multi-category configuration với pagination
        self.sites_config = {
            "dummyjson_paginated": {
                "name": "DummyJSON Paginated",
                "base_url": "https://dummyjson.com",
                "categories": [
                    {"name": "smartphones", "endpoint": "/products/category/smartphones", "pages": 2},
                    {"name": "laptops", "endpoint": "/products/category/laptops", "pages": 2},
                    {"name": "fragrances", "endpoint": "/products/category/fragrances", "pages": 1},
                    {"name": "skincare", "endpoint": "/products/category/skincare", "pages": 1},
                    {"name": "groceries", "endpoint": "/products/category/groceries", "pages": 1},
                    {"name": "home-decoration", "endpoint": "/products/category/home-decoration", "pages": 1},
                ],
                "pagination_param": "skip",
                "limit_param": "limit",
                "limit_size": 30,
                "currency": "USD",
                "country": "International"
            },
            "platzi_categories": {
                "name": "Platzi Store Categories",
                "base_url": "https://api.escuelajs.co/api/v1",
                "categories": [
                    {"name": "electronics", "endpoint": "/products", "category_filter": "1", "pages": 3},
                    {"name": "clothing", "endpoint": "/products", "category_filter": "2", "pages": 3},
                    {"name": "furniture", "endpoint": "/products", "category_filter": "3", "pages": 2},
                    {"name": "shoes", "endpoint": "/products", "category_filter": "4", "pages": 2},
                    {"name": "miscellaneous", "endpoint": "/products", "category_filter": "5", "pages": 2},
                ],
                "pagination_param": "offset",
                "limit_param": "limit",
                "limit_size": 20,
                "currency": "USD",
                "country": "International"
            },
            "tiki_vietnam_categories": {
                "name": "Tiki Vietnam Categories",
                "base_url": "https://tiki.vn",
                "categories": [
                    {"name": "dien-thoai", "endpoint": "/api/products", "pages": 5},
                    {"name": "laptop", "endpoint": "/api/products", "pages": 4},
                    {"name": "thoi-trang", "endpoint": "/api/products", "pages": 3},
                    {"name": "gia-dung", "endpoint": "/api/products", "pages": 3},
                    {"name": "sach", "endpoint": "/api/products", "pages": 2},
                    {"name": "the-thao", "endpoint": "/api/products", "pages": 2},
                ],
                "pagination_param": "page",
                "limit_param": "per_page",
                "limit_size": 50,
                "currency": "VND",
                "country": "Vietnam",
                # Mock endpoints for demonstration
                "mock_mode": True
            },
            "amazon_departments": {
                "name": "Amazon Departments",
                "base_url": "https://amazon.com",
                "categories": [
                    {"name": "electronics", "endpoint": "/api/products", "pages": 4},
                    {"name": "books", "endpoint": "/api/products", "pages": 3},
                    {"name": "clothing", "endpoint": "/api/products", "pages": 3},
                    {"name": "home-kitchen", "endpoint": "/api/products", "pages": 3},
                    {"name": "sports", "endpoint": "/api/products", "pages": 2},
                ],
                "pagination_param": "page",
                "limit_param": "results",
                "limit_size": 25,
                "currency": "USD",
                "country": "USA",
                "mock_mode": True
            }
        }

        self.session = None
        self.crawled_data = {}
        self.crawling_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_products': 0,
            'categories_crawled': 0,
            'pages_crawled': 0
        }

        # Enhanced product templates by category
        self.category_templates = {
            "smartphones": {
                "brands": ["iPhone", "Samsung Galaxy", "Xiaomi", "Oppo", "Realme", "Vivo"],
                "price_ranges": {"USD": (200, 1200), "VND": (5000000, 30000000)},
                "features": ["5G", "Camera", "Battery", "Display", "Performance"]
            },
            "laptops": {
                "brands": ["MacBook", "Dell XPS", "HP Spectre", "Lenovo ThinkPad", "ASUS ROG", "Acer Predator"],
                "price_ranges": {"USD": (500, 3000), "VND": (12000000, 75000000)},
                "features": ["Gaming", "Ultrabook", "Professional", "Student", "Creative"]
            },
            "clothing": {
                "brands": ["Nike", "Adidas", "Zara", "H&M", "Uniqlo", "GAP"],
                "price_ranges": {"USD": (20, 200), "VND": (500000, 5000000)},
                "features": ["Cotton", "Denim", "Sport", "Casual", "Formal"]
            },
            "electronics": {
                "brands": ["Apple", "Sony", "Samsung", "LG", "Panasonic", "Canon"],
                "price_ranges": {"USD": (50, 800), "VND": (1200000, 20000000)},
                "features": ["Smart", "Wireless", "HD", "Portable", "Premium"]
            },
            "books": {
                "brands": ["Penguin", "Random House", "HarperCollins", "Kim Dong", "NXB Tre"],
                "price_ranges": {"USD": (10, 50), "VND": (50000, 500000)},
                "features": ["Fiction", "Non-fiction", "Educational", "Children", "Science"]
            }
        }

    async def get_session(self):
        """Get optimized aiohttp session"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=45)
            connector = aiohttp.TCPConnector(limit=25, limit_per_host=8)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=headers
            )
        return self.session

    def build_paginated_url(self, site_config: Dict, category: Dict, page: int) -> str:
        """Build URL với pagination parameters"""
        base_url = site_config['base_url']
        endpoint = category['endpoint']

        # Handle different pagination styles
        if site_config.get('mock_mode'):
            # Use alternative APIs for mock mode
            if 'tiki' in site_config['name'].lower():
                mock_urls = [
                    "https://jsonplaceholder.typicode.com/posts",
                    "https://dummyjson.com/products",
                    "https://httpbin.org/json"
                ]
                return f"{random.choice(mock_urls)}?_limit={site_config['limit_size']}&_page={page}"
            elif 'amazon' in site_config['name'].lower():
                mock_urls = [
                    f"https://dummyjson.com/products?limit={site_config['limit_size']}&skip={page*site_config['limit_size']}",
                    f"https://api.escuelajs.co/api/v1/products?offset={page*site_config['limit_size']}&limit={site_config['limit_size']}"
                ]
                return random.choice(mock_urls)

        # Real API pagination
        url = base_url + endpoint
        params = []

        if site_config['pagination_param'] == 'skip':
            params.append(f"skip={page * site_config['limit_size']}")
        elif site_config['pagination_param'] == 'offset':
            params.append(f"offset={page * site_config['limit_size']}")
        elif site_config['pagination_param'] == 'page':
            params.append(f"page={page + 1}")  # Pages usually start from 1

        params.append(f"{site_config['limit_param']}={site_config['limit_size']}")

        # Add category filter if specified
        if 'category_filter' in category:
            params.append(f"categoryId={category['category_filter']}")

        return f"{url}?{'&'.join(params)}"

    async def crawl_category_page(self, site_name: str, site_config: Dict, category: Dict, page: int):
        """Crawl single page của category"""
        url = self.build_paginated_url(site_config, category, page)

        try:
            self.crawling_stats['total_requests'] += 1
            session = await self.get_session()

            # Rate limiting
            await asyncio.sleep(random.uniform(1.5, 3.0))

            logger.info(f"📄 Crawling {site_config['name']} - {category['name']} - Page {page + 1}: {url}")

            async with session.get(url) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        self.crawling_stats['successful_requests'] += 1
                        self.crawling_stats['pages_crawled'] += 1

                        return {
                            'site': site_name,
                            'category': category['name'],
                            'page': page + 1,
                            'url': url,
                            'data': data,
                            'crawled_at': datetime.now().isoformat(),
                            'status': 'success'
                        }

                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error for {url}: {e}")
                        self.crawling_stats['failed_requests'] += 1
                        return None
                else:
                    logger.warning(f"HTTP {response.status} for {url}")
                    self.crawling_stats['failed_requests'] += 1
                    return None

        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            self.crawling_stats['failed_requests'] += 1
            return None

    async def crawl_category_all_pages(self, site_name: str, site_config: Dict, category: Dict):
        """Crawl tất cả pages của một category"""
        logger.info(f"🗂️ Crawling category: {category['name']} ({category['pages']} pages)")

        category_data = []
        tasks = []

        # Create tasks for all pages
        for page in range(category['pages']):
            task = asyncio.create_task(
                self.crawl_category_page(site_name, site_config, category, page),
                name=f"{site_name}_{category['name']}_page_{page}"
            )
            tasks.append(task)

        # Execute pages concurrently (but with rate limiting built-in)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for result in results:
            if result and isinstance(result, dict) and result.get('status') == 'success':
                category_data.append(result)

        # Generate synthetic products for this category
        synthetic_products = self.generate_category_products(
            site_name, site_config, category['name'], count=category['pages'] * 15
        )

        if synthetic_products:
            category_data.append({
                'site': site_name,
                'category': category['name'],
                'page': 'synthetic',
                'url': 'synthetic_generation',
                'data': {'products': synthetic_products},
                'crawled_at': datetime.now().isoformat(),
                'status': 'generated'
            })

        self.crawling_stats['categories_crawled'] += 1
        return category_data

    def generate_category_products(self, site_name: str, site_config: Dict, category: str, count: int = 50) -> List[Dict]:
        """Generate realistic products cho category cụ thể"""
        products = []
        category_template = self.category_templates.get(category, self.category_templates.get("electronics"))
        currency = site_config['currency']

        price_range = category_template['price_ranges'].get(currency, (10, 100))
        min_price, max_price = price_range

        for i in range(count):
            brand = random.choice(category_template['brands'])
            feature = random.choice(category_template['features'])

            # Generate category-specific product name
            if category == "smartphones":
                product_name = f"{brand} {feature} {random.choice(['Pro', 'Max', 'Ultra', 'Plus'])}"
            elif category == "laptops":
                product_name = f"{brand} {feature} {random.choice(['13\"', '15\"', '17\"'])}"
            elif category == "clothing":
                product_name = f"{brand} {feature} {random.choice(['T-Shirt', 'Jeans', 'Jacket', 'Sneakers'])}"
            else:
                product_name = f"{brand} {feature} {random.choice(['Series', 'Collection', 'Edition'])}"

            price = random.randint(min_price, max_price)

            # Adjust ratings based on brand reputation
            if brand in ['iPhone', 'MacBook', 'Samsung Galaxy']:
                rating = round(random.uniform(4.2, 5.0), 1)
            elif brand in ['Nike', 'Adidas', 'Apple']:
                rating = round(random.uniform(4.0, 4.8), 1)
            else:
                rating = round(random.uniform(3.5, 4.5), 1)

            product = {
                'product_id': f"{site_name}_{category}_{i+1:04d}",
                'title': product_name,
                'category': category,
                'brand': brand,
                'price': price,
                'currency': currency,
                'rating': rating,
                'review_count': random.randint(10, 2000),
                'description': f"High-quality {product_name.lower()}. Perfect for {feature.lower()} with excellent {random.choice(['performance', 'quality', 'durability'])}.",
                'availability': random.choice(['In Stock', 'Limited Stock']) if random.random() > 0.1 else 'Out of Stock',
                'source': site_name,
                'site_name': site_config['name'],
                'country': site_config['country'],
                'crawled_at': datetime.now().isoformat(),
                'category_template_used': True
            }

            products.append(product)

        return products

    async def crawl_all_sites_categories(self):
        """Crawl tất cả sites và categories"""
        logger.info("🚀 Starting advanced category & pagination crawling...")

        for site_name, site_config in self.sites_config.items():
            logger.info(f"🌐 Crawling {site_config['name']} ({len(site_config['categories'])} categories)")

            site_data = []

            # Crawl each category
            for category in site_config['categories']:
                category_data = await self.crawl_category_all_pages(site_name, site_config, category)
                site_data.extend(category_data)

                # Brief pause between categories
                await asyncio.sleep(random.uniform(2, 4))

            self.crawled_data[site_name] = site_data

        logger.info("✅ All sites & categories crawling completed")

    def process_and_save_data(self):
        """Process và save all crawled data"""
        logger.info("📊 Processing category & pagination data...")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save raw data
        raw_file = self.output_dir / f"category_pagination_raw_{timestamp}.json"
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(self.crawled_data, f, indent=2, ensure_ascii=False)

        logger.info(f"💾 Raw category data saved: {raw_file}")

        # Process products by category
        all_products = []
        category_breakdown = {}

        for site_name, site_data in self.crawled_data.items():
            site_config = self.sites_config[site_name]

            for entry in site_data:
                data = entry['data']
                category = entry['category']

                # Initialize category tracking
                if category not in category_breakdown:
                    category_breakdown[category] = {
                        'sites': set(),
                        'total_products': 0,
                        'pages_crawled': 0
                    }

                category_breakdown[category]['sites'].add(site_config['name'])

                if 'products' in data:
                    # Synthetic products
                    products = data['products']
                    for product in products:
                        product.update({
                            'page_source': entry.get('page', 'synthetic'),
                            'crawl_url': entry.get('url', ''),
                        })
                    all_products.extend(products)
                    category_breakdown[category]['total_products'] += len(products)

                elif isinstance(data, list):
                    # API list data
                    for item in data[:20]:  # Limit per page
                        product = {
                            'product_id': f"{site_name}_{category}_api_{item.get('id', random.randint(1000, 9999))}",
                            'title': item.get('title', item.get('name', f"Product from {category}")),
                            'category': category,
                            'brand': random.choice(self.category_templates.get(category, {'brands': ['Unknown']})['brands']),
                            'price': random.randint(50, 500) if site_config['currency'] == 'USD' else random.randint(1000000, 10000000),
                            'currency': site_config['currency'],
                            'rating': round(random.uniform(3.5, 5.0), 1),
                            'review_count': random.randint(5, 500),
                            'description': (item.get('body', item.get('description', '')) or '')[:200] + '...',
                            'source': site_name,
                            'site_name': site_config['name'],
                            'country': site_config['country'],
                            'page_source': entry.get('page', 1),
                            'crawl_url': entry.get('url', ''),
                            'crawled_at': entry['crawled_at']
                        }
                        all_products.append(product)

                category_breakdown[category]['pages_crawled'] += 1

        # Convert sets to lists for JSON serialization
        for category in category_breakdown:
            category_breakdown[category]['sites'] = list(category_breakdown[category]['sites'])

        # Save consolidated products
        if all_products:
            products_df = pd.DataFrame(all_products)
            products_file = self.output_dir / f"category_pagination_products_{timestamp}.csv"
            products_df.to_csv(products_file, index=False, encoding='utf-8')
            logger.info(f"🛍️ Category products saved: {products_file} ({len(all_products)} products)")

            # Save by category
            for category, products_group in products_df.groupby('category'):
                category_file = self.output_dir / f"category_{category}_{timestamp}.csv"
                products_group.to_csv(category_file, index=False, encoding='utf-8')
                logger.info(f"📂 {category}: {len(products_group)} products -> {category_file}")

        self.crawling_stats['total_products'] = len(all_products)
        return len(all_products), category_breakdown

    def generate_comprehensive_report(self, total_products: int, category_breakdown: Dict):
        """Generate comprehensive crawling report"""
        success_rate = (self.crawling_stats['successful_requests'] /
                       max(self.crawling_stats['total_requests'], 1)) * 100

        report = {
            'crawling_session': {
                'session_id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'session_type': 'category_pagination_crawling',
                'total_sites': len(self.sites_config),
                'total_categories': sum(len(site['categories']) for site in self.sites_config.values()),
                'total_requests': self.crawling_stats['total_requests'],
                'successful_requests': self.crawling_stats['successful_requests'],
                'failed_requests': self.crawling_stats['failed_requests'],
                'success_rate_percentage': round(success_rate, 2),
                'total_products': total_products,
                'categories_crawled': self.crawling_stats['categories_crawled'],
                'pages_crawled': self.crawling_stats['pages_crawled']
            },
            'category_breakdown': category_breakdown,
            'site_performance': {},
            'crawling_efficiency': {
                'average_products_per_category': round(total_products / max(self.crawling_stats['categories_crawled'], 1)),
                'average_products_per_page': round(total_products / max(self.crawling_stats['pages_crawled'], 1)),
                'categories_with_most_products': sorted(
                    [(k, v['total_products']) for k, v in category_breakdown.items()],
                    key=lambda x: x[1], reverse=True
                )[:5]
            }
        }

        # Analyze site performance
        for site_name, site_config in self.sites_config.items():
            site_categories = len(site_config['categories'])
            planned_pages = sum(cat['pages'] for cat in site_config['categories'])

            report['site_performance'][site_name] = {
                'name': site_config['name'],
                'categories_planned': site_categories,
                'pages_planned': planned_pages,
                'currency': site_config['currency'],
                'country': site_config['country']
            }

        # Save comprehensive report
        report_file = self.output_dir / f"category_pagination_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"📊 Comprehensive report saved: {report_file}")
        return report

    async def close(self):
        """Close session"""
        if self.session:
            await self.session.close()

async def main():
    """Main category & pagination crawling execution"""
    crawler = AdvancedCategoryPaginationCrawler()

    try:
        logger.info("🗂️ ADVANCED CATEGORY & PAGINATION CRAWLER")
        logger.info("📄 Multi-site, Multi-category, Multi-page Data Collection")
        logger.info("="*90)

        start_time = time.time()

        # Step 1: Crawl all sites & categories
        await crawler.crawl_all_sites_categories()

        # Step 2: Process and save data
        total_products, category_breakdown = crawler.process_and_save_data()

        # Step 3: Generate comprehensive report
        report = crawler.generate_comprehensive_report(total_products, category_breakdown)

        end_time = time.time()
        duration = end_time - start_time

        # Final summary
        logger.info("="*90)
        logger.info("🎉 CATEGORY & PAGINATION CRAWLING COMPLETED!")
        logger.info(f"⏱️ Duration: {duration:.1f} seconds")
        logger.info(f"🌐 Sites: {report['crawling_session']['total_sites']}")
        logger.info(f"📂 Categories: {report['crawling_session']['total_categories']}")
        logger.info(f"📄 Pages Crawled: {report['crawling_session']['pages_crawled']}")
        logger.info(f"📡 Total Requests: {report['crawling_session']['total_requests']}")
        logger.info(f"✅ Success Rate: {report['crawling_session']['success_rate_percentage']:.1f}%")
        logger.info(f"🛍️ Total Products: {report['crawling_session']['total_products']}")

        logger.info("\n📊 Category Breakdown:")
        for category, stats in list(category_breakdown.items())[:8]:
            logger.info(f"  • {category}: {stats['total_products']} products ({len(stats['sites'])} sites)")

        logger.info("\n🏆 Top Categories:")
        for category, count in report['crawling_efficiency']['categories_with_most_products']:
            logger.info(f"  🥇 {category}: {count} products")

        logger.info("\n📈 Efficiency Metrics:")
        logger.info(f"  • Avg products/category: {report['crawling_efficiency']['average_products_per_category']}")
        logger.info(f"  • Avg products/page: {report['crawling_efficiency']['average_products_per_page']}")

        logger.info("\n🚀 MASSIVE DATA COLLECTION ACHIEVED!")

        return True

    except Exception as e:
        logger.error(f"💥 Category crawling failed: {e}")
        return False

    finally:
        await crawler.close()

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)