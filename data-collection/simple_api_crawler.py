#!/usr/bin/env python3
"""
Simple API Crawler - Khởi động Web Crawling cho DSS
Crawl data từ các APIs miễn phí và lưu vào scraped_data
"""

import asyncio
import aiohttp
import json
import logging
import time
from datetime import datetime
from pathlib import Path
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleAPICrawler:
    def __init__(self):
        self.output_dir = Path("../data/raw/scraped_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Target APIs từ config
        self.api_sources = {
            "dummyjson": {
                "base_url": "https://dummyjson.com",
                "endpoints": {
                    "products": "/products?limit=100",
                    "users": "/users?limit=50",
                    "carts": "/carts?limit=30"
                }
            },
            "platzi": {
                "base_url": "https://api.escuelajs.co/api/v1",
                "endpoints": {
                    "products": "/products?offset=0&limit=100",
                    "categories": "/categories?limit=20"
                }
            },
            "reqres": {
                "base_url": "https://reqres.in/api",
                "endpoints": {
                    "users": "/users?per_page=12"
                }
            }
        }

        self.session = None
        self.crawled_data = {}

    async def get_session(self):
        """Get aiohttp session"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    async def fetch_api(self, source_name: str, endpoint_name: str, url: str):
        """Fetch data từ API endpoint"""
        try:
            session = await self.get_session()

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9'
            }

            logger.info(f"🔄 Fetching {source_name}/{endpoint_name}: {url}")

            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()

                    # Store crawled data
                    if source_name not in self.crawled_data:
                        self.crawled_data[source_name] = {}

                    self.crawled_data[source_name][endpoint_name] = {
                        'data': data,
                        'crawled_at': datetime.now().isoformat(),
                        'url': url,
                        'status': 'success'
                    }

                    logger.info(f"✅ Successfully crawled {source_name}/{endpoint_name}")
                    return data
                else:
                    logger.warning(f"⚠️ HTTP {response.status} for {url}")
                    return None

        except Exception as e:
            logger.error(f"❌ Error crawling {source_name}/{endpoint_name}: {e}")
            return None

        # Rate limiting
        await asyncio.sleep(1)

    async def crawl_all_apis(self):
        """Crawl tất cả APIs"""
        logger.info("🚀 Starting API crawling...")

        tasks = []

        for source_name, config in self.api_sources.items():
            for endpoint_name, endpoint_path in config['endpoints'].items():
                url = config['base_url'] + endpoint_path
                task = self.fetch_api(source_name, endpoint_name, url)
                tasks.append(task)

        # Run all crawling tasks concurrently
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("✅ API crawling completed")

    def process_and_save_data(self):
        """Process và save crawled data"""
        logger.info("📊 Processing crawled data...")

        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 1. Save complete raw data
        raw_file = self.output_dir / f"api_crawled_raw_{timestamp}.json"
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(self.crawled_data, f, indent=2, ensure_ascii=False)

        logger.info(f"💾 Raw data saved: {raw_file}")

        # 2. Process products data
        all_products = []

        for source_name, endpoints in self.crawled_data.items():
            if 'products' in endpoints and endpoints['products']['status'] == 'success':
                products_data = endpoints['products']['data']

                # Handle different API response formats
                if isinstance(products_data, dict) and 'products' in products_data:
                    # DummyJSON format
                    products = products_data['products']
                elif isinstance(products_data, list):
                    # Direct array format
                    products = products_data
                else:
                    products = []

                for product in products:
                    processed_product = {
                        'source': source_name,
                        'product_id': str(product.get('id', '')),
                        'title': product.get('title', product.get('name', '')),
                        'price': product.get('price', 0),
                        'category': product.get('category', ''),
                        'description': product.get('description', '')[:200] + '...' if product.get('description') else '',
                        'image': product.get('thumbnail', product.get('image', '')),
                        'rating': product.get('rating', 0),
                        'brand': product.get('brand', ''),
                        'crawled_at': endpoints['products']['crawled_at'],
                        'source_url': endpoints['products']['url']
                    }
                    all_products.append(processed_product)

        # Save products CSV
        if all_products:
            products_df = pd.DataFrame(all_products)
            products_file = self.output_dir / f"crawled_products_{timestamp}.csv"
            products_df.to_csv(products_file, index=False, encoding='utf-8')
            logger.info(f"📦 Products data saved: {products_file} ({len(all_products)} products)")

        # 3. Process users data
        all_users = []

        for source_name, endpoints in self.crawled_data.items():
            if 'users' in endpoints and endpoints['users']['status'] == 'success':
                users_data = endpoints['users']['data']

                # Handle different formats
                if isinstance(users_data, dict) and 'data' in users_data:
                    users = users_data['data']
                elif isinstance(users_data, dict) and 'users' in users_data:
                    users = users_data['users']
                elif isinstance(users_data, list):
                    users = users_data
                else:
                    users = []

                for user in users:
                    processed_user = {
                        'source': source_name,
                        'user_id': str(user.get('id', '')),
                        'first_name': user.get('first_name', user.get('firstName', '')),
                        'last_name': user.get('last_name', user.get('lastName', '')),
                        'email': user.get('email', ''),
                        'phone': user.get('phone', ''),
                        'address': str(user.get('address', {})) if user.get('address') else '',
                        'crawled_at': endpoints['users']['crawled_at']
                    }
                    all_users.append(processed_user)

        # Save users CSV
        if all_users:
            users_df = pd.DataFrame(all_users)
            users_file = self.output_dir / f"crawled_users_{timestamp}.csv"
            users_df.to_csv(users_file, index=False, encoding='utf-8')
            logger.info(f"👥 Users data saved: {users_file} ({len(all_users)} users)")

    def generate_crawling_report(self):
        """Generate crawling summary report"""
        report = {
            'crawling_session': {
                'timestamp': datetime.now().isoformat(),
                'total_sources': len(self.api_sources),
                'successful_crawls': 0,
                'failed_crawls': 0,
                'total_records': 0
            },
            'sources_detail': {}
        }

        for source_name, endpoints in self.crawled_data.items():
            source_detail = {
                'endpoints_crawled': len(endpoints),
                'successful_endpoints': 0,
                'records_collected': 0
            }

            for endpoint_name, endpoint_data in endpoints.items():
                if endpoint_data['status'] == 'success':
                    source_detail['successful_endpoints'] += 1
                    report['crawling_session']['successful_crawls'] += 1

                    # Count records
                    data = endpoint_data['data']
                    if isinstance(data, dict):
                        if 'products' in data:
                            records = len(data['products'])
                        elif 'users' in data:
                            records = len(data['users'])
                        elif 'data' in data:
                            records = len(data['data'])
                        else:
                            records = 1
                    elif isinstance(data, list):
                        records = len(data)
                    else:
                        records = 1

                    source_detail['records_collected'] += records
                    report['crawling_session']['total_records'] += records
                else:
                    report['crawling_session']['failed_crawls'] += 1

            report['sources_detail'][source_name] = source_detail

        # Save report
        report_file = self.output_dir / f"crawling_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"📋 Crawling report saved: {report_file}")
        return report

    async def close(self):
        """Close session"""
        if self.session:
            await self.session.close()

async def main():
    """Main crawling execution"""
    crawler = SimpleAPICrawler()

    try:
        logger.info("🕷️ Starting Simple API Crawler for DSS E-commerce")
        logger.info("="*60)

        start_time = time.time()

        # Step 1: Crawl APIs
        await crawler.crawl_all_apis()

        # Step 2: Process and save data
        crawler.process_and_save_data()

        # Step 3: Generate report
        report = crawler.generate_crawling_report()

        end_time = time.time()
        duration = end_time - start_time

        # Summary
        logger.info("="*60)
        logger.info("🎉 CRAWLING SESSION COMPLETED!")
        logger.info(f"⏱️ Duration: {duration:.1f} seconds")
        logger.info(f"📊 Total Sources: {report['crawling_session']['total_sources']}")
        logger.info(f"✅ Successful Crawls: {report['crawling_session']['successful_crawls']}")
        logger.info(f"❌ Failed Crawls: {report['crawling_session']['failed_crawls']}")
        logger.info(f"📦 Total Records: {report['crawling_session']['total_records']}")

        logger.info("\n🎯 Next Steps:")
        logger.info("1. Check data/raw/scraped_data/ for results")
        logger.info("2. Review crawling_report_*.json for details")
        logger.info("3. Ready for Vietnamese site scraping!")

        return True

    except Exception as e:
        logger.error(f"💥 Crawling session failed: {e}")
        return False

    finally:
        await crawler.close()

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)