#!/usr/bin/env python3
"""
E-commerce Web Crawler for Multi-source Data Collection
Supports Vietnamese and International e-commerce sites
"""

import asyncio
import aiohttp
import json
import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, urlparse
import random
from fake_useragent import UserAgent

import pandas as pd
from bs4 import BeautifulSoup
import requests
from kafka import KafkaProducer
from pymongo import MongoClient
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ProductData:
    """Product data structure"""
    source: str
    product_id: str
    name: str
    price: float
    original_price: Optional[float]
    currency: str
    category: str
    brand: Optional[str]
    description: str
    images: List[str]
    ratings: Optional[float]
    review_count: Optional[int]
    availability: bool
    seller_name: Optional[str]
    shipping_info: Optional[str]
    specifications: Dict[str, Any]
    url: str
    scraped_at: datetime
    location: Optional[str] = None

@dataclass
class ReviewData:
    """Review data structure"""
    source: str
    product_id: str
    review_id: str
    customer_name: Optional[str]
    rating: float
    review_text: str
    review_date: datetime
    helpful_votes: Optional[int]
    verified_purchase: bool
    sentiment_score: Optional[float] = None

class EcommerceWebCrawler:
    """Multi-source E-commerce Web Crawler"""

    def __init__(self, config_path: str = "config/data_sources.json"):
        self.load_config(config_path)
        self.setup_connections()
        self.ua = UserAgent()
        self.session = None

        # Rate limiting
        self.request_delays = {
            'shopee': 2,
            'tiki': 1.5,
            'sendo': 3,
            'fptshop': 2,
            'default': 1
        }

        # Headers for different sites
        self.headers_templates = {
            'default': {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            },
            'api': {
                'User-Agent': self.ua.random,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        }

    def load_config(self, config_path: str):
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
                logger.info("✅ Configuration loaded successfully")
        except Exception as e:
            logger.error(f"❌ Failed to load config: {e}")
            raise

    def setup_connections(self):
        """Setup database and message queue connections"""
        try:
            # Kafka producer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )

            # MongoDB connection
            self.mongo_client = MongoClient('mongodb://localhost:27017/')
            self.mongo_db = self.mongo_client.dss_streaming

            # Redis for caching
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

            logger.info("✅ Database connections established")

        except Exception as e:
            logger.error(f"❌ Failed to setup connections: {e}")
            raise

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create async HTTP session"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self.headers_templates['default']
            )
        return self.session

    async def fetch_page(self, url: str, site_name: str = 'default',
                        headers: Optional[Dict] = None) -> Optional[str]:
        """Fetch webpage with rate limiting and error handling"""
        try:
            session = await self.get_session()

            # Apply rate limiting
            delay = self.request_delays.get(site_name, self.request_delays['default'])
            await asyncio.sleep(delay + random.uniform(0.5, 1.5))

            # Custom headers if provided
            request_headers = headers or self.headers_templates['default']

            async with session.get(url, headers=request_headers) as response:
                if response.status == 200:
                    content = await response.text()
                    logger.info(f"✅ Successfully fetched: {url}")
                    return content
                else:
                    logger.warning(f"⚠️ HTTP {response.status} for {url}")
                    return None

        except Exception as e:
            logger.error(f"❌ Error fetching {url}: {e}")
            return None

    def parse_shopee_products(self, html_content: str, base_url: str) -> List[ProductData]:
        """Parse Shopee product data"""
        products = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find product containers (this is a simplified example)
            product_cards = soup.find_all('div', class_=['col-xs-2-4', 'shopee-search-item-result__item'])

            for card in product_cards:
                try:
                    # Extract product information
                    name_elem = card.find(['div', 'span'], class_=['_10Wbs-', 'shopee-item-card__name'])
                    price_elem = card.find(['span', 'div'], class_=['_3c5u7X', 'shopee-item-card__current-price'])
                    image_elem = card.find('img')
                    rating_elem = card.find(['span'], class_=['shopee-rating-stars__rating-num'])

                    if name_elem and price_elem:
                        # Clean and extract data
                        name = name_elem.get_text(strip=True)
                        price_text = price_elem.get_text(strip=True)
                        price = self.extract_price(price_text)

                        image_url = image_elem.get('src') if image_elem else ''
                        rating = float(rating_elem.get_text(strip=True)) if rating_elem else None

                        # Get product URL
                        link_elem = card.find('a')
                        product_url = urljoin(base_url, link_elem.get('href')) if link_elem else base_url

                        product = ProductData(
                            source='shopee',
                            product_id=self.generate_product_id(product_url),
                            name=name,
                            price=price,
                            original_price=None,
                            currency='VND',
                            category='general',
                            brand=None,
                            description=name,
                            images=[image_url] if image_url else [],
                            ratings=rating,
                            review_count=None,
                            availability=True,
                            seller_name=None,
                            shipping_info=None,
                            specifications={},
                            url=product_url,
                            scraped_at=datetime.now(),
                            location='Vietnam'
                        )

                        products.append(product)

                except Exception as e:
                    logger.warning(f"⚠️ Error parsing individual product: {e}")
                    continue

            logger.info(f"📊 Parsed {len(products)} products from Shopee")

        except Exception as e:
            logger.error(f"❌ Error parsing Shopee products: {e}")

        return products

    def parse_tiki_products(self, html_content: str, base_url: str) -> List[ProductData]:
        """Parse Tiki product data"""
        products = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Tiki product containers
            product_cards = soup.find_all('div', class_=['product-item'])

            for card in product_cards:
                try:
                    name_elem = card.find(['h3', 'a'], class_=['name'])
                    price_elem = card.find(['span', 'div'], class_=['price-discount__price'])
                    image_elem = card.find('img')

                    if name_elem and price_elem:
                        name = name_elem.get_text(strip=True)
                        price_text = price_elem.get_text(strip=True)
                        price = self.extract_price(price_text)

                        image_url = image_elem.get('src') if image_elem else ''

                        product = ProductData(
                            source='tiki',
                            product_id=self.generate_product_id(name),
                            name=name,
                            price=price,
                            original_price=None,
                            currency='VND',
                            category='general',
                            brand=None,
                            description=name,
                            images=[image_url] if image_url else [],
                            ratings=None,
                            review_count=None,
                            availability=True,
                            seller_name=None,
                            shipping_info=None,
                            specifications={},
                            url=base_url,
                            scraped_at=datetime.now(),
                            location='Vietnam'
                        )

                        products.append(product)

                except Exception as e:
                    logger.warning(f"⚠️ Error parsing Tiki product: {e}")
                    continue

            logger.info(f"📊 Parsed {len(products)} products from Tiki")

        except Exception as e:
            logger.error(f"❌ Error parsing Tiki products: {e}")

        return products

    def extract_price(self, price_text: str) -> float:
        """Extract numeric price from text"""
        try:
            # Remove currency symbols and separators
            price_clean = price_text.replace('₫', '').replace('VND', '').replace(',', '').replace('.', '')
            price_clean = ''.join(filter(str.isdigit, price_clean))

            if price_clean:
                return float(price_clean)
            else:
                return 0.0

        except Exception as e:
            logger.warning(f"⚠️ Error extracting price from '{price_text}': {e}")
            return 0.0

    def generate_product_id(self, identifier: str) -> str:
        """Generate unique product ID"""
        import hashlib
        return hashlib.md5(identifier.encode()).hexdigest()[:12]

    async def crawl_site(self, site_config: Dict[str, Any]) -> List[ProductData]:
        """Crawl a specific e-commerce site"""
        all_products = []
        site_name = site_config.get('name', 'unknown')

        try:
            base_url = site_config['base_url']
            target_pages = site_config.get('target_pages', ['/'])

            for page_path in target_pages:
                full_url = urljoin(base_url, page_path)

                # Check cache first
                cache_key = f"crawl:{site_name}:{page_path}"
                cached_data = self.redis_client.get(cache_key)

                if cached_data:
                    logger.info(f"📦 Using cached data for {full_url}")
                    cached_products = json.loads(cached_data)
                    all_products.extend([ProductData(**p) for p in cached_products])
                    continue

                # Fetch page content
                html_content = await self.fetch_page(full_url, site_name)

                if html_content:
                    # Parse based on site
                    if 'shopee' in site_name.lower():
                        products = self.parse_shopee_products(html_content, base_url)
                    elif 'tiki' in site_name.lower():
                        products = self.parse_tiki_products(html_content, base_url)
                    else:
                        products = self.parse_generic_ecommerce(html_content, base_url, site_name)

                    all_products.extend(products)

                    # Cache results for 1 hour
                    if products:
                        cache_data = [asdict(p) for p in products]
                        self.redis_client.setex(cache_key, 3600, json.dumps(cache_data, default=str))

                # Respectful delay between pages
                await asyncio.sleep(random.uniform(2, 4))

        except Exception as e:
            logger.error(f"❌ Error crawling {site_name}: {e}")

        return all_products

    def parse_generic_ecommerce(self, html_content: str, base_url: str, site_name: str) -> List[ProductData]:
        """Generic parser for e-commerce sites"""
        products = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Generic selectors for common e-commerce patterns
            product_selectors = [
                '.product-item', '.product-card', '.item-card',
                '[data-product-id]', '.search-item', '.product-box'
            ]

            product_cards = []
            for selector in product_selectors:
                cards = soup.select(selector)
                if cards:
                    product_cards = cards
                    break

            for card in product_cards[:20]:  # Limit to first 20 products
                try:
                    # Try different patterns for name
                    name_elem = (card.find(['h1', 'h2', 'h3', 'h4'], class_=lambda x: x and any(
                        word in x.lower() for word in ['name', 'title', 'product'])) or
                               card.find(['a', 'span', 'div'], class_=lambda x: x and 'name' in x.lower()))

                    # Try different patterns for price
                    price_elem = card.find(['span', 'div'], class_=lambda x: x and any(
                        word in x.lower() for word in ['price', 'cost', 'amount']))

                    if name_elem:
                        name = name_elem.get_text(strip=True)
                        price_text = price_elem.get_text(strip=True) if price_elem else "0"
                        price = self.extract_price(price_text)

                        # Get image
                        image_elem = card.find('img')
                        image_url = image_elem.get('src') if image_elem else ''

                        product = ProductData(
                            source=site_name,
                            product_id=self.generate_product_id(f"{site_name}_{name}"),
                            name=name,
                            price=price,
                            original_price=None,
                            currency='VND',
                            category='general',
                            brand=None,
                            description=name,
                            images=[image_url] if image_url else [],
                            ratings=None,
                            review_count=None,
                            availability=True,
                            seller_name=None,
                            shipping_info=None,
                            specifications={},
                            url=base_url,
                            scraped_at=datetime.now(),
                            location='Vietnam'
                        )

                        products.append(product)

                except Exception as e:
                    continue

            logger.info(f"📊 Parsed {len(products)} products from {site_name}")

        except Exception as e:
            logger.error(f"❌ Error in generic parser for {site_name}: {e}")

        return products

    def save_to_storage(self, products: List[ProductData]):
        """Save products to multiple storage systems"""
        try:
            for product in products:
                product_dict = asdict(product)

                # Send to Kafka for real-time processing
                self.kafka_producer.send(
                    'products_raw',
                    key=product.product_id,
                    value=product_dict
                )

                # Save to MongoDB for data lake
                self.mongo_db.products_raw.insert_one(product_dict)

            # Flush Kafka messages
            self.kafka_producer.flush()

            logger.info(f"💾 Saved {len(products)} products to storage systems")

        except Exception as e:
            logger.error(f"❌ Error saving to storage: {e}")

    async def run_crawling_session(self):
        """Run a complete crawling session"""
        logger.info("🚀 Starting e-commerce crawling session...")

        try:
            web_scraping_config = self.config['data_sources']['web_scraping']

            all_products = []

            # Crawl Vietnamese sites
            vietnamese_sites = web_scraping_config.get('vietnamese_sites', {})
            for site_name, site_config in vietnamese_sites.items():
                site_config['name'] = site_name
                logger.info(f"🌐 Crawling {site_name}...")

                products = await self.crawl_site(site_config)
                all_products.extend(products)

                # Break between sites
                await asyncio.sleep(random.uniform(5, 10))

            # Save all collected data
            if all_products:
                self.save_to_storage(all_products)

                # Generate summary
                summary = {
                    'total_products': len(all_products),
                    'sources': list(set(p.source for p in all_products)),
                    'crawling_session': datetime.now().isoformat(),
                    'success': True
                }

                logger.info(f"✅ Crawling session completed: {summary}")
                return summary
            else:
                logger.warning("⚠️ No products collected in this session")
                return {'success': False, 'error': 'No products collected'}

        except Exception as e:
            logger.error(f"❌ Crawling session failed: {e}")
            return {'success': False, 'error': str(e)}

        finally:
            if self.session and not self.session.closed:
                await self.session.close()

    def close_connections(self):
        """Close all connections"""
        try:
            if hasattr(self, 'kafka_producer'):
                self.kafka_producer.close()
            if hasattr(self, 'mongo_client'):
                self.mongo_client.close()
            logger.info("🔌 All connections closed")
        except Exception as e:
            logger.error(f"❌ Error closing connections: {e}")

# Main execution function
async def main():
    """Main execution function"""
    crawler = EcommerceWebCrawler()

    try:
        # Run crawling session
        result = await crawler.run_crawling_session()
        print(f"📊 Crawling Results: {json.dumps(result, indent=2)}")

    finally:
        crawler.close_connections()

if __name__ == "__main__":
    asyncio.run(main())