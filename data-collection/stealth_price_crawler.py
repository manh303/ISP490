#!/usr/bin/env python3
"""
Stealth Price Crawler for Lazada
Sử dụng chiến thuật stealth cao cấp để tránh detection và extract prices
"""

import time
import json
import logging
import random
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

# Basic requests approach for comparison
import requests
from bs4 import BeautifulSoup

# Selenium with minimal footprint
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stealth_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StealthPriceCrawler:
    """Ultra-stealth crawler focusing on price extraction"""

    def __init__(self, headless: bool = True, output_dir: str = None):
        self.headless = headless
        self.output_dir = Path(output_dir) if output_dir else Path("../data/stealth_lazada_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Minimal driver setup
        self.driver = None
        self.session = requests.Session()

        # Ultra-stealth configuration
        self.stealth_config = {
            'user_agents': [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
            ],
            'delays': {
                'between_requests': (8, 15),
                'page_load': (5, 10),
                'human_behavior': (2, 5)
            }
        }

        # Comprehensive price selectors
        self.price_patterns = [
            r'\"price\":\s*\"?(\d+)\"?',
            r'\"current_price\":\s*\"?(\d+)\"?',
            r'\"selling_price\":\s*\"?(\d+)\"?',
            r'data-price=\"(\d+)\"',
            r'\"originalPrice\":\s*(\d+)',
            r'\"salePrice\":\s*(\d+)',
            r'price[\"\']\s*:\s*[\"\']*(\d+)',
            r'₫\s*([\d,\.]+)',
            r'VND\s*([\d,\.]+)',
            r'(\d{4,})',  # Simple 4+ digit numbers
        ]

        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'total_products': 0,
            'products_with_prices': 0,
            'requests_made': 0,
            'success_rate': 0.0
        }

    def setup_minimal_driver(self) -> bool:
        """Setup minimal Chrome driver for specific tasks only"""
        try:
            options = Options()

            if self.headless:
                options.add_argument("--headless=new")

            # Minimal stealth options
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)

            # Reduce resource usage
            options.add_argument("--disable-images")
            options.add_argument("--disable-javascript")  # Initially disabled
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-extensions")
            options.add_argument("--no-first-run")
            options.add_argument("--disable-default-apps")

            # Random user agent
            user_agent = random.choice(self.stealth_config['user_agents'])
            options.add_argument(f"--user-agent={user_agent}")

            self.driver = webdriver.Chrome(options=options)

            # Stealth scripts
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            logger.info("SUCCESS: Minimal driver setup completed")
            return True

        except Exception as e:
            logger.error(f"Driver setup failed: {e}")
            return False

    def setup_requests_session(self):
        """Setup requests session with stealth headers"""
        headers = {
            'User-Agent': random.choice(self.stealth_config['user_agents']),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }

        self.session.headers.update(headers)
        logger.info("Requests session configured")

    def stealth_delay(self, delay_type: str = 'between_requests'):
        """Stealth delays"""
        if delay_type in self.stealth_config['delays']:
            min_delay, max_delay = self.stealth_config['delays'][delay_type]
            delay = random.uniform(min_delay, max_delay)
            logger.debug(f"Stealth delay ({delay_type}): {delay:.1f}s")
            time.sleep(delay)

    def extract_price_from_html(self, html_content: str) -> float:
        """Extract price from HTML using multiple patterns"""
        try:
            # Remove whitespace and normalize
            clean_html = re.sub(r'\\s+', ' ', html_content)

            for pattern in self.price_patterns:
                matches = re.findall(pattern, clean_html, re.IGNORECASE)
                if matches:
                    for match in matches:
                        # Clean the match
                        price_str = re.sub(r'[,\.\s]', '', str(match))
                        if price_str.isdigit() and len(price_str) >= 4:  # Reasonable price length
                            price = float(price_str)
                            if 1000 <= price <= 100000000:  # Reasonable price range
                                logger.debug(f"Price found: {price} using pattern: {pattern}")
                                return price

            return 0.0

        except Exception as e:
            logger.debug(f"Price extraction error: {e}")
            return 0.0

    def get_product_page_content(self, product_url: str) -> str:
        """Get product page content using requests first, selenium if needed"""
        try:
            # Method 1: Requests approach
            try:
                response = self.session.get(product_url, timeout=10)
                self.stats['requests_made'] += 1

                if response.status_code == 200:
                    return response.text
                else:
                    logger.debug(f"Requests failed with status: {response.status_code}")
            except Exception as e:
                logger.debug(f"Requests method failed: {e}")

            # Method 2: Selenium approach (fallback)
            if self.driver:
                try:
                    self.driver.get(product_url)
                    time.sleep(3)  # Wait for dynamic content
                    return self.driver.page_source
                except Exception as e:
                    logger.debug(f"Selenium method failed: {e}")

            return ""

        except Exception as e:
            logger.warning(f"Could not get content for {product_url}: {e}")
            return ""

    def extract_product_info_stealth(self, product_element, use_selenium: bool = False) -> Optional[Dict[str, Any]]:
        """Extract product info using stealth methods"""
        try:
            product_data = {}

            if use_selenium:
                # Selenium extraction
                try:
                    link_element = product_element.find_element(By.CSS_SELECTOR, "a[href]")
                    product_url = link_element.get_attribute("href")
                    product_data['url'] = product_url

                    # Product name
                    name_selectors = ["[title]", "a[title]", "h3", ".title"]
                    for selector in name_selectors:
                        try:
                            name_element = product_element.find_element(By.CSS_SELECTOR, selector)
                            product_name = name_element.get_attribute("title") or name_element.text
                            if product_name and product_name.strip():
                                product_data['name'] = product_name.strip()
                                break
                        except:
                            continue
                    else:
                        product_data['name'] = "Unknown Product"

                except Exception as e:
                    logger.debug(f"Selenium extraction error: {e}")
                    return None

            else:
                # BeautifulSoup extraction (for requests method)
                try:
                    links = product_element.find_all('a', href=True)
                    if links:
                        product_url = links[0]['href']
                        if not product_url.startswith('http'):
                            product_url = f"https://www.lazada.vn{product_url}"
                        product_data['url'] = product_url

                        # Product name
                        name_elem = product_element.find(attrs={'title': True})
                        if name_elem:
                            product_data['name'] = name_elem.get('title', '').strip()
                        else:
                            product_data['name'] = "Unknown Product"
                    else:
                        return None
                except:
                    return None

            # Extract product ID
            url = product_data.get('url', '')
            product_id_match = re.search(r'i(\d+)', url)
            if product_id_match:
                product_data['product_id'] = product_id_match.group(1)
            else:
                product_data['product_id'] = str(uuid.uuid4())

            # Price extraction from product page
            price = 0.0
            try:
                # Get product page content
                page_content = self.get_product_page_content(product_data['url'])
                if page_content:
                    price = self.extract_price_from_html(page_content)

                self.stealth_delay('human_behavior')  # Delay after each request

            except Exception as e:
                logger.debug(f"Price extraction failed: {e}")

            product_data['price'] = price

            # Basic defaults
            product_data.update({
                'brand': None,
                'category': 'Electronics',
                'rating': None,
                'images': [],
                'crawl_timestamp': datetime.now(),
                'platform': 'lazada_vn'
            })

            # Track price success
            if price > 0:
                self.stats['products_with_prices'] += 1

            self.stats['total_products'] += 1
            return product_data

        except Exception as e:
            logger.error(f"Product extraction error: {e}")
            return None

    def crawl_category_stealth(self, category_url: str, category_name: str, max_pages: int = 2) -> List[Dict[str, Any]]:
        """Stealth category crawling"""
        products = []
        logger.info(f"Starting stealth crawl: {category_name}")

        try:
            for page in range(1, max_pages + 1):
                try:
                    # Construct URL
                    if '?' in category_url:
                        page_url = f"{category_url}&page={page}"
                    else:
                        page_url = f"{category_url}?page={page}"

                    logger.info(f"Processing page {page}/{max_pages}: {page_url}")

                    # Try requests method first
                    try:
                        response = self.session.get(page_url, timeout=15)
                        self.stats['requests_made'] += 1

                        if response.status_code == 200:
                            soup = BeautifulSoup(response.content, 'html.parser')

                            # Find product containers
                            product_containers = soup.find_all(attrs={'data-qa-locator': 'product-item'})
                            if not product_containers:
                                product_containers = soup.find_all('div', class_=lambda x: x and 'product' in x.lower())

                            logger.info(f"Found {len(product_containers)} products using requests method")

                            # Extract products
                            page_products = []
                            for i, container in enumerate(product_containers[:10]):  # Limit to first 10 for testing
                                try:
                                    product_data = self.extract_product_info_stealth(container, use_selenium=False)
                                    if product_data:
                                        product_data['category'] = category_name
                                        page_products.append(product_data)

                                    # Small delay between products
                                    if i > 0 and i % 3 == 0:
                                        time.sleep(random.uniform(1, 3))

                                except Exception as e:
                                    logger.debug(f"Product {i+1} extraction error: {e}")

                            products.extend(page_products)
                            logger.info(f"Page {page} completed: {len(page_products)} products, {sum(1 for p in page_products if p.get('price', 0) > 0)} with prices")

                        else:
                            logger.warning(f"Page {page} failed with status: {response.status_code}")

                    except Exception as e:
                        logger.warning(f"Requests method failed for page {page}: {e}")

                    # Stealth delay between pages
                    self.stealth_delay('between_requests')

                except Exception as e:
                    logger.error(f"Page {page} error: {e}")
                    continue

            logger.info(f"Category {category_name} completed: {len(products)} total products")

        except Exception as e:
            logger.error(f"Category crawl error: {e}")

        return products

    def run_stealth_crawl(self, categories: List[str] = None, max_pages_per_category: int = 2) -> Dict[str, Any]:
        """Run stealth crawl"""
        logger.info("Starting Stealth Price Crawler")

        try:
            # Setup
            self.setup_requests_session()
            if not self.setup_minimal_driver():
                logger.warning("Driver setup failed, continuing with requests only")

            # Categories
            electronics_categories = {
                'smartphones': {
                    'url': 'https://www.lazada.vn/dien-thoai-di-dong/',
                    'name': 'Điện Thoại Di Động'
                }
            }

            categories_to_crawl = categories or ['smartphones']
            all_products = []

            for category_key in categories_to_crawl:
                if category_key not in electronics_categories:
                    continue

                category_info = electronics_categories[category_key]
                category_products = self.crawl_category_stealth(
                    category_info['url'],
                    category_info['name'],
                    max_pages_per_category
                )

                all_products.extend(category_products)

            # Calculate success rate
            self.stats['success_rate'] = (self.stats['products_with_prices'] / max(self.stats['total_products'], 1)) * 100

            # Save results
            if all_products:
                self.save_stealth_results(all_products)

            return {
                'success': True,
                'products_count': len(all_products),
                'products_with_prices': self.stats['products_with_prices'],
                'price_success_rate': self.stats['success_rate'],
                'requests_made': self.stats['requests_made'],
                'products': all_products[:3] if all_products else []
            }

        except Exception as e:
            logger.error(f"Stealth crawl failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }

        finally:
            if self.driver:
                self.driver.quit()

    def save_stealth_results(self, products: List[Dict[str, Any]]):
        """Save stealth crawl results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save JSON
        json_file = self.output_dir / f"stealth_lazada_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False, default=str)

        # Save CSV
        df = pd.DataFrame(products)
        csv_file = self.output_dir / f"stealth_lazada_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')

        logger.info(f"Results saved: {json_file}")

def main():
    """Test stealth crawler"""
    print("Stealth Price Crawler Test")
    print("=" * 40)

    crawler = StealthPriceCrawler(headless=True)
    result = crawler.run_stealth_crawl(
        categories=['smartphones'],
        max_pages_per_category=2
    )

    if result['success']:
        print(f"SUCCESS: {result['products_count']} products")
        print(f"Price extraction: {result['products_with_prices']} products ({result['price_success_rate']:.1f}%)")
        print(f"Requests made: {result['requests_made']}")

        # Show samples with prices
        for product in result['products']:
            if product.get('price', 0) > 0:
                print(f"Sample: {product['name']} - {product['price']:,.0f} VND")
    else:
        print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    main()