#!/usr/bin/env python3
"""
Lazada Electronics Specialized Crawler
Thu thập dữ liệu thiết bị điện tử từ Lazada Việt Nam
"""

import time
import json
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import pandas as pd
import uuid
import re
from urllib.parse import urljoin, urlparse, quote_plus

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Database imports
import sqlite3
import psycopg2
from sqlalchemy import create_engine
import pymongo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lazada_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class LazadaProduct:
    """Lazada product data structure for electronics"""
    product_id: str
    name: str
    price: float
    original_price: Optional[float]
    discount_percent: Optional[float]
    category: str
    subcategory: Optional[str]
    brand: Optional[str]
    rating: Optional[float]
    review_count: Optional[int]
    sold_count: Optional[str]
    seller_name: Optional[str]
    seller_rating: Optional[float]
    description: Optional[str]
    key_features: List[str]
    specifications: Dict[str, Any]
    images: List[str]
    availability: bool
    shipping_info: Dict[str, Any]
    location: Optional[str]
    url: str
    crawl_timestamp: datetime
    platform: str = "lazada_vn"

class LazadaElectronicsCrawler:
    """Chuyên crawler cho thiết bị điện tử Lazada"""

    def __init__(self, headless: bool = True, output_dir: str = None):
        self.headless = headless
        self.output_dir = Path(output_dir) if output_dir else Path("../data/lazada_electronics")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Driver configuration
        self.driver = None
        self.wait = None

        # Crawling statistics
        self.stats = {
            'start_time': datetime.now(),
            'total_products_found': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'pages_processed': 0,
            'categories_processed': 0,
            'errors': []
        }

        # Electronics categories in Lazada Vietnam
        self.electronics_categories = {
            'smartphones': {
                'url': 'https://www.lazada.vn/dien-thoai-di-dong/',
                'name': 'Điện Thoại Di Động',
                'subcategories': ['iphone', 'samsung', 'oppo', 'xiaomi', 'huawei', 'realme']
            },
            'laptops': {
                'url': 'https://www.lazada.vn/may-tinh-xach-tay/',
                'name': 'Máy Tính Xách Tay',
                'subcategories': ['gaming-laptop', 'ultrabook', 'business-laptop']
            },
            'tablets': {
                'url': 'https://www.lazada.vn/may-tinh-bang/',
                'name': 'Máy Tính Bảng',
                'subcategories': ['ipad', 'android-tablet', 'windows-tablet']
            },
            'smartwatch': {
                'url': 'https://www.lazada.vn/dong-ho-thong-minh/',
                'name': 'Đồng Hồ Thông Minh',
                'subcategories': ['apple-watch', 'samsung-watch', 'fitness-tracker']
            },
            'headphones': {
                'url': 'https://www.lazada.vn/tai-nghe/',
                'name': 'Tai Nghe',
                'subcategories': ['true-wireless', 'gaming-headset', 'bluetooth-headphones']
            },
            'speakers': {
                'url': 'https://www.lazada.vn/loa/',
                'name': 'Loa',
                'subcategories': ['bluetooth-speaker', 'smart-speaker', 'portable-speaker']
            },
            'cameras': {
                'url': 'https://www.lazada.vn/may-anh/',
                'name': 'Máy Ảnh',
                'subcategories': ['dslr', 'mirrorless', 'action-camera']
            },
            'accessories': {
                'url': 'https://www.lazada.vn/phu-kien-dien-thoai/',
                'name': 'Phụ Kiện Điện Thoại',
                'subcategories': ['phone-case', 'screen-protector', 'charger', 'power-bank']
            }
        }

        # Rate limiting and delays
        self.min_delay = 2
        self.max_delay = 5
        self.page_load_timeout = 30
        self.element_timeout = 10

    def setup_driver(self) -> bool:
        """Setup Chrome WebDriver với cấu hình tối ưu"""
        try:
            chrome_options = Options()

            if self.headless:
                chrome_options.add_argument("--headless")

            # Anti-detection configurations
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            # Performance optimizations
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-javascript")  # Will enable later for specific interactions
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-extensions")

            # User agent rotation
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")

            # Window size
            chrome_options.add_argument("--window-size=1920,1080")

            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # Setup wait
            self.wait = WebDriverWait(self.driver, self.element_timeout)

            logger.info("✅ Chrome WebDriver initialized successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to setup WebDriver: {e}")
            return False

    def random_delay(self, min_delay: float = None, max_delay: float = None):
        """Random delay để tránh detection"""
        min_d = min_delay or self.min_delay
        max_d = max_delay or self.max_delay
        delay = random.uniform(min_d, max_d)
        logger.debug(f"💤 Sleeping for {delay:.2f} seconds")
        time.sleep(delay)

    def scroll_page_slowly(self):
        """Scroll trang một cách tự nhiên"""
        try:
            # Get page height
            last_height = self.driver.execute_script("return document.body.scrollHeight")

            # Scroll down slowly
            for i in range(0, last_height, 300):
                self.driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(0.5)

            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

        except Exception as e:
            logger.warning(f"⚠️ Error during page scrolling: {e}")

    def extract_product_from_listing(self, product_element) -> Optional[Dict[str, Any]]:
        """Extract product info từ listing page element"""
        try:
            product_data = {}

            # Product link and ID
            try:
                link_element = product_element.find_element(By.CSS_SELECTOR, "a[href]")
                product_url = link_element.get_attribute("href")
                product_data['url'] = product_url

                # Extract product ID from URL
                url_parts = product_url.split('/')
                for part in url_parts:
                    if part.startswith('i') and part[1:].isdigit():
                        product_data['product_id'] = part[1:]
                        break
                else:
                    product_data['product_id'] = str(uuid.uuid4())

            except NoSuchElementException:
                logger.warning("⚠️ Could not find product link")
                return None

            # Product name
            try:
                name_selectors = [
                    "[data-qa-locator='product-item'] [title]",
                    ".title-wrapper a",
                    ".product-title",
                    "h3 a",
                    "[data-qa-locator='product-item'] a[title]"
                ]

                for selector in name_selectors:
                    try:
                        name_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        product_name = name_element.get_attribute("title") or name_element.text
                        if product_name and product_name.strip():
                            product_data['name'] = product_name.strip()
                            break
                    except NoSuchElementException:
                        continue
                else:
                    product_data['name'] = "Unknown Product"

            except Exception as e:
                logger.warning(f"⚠️ Error extracting product name: {e}")
                product_data['name'] = "Unknown Product"

            # Price information
            try:
                price_selectors = [
                    ".price-current",
                    ".currency-value",
                    "[data-qa-locator='product-price']",
                    ".price .currency"
                ]

                for selector in price_selectors:
                    try:
                        price_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        price_text = price_element.text.strip()
                        # Extract numeric price
                        price_numbers = re.findall(r'[\d,]+', price_text.replace('.', ','))
                        if price_numbers:
                            price_str = price_numbers[0].replace(',', '')
                            product_data['price'] = float(price_str)
                            break
                    except (NoSuchElementException, ValueError):
                        continue
                else:
                    product_data['price'] = 0.0

            except Exception as e:
                logger.warning(f"⚠️ Error extracting price: {e}")
                product_data['price'] = 0.0

            # Original price and discount
            try:
                original_price_selectors = [
                    ".price-original",
                    ".original-price",
                    ".price-line-through"
                ]

                for selector in original_price_selectors:
                    try:
                        original_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        original_text = original_element.text.strip()
                        original_numbers = re.findall(r'[\d,]+', original_text.replace('.', ','))
                        if original_numbers:
                            original_str = original_numbers[0].replace(',', '')
                            product_data['original_price'] = float(original_str)

                            # Calculate discount
                            if product_data.get('price', 0) > 0:
                                discount = ((product_data['original_price'] - product_data['price']) /
                                          product_data['original_price']) * 100
                                product_data['discount_percent'] = round(discount, 2)
                            break
                    except (NoSuchElementException, ValueError):
                        continue
                else:
                    product_data['original_price'] = None
                    product_data['discount_percent'] = None

            except Exception as e:
                logger.warning(f"⚠️ Error extracting original price: {e}")
                product_data['original_price'] = None
                product_data['discount_percent'] = None

            # Rating and reviews
            try:
                rating_selectors = [
                    ".rating-average",
                    ".review-score",
                    "[data-qa-locator='product-rating']"
                ]

                for selector in rating_selectors:
                    try:
                        rating_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        rating_text = rating_element.text.strip()
                        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                        if rating_match:
                            product_data['rating'] = float(rating_match.group(1))
                            break
                    except (NoSuchElementException, ValueError):
                        continue
                else:
                    product_data['rating'] = None

                # Review count
                review_selectors = [
                    ".review-count",
                    ".reviews-count",
                    "[data-qa-locator='product-review-count']"
                ]

                for selector in review_selectors:
                    try:
                        review_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        review_text = review_element.text.strip()
                        review_numbers = re.findall(r'\d+', review_text)
                        if review_numbers:
                            product_data['review_count'] = int(review_numbers[0])
                            break
                    except (NoSuchElementException, ValueError):
                        continue
                else:
                    product_data['review_count'] = None

            except Exception as e:
                logger.warning(f"⚠️ Error extracting rating/reviews: {e}")
                product_data['rating'] = None
                product_data['review_count'] = None

            # Sold count
            try:
                sold_selectors = [
                    ".sold-count",
                    "[data-qa-locator='product-sold']",
                    ".sales-count"
                ]

                for selector in sold_selectors:
                    try:
                        sold_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        sold_text = sold_element.text.strip()
                        if 'đã bán' in sold_text.lower() or 'sold' in sold_text.lower():
                            product_data['sold_count'] = sold_text
                            break
                    except NoSuchElementException:
                        continue
                else:
                    product_data['sold_count'] = None

            except Exception as e:
                logger.warning(f"⚠️ Error extracting sold count: {e}")
                product_data['sold_count'] = None

            # Image
            try:
                img_selectors = [
                    "img[src]",
                    ".product-image img",
                    "[data-qa-locator='product-image'] img"
                ]

                for selector in img_selectors:
                    try:
                        img_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        img_src = img_element.get_attribute("src")
                        if img_src:
                            product_data['images'] = [img_src]
                            break
                    except NoSuchElementException:
                        continue
                else:
                    product_data['images'] = []

            except Exception as e:
                logger.warning(f"⚠️ Error extracting image: {e}")
                product_data['images'] = []

            # Set defaults
            product_data.update({
                'brand': None,
                'seller_name': None,
                'seller_rating': None,
                'description': None,
                'key_features': [],
                'specifications': {},
                'availability': True,
                'shipping_info': {},
                'location': None,
                'category': 'Electronics',
                'subcategory': None,
                'crawl_timestamp': datetime.now(),
                'platform': 'lazada_vn'
            })

            return product_data

        except Exception as e:
            logger.error(f"❌ Error extracting product from listing: {e}")
            return None

    def crawl_category_page(self, category_url: str, category_name: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """Crawl một category page và extract products"""
        products = []

        try:
            logger.info(f"🛒 Crawling category: {category_name} - {category_url}")

            for page in range(1, max_pages + 1):
                try:
                    # Construct page URL
                    if '?' in category_url:
                        page_url = f"{category_url}&page={page}"
                    else:
                        page_url = f"{category_url}?page={page}"

                    logger.info(f"📄 Processing page {page}/{max_pages}: {page_url}")

                    # Navigate to page
                    self.driver.get(page_url)
                    time.sleep(3)

                    # Wait for products to load
                    try:
                        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-qa-locator='product-item']")))
                    except TimeoutException:
                        logger.warning(f"⚠️ Timeout waiting for products on page {page}")
                        continue

                    # Scroll to load all products
                    self.scroll_page_slowly()

                    # Find all product elements
                    product_selectors = [
                        "[data-qa-locator='product-item']",
                        ".product-item",
                        ".item-product",
                        ".product-card"
                    ]

                    product_elements = []
                    for selector in product_selectors:
                        try:
                            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            if elements:
                                product_elements = elements
                                logger.info(f"✅ Found {len(elements)} products using selector: {selector}")
                                break
                        except Exception as e:
                            logger.debug(f"Selector {selector} failed: {e}")
                            continue

                    if not product_elements:
                        logger.warning(f"⚠️ No products found on page {page}")
                        continue

                    # Extract data from each product
                    page_products = []
                    for i, element in enumerate(product_elements):
                        try:
                            product_data = self.extract_product_from_listing(element)
                            if product_data:
                                product_data['category'] = category_name
                                page_products.append(product_data)
                                self.stats['successful_extractions'] += 1
                            else:
                                self.stats['failed_extractions'] += 1

                        except Exception as e:
                            logger.warning(f"⚠️ Error extracting product {i+1}: {e}")
                            self.stats['failed_extractions'] += 1
                            continue

                    products.extend(page_products)
                    self.stats['pages_processed'] += 1
                    self.stats['total_products_found'] += len(page_products)

                    logger.info(f"✅ Page {page} completed: {len(page_products)} products extracted")

                    # Random delay between pages
                    self.random_delay(3, 6)

                except Exception as e:
                    logger.error(f"❌ Error processing page {page}: {e}")
                    self.stats['errors'].append(f"Page {page}: {str(e)}")
                    continue

            self.stats['categories_processed'] += 1
            logger.info(f"✅ Category {category_name} completed: {len(products)} total products")

        except Exception as e:
            logger.error(f"❌ Error crawling category {category_name}: {e}")
            self.stats['errors'].append(f"Category {category_name}: {str(e)}")

        return products

    def crawl_electronics_categories(self, categories: List[str] = None, max_pages_per_category: int = 3) -> List[Dict[str, Any]]:
        """Crawl multiple electronics categories"""
        all_products = []

        # Determine which categories to crawl
        categories_to_crawl = categories or list(self.electronics_categories.keys())

        logger.info(f"🚀 Starting Lazada electronics crawl for {len(categories_to_crawl)} categories")

        for category_key in categories_to_crawl:
            if category_key not in self.electronics_categories:
                logger.warning(f"⚠️ Unknown category: {category_key}")
                continue

            category_info = self.electronics_categories[category_key]
            category_products = self.crawl_category_page(
                category_info['url'],
                category_info['name'],
                max_pages_per_category
            )

            all_products.extend(category_products)

            # Longer delay between categories
            self.random_delay(5, 10)

        return all_products

    def save_products(self, products: List[Dict[str, Any]], filename_prefix: str = "lazada_electronics"):
        """Save products to multiple formats"""
        if not products:
            logger.warning("⚠️ No products to save")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save to JSON
        json_file = self.output_dir / f"{filename_prefix}_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False, default=str)

        # Save to CSV
        df = pd.DataFrame(products)
        csv_file = self.output_dir / f"{filename_prefix}_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')

        # Save to JSONL for streaming
        jsonl_file = self.output_dir / f"{filename_prefix}_{timestamp}.jsonl"
        with open(jsonl_file, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False, default=str) + '\n')

        logger.info(f"💾 Saved {len(products)} products to:")
        logger.info(f"   📄 JSON: {json_file}")
        logger.info(f"   📊 CSV: {csv_file}")
        logger.info(f"   📝 JSONL: {jsonl_file}")

    def save_to_database(self, products: List[Dict[str, Any]]):
        """Save products to PostgreSQL database"""
        if not products:
            return

        try:
            # Convert to DataFrame
            df = pd.DataFrame(products)

            # Database connection (adjust as needed)
            db_config = {
                'host': 'localhost',
                'database': 'ecommerce_dss',
                'user': 'postgres',
                'password': 'your_password',
                'port': 5432
            }

            engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

            # Save to products table
            df.to_sql('lazada_products_raw', engine, if_exists='append', index=False)

            logger.info(f"💾 Saved {len(products)} products to PostgreSQL database")

        except Exception as e:
            logger.error(f"❌ Error saving to database: {e}")

    def generate_crawl_report(self) -> Dict[str, Any]:
        """Generate comprehensive crawl report"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']

        report = {
            'crawl_session': {
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration.total_seconds(),
                'duration_formatted': str(duration)
            },
            'performance': {
                'categories_processed': self.stats['categories_processed'],
                'pages_processed': self.stats['pages_processed'],
                'total_products_found': self.stats['total_products_found'],
                'successful_extractions': self.stats['successful_extractions'],
                'failed_extractions': self.stats['failed_extractions'],
                'success_rate': (self.stats['successful_extractions'] / max(self.stats['total_products_found'], 1)) * 100
            },
            'errors': {
                'error_count': len(self.stats['errors']),
                'errors': self.stats['errors']
            },
            'output_files': {
                'directory': str(self.output_dir),
                'formats': ['JSON', 'CSV', 'JSONL']
            }
        }

        return report

    def run_crawler(self, categories: List[str] = None, max_pages_per_category: int = 3) -> Dict[str, Any]:
        """Main crawler execution"""
        logger.info("🚀 Starting Lazada Electronics Crawler")

        try:
            # Setup driver
            if not self.setup_driver():
                raise Exception("Failed to setup WebDriver")

            # Crawl products
            products = self.crawl_electronics_categories(categories, max_pages_per_category)

            if products:
                # Save products
                self.save_products(products)

                # Optionally save to database
                # self.save_to_database(products)

                logger.info(f"✅ Crawl completed successfully: {len(products)} products collected")
            else:
                logger.warning("⚠️ No products were collected")

            # Generate report
            report = self.generate_crawl_report()

            # Save report
            report_file = self.output_dir / f"crawl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)

            logger.info(f"📊 Crawl report saved: {report_file}")

            return {
                'success': True,
                'products_count': len(products),
                'report': report,
                'products': products[:10] if products else []  # Sample products
            }

        except Exception as e:
            logger.error(f"❌ Crawler failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'report': self.generate_crawl_report()
            }

        finally:
            # Cleanup
            if self.driver:
                self.driver.quit()
                logger.info("🔌 WebDriver closed")

# Main execution
def main():
    """Main execution function"""
    print("""
    🛒 Lazada Electronics Crawler
    ============================

    Thu thập dữ liệu thiết bị điện tử từ Lazada Việt Nam
    Các danh mục: Điện thoại, Laptop, Tablet, Smartwatch, Tai nghe, Loa, Máy ảnh, Phụ kiện
    """)

    # Initialize crawler
    crawler = LazadaElectronicsCrawler(headless=False)  # Set to True for production

    # Categories to crawl (can be customized)
    target_categories = ['smartphones', 'laptops', 'tablets', 'headphones']

    # Run crawler with enhanced settings
    result = crawler.run_crawler(
        categories=target_categories,
        max_pages_per_category=10  # Increased from 2 to 10 for more comprehensive data
    )

    # Print results
    if result['success']:
        print(f"\n✅ Crawl thành công!")
        print(f"📊 Tổng sản phẩm: {result['products_count']}")
        print(f"⏱️  Thời gian: {result['report']['crawl_session']['duration_formatted']}")
        print(f"📈 Tỷ lệ thành công: {result['report']['performance']['success_rate']:.1f}%")

        if result['products']:
            print(f"\n🔍 Mẫu sản phẩm đầu tiên:")
            sample = result['products'][0]
            print(f"   Tên: {sample.get('name', 'N/A')}")
            print(f"   Giá: {sample.get('price', 'N/A'):,} VND")
            print(f"   Danh mục: {sample.get('category', 'N/A')}")
            print(f"   Rating: {sample.get('rating', 'N/A')}")
    else:
        print(f"\n❌ Crawl thất bại: {result['error']}")

if __name__ == "__main__":
    main()