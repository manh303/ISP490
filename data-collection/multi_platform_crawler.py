#!/usr/bin/env python3
"""
Multi-Platform Electronics Crawler
Crawl full electronics from both Lazada and Tiki
Clean code without Unicode for Windows compatibility
"""

import time
import json
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import csv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class MultiPlatformCrawler:
    """Crawler for both Lazada and Tiki electronics"""

    def __init__(self, headless: bool = True, verbose: bool = True):
        self.headless = headless
        self.verbose = verbose
        self.output_dir = Path("../data/multi_platform")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.driver = None
        self.wait = None

        # Results tracking
        self.results = {
            'start_time': datetime.now(),
            'total_products': 0,
            'products_with_prices': 0,
            'lazada_products': 0,
            'tiki_products': 0,
            'pages_crawled': 0,
            'categories_completed': 0
        }

        # Electronics categories for both platforms
        self.categories = {
            'lazada': {
                'smartphones': 'https://www.lazada.vn/dien-thoai-di-dong/',
                'laptops': 'https://www.lazada.vn/may-tinh-xach-tay/',
                'tablets': 'https://www.lazada.vn/may-tinh-bang/',
                'headphones': 'https://www.lazada.vn/tai-nghe/',
                'speakers': 'https://www.lazada.vn/loa/',
                'smartwatches': 'https://www.lazada.vn/dong-ho-thong-minh/',
                'cameras': 'https://www.lazada.vn/may-anh/',
                'accessories': 'https://www.lazada.vn/phu-kien-dien-thoai/'
            },
            'tiki': {
                'smartphones': 'https://tiki.vn/dien-thoai-smartphone/c1795',
                'laptops': 'https://tiki.vn/laptop/c1846',
                'tablets': 'https://tiki.vn/may-tinh-bang/c1794',
                'headphones': 'https://tiki.vn/tai-nghe/c1882',
                'speakers': 'https://tiki.vn/loa/c1883',
                'smartwatches': 'https://tiki.vn/dong-ho-thong-minh/c1884',
                'cameras': 'https://tiki.vn/may-anh/c1801',
                'accessories': 'https://tiki.vn/phu-kien-dien-thoai/c1815'
            }
        }

    def log(self, message):
        """Simple logging without Unicode"""
        if self.verbose:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {message}")

    def setup_driver(self):
        """Setup Chrome driver for crawling"""
        self.log("Setting up Chrome driver...")

        options = Options()
        if self.headless:
            options.add_argument("--headless=new")

        # Basic options for stability
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

        # User agent
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Performance
        options.add_argument("--disable-extensions")
        options.add_argument("--no-first-run")
        options.add_argument("--disable-default-apps")

        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(30)
        self.wait = WebDriverWait(self.driver, 15)

        self.log("Driver ready")

    def wait_and_scroll(self, platform):
        """Platform-specific wait and scroll"""
        try:
            # Basic page load wait
            time.sleep(3)

            if platform == 'lazada':
                # Lazada specific scrolling
                for i in range(6):
                    scroll_height = i * 400
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_height});")
                    time.sleep(1)

                # Check for lazy loading
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)

            elif platform == 'tiki':
                # Tiki specific scrolling
                for i in range(8):
                    scroll_height = i * 500
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_height});")
                    time.sleep(0.8)

                # Tiki lazy loading
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

            # Return to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

        except Exception as e:
            self.log(f"Scroll warning: {e}")

    def extract_lazada_price(self, product_element):
        """Extract price from Lazada product"""
        try:
            # Scroll element into view
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", product_element)
                time.sleep(0.3)
            except:
                pass

            # Lazada price selectors
            price_selectors = [
                ".currency-value",
                ".price-current .currency-value",
                ".price .currency",
                "[data-qa-locator='product-price'] .currency-value",
                ".item-price .currency-value"
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = elem.text.strip()
                        if price_text:
                            # Extract numbers
                            numbers = re.findall(r'[\d,\.]+', price_text)
                            for num_str in numbers:
                                clean_num = re.sub(r'[,\.]', '', num_str)
                                if clean_num.isdigit() and len(clean_num) >= 4:
                                    price = int(clean_num)
                                    if 1000 <= price <= 100000000:
                                        return price
                except:
                    continue

            return 0

        except Exception as e:
            self.log(f"Lazada price extraction error: {e}")
            return 0

    def extract_tiki_price(self, product_element):
        """Extract price from Tiki product"""
        try:
            # Scroll element into view
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", product_element)
                time.sleep(0.3)
            except:
                pass

            # Tiki price selectors
            price_selectors = [
                ".price-current",
                ".price",
                "[data-qa='price-current']",
                ".product-price .price-current",
                ".price-discount__price",
                ".final-price"
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = elem.text.strip()
                        if price_text:
                            # Clean Tiki price (remove ₫ and spaces)
                            clean_text = price_text.replace('₫', '').replace('.', '').replace(',', '').replace(' ', '')
                            if clean_text.isdigit() and len(clean_text) >= 4:
                                price = int(clean_text)
                                if 1000 <= price <= 100000000:
                                    return price
                except:
                    continue

            return 0

        except Exception as e:
            self.log(f"Tiki price extraction error: {e}")
            return 0

    def extract_lazada_product(self, product_element, index):
        """Extract Lazada product info"""
        try:
            product = {'platform': 'lazada'}

            # URL
            try:
                link = product_element.find_element(By.CSS_SELECTOR, "a[href]")
                product['url'] = link.get_attribute("href")

                # ID from URL
                id_match = re.search(r'i(\d+)', product['url'])
                product['id'] = id_match.group(1) if id_match else f"lazada_{index}"
            except:
                return None

            # Name
            try:
                name_selectors = ["[title]", "a[title]", "h3", ".title"]
                for selector in name_selectors:
                    try:
                        elem = product_element.find_element(By.CSS_SELECTOR, selector)
                        title = elem.get_attribute("title") or elem.text
                        if title and title.strip() and len(title.strip()) > 5:
                            product['name'] = title.strip()
                            break
                    except:
                        continue
                else:
                    product['name'] = f"Lazada Product {index}"
            except:
                product['name'] = f"Lazada Product {index}"

            # Price
            product['price'] = self.extract_lazada_price(product_element)

            # Basic info
            product['category'] = 'Electronics'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Lazada product extraction error: {e}")
            return None

    def extract_tiki_product(self, product_element, index):
        """Extract Tiki product info"""
        try:
            product = {'platform': 'tiki'}

            # URL
            try:
                link = product_element.find_element(By.CSS_SELECTOR, "a[href]")
                product['url'] = link.get_attribute("href")

                # ID from URL
                id_match = re.search(r'p(\d+)', product['url'])
                product['id'] = id_match.group(1) if id_match else f"tiki_{index}"
            except:
                return None

            # Name
            try:
                name_selectors = ["[data-qa='product-name']", ".name", "h3", "[title]"]
                for selector in name_selectors:
                    try:
                        elem = product_element.find_element(By.CSS_SELECTOR, selector)
                        title = elem.get_attribute("title") or elem.text
                        if title and title.strip() and len(title.strip()) > 5:
                            product['name'] = title.strip()
                            break
                    except:
                        continue
                else:
                    product['name'] = f"Tiki Product {index}"
            except:
                product['name'] = f"Tiki Product {index}"

            # Price
            product['price'] = self.extract_tiki_price(product_element)

            # Basic info
            product['category'] = 'Electronics'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Tiki product extraction error: {e}")
            return None

    def crawl_lazada_page(self, url, page_num):
        """Crawl one Lazada page"""
        self.log(f"Crawling Lazada page {page_num}: {url}")

        try:
            self.driver.get(url)
            self.wait_and_scroll('lazada')

            # Find products
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")
            self.log(f"Found {len(product_elements)} Lazada products")

            products = []
            for i, element in enumerate(product_elements, 1):
                product = self.extract_lazada_product(element, i)
                if product:
                    products.append(product)
                    if product['price'] > 0:
                        self.results['products_with_prices'] += 1

                # Small delay
                if i % 10 == 0:
                    time.sleep(0.5)

            self.results['lazada_products'] += len(products)
            self.log(f"Lazada page {page_num}: {len(products)} products extracted")
            return products

        except Exception as e:
            self.log(f"Lazada page error: {e}")
            return []

    def crawl_tiki_page(self, url, page_num):
        """Crawl one Tiki page"""
        self.log(f"Crawling Tiki page {page_num}: {url}")

        try:
            self.driver.get(url)
            self.wait_and_scroll('tiki')

            # Find products - Tiki has different selectors
            product_selectors = [
                "[data-qa='product-item']",
                ".product-item",
                "[data-view-id='product_list_item']",
                ".ProductItem"
            ]

            product_elements = []
            for selector in product_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        product_elements = elements
                        break
                except:
                    continue

            self.log(f"Found {len(product_elements)} Tiki products")

            products = []
            for i, element in enumerate(product_elements, 1):
                product = self.extract_tiki_product(element, i)
                if product:
                    products.append(product)
                    if product['price'] > 0:
                        self.results['products_with_prices'] += 1

                # Small delay
                if i % 10 == 0:
                    time.sleep(0.5)

            self.results['tiki_products'] += len(products)
            self.log(f"Tiki page {page_num}: {len(products)} products extracted")
            return products

        except Exception as e:
            self.log(f"Tiki page error: {e}")
            return []

    def crawl_category(self, platform, category_name, base_url, max_pages=5):
        """Crawl a category from specific platform"""
        self.log(f"Starting {platform} {category_name} crawl: {max_pages} pages")

        all_products = []

        for page in range(1, max_pages + 1):
            try:
                # Build page URL
                if platform == 'lazada':
                    if '?' in base_url:
                        page_url = f"{base_url}&page={page}"
                    else:
                        page_url = f"{base_url}?page={page}"

                    page_products = self.crawl_lazada_page(page_url, page)

                elif platform == 'tiki':
                    if '?' in base_url:
                        page_url = f"{base_url}&p={page}"
                    else:
                        page_url = f"{base_url}?p={page}"

                    page_products = self.crawl_tiki_page(page_url, page)

                else:
                    continue

                # Add category info
                for product in page_products:
                    product['category'] = category_name

                all_products.extend(page_products)
                self.results['pages_crawled'] += 1

                # Delay between pages
                if page < max_pages:
                    delay = random.uniform(2, 5)
                    self.log(f"Waiting {delay:.1f}s before next page...")
                    time.sleep(delay)

            except Exception as e:
                self.log(f"Page {page} error: {e}")
                continue

        self.results['categories_completed'] += 1
        self.results['total_products'] += len(all_products)

        self.log(f"{platform} {category_name} completed: {len(all_products)} products")
        return all_products

    def crawl_all_electronics(self, max_pages_per_category=3, platforms=None):
        """Crawl all electronics from both platforms"""
        if platforms is None:
            platforms = ['lazada', 'tiki']

        self.log(f"Starting full electronics crawl: {platforms}, {max_pages_per_category} pages per category")

        all_products = []

        for platform in platforms:
            if platform not in self.categories:
                continue

            self.log(f"=== Starting {platform.upper()} crawl ===")

            for category_name, category_url in self.categories[platform].items():
                try:
                    category_products = self.crawl_category(
                        platform, category_name, category_url, max_pages_per_category
                    )
                    all_products.extend(category_products)

                    # Delay between categories
                    if len(self.categories[platform]) > 1:
                        delay = random.uniform(5, 10)
                        self.log(f"Inter-category delay: {delay:.1f}s")
                        time.sleep(delay)

                except Exception as e:
                    self.log(f"Category {category_name} error: {e}")
                    continue

            self.log(f"=== {platform.upper()} crawl completed ===")

        return all_products

    def save_results(self, products):
        """Save crawl results"""
        if not products:
            self.log("No products to save")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Statistics
        duration = datetime.now() - self.results['start_time']
        products_with_prices = [p for p in products if p.get('price', 0) > 0]

        platform_stats = {}
        for platform in ['lazada', 'tiki']:
            platform_products = [p for p in products if p.get('platform') == platform]
            platform_with_prices = [p for p in platform_products if p.get('price', 0) > 0]
            platform_stats[platform] = {
                'total': len(platform_products),
                'with_prices': len(platform_with_prices),
                'price_rate': (len(platform_with_prices) / len(platform_products) * 100) if platform_products else 0
            }

        # Results data
        results_data = {
            'crawl_info': {
                'timestamp': timestamp,
                'duration': str(duration),
                'total_products': len(products),
                'products_with_prices': len(products_with_prices),
                'overall_price_rate': (len(products_with_prices) / len(products)) * 100 if products else 0,
                'pages_crawled': self.results['pages_crawled'],
                'categories_completed': self.results['categories_completed'],
                'platform_stats': platform_stats
            },
            'products': products
        }

        # Save JSON
        json_file = self.output_dir / f"multi_platform_crawl_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2, ensure_ascii=False)

        # Save CSV
        csv_file = self.output_dir / f"multi_platform_crawl_{timestamp}.csv"
        if products:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=products[0].keys())
                writer.writeheader()
                writer.writerows(products)

        self.log(f"Results saved: {json_file}")

        # Print summary
        self.log("=== CRAWL SUMMARY ===")
        self.log(f"Total products: {len(products)}")
        self.log(f"Products with prices: {len(products_with_prices)} ({results_data['crawl_info']['overall_price_rate']:.1f}%)")
        self.log(f"Pages crawled: {self.results['pages_crawled']}")
        self.log(f"Categories completed: {self.results['categories_completed']}")
        self.log(f"Duration: {duration}")

        for platform, stats in platform_stats.items():
            self.log(f"{platform.capitalize()}: {stats['total']} products, {stats['with_prices']} with prices ({stats['price_rate']:.1f}%)")

        # Show price samples
        if products_with_prices:
            self.log("=== PRICE SAMPLES ===")
            for i, product in enumerate(products_with_prices[:5]):
                self.log(f"{i+1}. [{product['platform']}] {product['name'][:50]}... - {product['price']:,} VND")

    def run(self, max_pages_per_category=3, platforms=None):
        """Run the full crawl"""
        try:
            self.setup_driver()
            products = self.crawl_all_electronics(max_pages_per_category, platforms)
            self.save_results(products)
            return len(products)

        except Exception as e:
            self.log(f"Crawler failed: {e}")
            return 0

        finally:
            if self.driver:
                self.driver.quit()
                self.log("Browser closed")

def main():
    """Main function"""
    print("Multi-Platform Electronics Crawler")
    print("Lazada + Tiki Full Electronics Collection")
    print("=" * 50)

    try:
        # Configuration
        print("Choose crawl scope:")
        print("1. Quick test (1 page per category)")
        print("2. Medium crawl (3 pages per category)")
        print("3. Full crawl (5 pages per category)")
        print("4. Custom")

        choice = input("Choice (1-4, default 2): ").strip()
        if choice == '1':
            max_pages = 1
        elif choice == '3':
            max_pages = 5
        elif choice == '4':
            max_pages = int(input("Pages per category: "))
        else:
            max_pages = 3

        # Platform selection
        platform_choice = input("Platforms (1=Lazada, 2=Tiki, 3=Both, default 3): ").strip()
        if platform_choice == '1':
            platforms = ['lazada']
        elif platform_choice == '2':
            platforms = ['tiki']
        else:
            platforms = ['lazada', 'tiki']

        print(f"\nStarting crawl: {platforms}, {max_pages} pages per category")
        print("This will crawl ALL electronics categories...")

        # Run crawler
        crawler = MultiPlatformCrawler(headless=True, verbose=True)
        total_products = crawler.run(max_pages, platforms)

        if total_products > 0:
            print(f"\nSUCCESS: Collected {total_products} products!")
        else:
            print("\nFAILED: No products collected")

    except KeyboardInterrupt:
        print("\nCancelled by user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()