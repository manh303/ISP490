#!/usr/bin/env python3
"""
Smartphones Focused Crawler
Focus on smartphones from both Lazada and Tiki for maximum data collection
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
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class SmartphonesFocusedCrawler:
    """Focused crawler for smartphones from Lazada and Tiki"""

    def __init__(self, headless: bool = True, verbose: bool = True):
        self.headless = headless
        self.verbose = verbose
        self.output_dir = Path("../data/smartphones_data")
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
            'pages_crawled': 0
        }

        # Smartphone URLs
        self.smartphone_urls = {
            'lazada': 'https://www.lazada.vn/dien-thoai-di-dong/',
            'tiki': 'https://tiki.vn/dien-thoai-smartphone/c1795'
        }

    def log(self, message):
        """Simple logging"""
        if self.verbose:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {message}")

    def setup_driver(self):
        """Setup Chrome driver"""
        self.log("Setting up Chrome driver...")

        options = Options()
        if self.headless:
            options.add_argument("--headless=new")

        # Basic options
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")

        # User agent for mobile compatibility
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Performance
        options.add_argument("--disable-extensions")
        options.add_argument("--no-first-run")
        options.add_argument("--disable-default-apps")

        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(30)
        self.wait = WebDriverWait(self.driver, 15)

        self.log("Driver ready")

    def smart_scroll_and_wait(self, platform):
        """Smart scrolling for different platforms"""
        try:
            # Initial wait
            time.sleep(3)

            if platform == 'lazada':
                # Lazada: Multiple scroll points to trigger lazy loading
                self.log("Performing Lazada-specific scrolling...")

                # Scroll sequence for Lazada
                scroll_points = [0, 500, 1000, 1500, 2000, 2500]
                for point in scroll_points:
                    self.driver.execute_script(f"window.scrollTo(0, {point});")
                    time.sleep(1.5)

                # Full scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)

                # Additional wait for AJAX
                time.sleep(2)

            elif platform == 'tiki':
                # Tiki: Different scrolling pattern
                self.log("Performing Tiki-specific scrolling...")

                # Tiki scroll sequence
                for i in range(8):
                    scroll_height = i * 600
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_height});")
                    time.sleep(1)

                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)

            # Return to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

        except Exception as e:
            self.log(f"Scroll warning: {e}")

    def extract_lazada_price_enhanced(self, product_element):
        """Enhanced Lazada price extraction"""
        try:
            # First, try to trigger price display
            try:
                # Scroll element into view and hover
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", product_element)
                time.sleep(0.5)

                # Hover to trigger price display
                ActionChains(self.driver).move_to_element(product_element).perform()
                time.sleep(0.3)
            except:
                pass

            # Enhanced price selectors for Lazada
            price_selectors = [
                ".currency-value",
                ".price-current .currency-value",
                ".price .currency",
                "[data-qa-locator='product-price'] .currency-value",
                ".item-price .currency-value",
                ".selling-price",
                ".final-price",
                "[data-price]"
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        # Try text content
                        price_text = elem.text.strip()
                        if price_text:
                            numbers = re.findall(r'[\d,\.]+', price_text)
                            for num_str in numbers:
                                clean_num = re.sub(r'[,\.]', '', num_str)
                                if clean_num.isdigit() and len(clean_num) >= 4:
                                    price = int(clean_num)
                                    if 10000 <= price <= 100000000:  # Reasonable smartphone price
                                        return price

                        # Try data attributes
                        data_price = elem.get_attribute('data-price')
                        if data_price and data_price.isdigit():
                            price = int(data_price)
                            if 10000 <= price <= 100000000:
                                return price
                except:
                    continue

            # JavaScript method for dynamic content
            try:
                js_price = self.driver.execute_script("""
                    var element = arguments[0];

                    // Try multiple selectors
                    var selectors = ['.currency-value', '.price', '[data-price]'];
                    for (var selector of selectors) {
                        var priceEl = element.querySelector(selector);
                        if (priceEl) {
                            var text = priceEl.textContent || priceEl.innerText;
                            if (text) {
                                var numbers = text.match(/\\d+/g);
                                if (numbers) {
                                    var price = numbers.join('');
                                    if (price.length >= 4) return parseInt(price);
                                }
                            }

                            var dataPrice = priceEl.getAttribute('data-price');
                            if (dataPrice) return parseInt(dataPrice);
                        }
                    }
                    return 0;
                """, product_element)

                if js_price and 10000 <= js_price <= 100000000:
                    return js_price

            except:
                pass

            return 0

        except Exception as e:
            self.log(f"Lazada price extraction error: {e}")
            return 0

    def extract_tiki_price_enhanced(self, product_element):
        """Enhanced Tiki price extraction"""
        try:
            # Trigger price display
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", product_element)
                time.sleep(0.5)
                ActionChains(self.driver).move_to_element(product_element).perform()
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
                ".final-price",
                ".price-regular",
                "[data-price]"
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = elem.text.strip()
                        if price_text:
                            # Clean Tiki price (₫ symbol and formatting)
                            clean_text = price_text.replace('₫', '').replace('.', '').replace(',', '').replace(' ', '')
                            if clean_text.isdigit() and len(clean_text) >= 4:
                                price = int(clean_text)
                                if 10000 <= price <= 100000000:
                                    return price

                        # Check data attributes
                        data_price = elem.get_attribute('data-price')
                        if data_price and data_price.replace('.', '').replace(',', '').isdigit():
                            price = int(data_price.replace('.', '').replace(',', ''))
                            if 10000 <= price <= 100000000:
                                return price
                except:
                    continue

            # Tiki JavaScript extraction
            try:
                js_price = self.driver.execute_script("""
                    var element = arguments[0];
                    var selectors = ['.price-current', '.price', '[data-price]'];

                    for (var selector of selectors) {
                        var priceEl = element.querySelector(selector);
                        if (priceEl) {
                            var text = priceEl.textContent || priceEl.innerText;
                            if (text) {
                                var cleanText = text.replace(/[₫\\.\\,\\s]/g, '');
                                if (cleanText && /^\\d+$/.test(cleanText)) {
                                    return parseInt(cleanText);
                                }
                            }
                        }
                    }
                    return 0;
                """, product_element)

                if js_price and 10000 <= js_price <= 100000000:
                    return js_price

            except:
                pass

            return 0

        except Exception as e:
            self.log(f"Tiki price extraction error: {e}")
            return 0

    def extract_lazada_smartphone(self, product_element, index):
        """Extract Lazada smartphone"""
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

            # Name with enhanced selectors
            try:
                name_selectors = [
                    "[data-qa-locator='product-item'] [title]",
                    "[title]",
                    "a[title]",
                    "h3",
                    ".title",
                    ".product-title"
                ]

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
                    product['name'] = f"Lazada Smartphone {index}"
            except:
                product['name'] = f"Lazada Smartphone {index}"

            # Enhanced price extraction
            product['price'] = self.extract_lazada_price_enhanced(product_element)

            # Additional info
            product['category'] = 'smartphones'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Lazada product extraction error: {e}")
            return None

    def extract_tiki_smartphone(self, product_element, index):
        """Extract Tiki smartphone"""
        try:
            product = {'platform': 'tiki'}

            # URL - Tiki specific selectors
            try:
                link_selectors = ["a[href]", "[data-qa='product-item'] a", ".product-item a"]
                for selector in link_selectors:
                    try:
                        link = product_element.find_element(By.CSS_SELECTOR, selector)
                        product['url'] = link.get_attribute("href")
                        if product['url']:
                            break
                    except:
                        continue

                if 'url' not in product:
                    return None

                # ID from URL
                id_match = re.search(r'p(\d+)', product['url'])
                product['id'] = id_match.group(1) if id_match else f"tiki_{index}"
            except:
                return None

            # Name with Tiki selectors
            try:
                name_selectors = [
                    "[data-qa='product-name']",
                    ".name",
                    "h3",
                    "[title]",
                    ".product-name",
                    ".item-title"
                ]

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
                    product['name'] = f"Tiki Smartphone {index}"
            except:
                product['name'] = f"Tiki Smartphone {index}"

            # Enhanced price extraction
            product['price'] = self.extract_tiki_price_enhanced(product_element)

            # Additional info
            product['category'] = 'smartphones'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Tiki product extraction error: {e}")
            return None

    def crawl_lazada_smartphones(self, max_pages=5):
        """Crawl Lazada smartphones"""
        self.log(f"Starting Lazada smartphones crawl: {max_pages} pages")

        all_products = []

        for page in range(1, max_pages + 1):
            try:
                # Build URL
                if '?' in self.smartphone_urls['lazada']:
                    page_url = f"{self.smartphone_urls['lazada']}&page={page}"
                else:
                    page_url = f"{self.smartphone_urls['lazada']}?page={page}"

                self.log(f"Crawling Lazada page {page}: {page_url}")

                # Load page
                self.driver.get(page_url)
                self.smart_scroll_and_wait('lazada')

                # Find products
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")
                self.log(f"Found {len(product_elements)} Lazada products on page {page}")

                # Extract products
                page_products = []
                for i, element in enumerate(product_elements, 1):
                    product = self.extract_lazada_smartphone(element, i)
                    if product:
                        page_products.append(product)
                        if product['price'] > 0:
                            self.results['products_with_prices'] += 1

                    # Small delay every 10 products
                    if i % 10 == 0:
                        time.sleep(0.8)

                all_products.extend(page_products)
                self.results['pages_crawled'] += 1
                self.results['lazada_products'] += len(page_products)

                prices_found = len([p for p in page_products if p['price'] > 0])
                self.log(f"Lazada page {page}: {len(page_products)} products, {prices_found} with prices")

                # Delay between pages
                if page < max_pages:
                    delay = random.uniform(3, 6)
                    self.log(f"Page delay: {delay:.1f}s")
                    time.sleep(delay)

            except Exception as e:
                self.log(f"Lazada page {page} error: {e}")
                continue

        return all_products

    def crawl_tiki_smartphones(self, max_pages=5):
        """Crawl Tiki smartphones"""
        self.log(f"Starting Tiki smartphones crawl: {max_pages} pages")

        all_products = []

        for page in range(1, max_pages + 1):
            try:
                # Build URL
                if '?' in self.smartphone_urls['tiki']:
                    page_url = f"{self.smartphone_urls['tiki']}&p={page}"
                else:
                    page_url = f"{self.smartphone_urls['tiki']}?p={page}"

                self.log(f"Crawling Tiki page {page}: {page_url}")

                # Load page
                self.driver.get(page_url)
                self.smart_scroll_and_wait('tiki')

                # Find products with multiple selectors
                product_selectors = [
                    "[data-qa='product-item']",
                    ".product-item",
                    "[data-view-id='product_list_item']",
                    ".ProductItem",
                    ".item"
                ]

                product_elements = []
                for selector in product_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            product_elements = elements
                            self.log(f"Found {len(elements)} Tiki products using {selector}")
                            break
                    except:
                        continue

                if not product_elements:
                    self.log(f"No Tiki products found on page {page}")
                    continue

                # Extract products
                page_products = []
                for i, element in enumerate(product_elements, 1):
                    product = self.extract_tiki_smartphone(element, i)
                    if product:
                        page_products.append(product)
                        if product['price'] > 0:
                            self.results['products_with_prices'] += 1

                    # Small delay
                    if i % 10 == 0:
                        time.sleep(0.8)

                all_products.extend(page_products)
                self.results['pages_crawled'] += 1
                self.results['tiki_products'] += len(page_products)

                prices_found = len([p for p in page_products if p['price'] > 0])
                self.log(f"Tiki page {page}: {len(page_products)} products, {prices_found} with prices")

                # Delay between pages
                if page < max_pages:
                    delay = random.uniform(3, 6)
                    self.log(f"Page delay: {delay:.1f}s")
                    time.sleep(delay)

            except Exception as e:
                self.log(f"Tiki page {page} error: {e}")
                continue

        return all_products

    def save_results(self, products):
        """Save results"""
        if not products:
            self.log("No products to save")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        duration = datetime.now() - self.results['start_time']

        # Statistics
        products_with_prices = [p for p in products if p.get('price', 0) > 0]
        lazada_products = [p for p in products if p.get('platform') == 'lazada']
        tiki_products = [p for p in products if p.get('platform') == 'tiki']

        # Results data
        results_data = {
            'crawl_info': {
                'timestamp': timestamp,
                'duration': str(duration),
                'total_products': len(products),
                'products_with_prices': len(products_with_prices),
                'price_success_rate': (len(products_with_prices) / len(products)) * 100 if products else 0,
                'pages_crawled': self.results['pages_crawled'],
                'platform_breakdown': {
                    'lazada': {
                        'total': len(lazada_products),
                        'with_prices': len([p for p in lazada_products if p['price'] > 0])
                    },
                    'tiki': {
                        'total': len(tiki_products),
                        'with_prices': len([p for p in tiki_products if p['price'] > 0])
                    }
                }
            },
            'products': products
        }

        # Save JSON
        json_file = self.output_dir / f"smartphones_crawl_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2, ensure_ascii=False)

        # Save CSV
        csv_file = self.output_dir / f"smartphones_crawl_{timestamp}.csv"
        if products:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=products[0].keys())
                writer.writeheader()
                writer.writerows(products)

        self.log(f"Results saved: {json_file}")

        # Summary
        self.log("=== SMARTPHONES CRAWL SUMMARY ===")
        self.log(f"Total smartphones: {len(products)}")
        self.log(f"With prices: {len(products_with_prices)} ({results_data['crawl_info']['price_success_rate']:.1f}%)")
        self.log(f"Lazada: {len(lazada_products)} smartphones")
        self.log(f"Tiki: {len(tiki_products)} smartphones")
        self.log(f"Pages crawled: {self.results['pages_crawled']}")
        self.log(f"Duration: {duration}")

        # Price samples
        if products_with_prices:
            self.log("=== PRICE SAMPLES ===")
            for i, product in enumerate(products_with_prices[:5]):
                self.log(f"{i+1}. [{product['platform']}] {product['name'][:50]}... - {product['price']:,} VND")

    def run(self, max_pages_per_platform=5):
        """Run focused smartphones crawl"""
        try:
            self.setup_driver()

            # Crawl both platforms
            self.log("=== STARTING SMARTPHONES FOCUSED CRAWL ===")

            all_products = []

            # Lazada smartphones
            lazada_products = self.crawl_lazada_smartphones(max_pages_per_platform)
            all_products.extend(lazada_products)

            # Delay between platforms
            if lazada_products:
                delay = random.uniform(10, 15)
                self.log(f"Inter-platform delay: {delay:.1f}s")
                time.sleep(delay)

            # Tiki smartphones
            tiki_products = self.crawl_tiki_smartphones(max_pages_per_platform)
            all_products.extend(tiki_products)

            # Save results
            self.results['total_products'] = len(all_products)
            self.save_results(all_products)

            return len(all_products)

        except Exception as e:
            self.log(f"Crawler failed: {e}")
            return 0

        finally:
            if self.driver:
                self.driver.quit()
                self.log("Browser closed")

def main():
    """Main function"""
    print("Smartphones Focused Crawler")
    print("Lazada + Tiki Smartphones Collection")
    print("=" * 50)

    try:
        pages = input("Pages per platform (default 3): ").strip()
        max_pages = int(pages) if pages.isdigit() else 3

        print(f"\nStarting smartphones crawl: {max_pages} pages per platform")
        print("Focus: Maximum smartphone data collection with price extraction")

        # Run crawler
        crawler = SmartphonesFocusedCrawler(headless=True, verbose=True)
        total_products = crawler.run(max_pages)

        if total_products > 0:
            print(f"\nSUCCESS: Collected {total_products} smartphones!")
        else:
            print("\nFAILED: No smartphones collected")

    except KeyboardInterrupt:
        print("\nCancelled by user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()