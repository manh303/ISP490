#!/usr/bin/env python3
"""
Fixed Tiki Crawler - Unicode-safe extraction
"""

import time
import json
import csv
import os
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class FixedTikiCrawler:
    def __init__(self):
        self.driver = None
        self.results = {
            'total_products': 0,
            'products_with_prices': 0,
            'pages_crawled': 0,
            'start_time': datetime.now(),
            'tiki_products': 0
        }

    def setup_driver(self):
        """Setup Chrome driver with anti-detection"""
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        self.driver = webdriver.Chrome(options=options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    def log(self, message):
        """Unicode-safe logging"""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            # Convert to ASCII-safe format
            safe_message = message.encode('ascii', 'ignore').decode('ascii')
            print(f"[{timestamp}] {safe_message}")
        except:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [LOG MESSAGE]")

    def wait_and_scroll(self):
        """Wait and scroll to load products"""
        try:
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, 1000);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, 2000);")
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, 3000);")
            time.sleep(1)
        except:
            pass

    def safe_text_extract(self, element, attribute=None):
        """Safely extract text with Unicode handling"""
        try:
            if attribute:
                text = element.get_attribute(attribute)
            else:
                text = element.text

            if text:
                # Clean and normalize text
                text = text.strip()
                # Convert problematic characters
                text = text.replace('\u20ab', ' VND')  # Vietnamese dong symbol
                text = text.replace('\u1ed1', 'o')     # Vietnamese o with circumflex
                text = text.replace('\u0110', 'D')     # Vietnamese D with stroke
                return text
            return ""
        except:
            return ""

    def extract_tiki_price(self, product_element):
        """Extract Tiki price with enhanced selectors"""
        try:
            price_selectors = [
                ".price-current",
                "[data-qa='product-price']",
                ".price",
                ".current-price",
                ".product-price .price-current",
                ".price-wrap .price-current",
                ".style__CurrentPrice",
                ".price-discount__price",
                ".final-price"
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = self.safe_text_extract(elem)
                        if price_text:
                            # Clean price text
                            clean_text = re.sub(r'[^\d]', '', price_text)
                            if clean_text.isdigit() and len(clean_text) >= 4:
                                price = int(clean_text)
                                if 1000 <= price <= 100000000:
                                    return price
                except:
                    continue

            return 0

        except Exception as e:
            self.log(f"Tiki price extraction error: {str(e)}")
            return 0

    def extract_tiki_product(self, product_element, index):
        """Extract Tiki product info with Unicode safety"""
        try:
            product = {'platform': 'tiki'}

            # URL - Tiki products are <a> tags themselves
            try:
                url = product_element.get_attribute("href")
                if url:
                    # Handle relative URLs
                    if url.startswith('//'):
                        url = 'https:' + url
                    elif url.startswith('/'):
                        url = 'https://tiki.vn' + url

                    product['url'] = url

                    # Extract ID from URL
                    id_match = re.search(r'p(\d+)', url)
                    if id_match:
                        product['id'] = id_match.group(1)
                    else:
                        # Try spid pattern
                        spid_match = re.search(r'spid=(\d+)', url)
                        product['id'] = spid_match.group(1) if spid_match else f"tiki_{index}"
                else:
                    return None
            except:
                return None

            # Name - try multiple strategies
            try:
                # Strategy 1: Look for nested elements with product name
                name_selectors = [
                    ".name",
                    ".product-name",
                    "[data-qa='product-name']",
                    ".title",
                    "img[alt]"
                ]

                product_name = ""
                for selector in name_selectors:
                    try:
                        name_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                        if selector == "img[alt]":
                            product_name = self.safe_text_extract(name_elem, 'alt')
                        else:
                            product_name = self.safe_text_extract(name_elem)

                        if product_name and len(product_name) > 5:
                            break
                    except:
                        continue

                # Strategy 2: Extract from data attributes
                if not product_name:
                    try:
                        data_content = product_element.get_attribute('data-view-content')
                        if data_content:
                            # Try to parse JSON content
                            import json
                            data = json.loads(data_content.replace('&quot;', '"'))
                            if 'name' in data:
                                product_name = data['name']
                    except:
                        pass

                # Strategy 3: Use any text content as fallback
                if not product_name:
                    product_name = self.safe_text_extract(product_element)
                    if len(product_name) > 100:
                        product_name = product_name[:100] + "..."

                product['name'] = product_name if product_name else f"Tiki Product {index}"

            except:
                product['name'] = f"Tiki Product {index}"

            # Price
            product['price'] = self.extract_tiki_price(product_element)

            # Basic info
            product['category'] = 'Electronics'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Tiki product extraction error: {str(e)}")
            return None

    def crawl_tiki_page(self, url, page_num):
        """Crawl one Tiki page with fixed selectors"""
        self.log(f"Crawling Tiki page {page_num}: {url}")

        try:
            self.driver.get(url)
            self.wait_and_scroll()

            # Use the working selector from our debug
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, ".product-item")
            self.log(f"Found {len(product_elements)} Tiki products")

            products = []
            for i, element in enumerate(product_elements, 1):
                try:
                    product = self.extract_tiki_product(element, i)
                    if product and product.get('url'):
                        products.append(product)
                        if product['price'] > 0:
                            self.results['products_with_prices'] += 1

                        # Log progress every 10 products
                        if i % 10 == 0:
                            self.log(f"Processed {i}/{len(product_elements)} products")
                except Exception as e:
                    self.log(f"Error processing product {i}: {str(e)}")
                    continue

                # Small delay to avoid detection
                if i % 10 == 0:
                    time.sleep(0.5)

            self.results['tiki_products'] += len(products)
            self.log(f"Tiki page {page_num}: {len(products)} products successfully extracted")
            return products

        except Exception as e:
            self.log(f"Tiki page error: {str(e)}")
            return []

    def run_test(self):
        """Run test crawl of Tiki electronics"""
        self.log("Starting Fixed Tiki Crawler Test")

        try:
            self.setup_driver()

            # Test Tiki smartphones
            url = "https://tiki.vn/dien-thoai-smartphone/c1795"
            products = self.crawl_tiki_page(url, 1)

            self.results['total_products'] = len(products)
            self.results['pages_crawled'] = 1

            # Save results
            if products:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                # JSON output
                output_file = f"C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/fixed_tiki_test_{timestamp}.json"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

                # Convert datetime to string for JSON serialization
                results_copy = self.results.copy()
                results_copy['start_time'] = self.results['start_time'].isoformat()

                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'results': results_copy,
                        'products': products,
                        'timestamp': timestamp
                    }, f, ensure_ascii=False, indent=2)

                self.log(f"Results saved to: {output_file}")

                # Print sample
                self.log("=== SAMPLE PRODUCTS ===")
                for i, product in enumerate(products[:3], 1):
                    self.log(f"{i}. Name: {product['name'][:50]}...")
                    self.log(f"   URL: {product['url']}")
                    self.log(f"   Price: {product['price']}")
                    self.log("")

            # Final statistics
            self.log("=== FINAL RESULTS ===")
            self.log(f"Total products extracted: {self.results['total_products']}")
            self.log(f"Products with prices: {self.results['products_with_prices']}")
            self.log(f"Success rate: {len(products)}/50 expected products")

        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    crawler = FixedTikiCrawler()
    crawler.run_test()