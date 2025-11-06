#!/usr/bin/env python3
"""
Updated Multi-Platform Crawler - Fixed Tiki extraction + Lazada
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

class UpdatedMultiPlatformCrawler:
    def __init__(self):
        self.driver = None
        self.results = {
            'total_products': 0,
            'products_with_prices': 0,
            'pages_crawled': 0,
            'start_time': datetime.now(),
            'lazada_products': 0,
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
            safe_message = message.encode('ascii', 'ignore').decode('ascii')
            print(f"[{timestamp}] {safe_message}")
        except:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [LOG MESSAGE]")

    def wait_and_scroll(self, platform):
        """Platform-specific wait and scroll"""
        try:
            if platform == 'lazada':
                time.sleep(3)
                self.driver.execute_script("window.scrollTo(0, 2000);")
                time.sleep(2)
                self.driver.execute_script("window.scrollTo(0, 4000);")
                time.sleep(2)
            else:  # tiki
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
                text = text.strip()
                # Convert problematic characters
                text = text.replace('\u20ab', ' VND')
                text = text.replace('\u1ed1', 'o')
                text = text.replace('\u0110', 'D')
                return text
            return ""
        except:
            return ""

    def extract_lazada_price(self, product_element):
        """Extract Lazada price with enhanced methods"""
        try:
            price_selectors = [
                ".current-price",
                ".price-current",
                ".price",
                ".discounted-price",
                ".product-price .current-price",
                ".product-price .price-current",
                ".money-symbol + span",
                ".price-wrap .current-price"
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = self.safe_text_extract(elem)
                        if price_text:
                            clean_text = re.sub(r'[^\d]', '', price_text)
                            if clean_text.isdigit() and len(clean_text) >= 4:
                                price = int(clean_text)
                                if 1000 <= price <= 100000000:
                                    return price
                except:
                    continue

            return 0

        except Exception as e:
            self.log(f"Lazada price extraction error: {str(e)}")
            return 0

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
                        title = self.safe_text_extract(elem, "title") if elem.get_attribute("title") else self.safe_text_extract(elem)
                        if title and len(title) > 5:
                            product['name'] = title
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
            self.log(f"Lazada product extraction error: {str(e)}")
            return None

    def extract_tiki_product(self, product_element, index):
        """Extract Tiki product info with FIXED selectors"""
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

            # Name - Enhanced extraction
            try:
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
                            import json
                            data = json.loads(data_content.replace('&quot;', '"'))
                            if 'name' in data:
                                product_name = self.safe_text_extract(None)
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
            self.log(f"Lazada page error: {str(e)}")
            return []

    def crawl_tiki_page(self, url, page_num):
        """Crawl one Tiki page with FIXED selectors"""
        self.log(f"Crawling Tiki page {page_num}: {url}")

        try:
            self.driver.get(url)
            self.wait_and_scroll('tiki')

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

    def crawl_category(self, platform, category_name, base_url, max_pages=2):
        """Crawl a category from specific platform"""
        self.log(f"Starting {platform} {category_name} crawl: {max_pages} pages")

        all_products = []

        for page_num in range(1, max_pages + 1):
            try:
                if platform == 'lazada':
                    if page_num == 1:
                        url = base_url
                    else:
                        url = f"{base_url}?page={page_num}"
                    products = self.crawl_lazada_page(url, page_num)
                else:  # tiki
                    if page_num == 1:
                        url = base_url
                    else:
                        url = f"{base_url}?page={page_num}"
                    products = self.crawl_tiki_page(url, page_num)

                if products:
                    all_products.extend(products)
                    self.results['pages_crawled'] += 1
                else:
                    self.log(f"No products found on page {page_num}, stopping")
                    break

                # Delay between pages
                if page_num < max_pages:
                    time.sleep(3)

            except Exception as e:
                self.log(f"Error on page {page_num}: {str(e)}")
                break

        return all_products

    def run_full_crawl(self):
        """Run comprehensive multi-platform crawl"""
        self.log("Starting Updated Multi-Platform Electronics Crawl")

        try:
            self.setup_driver()

            # Define platforms and categories
            categories = {
                'lazada': {
                    'smartphones': 'https://www.lazada.vn/dien-thoai-di-dong/',
                    'laptops': 'https://www.lazada.vn/may-tinh-xach-tay/',
                    'tablets': 'https://www.lazada.vn/may-tinh-bang/'
                },
                'tiki': {
                    'smartphones': 'https://tiki.vn/dien-thoai-smartphone/c1795',
                    'laptops': 'https://tiki.vn/laptop/c1846',
                    'tablets': 'https://tiki.vn/may-tinh-bang/c1883'
                }
            }

            all_products = []

            # Crawl both platforms
            for platform, platform_categories in categories.items():
                self.log(f"=== Starting {platform.upper()} crawl ===")

                for category_name, base_url in platform_categories.items():
                    self.log(f"Crawling {platform} {category_name}")
                    products = self.crawl_category(platform, category_name, base_url, max_pages=2)
                    all_products.extend(products)

                    # Break between categories
                    time.sleep(2)

            self.results['total_products'] = len(all_products)

            # Save results
            if all_products:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                # JSON output
                output_file = f"C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/updated_multi_platform_{timestamp}.json"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

                # Convert datetime to string for JSON serialization
                results_copy = self.results.copy()
                results_copy['start_time'] = self.results['start_time'].isoformat()

                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'results': results_copy,
                        'products': all_products,
                        'timestamp': timestamp
                    }, f, ensure_ascii=False, indent=2)

                self.log(f"Results saved to: {output_file}")

                # Print sample results
                self.log("=== SAMPLE PRODUCTS ===")
                lazada_products = [p for p in all_products if p['platform'] == 'lazada']
                tiki_products = [p for p in all_products if p['platform'] == 'tiki']

                self.log(f"Lazada samples ({len(lazada_products)} total):")
                for i, product in enumerate(lazada_products[:2], 1):
                    self.log(f"  {i}. {product['name'][:50]}... - Price: {product['price']}")

                self.log(f"Tiki samples ({len(tiki_products)} total):")
                for i, product in enumerate(tiki_products[:2], 1):
                    self.log(f"  {i}. {product['name'][:50]}... - Price: {product['price']}")

            # Final statistics
            self.log("=== FINAL RESULTS ===")
            self.log(f"Total products: {self.results['total_products']}")
            self.log(f"Lazada products: {self.results['lazada_products']}")
            self.log(f"Tiki products: {self.results['tiki_products']}")
            self.log(f"Products with prices: {self.results['products_with_prices']}")
            self.log(f"Pages crawled: {self.results['pages_crawled']}")

        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    crawler = UpdatedMultiPlatformCrawler()
    crawler.run_full_crawl()