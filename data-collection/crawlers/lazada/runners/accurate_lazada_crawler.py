#!/usr/bin/env python3
"""
Accurate Lazada Crawler - Based on Real DOM Structure
Updated with selectors from structure.html analysis
"""

import time
import json
import os
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

class AccurateLazadaCrawler:
    def __init__(self):
        self.driver = None
        self.results = {
            'total_products': 0,
            'products_with_prices': 0,
            'pages_crawled': 0,
            'start_time': datetime.now(),
            'lazada_products': 0
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

    def safe_text_extract(self, element, attribute=None):
        """Safely extract text with Unicode handling"""
        try:
            if attribute:
                text = element.get_attribute(attribute)
            else:
                text = element.text

            if text:
                text = text.strip()
                # Clean Vietnamese characters for console compatibility
                text = text.replace('\u20ab', ' VND')
                text = text.replace('\u1ed1', 'o')
                text = text.replace('\u0110', 'D')
                return text
            return ""
        except:
            return ""

    def wait_and_scroll(self):
        """Wait and scroll to load products"""
        try:
            time.sleep(3)
            # Scroll to trigger lazy loading
            self.driver.execute_script("window.scrollTo(0, 1000);")
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, 2500);")
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, 4000);")
            time.sleep(1)
            # Scroll back up to ensure all content is loaded
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
        except:
            pass

    def extract_lazada_price(self, product_element):
        """Extract price using accurate selectors from structure.html"""
        try:
            # Based on structure.html: <div class="aBrP0"><span class="ooOxS">₫1,050,000</span></div>
            price_selectors = [
                ".aBrP0 .ooOxS",  # Primary price selector from structure
                ".aBrP0 span",    # Alternative within price container
                ".ooOxS",         # Direct price class
                ".aBrP0"          # Price container fallback
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = self.safe_text_extract(elem)
                        if price_text:
                            # Clean Vietnamese price (₫1,050,000 -> 1050000)
                            clean_text = re.sub(r'[^\d]', '', price_text)
                            if clean_text.isdigit() and len(clean_text) >= 4:
                                price = int(clean_text)
                                if 1000 <= price <= 100000000:
                                    return price
                except:
                    continue

            return 0

        except Exception as e:
            self.log(f"Price extraction error: {str(e)}")
            return 0

    def extract_lazada_product(self, product_element, index):
        """Extract product using accurate selectors from structure.html"""
        try:
            product = {'platform': 'lazada'}

            # URL extraction - Based on structure: <a age="0" href="//www.lazada.vn/products/pdp-i3212441700.html"
            try:
                link_selectors = [
                    ".RfADt a[href]",  # Primary link selector from structure
                    "a[href*='/products/']",  # Any link to products
                    ".buTCk a[href]",  # Link within product info container
                    "a[href]"  # Fallback to any link
                ]

                url = ""
                for selector in link_selectors:
                    try:
                        link = product_element.find_element(By.CSS_SELECTOR, selector)
                        url = link.get_attribute("href")
                        if url and '/products/' in url:
                            break
                    except:
                        continue

                if url:
                    # Handle relative URLs
                    if url.startswith('//'):
                        url = 'https:' + url
                    elif url.startswith('/'):
                        url = 'https://www.lazada.vn' + url

                    product['url'] = url

                    # Extract ID from URL pattern: pdp-i3212441700.html
                    id_match = re.search(r'pdp-i(\d+)', url)
                    if id_match:
                        product['id'] = id_match.group(1)
                    else:
                        # Alternative ID extraction
                        id_match = re.search(r'i(\d+)', url)
                        product['id'] = id_match.group(1) if id_match else f"lazada_{index}"
                else:
                    return None

            except:
                return None

            # Name extraction - Based on structure: <a title="[Genuine] Opp0 Ffind N5 Phone..."
            try:
                name_selectors = [
                    ".RfADt a[title]",  # Primary title from structure
                    ".buTCk a[title]",  # Alternative title location
                    "a[title]",         # Any element with title
                    ".RfADt a",         # Text content fallback
                    ".buTCk"            # Container text fallback
                ]

                product_name = ""
                for selector in name_selectors:
                    try:
                        name_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                        if selector.endswith('[title]'):
                            product_name = self.safe_text_extract(name_elem, 'title')
                        else:
                            product_name = self.safe_text_extract(name_elem)

                        if product_name and len(product_name) > 10:
                            break
                    except:
                        continue

                product['name'] = product_name if product_name else f"Lazada Product {index}"

            except:
                product['name'] = f"Lazada Product {index}"

            # Price extraction
            product['price'] = self.extract_lazada_price(product_element)

            # Additional data from structure analysis
            try:
                # Extract data-item-id if available
                item_id = product_element.get_attribute('data-item-id')
                if item_id:
                    product['data_item_id'] = item_id
            except:
                pass

            # Basic metadata
            product['category'] = 'Electronics'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Product extraction error: {str(e)}")
            return None

    def crawl_lazada_page(self, url, page_num):
        """Crawl Lazada page using accurate selectors"""
        self.log(f"Crawling Lazada page {page_num}: {url}")

        try:
            self.driver.get(url)
            self.wait_and_scroll()

            # Product container selector from structure.html
            # Main container: <div class="_17mcb" data-qa-locator="general-products"
            # Individual products: <div class="Bm3ON" data-qa-locator="product-item"

            product_selectors = [
                '.Bm3ON[data-qa-locator="product-item"]',  # Exact from structure
                '.Bm3ON',  # Class only fallback
                '[data-qa-locator="product-item"]'  # Data attribute fallback
            ]

            product_elements = []
            for selector in product_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        product_elements = elements
                        self.log(f"Found {len(elements)} products using selector: {selector}")
                        break
                except:
                    continue

            if not product_elements:
                self.log("No products found with any selector")
                return []

            products = []
            for i, element in enumerate(product_elements, 1):
                try:
                    product = self.extract_lazada_product(element, i)
                    if product and product.get('url'):
                        products.append(product)
                        if product['price'] > 0:
                            self.results['products_with_prices'] += 1

                        # Progress logging
                        if i % 10 == 0:
                            self.log(f"Processed {i}/{len(product_elements)} products")

                except Exception as e:
                    self.log(f"Error processing product {i}: {str(e)}")
                    continue

                # Small delay to avoid detection
                if i % 5 == 0:
                    time.sleep(0.3)

            self.results['lazada_products'] += len(products)
            self.log(f"Lazada page {page_num}: {len(products)} products extracted")
            return products

        except Exception as e:
            self.log(f"Lazada page error: {str(e)}")
            return []

    def run_test(self):
        """Test the accurate crawler"""
        self.log("Starting Accurate Lazada Crawler Test")

        try:
            self.setup_driver()

            # Test with Mobiles category as provided in user example
            url = "https://www.lazada.vn/catalog/?q=Mobiles&from=hp_categories"
            products = self.crawl_lazada_page(url, 1)

            self.results['total_products'] = len(products)
            self.results['pages_crawled'] = 1

            # Save results
            if products:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/accurate_lazada_test_{timestamp}.json"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

                # Convert datetime for JSON serialization
                results_copy = self.results.copy()
                results_copy['start_time'] = self.results['start_time'].isoformat()

                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'results': results_copy,
                        'products': products,
                        'timestamp': timestamp
                    }, f, ensure_ascii=False, indent=2)

                self.log(f"Results saved to: {output_file}")

                # Display sample results
                self.log("=== SAMPLE PRODUCTS ===")
                for i, product in enumerate(products[:3], 1):
                    self.log(f"{i}. Name: {product['name'][:60]}...")
                    self.log(f"   URL: {product['url']}")
                    self.log(f"   Price: {product['price']}")
                    self.log(f"   ID: {product['id']}")
                    self.log("")

            # Final statistics
            self.log("=== FINAL RESULTS ===")
            self.log(f"Total products: {self.results['total_products']}")
            self.log(f"Products with prices: {self.results['products_with_prices']}")
            if products:
                price_percentage = (self.results['products_with_prices'] / len(products)) * 100
                self.log(f"Price extraction success rate: {price_percentage:.1f}%")

        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    crawler = AccurateLazadaCrawler()
    crawler.run_test()