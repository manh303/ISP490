#!/usr/bin/env python3
"""
Working Lazada Crawler - Based on Live Testing Results
Uses category pages instead of search to avoid anti-bot protection
"""

import time
import json
import os
import re
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

class WorkingLazadaCrawler:
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
        """Setup undetected ChromeDriver for better anti-bot evasion"""
        try:
            # Try undetected chromedriver first
            self.driver = uc.Chrome()
            return True
        except Exception as e:
            print(f"Undetected ChromeDriver failed: {e}")
            print("Falling back to regular ChromeDriver...")

            # Fallback to regular ChromeDriver
            options = Options()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            return True

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
                text = text.replace('\u1ec7', 'e')
                return text
            return ""
        except:
            return ""

    def wait_and_scroll(self):
        """Wait and scroll to load products"""
        try:
            # Initial wait for page load
            time.sleep(5)

            # Gentle scrolling to trigger lazy loading
            for i in range(4):
                scroll_position = (i + 1) * 800
                self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                time.sleep(2)

            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

        except:
            pass

    def extract_price(self, product_element):
        """Extract price using working selectors"""
        try:
            # Try multiple price selectors
            price_selectors = [
                'span[class*="price"]',
                'div[class*="price"]',
                '[class*="ooOxS"]',  # From structure.html
                '[class*="aBrP0"]',  # Price container from structure.html
                'span:contains("₫")',
                '[data-price]'
            ]

            for selector in price_selectors:
                try:
                    if ':contains(' in selector:
                        # Skip this selector as it's not valid CSS selector
                        continue

                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in price_elements:
                        price_text = self.safe_text_extract(elem)
                        if price_text and ('VND' in price_text or '₫' in price_text or price_text.replace(',', '').isdigit()):
                            # Extract numbers only
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

    def extract_product(self, product_link, index):
        """Extract product info from product link"""
        try:
            product = {'platform': 'lazada'}

            # URL
            url = product_link.get_attribute('href')
            if not url or '/products/' not in url:
                return None

            # Handle relative URLs
            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                url = 'https://www.lazada.vn' + url

            product['url'] = url

            # Extract ID from URL
            id_match = re.search(r'pdp-i(\d+)', url)
            if id_match:
                product['id'] = id_match.group(1)
            else:
                id_match = re.search(r'i(\d+)', url)
                product['id'] = id_match.group(1) if id_match else f"lazada_{index}"

            # Name - try multiple methods
            try:
                # Method 1: title attribute
                title = product_link.get_attribute('title')
                if title and len(title) > 10:
                    product['name'] = self.safe_text_extract(product_link, 'title')
                else:
                    # Method 2: text content
                    text = self.safe_text_extract(product_link)
                    if text and len(text) > 10:
                        product['name'] = text
                    else:
                        # Method 3: alt attribute of images inside
                        try:
                            img = product_link.find_element(By.TAG_NAME, 'img')
                            alt_text = img.get_attribute('alt')
                            if alt_text and len(alt_text) > 10:
                                product['name'] = self.safe_text_extract(img, 'alt')
                            else:
                                product['name'] = f"Lazada Product {index}"
                        except:
                            product['name'] = f"Lazada Product {index}"
            except:
                product['name'] = f"Lazada Product {index}"

            # Price - look in parent container
            try:
                # Go up to find price container
                parent = product_link.find_element(By.XPATH, '..')
                for _ in range(3):  # Try up to 3 levels up
                    price = self.extract_price(parent)
                    if price > 0:
                        product['price'] = price
                        break
                    parent = parent.find_element(By.XPATH, '..')
                else:
                    product['price'] = 0
            except:
                product['price'] = 0

            # Basic metadata
            product['category'] = 'Electronics'
            product['crawl_time'] = datetime.now().isoformat()

            return product

        except Exception as e:
            self.log(f"Product extraction error: {str(e)}")
            return None

    def crawl_category_page(self, url, page_num):
        """Crawl Lazada category page"""
        self.log(f"Crawling Lazada page {page_num}: {url}")

        try:
            self.driver.get(url)
            self.wait_and_scroll()

            # Check for anti-bot redirect
            current_url = self.driver.current_url
            if "punish" in current_url:
                self.log("Detected anti-bot redirect, skipping...")
                return []

            # Find product links using working selector
            product_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/products/"]')
            self.log(f"Found {len(product_links)} product links")

            if not product_links:
                # Try alternative selectors
                alt_selectors = [
                    '[class*="item-"] a[href*="/products/"]',
                    '[data-qa-locator="product-item"] a',
                    'div[class*="product"] a'
                ]

                for selector in alt_selectors:
                    try:
                        product_links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if product_links:
                            self.log(f"Found {len(product_links)} products with selector: {selector}")
                            break
                    except:
                        continue

            products = []
            for i, link in enumerate(product_links, 1):
                try:
                    product = self.extract_product(link, i)
                    if product and product.get('url'):
                        products.append(product)
                        if product['price'] > 0:
                            self.results['products_with_prices'] += 1

                        # Progress logging
                        if i % 5 == 0:
                            self.log(f"Processed {i}/{len(product_links)} products")

                except Exception as e:
                    self.log(f"Error processing product {i}: {str(e)}")
                    continue

                # Small delay
                if i % 10 == 0:
                    time.sleep(0.5)

            self.results['lazada_products'] += len(products)
            self.log(f"Page {page_num}: {len(products)} products extracted")
            return products

        except Exception as e:
            self.log(f"Page crawling error: {str(e)}")
            return []

    def run_test(self):
        """Test working crawler"""
        self.log("Starting Working Lazada Crawler Test")

        try:
            if not self.setup_driver():
                self.log("Driver setup failed")
                return

            # Test with working category URL
            url = "https://www.lazada.vn/dien-thoai-di-dong/"
            products = self.crawl_category_page(url, 1)

            self.results['total_products'] = len(products)
            self.results['pages_crawled'] = 1

            # Save results
            if products:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/working_lazada_test_{timestamp}.json"
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
                try:
                    self.driver.quit()
                except:
                    pass

if __name__ == "__main__":
    crawler = WorkingLazadaCrawler()
    crawler.run_test()