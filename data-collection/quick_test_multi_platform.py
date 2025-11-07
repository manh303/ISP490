#!/usr/bin/env python3
"""
Quick Test Multi-Platform - Test both Lazada and Tiki extraction
"""

import time
import json
import os
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

class QuickTestMultiPlatform:
    def __init__(self):
        self.driver = None
        self.results = {
            'total_products': 0,
            'products_with_prices': 0,
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

    def safe_text_extract(self, element, attribute=None):
        """Safely extract text with Unicode handling"""
        try:
            if attribute:
                text = element.get_attribute(attribute)
            else:
                text = element.text

            if text:
                text = text.strip()
                text = text.replace('\u20ab', ' VND')
                text = text.replace('\u1ed1', 'o')
                text = text.replace('\u0110', 'D')
                return text
            return ""
        except:
            return ""

    def extract_price(self, product_element, platform):
        """Extract price for both platforms"""
        try:
            if platform == 'lazada':
                selectors = [".current-price", ".price-current", ".price"]
            else:  # tiki
                selectors = [".price-current", ".price", ".current-price"]

            for selector in selectors:
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
        except:
            return 0

    def test_lazada(self):
        """Test Lazada extraction"""
        self.log("Testing Lazada smartphones extraction")

        try:
            url = "https://www.lazada.vn/dien-thoai-di-dong/"
            self.driver.get(url)

            # Wait and scroll
            time.sleep(3)
            self.driver.execute_script("window.scrollTo(0, 2000);")
            time.sleep(2)

            # Find products
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")
            self.log(f"Found {len(product_elements)} Lazada products")

            products = []
            for i, element in enumerate(product_elements[:5], 1):  # Test first 5
                try:
                    product = {'platform': 'lazada'}

                    # URL
                    link = element.find_element(By.CSS_SELECTOR, "a[href]")
                    product['url'] = link.get_attribute("href")

                    # ID
                    id_match = re.search(r'i(\d+)', product['url'])
                    product['id'] = id_match.group(1) if id_match else f"lazada_{i}"

                    # Name
                    try:
                        title_elem = element.find_element(By.CSS_SELECTOR, "[title]")
                        product['name'] = self.safe_text_extract(title_elem, "title")
                    except:
                        product['name'] = f"Lazada Product {i}"

                    # Price
                    product['price'] = self.extract_price(element, 'lazada')

                    product['category'] = 'Electronics'
                    product['crawl_time'] = datetime.now().isoformat()

                    products.append(product)
                    if product['price'] > 0:
                        self.results['products_with_prices'] += 1

                except Exception as e:
                    self.log(f"Error extracting Lazada product {i}: {str(e)}")
                    continue

            self.results['lazada_products'] = len(products)
            self.log(f"Lazada: {len(products)} products extracted")
            return products

        except Exception as e:
            self.log(f"Lazada test error: {str(e)}")
            return []

    def test_tiki(self):
        """Test Tiki extraction"""
        self.log("Testing Tiki smartphones extraction")

        try:
            url = "https://tiki.vn/dien-thoai-smartphone/c1795"
            self.driver.get(url)

            # Wait and scroll
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, 1000);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, 2000);")
            time.sleep(2)

            # Find products using working selector
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, ".product-item")
            self.log(f"Found {len(product_elements)} Tiki products")

            products = []
            for i, element in enumerate(product_elements[:5], 1):  # Test first 5
                try:
                    product = {'platform': 'tiki'}

                    # URL - element is the <a> tag itself
                    url = element.get_attribute("href")
                    if url:
                        if url.startswith('//'):
                            url = 'https:' + url
                        elif url.startswith('/'):
                            url = 'https://tiki.vn' + url
                        product['url'] = url

                        # ID from URL
                        id_match = re.search(r'p(\d+)', url)
                        if id_match:
                            product['id'] = id_match.group(1)
                        else:
                            spid_match = re.search(r'spid=(\d+)', url)
                            product['id'] = spid_match.group(1) if spid_match else f"tiki_{i}"
                    else:
                        continue

                    # Name
                    try:
                        # Try multiple name extraction methods
                        name_selectors = [".name", ".product-name", "img[alt]"]
                        product_name = ""

                        for selector in name_selectors:
                            try:
                                name_elem = element.find_element(By.CSS_SELECTOR, selector)
                                if selector == "img[alt]":
                                    product_name = self.safe_text_extract(name_elem, 'alt')
                                else:
                                    product_name = self.safe_text_extract(name_elem)

                                if product_name and len(product_name) > 5:
                                    break
                            except:
                                continue

                        product['name'] = product_name if product_name else f"Tiki Product {i}"

                    except:
                        product['name'] = f"Tiki Product {i}"

                    # Price
                    product['price'] = self.extract_price(element, 'tiki')

                    product['category'] = 'Electronics'
                    product['crawl_time'] = datetime.now().isoformat()

                    products.append(product)
                    if product['price'] > 0:
                        self.results['products_with_prices'] += 1

                except Exception as e:
                    self.log(f"Error extracting Tiki product {i}: {str(e)}")
                    continue

            self.results['tiki_products'] = len(products)
            self.log(f"Tiki: {len(products)} products extracted")
            return products

        except Exception as e:
            self.log(f"Tiki test error: {str(e)}")
            return []

    def run_test(self):
        """Run quick test of both platforms"""
        self.log("Starting Quick Multi-Platform Test")

        try:
            self.setup_driver()

            # Test both platforms
            lazada_products = self.test_lazada()
            time.sleep(3)  # Break between platforms
            tiki_products = self.test_tiki()

            all_products = lazada_products + tiki_products
            self.results['total_products'] = len(all_products)

            # Save results
            if all_products:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/quick_multi_platform_test_{timestamp}.json"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

                results_copy = self.results.copy()
                results_copy['start_time'] = self.results['start_time'].isoformat()

                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'results': results_copy,
                        'products': all_products,
                        'timestamp': timestamp
                    }, f, ensure_ascii=False, indent=2)

                self.log(f"Results saved to: {output_file}")

                # Print samples
                self.log("=== SAMPLE RESULTS ===")
                if lazada_products:
                    self.log(f"Lazada sample: {lazada_products[0]['name'][:50]}... - Price: {lazada_products[0]['price']}")
                if tiki_products:
                    self.log(f"Tiki sample: {tiki_products[0]['name'][:50]}... - Price: {tiki_products[0]['price']}")

            # Final stats
            self.log("=== FINAL RESULTS ===")
            self.log(f"Total products: {self.results['total_products']}")
            self.log(f"Lazada: {self.results['lazada_products']} products")
            self.log(f"Tiki: {self.results['tiki_products']} products")
            self.log(f"Products with prices: {self.results['products_with_prices']}")

        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    tester = QuickTestMultiPlatform()
    tester.run_test()