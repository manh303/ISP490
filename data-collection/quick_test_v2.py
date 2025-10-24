#!/usr/bin/env python3
"""
Quick Test V2 - Test pagination crawler with 1 category only
"""

import time
import json
import os
import re
import random
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

class QuickTestCrawlerV2:
    def __init__(self):
        self.driver = None
        self.results = {
            'total_products': 0,
            'products_with_prices': 0,
            'products_with_discounts': 0,
            'products_with_ratings': 0,
            'pages_crawled': 0,
            'start_time': datetime.now()
        }

    def setup_driver(self):
        """Setup simple undetected ChromeDriver"""
        try:
            self.driver = uc.Chrome()
            return True
        except Exception as e:
            print(f"Driver setup failed: {e}")
            return False

    def log(self, message):
        """Simple logging"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")

    def safe_text_extract(self, element, attribute=None):
        """Safely extract text"""
        try:
            if attribute:
                text = element.get_attribute(attribute)
            else:
                text = element.text
            return text.strip() if text else ""
        except:
            return ""

    def build_page_url(self, base_url, page_number):
        """Build paginated URL"""
        try:
            parsed = urlparse(base_url)
            query_params = parse_qs(parsed.query)
            query_params['page'] = [str(page_number)]
            new_query = urlencode(query_params, doseq=True)
            new_parsed = parsed._replace(query=new_query)
            return urlunparse(new_parsed)
        except:
            separator = '&' if '?' in base_url else '?'
            if 'page=' in base_url:
                return re.sub(r'page=\d+', f'page={page_number}', base_url)
            else:
                return f"{base_url}{separator}page={page_number}"

    def extract_product(self, product_element, index):
        """Extract enhanced product data"""
        try:
            product = {'platform': 'lazada'}

            # ID from data attribute
            data_item_id = product_element.get_attribute('data-item-id')
            product['id'] = data_item_id or f"product_{index}"

            # URL
            try:
                url_elem = product_element.find_element(By.CSS_SELECTOR, 'a[href*="/products/"]')
                url = url_elem.get_attribute('href')
                if url.startswith('//'):
                    url = 'https:' + url
                elif url.startswith('/'):
                    url = 'https://www.lazada.vn' + url
                product['url'] = url
            except:
                return None

            # Name from title attribute
            try:
                title_elem = product_element.find_element(By.CSS_SELECTOR, 'a[title]')
                name = self.safe_text_extract(title_elem, 'title')
                product['name'] = name if name else f"Product {index}"
            except:
                product['name'] = f"Product {index}"

            # Price using real selector
            price = 0
            try:
                price_elem = product_element.find_element(By.CSS_SELECTOR, '.ooOxS')
                price_text = self.safe_text_extract(price_elem)
                if price_text and '₫' in price_text:
                    clean_price = re.sub(r'[^\d]', '', price_text)
                    if clean_price.isdigit() and len(clean_price) >= 4:
                        price_val = int(clean_price)
                        if 1000 <= price_val <= 100000000:
                            price = price_val
                            self.results['products_with_prices'] += 1
            except:
                pass
            product['price'] = price

            # Discount
            discount = None
            try:
                discount_elem = product_element.find_element(By.CSS_SELECTOR, '.IcOsH')
                discount_text = self.safe_text_extract(discount_elem)
                if discount_text and 'Off' in discount_text:
                    discount = discount_text
                    self.results['products_with_discounts'] += 1
            except:
                pass
            product['discount'] = discount

            # Rating
            rating_average = None
            rating_count = None
            try:
                filled_stars = product_element.find_elements(By.CSS_SELECTOR, '._9-ogB.Dy1nx')
                if filled_stars:
                    rating_average = len(filled_stars)
                    self.results['products_with_ratings'] += 1
            except:
                pass

            try:
                rating_count_elem = product_element.find_element(By.CSS_SELECTOR, '.qzqFw')
                rating_text = self.safe_text_extract(rating_count_elem)
                if rating_text:
                    count_match = re.search(r'\((\d+)\)', rating_text)
                    if count_match:
                        rating_count = int(count_match.group(1))
            except:
                pass

            product['rating_average'] = rating_average
            product['rating_count'] = rating_count

            # Location
            location = None
            try:
                location_elem = product_element.find_element(By.CSS_SELECTOR, '.oa6ri')
                location = self.safe_text_extract(location_elem, 'title') or self.safe_text_extract(location_elem)
            except:
                pass
            product['location'] = location

            product['crawl_time'] = datetime.now().isoformat()
            return product

        except Exception as e:
            self.log(f"Product extraction error: {str(e)}")
            return None

    def crawl_pages(self, base_url, max_pages=3):
        """Crawl multiple pages"""
        self.log(f"Crawling {max_pages} pages from: {base_url}")
        all_products = []

        for page_num in range(1, max_pages + 1):
            try:
                page_url = self.build_page_url(base_url, page_num)
                self.log(f"Page {page_num}: {page_url}")

                self.driver.get(page_url)
                time.sleep(5)

                # Scroll to load content
                for i in range(3):
                    scroll_position = (i + 1) * 800
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                    time.sleep(2)

                # Check for anti-bot
                current_url = self.driver.current_url
                if "punish" in current_url or "captcha" in current_url:
                    self.log(f"Anti-bot detected on page {page_num}")
                    continue

                # Find products
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-qa-locator="product-item"]')
                self.log(f"Found {len(product_elements)} products on page {page_num}")

                if not product_elements:
                    self.log(f"No products on page {page_num}, stopping")
                    break

                # Extract products
                page_products = []
                for i, element in enumerate(product_elements, 1):
                    try:
                        product = self.extract_product(element, i)
                        if product and product.get('url'):
                            page_products.append(product)

                        if i % 20 == 0:
                            self.log(f"Processed {i}/{len(product_elements)} products")

                    except Exception as e:
                        continue

                    if i % 10 == 0:
                        time.sleep(0.5)

                all_products.extend(page_products)
                self.results['pages_crawled'] += 1
                self.log(f"Page {page_num}: {len(page_products)} products extracted")

                # Delay between pages
                time.sleep(random.uniform(3, 5))

            except Exception as e:
                self.log(f"Error on page {page_num}: {str(e)}")
                continue

        self.log(f"Total: {len(all_products)} products from {max_pages} pages")
        return all_products

    def run_test(self):
        """Run quick test"""
        self.log("Starting Quick Test V2")

        try:
            if not self.setup_driver():
                return

            # Test with simple smartphones category that worked before
            base_url = "https://www.lazada.vn/dien-thoai-di-dong/"

            all_products = self.crawl_pages(base_url, max_pages=3)
            self.results['total_products'] = len(all_products)

            # Save results
            if all_products:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/quick_test_v2_{timestamp}.json"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

                results_copy = self.results.copy()
                results_copy['start_time'] = self.results['start_time'].isoformat()
                results_copy['end_time'] = datetime.now().isoformat()
                results_copy['duration'] = str(datetime.now() - self.results['start_time'])

                # Additional statistics
                products_with_images = sum(1 for p in all_products if p.get('images'))
                products_with_location = sum(1 for p in all_products if p.get('location'))

                results_copy.update({
                    'products_with_images': products_with_images,
                    'products_with_location': products_with_location,
                    'price_success_rate': (self.results['products_with_prices'] / len(all_products) * 100) if all_products else 0,
                    'discount_rate': (self.results['products_with_discounts'] / len(all_products) * 100) if all_products else 0
                })

                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'metadata': results_copy,
                        'products': all_products,
                        'timestamp': timestamp
                    }, f, ensure_ascii=False, indent=2)

                self.log(f"Results saved to: {output_file}")

                # Display results
                self.log("=== QUICK TEST RESULTS ===")
                self.log(f"Total products: {self.results['total_products']}")
                self.log(f"Pages crawled: {self.results['pages_crawled']}")
                self.log(f"Products with prices: {self.results['products_with_prices']}")
                self.log(f"Products with discounts: {self.results['products_with_discounts']}")
                self.log(f"Products with ratings: {self.results['products_with_ratings']}")
                self.log(f"Products with location: {products_with_location}")

                if all_products:
                    price_rate = (self.results['products_with_prices'] / len(all_products)) * 100
                    self.log(f"Price extraction success rate: {price_rate:.1f}%")

                # Sample product
                if all_products:
                    sample = all_products[0]
                    self.log("=== SAMPLE PRODUCT ===")
                    self.log(f"Name: {sample['name'][:60]}...")
                    self.log(f"Price: {sample['price']:,} VND")
                    self.log(f"Rating: {sample.get('rating_average', 'N/A')} ({sample.get('rating_count', 'N/A')} reviews)")
                    self.log(f"Discount: {sample.get('discount', 'None')}")
                    self.log(f"Location: {sample.get('location', 'N/A')}")

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

if __name__ == "__main__":
    crawler = QuickTestCrawlerV2()
    crawler.run_test()