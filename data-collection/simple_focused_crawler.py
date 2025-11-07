#!/usr/bin/env python3
"""
Simple Focused Lazada Crawler
Chỉ tập trung vào crawl nhiều sản phẩm và lấy price - không có database
"""

import time
import json
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class SimpleLazadaCrawler:
    """Simple crawler chỉ để crawl sản phẩm - không database"""

    def __init__(self, headless: bool = False):  # Mặc định hiển thị browser để debug
        self.headless = headless
        self.output_dir = Path("../data/simple_crawl")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.driver = None
        self.wait = None

        # Track kết quả
        self.results = {
            'total_products': 0,
            'products_with_prices': 0,
            'pages_crawled': 0,
            'start_time': datetime.now()
        }

    def setup_driver(self):
        """Setup Chrome driver đơn giản"""
        print("Setting up Chrome driver...")

        options = Options()
        if self.headless:
            options.add_argument("--headless=new")

        # Cấu hình cơ bản
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

        # User agent thường
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 10)

        print("✓ Driver ready")

    def wait_and_scroll(self):
        """Đợi trang load và scroll để trigger lazy loading"""
        try:
            # Đợi trang load cơ bản
            time.sleep(3)

            # Scroll từ từ để trigger lazy loading
            print("Scrolling to trigger product loading...")

            # Scroll xuống dần
            for i in range(5):
                scroll_height = i * 500
                self.driver.execute_script(f"window.scrollTo(0, {scroll_height});")
                time.sleep(1)

                # Check xem có products xuất hiện chưa
                products = self.driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")
                print(f"Scroll {i+1}: Found {len(products)} products")

            # Scroll về đầu
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            # Scroll xuống lại một lần nữa để ensure tất cả products loaded
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)

        except Exception as e:
            print(f"Warning during scroll: {e}")

    def extract_price_smart(self, product_element):
        """Smart price extraction với multiple methods"""
        try:
            # Method 1: Scroll element vào view trước
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", product_element)
                time.sleep(0.5)
            except:
                pass

            # Method 2: Tìm price trong nhiều selectors
            price_selectors = [
                ".currency-value",
                ".price-current",
                ".price .currency",
                "[data-qa-locator='product-price']",
                ".item-price",
                ".selling-price",
                ".price-box"
            ]

            for selector in price_selectors:
                try:
                    price_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                    for price_elem in price_elements:
                        price_text = price_elem.text.strip()
                        if price_text:
                            # Tìm số trong text
                            numbers = re.findall(r'[\d,\.]+', price_text)
                            if numbers:
                                # Lấy số lớn nhất (thường là giá)
                                for num_str in numbers:
                                    clean_num = num_str.replace(',', '').replace('.', '')
                                    if clean_num.isdigit() and len(clean_num) >= 4:
                                        price = int(clean_num)
                                        if 1000 <= price <= 100000000:  # Giá hợp lý
                                            return price
                except:
                    continue

            # Method 3: JavaScript để force refresh price
            try:
                js_script = """
                var element = arguments[0];

                // Trigger events để lazy load price
                var event = new Event('mouseover', {bubbles: true});
                element.dispatchEvent(event);

                setTimeout(function() {
                    // Tìm price elements
                    var priceSelectors = ['.currency-value', '.price', '[data-price]'];
                    for (var selector of priceSelectors) {
                        var priceEl = element.querySelector(selector);
                        if (priceEl && priceEl.textContent) {
                            var text = priceEl.textContent;
                            var numbers = text.match(/\d+/g);
                            if (numbers) {
                                var price = numbers.join('');
                                if (price.length >= 4) {
                                    return price;
                                }
                            }
                        }
                    }
                    return null;
                }, 100);
                """

                js_result = self.driver.execute_script(js_script, product_element)
                if js_result:
                    try:
                        price = int(js_result)
                        if 1000 <= price <= 100000000:
                            return price
                    except:
                        pass

            except:
                pass

            return 0

        except Exception as e:
            print(f"Price extraction error: {e}")
            return 0

    def extract_product_info(self, product_element, index):
        """Extract thông tin sản phẩm"""
        try:
            product = {}

            # URL
            try:
                link = product_element.find_element(By.CSS_SELECTOR, "a[href]")
                product['url'] = link.get_attribute("href")

                # ID từ URL
                id_match = re.search(r'i(\d+)', product['url'])
                product['id'] = id_match.group(1) if id_match else f"product_{index}"
            except:
                print(f"  Product {index}: No URL found")
                return None

            # Tên sản phẩm
            try:
                name_selectors = ["[title]", "a[title]", "h3", ".title"]
                product_name = None

                for selector in name_selectors:
                    try:
                        name_elem = product_element.find_element(By.CSS_SELECTOR, selector)
                        title = name_elem.get_attribute("title") or name_elem.text
                        if title and title.strip() and len(title.strip()) > 3:
                            product_name = title.strip()
                            break
                    except:
                        continue

                product['name'] = product_name or f"Product {index}"
            except:
                product['name'] = f"Product {index}"

            # Price - quan trọng nhất
            print(f"  Extracting price for product {index}...")
            price = self.extract_price_smart(product_element)
            product['price'] = price

            if price > 0:
                print(f"  ✓ Price found: {price:,} VND")
                self.results['products_with_prices'] += 1
            else:
                print(f"  ✗ No price found")

            # Thông tin khác
            product['category'] = 'Electronics'
            product['crawl_time'] = datetime.now().isoformat()

            self.results['total_products'] += 1
            return product

        except Exception as e:
            print(f"  Error extracting product {index}: {e}")
            return None

    def crawl_page(self, url, page_num):
        """Crawl một trang"""
        print(f"\n--- Crawling Page {page_num}: {url} ---")

        try:
            # Load trang
            self.driver.get(url)
            print("Page loaded, waiting and scrolling...")

            # Đợi và scroll
            self.wait_and_scroll()

            # Tìm products
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")
            print(f"Found {len(product_elements)} product elements")

            if not product_elements:
                print("❌ No products found on this page")
                return []

            # Extract từng product
            products = []
            for i, element in enumerate(product_elements, 1):
                print(f"\nExtracting product {i}/{len(product_elements)}...")

                product = self.extract_product_info(element, i)
                if product:
                    products.append(product)

                # Delay nhỏ giữa các products
                if i % 5 == 0:
                    time.sleep(1)

            self.results['pages_crawled'] += 1
            print(f"\n✓ Page {page_num} completed: {len(products)} products extracted")
            return products

        except Exception as e:
            print(f"❌ Error crawling page {page_num}: {e}")
            return []

    def crawl_multiple_pages(self, base_url, max_pages=5):
        """Crawl nhiều trang"""
        print(f"\n🚀 Starting crawl: {max_pages} pages from {base_url}")
        print("=" * 60)

        all_products = []

        for page in range(1, max_pages + 1):
            # Tạo URL cho từng trang
            if '?' in base_url:
                page_url = f"{base_url}&page={page}"
            else:
                page_url = f"{base_url}?page={page}"

            # Crawl trang
            page_products = self.crawl_page(page_url, page)
            all_products.extend(page_products)

            # Delay giữa các trang
            if page < max_pages:
                delay = random.uniform(3, 7)
                print(f"Waiting {delay:.1f}s before next page...")
                time.sleep(delay)

        return all_products

    def save_results(self, products):
        """Lưu kết quả đơn giản"""
        if not products:
            print("❌ No products to save")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Thống kê
        products_with_prices = [p for p in products if p.get('price', 0) > 0]

        # Data để save
        output_data = {
            'crawl_info': {
                'timestamp': timestamp,
                'total_products': len(products),
                'products_with_prices': len(products_with_prices),
                'price_success_rate': (len(products_with_prices) / len(products)) * 100 if products else 0,
                'pages_crawled': self.results['pages_crawled'],
                'duration': str(datetime.now() - self.results['start_time'])
            },
            'products': products
        }

        # Save JSON
        json_file = self.output_dir / f"lazada_crawl_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"\n💾 Results saved: {json_file}")

        # Print summary
        print(f"\n📊 CRAWL SUMMARY:")
        print(f"   Total products: {len(products)}")
        print(f"   Products with prices: {len(products_with_prices)}")
        print(f"   Price success rate: {output_data['crawl_info']['price_success_rate']:.1f}%")
        print(f"   Pages crawled: {self.results['pages_crawled']}")

        # Show price samples
        if products_with_prices:
            print(f"\n💰 PRICE SAMPLES:")
            for i, product in enumerate(products_with_prices[:5]):
                print(f"   {i+1}. {product['name'][:50]}... - {product['price']:,} VND")

    def run(self, category_url, max_pages=5):
        """Chạy crawler"""
        try:
            self.setup_driver()

            # Crawl
            products = self.crawl_multiple_pages(category_url, max_pages)

            # Save
            self.save_results(products)

            return len(products)

        except Exception as e:
            print(f"❌ Crawler failed: {e}")
            return 0

        finally:
            if self.driver:
                self.driver.quit()
                print("🔌 Browser closed")

def main():
    """Test crawler"""
    print("🛒 Simple Lazada Crawler")
    print("Focus: Nhiều sản phẩm + Price extraction")
    print("=" * 50)

    # Categories để test
    categories = {
        'smartphones': 'https://www.lazada.vn/dien-thoai-di-dong/',
        'laptops': 'https://www.lazada.vn/may-tinh-xach-tay/',
        'headphones': 'https://www.lazada.vn/tai-nghe/'
    }

    # Chọn category
    print("Available categories:")
    for i, (key, url) in enumerate(categories.items(), 1):
        print(f"{i}. {key}")

    try:
        choice = input("\nChoose category (1-3) or press Enter for smartphones: ").strip()
        if choice == '':
            choice = '1'

        category_list = list(categories.items())
        selected_key, selected_url = category_list[int(choice)-1]

        pages = input("Number of pages to crawl (default 3): ").strip()
        max_pages = int(pages) if pages.isdigit() else 3

        print(f"\n🎯 Crawling {selected_key}: {max_pages} pages")
        print(f"URL: {selected_url}")

        # Chạy crawler
        crawler = SimpleLazadaCrawler(headless=False)  # Hiển thị để xem
        total_products = crawler.run(selected_url, max_pages)

        if total_products > 0:
            print(f"\n🎉 SUCCESS: Collected {total_products} products!")
        else:
            print(f"\n❌ FAILED: No products collected")

    except (ValueError, IndexError):
        print("❌ Invalid choice")
    except KeyboardInterrupt:
        print("\n👋 Cancelled by user")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()