import time
import json
import csv
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
import random

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

class LazadaCrawler:
    def __init__(self):
        self.base_url = "https://www.lazada.vn/dien-thoai-di-dong/"
        self.categories = {
            "smartphones": "https://www.lazada.vn/tag/mobiles/?q=mobiles",
            "laptops": "https://www.lazada.vn/tag/laptops/?q=laptops",
            "tablets": "https://www.lazada.vn/tag/tablets/?q=tablets",
            "smartwatches": "https://www.lazada.vn/tag/smartwatch/?q=smartwatch",
            "tvs": "https://www.lazada.vn/tag/tv/?q=tv",
            "headphones": "https://www.lazada.vn/tag/headphones/?q=headphones",
            "cameras": "https://www.lazada.vn/tag/cameras/?q=cameras",
            "monitors": "https://www.lazada.vn/tag/monitors/?q=monitors",
            "destops-computers": "https://www.lazada.vn/tag/desktop-computer/?q=desktop+computer"
        }
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Thiết lập Chrome driver với các options tối ưu"""
        chrome_options = Options()

        # User agent giả lập browser thật
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Các options để tránh bị phát hiện
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Tắt images để load nhanh hơn (optional)
        # chrome_options.add_argument("--disable-images")

        # Chạy headless nếu không muốn hiện browser
        # chrome_options.add_argument("--headless")

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            # Xóa thuộc tính webdriver để tránh bị phát hiện
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            print("✅ Chrome driver đã được khởi tạo thành công")
        except Exception as e:
            print(f"❌ Lỗi khởi tạo Chrome driver: {e}")
            print("💡 Hãy đảm bảo đã cài đặt ChromeDriver và đặt trong PATH")
            raise

    def random_delay(self, min_delay=1, max_delay=3):
        """Delay ngẫu nhiên để tránh bị phát hiện là bot"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def scroll_page(self, scroll_pause_time=2, max_scrolls=5):
        """Cuộn trang để load lazy-loaded content"""
        print(f"🔄 Đang cuộn trang để load sản phẩm...")

        for i in range(max_scrolls):
            # Cuộn xuống cuối trang
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Chờ cho content load
            time.sleep(scroll_pause_time)

            print(f"   Đã cuộn lần {i+1}/{max_scrolls}")

    def extract_product_data(self, product_element, category) -> Optional[Dict[str, Any]]:
        """Trích xuất dữ liệu từ một element sản phẩm"""
        try:
            product_data = {
                "category": category,
                "title": "",
                "url": "",
                "image": "",
                "price_text": "",
                "price": None,
                "original_price_text": "",
                "original_price": None,
                "discount": "",
                "rating": None,
                "review_count": "",
                "location": "",
                "shop_name": ""
            }

            # Lấy link sản phẩm
            link_element = product_element.find_element(By.CSS_SELECTOR, "a")
            if link_element:
                href = link_element.get_attribute("href")
                if href and href.startswith("//"):
                    href = "https:" + href
                elif href and href.startswith("/"):
                    href = "https://www.lazada.vn" + href
                product_data["url"] = href

            # Lấy tên sản phẩm từ title attribute hoặc alt của img
            try:
                title_element = product_element.find_element(By.CSS_SELECTOR, "img")
                title = title_element.get_attribute("alt") or title_element.get_attribute("title")
                if title:
                    product_data["title"] = title.strip()
            except NoSuchElementException:
                pass

            # Nếu không có title từ img, thử tìm từ text
            if not product_data["title"]:
                try:
                    title_element = product_element.find_element(By.CSS_SELECTOR, "[title]")
                    product_data["title"] = title_element.get_attribute("title").strip()
                except NoSuchElementException:
                    pass

            # Lấy ảnh sản phẩm
            try:
                img_element = product_element.find_element(By.CSS_SELECTOR, "img")
                img_src = img_element.get_attribute("src") or img_element.get_attribute("data-src")
                if img_src:
                    product_data["image"] = img_src
            except NoSuchElementException:
                pass

            # Lấy giá hiện tại
            price_selectors = [
                ".price",
                "[class*='price']",
                ".currency",
                "[class*='Currency']"
            ]

            for selector in price_selectors:
                try:
                    price_element = product_element.find_element(By.CSS_SELECTOR, selector)
                    price_text = price_element.text.strip()
                    if price_text and any(char.isdigit() for char in price_text):
                        product_data["price_text"] = price_text
                        product_data["price"] = self.normalize_price(price_text)
                        break
                except NoSuchElementException:
                    continue

            # Lấy giá gốc (nếu có)
            try:
                original_price_element = product_element.find_element(By.CSS_SELECTOR, "[class*='origin'], [class*='Origin']")
                original_price_text = original_price_element.text.strip()
                if original_price_text:
                    product_data["original_price_text"] = original_price_text
                    product_data["original_price"] = self.normalize_price(original_price_text)
            except NoSuchElementException:
                pass

            # Lấy discount
            try:
                discount_element = product_element.find_element(By.CSS_SELECTOR, "[class*='discount'], [class*='Discount']")
                product_data["discount"] = discount_element.text.strip()
            except NoSuchElementException:
                pass

            # Lấy rating
            try:
                rating_element = product_element.find_element(By.CSS_SELECTOR, "[class*='rating'], [class*='Rating'], [class*='star']")
                rating_text = rating_element.text.strip()
                product_data["rating"] = self.normalize_rating(rating_text)
            except NoSuchElementException:
                pass

            # Lấy số lượng đánh giá
            try:
                review_element = product_element.find_element(By.CSS_SELECTOR, "[class*='review'], [class*='Review']")
                product_data["review_count"] = review_element.text.strip()
            except NoSuchElementException:
                pass

            # Lấy location
            try:
                location_element = product_element.find_element(By.CSS_SELECTOR, "[class*='location'], [class*='Location']")
                product_data["location"] = location_element.text.strip()
            except NoSuchElementException:
                pass

            # Chỉ trả về nếu có đủ thông tin cơ bản
            if product_data["title"] and product_data["url"]:
                return product_data
            else:
                return None

        except Exception as e:
            print(f"❌ Lỗi khi trích xuất dữ liệu sản phẩm: {e}")
            return None

    def normalize_price(self, price_text: str) -> Optional[int]:
        """Chuẩn hóa giá về dạng số"""
        if not price_text:
            return None

        # Loại bỏ tất cả ký tự không phải số
        digits = re.sub(r'[^\d]', '', price_text)
        if digits:
            try:
                return int(digits)
            except ValueError:
                return None
        return None

    def normalize_rating(self, rating_text: str) -> Optional[float]:
        """Chuẩn hóa rating về dạng số thực"""
        if not rating_text:
            return None

        # Tìm số dạng 4.5, 4,5 hoặc 4
        match = re.search(r'(\d+[.,]?\d*)', rating_text)
        if match:
            try:
                rating_str = match.group(1).replace(',', '.')
                return float(rating_str)
            except ValueError:
                return None
        return None

    def get_products_from_page(self, page_num=1, max_products=50) -> List[Dict[str, Any]]:
        """Lấy danh sách sản phẩm từ một trang"""
        if page_num > 1:
            url = f"{self.base_url}?page={page_num}"
        else:
            url = self.base_url

        print(f"🔍 Đang crawl trang {page_num}: {url}")

        try:
            # Mở trang
            self.driver.get(url)
            self.random_delay(2, 4)

            # Chờ trang load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                print("⚠️ Timeout chờ trang load")
                return []

            # Cuộn trang để load thêm sản phẩm
            self.scroll_page()

            # Tìm các elements chứa sản phẩm
            products = []

            # Các selectors có thể chứa sản phẩm Lazada
            product_selectors = [
                "[data-qa-locator*='product']",
                "[data-spm*='product']",
                "div[data-spm]",
                ".gridItem",
                ".product-item",
                "[class*='gridItem']",
                "[class*='product']"
            ]

            for selector in product_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"   Selector '{selector}': tìm thấy {len(elements)} elements")

                    if elements:
                        for element in elements[:max_products]:
                            # Kiểm tra xem element có chứa link sản phẩm không
                            try:
                                link = element.find_element(By.CSS_SELECTOR, "a[href*='/products/'], a[href*='lazada.vn']")
                                if link:
                                    product_data = self.extract_product_data(element)
                                    if product_data:
                                        products.append(product_data)
                                        print(f"   ✅ Đã trích xuất: {product_data['title'][:50]}...")
                            except NoSuchElementException:
                                continue

                        if products:  # Nếu đã tìm thấy sản phẩm, dừng tìm kiếm
                            break

                except Exception as e:
                    print(f"   ❌ Lỗi với selector '{selector}': {e}")
                    continue

            print(f"📊 Trang {page_num}: Đã trích xuất {len(products)} sản phẩm")
            return products

        except Exception as e:
            print(f"❌ Lỗi khi crawl trang {page_num}: {e}")
            return []

    def crawl_multiple_pages(self, max_pages=60, max_products_per_page=50) -> List[Dict[str, Any]]:
        """Crawl nhiều trang"""
        all_products = []

        for page in range(1, max_pages + 1):
            try:
                products = self.get_products_from_page(page, max_products_per_page)
                all_products.extend(products)

                # Delay giữa các trang
                if page < max_pages:
                    print(f"⏱️ Nghỉ giữa các trang...")
                    self.random_delay(3, 6)

            except Exception as e:
                print(f"❌ Lỗi crawl trang {page}: {e}")
                continue

        # Loại bỏ duplicate
        unique_products = []
        seen_urls = set()

        for product in all_products:
            url = product.get('url', '')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_products.append(product)

        print(f"📊 Tổng cộng: {len(unique_products)} sản phẩm unique từ {max_pages} trang")
        return unique_products

    def save_to_files(self, products: List[Dict[str, Any]], filename_prefix="lazada_products"):
        """Lưu dữ liệu vào file JSON và CSV"""
        if not products:
            print("⚠️ Không có dữ liệu để lưu")
            return

        # Tạo thư mục output
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # Lưu JSON
        json_file = output_dir / f"{filename_prefix}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        # Lưu JSONL
        jsonl_file = output_dir / f"{filename_prefix}.jsonl"
        with open(jsonl_file, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False) + '\n')

        # Lưu CSV
        csv_file = output_dir / f"{filename_prefix}.csv"
        if products:
            fieldnames = products[0].keys()
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(products)

        print(f"💾 Đã lưu dữ liệu:")
        print(f"   📄 JSON: {json_file}")
        print(f"   📄 JSONL: {jsonl_file}")
        print(f"   📄 CSV: {csv_file}")

    def close(self):
        """Đóng browser"""
        if self.driver:
            self.driver.quit()
            print("🔒 Đã đóng browser")

def main():
    crawler = LazadaCrawler()

    try:
        # Crawl 3 trang, tối đa 30 sản phẩm mỗi trang
        products = crawler.crawl_multiple_pages(max_pages=60, max_products_per_page=30)

        # Lưu kết quả
        if products:
            crawler.save_to_files(products)

            # In thống kê
            print(f"\n📈 Thống kê:")
            print(f"   Tổng sản phẩm: {len(products)}")

            # Thống kê giá
            prices = [p['price'] for p in products if p.get('price')]
            if prices:
                print(f"   Giá thấp nhất: {min(prices):,} VND")
                print(f"   Giá cao nhất: {max(prices):,} VND")
                print(f"   Giá trung bình: {sum(prices)//len(prices):,} VND")

            # Hiển thị vài sản phẩm mẫu
            print(f"\n🛍️ Vài sản phẩm mẫu:")
            for i, product in enumerate(products[:3]):
                print(f"   {i+1}. {product.get('title', 'N/A')[:60]}...")
                print(f"      Giá: {product.get('price_text', 'N/A')}")
                print(f"      URL: {product.get('url', 'N/A')[:80]}...")
                print()
        else:
            print("❌ Không crawl được sản phẩm nào")

    except KeyboardInterrupt:
        print("\n⛔ Dừng crawling theo yêu cầu người dùng")
    except Exception as e:
        print(f"❌ Lỗi trong quá trình crawling: {e}")
    finally:
        crawler.close()

if __name__ == "__main__":
    main()