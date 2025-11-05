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
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

class LazadaCrawlerImproved:
    def __init__(self, headless=False):
        self.base_url = "https://www.lazada.vn/dien-thoai-di-dong/"
        self.driver = None
        self.headless = headless
        self.setup_driver()

    def setup_driver(self):
        """Thiết lập Chrome driver với webdriver-manager tự động"""
        chrome_options = Options()

        # User agent giống browser thật
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Các options để tránh bị phát hiện
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Chạy headless nếu yêu cầu
        if self.headless:
            chrome_options.add_argument("--headless")

        # Window size
        chrome_options.add_argument("--window-size=1920,1080")

        try:
            # Tự động tải và setup ChromeDriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # Xóa thuộc tính webdriver để tránh bị phát hiện
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            print("✅ Chrome driver đã được khởi tạo thành công")
        except Exception as e:
            print(f"❌ Lỗi khởi tạo Chrome driver: {e}")
            raise

    def random_delay(self, min_delay=1, max_delay=3):
        """Delay ngẫu nhiên để tránh bị phát hiện là bot"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def scroll_and_wait(self, scroll_pause_time=3, max_scrolls=5):
        """Cuộn trang từ từ và chờ content load"""
        print(f"🔄 Đang cuộn trang để load sản phẩm...")

        last_height = self.driver.execute_script("return document.body.scrollHeight")

        for i in range(max_scrolls):
            # Cuộn xuống từng phần
            scroll_steps = 3
            for step in range(scroll_steps):
                scroll_position = (step + 1) * (last_height // scroll_steps)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                time.sleep(1)

            # Chờ cho content load
            time.sleep(scroll_pause_time)

            # Kiểm tra xem có content mới load không
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                print(f"   Không có content mới sau lần cuộn {i+1}")
                break
            else:
                print(f"   Lần cuộn {i+1}: Phát hiện content mới (height: {last_height} -> {new_height})")
                last_height = new_height

    def debug_element_structure(self, element, element_index=0):
        """Debug cấu trúc của element để tìm selectors đúng"""
        try:
            print(f"\n🔍 DEBUG ELEMENT {element_index}:")

            # Lấy HTML của element
            outer_html = element.get_attribute('outerHTML')[:500]
            print(f"   HTML snippet: {outer_html}...")

            # Tìm tất cả text trong element
            all_text = element.text.strip()
            if all_text:
                print(f"   All text: {all_text}")

            # Tìm tất cả elements con
            child_elements = element.find_elements(By.CSS_SELECTOR, "*")
            print(f"   Child elements: {len(child_elements)}")

            # Tìm text có chứa số (có thể là giá)
            for child in child_elements[:10]:  # Chỉ check 10 element đầu
                child_text = child.text.strip()
                if child_text and any(char.isdigit() for char in child_text):
                    class_name = child.get_attribute('class') or 'no-class'
                    tag_name = child.tag_name
                    print(f"   Text with numbers: '{child_text}' | Tag: {tag_name} | Class: {class_name}")

            # Tìm elements có class chứa từ khóa liên quan đến giá
            price_keywords = ['price', 'currency', 'cost', 'money', 'amount', 'value']
            for child in child_elements:
                class_name = child.get_attribute('class') or ''
                if any(keyword in class_name.lower() for keyword in price_keywords):
                    child_text = child.text.strip()
                    print(f"   Price-related class: '{child_text}' | Class: {class_name}")

        except Exception as e:
            print(f"   Debug error: {e}")

    def extract_product_data_improved(self, product_element, element_index=0) -> Optional[Dict[str, Any]]:
        """Trích xuất dữ liệu từ một element sản phẩm với debug"""
        try:
            product_data = {
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
                "sold_count": "",
                "location": "",
                "shop_name": ""
            }

            # Debug structure nếu là element đầu tiên
            if element_index < 3:
                self.debug_element_structure(product_element, element_index)

            # 1. Lấy link sản phẩm
            link_selectors = [
                "a[href*='/products/']",
                "a[href*='lazada.vn']",
                "a"
            ]

            for selector in link_selectors:
                try:
                    link_element = product_element.find_element(By.CSS_SELECTOR, selector)
                    href = link_element.get_attribute("href")
                    if href:
                        if href.startswith("//"):
                            href = "https:" + href
                        elif href.startswith("/"):
                            href = "https://www.lazada.vn" + href
                        product_data["url"] = href
                        break
                except NoSuchElementException:
                    continue

            # 2. Lấy tên sản phẩm
            title_selectors = [
                "img[alt]",
                "[title]",
                "img[title]",
                "[data-qa-locator*='name']"
            ]

            for selector in title_selectors:
                try:
                    element = product_element.find_element(By.CSS_SELECTOR, selector)
                    title = element.get_attribute("alt") or element.get_attribute("title") or element.text.strip()
                    if title and len(title) > 10:
                        product_data["title"] = title
                        break
                except NoSuchElementException:
                    continue

            # 3. Lấy ảnh sản phẩm
            img_selectors = ["img[src]", "img[data-src]", "img"]
            for selector in img_selectors:
                try:
                    img_element = product_element.find_element(By.CSS_SELECTOR, selector)
                    img_src = img_element.get_attribute("src") or img_element.get_attribute("data-src")
                    if img_src and any(ext in img_src.lower() for ext in ['.jpg', '.jpeg', '.png', '.webp']):
                        product_data["image"] = img_src
                        break
                except NoSuchElementException:
                    continue

            # 4. Lấy giá - CẢI THIỆN PHẦN NÀY
            # Phương pháp 1: Tìm theo text pattern (VND, ₫, số)
            all_elements = product_element.find_elements(By.CSS_SELECTOR, "*")
            price_candidates = []

            for elem in all_elements:
                try:
                    text = elem.text.strip()
                    if text and (
                        '₫' in text or
                        'VND' in text.upper() or
                        'VNĐ' in text.upper() or
                        (any(char.isdigit() for char in text) and len(text) > 4 and '.' in text)
                    ):
                        # Loại bỏ text quá dài (có thể là description)
                        if len(text) < 50:
                            price_candidates.append({
                                'text': text,
                                'element': elem,
                                'class': elem.get_attribute('class') or '',
                                'tag': elem.tag_name
                            })
                except:
                    continue

            # Sắp xếp candidates theo độ ưu tiên
            price_candidates.sort(key=lambda x: (
                'price' in x['class'].lower(),
                'currency' in x['class'].lower(),
                '₫' in x['text'],
                len(re.findall(r'\d', x['text']))
            ), reverse=True)

            if element_index < 3 and price_candidates:
                print(f"   💰 Price candidates for element {element_index}:")
                for i, candidate in enumerate(price_candidates[:5]):
                    print(f"      {i+1}. '{candidate['text']}' | Class: {candidate['class']} | Tag: {candidate['tag']}")

            # Lấy price candidate tốt nhất
            for candidate in price_candidates:
                text = candidate['text']
                if any(char.isdigit() for char in text):
                    # Kiểm tra xem có phải là giá gốc không (thường có gạch ngang hoặc màu xám)
                    style = candidate['element'].get_attribute('style') or ''
                    class_name = candidate['class'].lower()

                    is_original_price = (
                        'line-through' in style or
                        'text-decoration' in style or
                        'origin' in class_name or
                        'original' in class_name or
                        'old' in class_name
                    )

                    if is_original_price and not product_data["original_price_text"]:
                        product_data["original_price_text"] = text
                        product_data["original_price"] = self.normalize_price(text)
                    elif not is_original_price and not product_data["price_text"]:
                        product_data["price_text"] = text
                        product_data["price"] = self.normalize_price(text)

            # 5. Lấy discount
            discount_patterns = [r'[-]?\d+%', r'giảm\s+\d+%', r'off\s+\d+%']
            for elem in all_elements:
                try:
                    text = elem.text.strip()
                    for pattern in discount_patterns:
                        if re.search(pattern, text, re.IGNORECASE):
                            product_data["discount"] = text
                            break
                    if product_data["discount"]:
                        break
                except:
                    continue

            # 6. Lấy rating
            for elem in all_elements:
                try:
                    text = elem.text.strip()
                    # Tìm pattern rating (4.5, 4,5, etc.)
                    rating_match = re.search(r'(\d+[.,]\d+|\d+)\s*(sao|star|⭐)?', text.lower())
                    if rating_match:
                        rating_num = float(rating_match.group(1).replace(',', '.'))
                        if 0 <= rating_num <= 5:
                            product_data["rating"] = rating_num
                            break
                except:
                    continue

            # 7. Lấy số đánh giá và đã bán
            for elem in all_elements:
                try:
                    text = elem.text.strip()
                    # Số đánh giá
                    if 'đánh giá' in text.lower() or 'review' in text.lower():
                        product_data["review_count"] = text
                    # Số đã bán
                    elif 'đã bán' in text.lower() or 'sold' in text.lower():
                        product_data["sold_count"] = text
                    # Location
                    elif any(city in text for city in ['TP.HCM', 'Hà Nội', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ']):
                        product_data["location"] = text
                except:
                    continue

            # Debug kết quả
            if element_index < 3:
                print(f"   📊 EXTRACTED DATA:")
                print(f"      Title: {product_data['title'][:50]}...")
                print(f"      Price: {product_data['price_text']} ({product_data['price']})")
                print(f"      Original: {product_data['original_price_text']} ({product_data['original_price']})")
                print(f"      Discount: {product_data['discount']}")
                print(f"      Rating: {product_data['rating']}")
                print(f"      URL: {product_data['url'][:80]}...")

            # Chỉ trả về nếu có đủ thông tin cơ bản
            if product_data["title"] and product_data["url"] and len(product_data["title"]) > 10:
                return product_data
            else:
                return None

        except Exception as e:
            print(f"❌ Lỗi khi trích xuất sản phẩm {element_index}: {e}")
            import traceback
            traceback.print_exc()
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

    def get_products_from_page(self, page_num=1, max_products=20) -> List[Dict[str, Any]]:
        """Lấy danh sách sản phẩm từ một trang với debug chi tiết"""
        if page_num > 1:
            url = f"{self.base_url}?page={page_num}"
        else:
            url = self.base_url

        print(f"\n🔍 Đang crawl trang {page_num}: {url}")

        try:
            # Mở trang
            self.driver.get(url)
            self.random_delay(3, 5)

            # Chờ trang load
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                print("✅ Trang đã load xong")
            except TimeoutException:
                print("⚠️ Timeout chờ trang load")
                return []

            # Cuộn trang để load thêm sản phẩm
            self.scroll_and_wait()

            # Chờ thêm cho JS chạy xong
            print("⏱️ Chờ JavaScript load content...")
            time.sleep(5)

            # Debug HTML structure
            print(f"\n🔍 ANALYZING PAGE STRUCTURE...")
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            print(f"   📊 Page stats:")
            print(f"      Total links: {len(soup.find_all('a', href=True))}")
            product_links = [a for a in soup.find_all('a', href=True) if '/products/' in a.get('href', '')]
            print(f"      Product links: {len(product_links)}")
            print(f"      Elements with data-spm: {len(soup.find_all(attrs={'data-spm': True}))}")
            print(f"      Text containing '₫': {len([elem for elem in soup.find_all(text=True) if '₫' in str(elem)])}")

            # Thử các selectors khác nhau
            products = []

            # Các selectors để tìm product containers
            container_selectors = [
                "[data-qa-locator*='product']",
                "[data-spm*='product']",
                "div[data-spm]",
                "[class*='gridItem']",
                "[class*='product']",
                "[class*='item']",
                "a[href*='/products/']"
            ]

            for selector in container_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"\n   🎯 Testing selector: '{selector}' -> {len(elements)} elements")

                    if elements:
                        processed = 0
                        for i, element in enumerate(elements):
                            if processed >= max_products:
                                break

                            try:
                                # Kiểm tra element có chứa product link không
                                if selector == "a[href*='/products/']":
                                    # Đây là link trực tiếp, tìm container cha
                                    container = element.find_element(By.XPATH, "./ancestor::div[position()<=3]")
                                else:
                                    # Kiểm tra có link product bên trong không
                                    links = element.find_elements(By.CSS_SELECTOR, "a[href*='/products/']")
                                    if not links:
                                        continue
                                    container = element

                                product_data = self.extract_product_data_improved(container, i)
                                if product_data:
                                    products.append(product_data)
                                    processed += 1
                                    print(f"      ✅ Product {processed}: {product_data['title'][:50]}... | Price: {product_data['price_text']}")

                            except Exception as e:
                                continue

                        if products:
                            print(f"   ✅ Success with selector: '{selector}' - {len(products)} products")
                            break

                except Exception as e:
                    print(f"   ❌ Error with selector '{selector}': {e}")
                    continue

            print(f"\n📊 Trang {page_num}: Đã trích xuất {len(products)} sản phẩm")

            # In thống kê nhanh
            prices = [p for p in products if p.get('price')]
            if prices:
                print(f"   💰 Sản phẩm có giá: {len(prices)}/{len(products)}")

            return products

        except Exception as e:
            print(f"❌ Lỗi khi crawl trang {page_num}: {e}")
            import traceback
            traceback.print_exc()
            return []

    def crawl_multiple_pages(self, max_pages=2, max_products_per_page=10) -> List[Dict[str, Any]]:
        """Crawl nhiều trang với số lượng ít để test"""
        all_products = []

        for page in range(1, max_pages + 1):
            try:
                products = self.get_products_from_page(page, max_products_per_page)
                all_products.extend(products)

                if page < max_pages:
                    print(f"⏱️ Nghỉ {random.randint(5, 10)} giây trước khi chuyển trang...")
                    self.random_delay(5, 10)

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

        print(f"\n📊 TỔNG KẾT: {len(unique_products)} sản phẩm unique từ {max_pages} trang")
        return unique_products

    def save_to_files(self, products: List[Dict[str, Any]], filename_prefix="lazada_products"):
        """Lưu dữ liệu vào file"""
        if not products:
            print("⚠️ Không có dữ liệu để lưu")
            return

        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # JSON
        json_file = output_dir / f"{filename_prefix}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        # JSONL
        jsonl_file = output_dir / f"{filename_prefix}.jsonl"
        with open(jsonl_file, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False) + '\n')

        # CSV
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
    crawler = LazadaCrawlerImproved(headless=False)

    try:
        print("🚀 Bắt đầu crawl Lazada với debug mode...")

        # Test với ít trang và ít sản phẩm để debug
        products = crawler.crawl_multiple_pages(max_pages=1, max_products_per_page=10)

        if products:
            crawler.save_to_files(products)

            print(f"\n📈 Thống kê chi tiết:")
            print(f"   📱 Tổng sản phẩm: {len(products)}")

            # Thống kê giá
            products_with_price = [p for p in products if p.get('price')]
            print(f"   💰 Sản phẩm có giá: {len(products_with_price)}/{len(products)}")

            if products_with_price:
                prices = [p['price'] for p in products_with_price]
                print(f"   💰 Giá thấp nhất: {min(prices):,} VND")
                print(f"   💰 Giá cao nhất: {max(prices):,} VND")
                print(f"   💰 Giá trung bình: {sum(prices)//len(prices):,} VND")

            # Thống kê các field khác
            products_with_rating = [p for p in products if p.get('rating')]
            print(f"   ⭐ Sản phẩm có rating: {len(products_with_rating)}/{len(products)}")

            products_with_discount = [p for p in products if p.get('discount')]
            print(f"   🎯 Sản phẩm có discount: {len(products_with_discount)}/{len(products)}")

            # Hiển thị chi tiết
            print(f"\n🛍️ Chi tiết sản phẩm:")
            for i, product in enumerate(products[:5]):
                print(f"   {i+1}. {product.get('title', 'N/A')[:60]}...")
                print(f"      💰 Giá: {product.get('price_text', 'N/A')} ({product.get('price', 'N/A')})")
                if product.get('original_price_text'):
                    print(f"      🏷️ Giá gốc: {product.get('original_price_text')} ({product.get('original_price')})")
                if product.get('discount'):
                    print(f"      🎯 Giảm giá: {product.get('discount')}")
                if product.get('rating'):
                    print(f"      ⭐ Rating: {product.get('rating')}")
                print(f"      🔗 URL: {product.get('url', 'N/A')[:80]}...")
                print()
        else:
            print("❌ Không crawl được sản phẩm nào")
            print("💡 Có thể Lazada đã thay đổi cấu trúc hoặc chặn crawler")

    except KeyboardInterrupt:
        print("\n⛔ Dừng crawling theo yêu cầu người dùng")
    except Exception as e:
        print(f"❌ Lỗi trong quá trình crawling: {e}")
        import traceback
        traceback.print_exc()
    finally:
        crawler.close()

if __name__ == "__main__":
    main()