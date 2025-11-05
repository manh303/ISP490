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

class MultiCategoryLazadaCrawler:
    def __init__(self):
        # Danh sach cac categories de crawl
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
        chrome_options = Options()
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            print("Chrome driver khoi tao thanh cong")
        except Exception as e:
            print(f"Loi khoi tao Chrome driver: {e}")
            raise

    def random_delay(self, min_delay=1, max_delay=3):
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def scroll_page(self, scroll_pause_time=2, max_scrolls=5):
        print(f"  Dang cuon trang de load san pham...")
        for i in range(max_scrolls):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause_time)

    def extract_product_data(self, product_element, category) -> Optional[Dict[str, Any]]:
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

            # Lay link san pham tu div.RfADt > a
            try:
                link_element = product_element.find_element(By.CSS_SELECTOR, "div.RfADt > a[href]")
                href = link_element.get_attribute("href")
                if href:
                    if href.startswith("//"):
                        href = "https:" + href
                    elif href.startswith("/"):
                        href = "https://www.lazada.vn" + href
                    product_data["url"] = href

                    # Lay title tu text content cua link
                    title_text = link_element.text.strip()
                    if title_text:
                        product_data["title"] = title_text
            except NoSuchElementException:
                pass

            # Neu khong co title tu link, thu lay tu img alt
            if not product_data["title"]:
                try:
                    img_element = product_element.find_element(By.CSS_SELECTOR, "img[alt]")
                    alt_text = img_element.get_attribute("alt")
                    if alt_text:
                        product_data["title"] = alt_text.strip()
                except NoSuchElementException:
                    pass

            # Lay anh san pham
            try:
                img_selectors = [
                    "img[type='product']",
                    "img[alt*='']",
                    ".picture-wrapper img",
                    "img[src*='lazcdn.com']",
                    "img:first-of-type"
                ]

                for selector in img_selectors:
                    try:
                        img_element = product_element.find_element(By.CSS_SELECTOR, selector)
                        img_src = img_element.get_attribute("src")

                        # Uu tien URL thuc thay vi base64
                        if img_src and not img_src.startswith("data:"):
                            product_data["image"] = img_src
                            break
                        elif img_src and img_src.startswith("data:") and not product_data["image"]:
                            product_data["image"] = img_src
                    except NoSuchElementException:
                        continue

                # Neu van chua co anh, thu data-src (lazy loading)
                if not product_data["image"]:
                    try:
                        img_element = product_element.find_element(By.CSS_SELECTOR, "img[data-src]")
                        data_src = img_element.get_attribute("data-src")
                        if data_src:
                            product_data["image"] = data_src
                    except NoSuchElementException:
                        pass

            except NoSuchElementException:
                pass

            # Lay gia hien tai tu span.ooOxS
            try:
                price_element = product_element.find_element(By.CSS_SELECTOR, "span.ooOxS")
                price_text = price_element.text.strip()
                if price_text:
                    product_data["price_text"] = price_text
                    product_data["price"] = self.normalize_price(price_text)
            except NoSuchElementException:
                pass

            # Neu khong tim thay gia, thu tim cac selector khac
            if not product_data["price_text"]:
                price_selectors = [
                    "span[class*='price']",
                    "div[class*='price']",
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

                    if product_data["price_text"]:
                        break

            # Lay location tu span.oa6ri
            try:
                location_element = product_element.find_element(By.CSS_SELECTOR, "span.oa6ri")
                product_data["location"] = location_element.text.strip()
            except NoSuchElementException:
                pass

            # Lay discount tu voucher badge
            try:
                discount_element = product_element.find_element(By.CSS_SELECTOR, ".ic-dynamic-badge")
                product_data["discount"] = discount_element.text.strip()
            except NoSuchElementException:
                pass

            # Lay rating (dem so sao day trong Lazada)
            try:
                rating_container = None
                rating_selectors = [
                    ".mdmmT._32vUv",
                    "div:has(i._9-ogB)",
                    "*:has(.qzqFw)"
                ]

                for selector in rating_selectors:
                    try:
                        rating_container = product_element.find_element(By.CSS_SELECTOR, selector)
                        break
                    except NoSuchElementException:
                        continue

                if rating_container:
                    try:
                        filled_stars = rating_container.find_elements(By.CSS_SELECTOR, "i._9-ogB.Dy1nx")
                        star_count = len(filled_stars)
                        if star_count > 0:
                            product_data["rating"] = float(star_count)
                    except NoSuchElementException:
                        pass

            except Exception:
                pass

            # Lay review_count / sold_count
            try:
                # Uu tien lay tu span.qzqFw (Lazada review count)
                try:
                    qzqfw_element = product_element.find_element(By.CSS_SELECTOR, "span.qzqFw")
                    qzqfw_text = qzqfw_element.text.strip()
                    if qzqfw_text:
                        # Extract so tu trong ngoac: "(2)" -> "2"
                        review_match = re.search(r'\((\d+)\)', qzqfw_text)
                        if review_match:
                            product_data["review_count"] = review_match.group(1)
                        elif qzqfw_text.isdigit():
                            product_data["review_count"] = qzqfw_text
                except NoSuchElementException:
                    pass

                # Fallback: tim trong tat ca text
                if not product_data["review_count"]:
                    all_text = product_element.text
                    review_match = re.search(r'\((\d+)\)', all_text)
                    if review_match:
                        product_data["review_count"] = review_match.group(1)

            except Exception:
                pass

            # Lay shop_name (simplified)
            try:
                shop_selectors = [
                    "[class*='shop']",
                    "[class*='store']",
                    "[class*='seller']",
                    "span.oa6ri ~ *",
                    "span.oa6ri + *"
                ]

                for selector in shop_selectors:
                    try:
                        shop_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                        for elem in shop_elements:
                            text = elem.text.strip()
                            if text and 5 <= len(text) <= 50:
                                skip_words = ['voucher', 'giam', 'gia', '₫', 'shipping', 'free']
                                if not any(skip_word in text.lower() for skip_word in skip_words):
                                    if not re.search(r'\d+.*₫|\d+.*đ', text):
                                        product_data["shop_name"] = text
                                        break
                    except NoSuchElementException:
                        continue
                    if product_data["shop_name"]:
                        break

            except Exception:
                pass

            # Chi tra ve neu co thong tin co ban
            if product_data["title"] and product_data["url"]:
                return product_data
            else:
                return None

        except Exception as e:
            return None

    def normalize_price(self, price_text: str) -> Optional[int]:
        if not price_text:
            return None

        # Loai bo tat ca ky tu khong phai so
        digits = re.sub(r'[^\d]', '', price_text)
        if digits:
            try:
                return int(digits)
            except ValueError:
                return None
        return None

    def build_paginated_url(self, base_url: str, page: int) -> str:
        """Build paginated URL handling existing query parameters"""
        if page == 1:
            return base_url

        if '?' in base_url:
            # URL already has query parameters, append with &
            return f"{base_url}&page={page}"
        else:
            # URL has no query parameters, append with ?
            return f"{base_url}?page={page}"

    def crawl_category(self, category_name: str, category_url: str, max_pages=5, max_products_per_page=20) -> List[Dict[str, Any]]:
        print(f"\n=== Crawling {category_name.upper()} ===")
        all_products = []

        for page in range(1, max_pages + 1):
            try:
                url = self.build_paginated_url(category_url, page)

                print(f"  Trang {page}: {url}")

                self.driver.get(url)
                self.random_delay(2, 4)

                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                except TimeoutException:
                    print(f"    Timeout cho trang {page}")
                    continue

                self.scroll_page()

                # Tim product containers
                try:
                    product_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.Bm3ON[data-qa-locator="product-item"]')
                    print(f"    Tim thay {len(product_elements)} san pham")

                    page_products = []
                    for i, element in enumerate(product_elements[:max_products_per_page]):
                        product_data = self.extract_product_data(element, category_name)
                        if product_data:
                            page_products.append(product_data)

                    all_products.extend(page_products)
                    print(f"    Da lay {len(page_products)} san pham tu trang {page}")

                except Exception as e:
                    print(f"    Loi tim san pham trang {page}: {e}")

                if page < max_pages:
                    self.random_delay(3, 6)

            except Exception as e:
                print(f"  Loi crawl trang {page}: {e}")
                continue

        print(f"=== {category_name}: Tong cong {len(all_products)} san pham ===")
        return all_products

    def crawl_all_categories(self, max_pages_per_category=5, max_products_per_page=20):
        """Crawl tat ca categories"""
        all_data = []

        for category_name, category_url in self.categories.items():
            try:
                products = self.crawl_category(category_name, category_url, max_pages_per_category, max_products_per_page)
                all_data.extend(products)

                # Nghi giua cac categories
                if category_name != list(self.categories.keys())[-1]:  # Khong nghi sau category cuoi
                    print(f"\nNghi giua cac categories...")
                    self.random_delay(5, 10)

            except Exception as e:
                print(f"Loi crawl category {category_name}: {e}")
                continue

        return all_data

    def save_to_files(self, products: List[Dict[str, Any]], filename_prefix="multi_category_lazada"):
        if not products:
            print("Khong co du lieu de luu")
            return

        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # 1. Luu full data JSON
        json_file = output_dir / f"{filename_prefix}_full.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        # 2. Luu full data CSV
        csv_file = output_dir / f"{filename_prefix}_full.csv"
        if products:
            fieldnames = products[0].keys()
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(products)

        # 3. Luu chi URLs CSV
        urls_csv = output_dir / f"{filename_prefix}_urls.csv"
        with open(urls_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['category', 'title', 'url', 'price'])
            for product in products:
                writer.writerow([
                    product.get('category', ''),
                    product.get('title', ''),
                    product.get('url', ''),
                    product.get('price_text', '')
                ])

        # 4. Luu URLs theo category
        category_urls = {}
        for product in products:
            category = product.get('category', 'unknown')
            if category not in category_urls:
                category_urls[category] = []
            category_urls[category].append({
                'title': product.get('title', ''),
                'url': product.get('url', ''),
                'price': product.get('price_text', ''),
                'rating': product.get('rating', ''),
                'shop': product.get('shop_name', '')
            })

        for category, urls in category_urls.items():
            category_file = output_dir / f"{filename_prefix}_{category}_urls.csv"
            with open(category_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['title', 'url', 'price', 'rating', 'shop'])
                for item in urls:
                    writer.writerow([item['title'], item['url'], item['price'], item['rating'], item['shop']])

        print(f"\nDa luu du lieu:")
        print(f"  Full JSON: {json_file}")
        print(f"  Full CSV: {csv_file}")
        print(f"  URLs CSV: {urls_csv}")
        print(f"  Category URLs: {len(category_urls)} files")

        # Thong ke
        print(f"\nThong ke crawl:")
        print(f"  Tong san pham: {len(products)}")
        for category, urls in category_urls.items():
            print(f"  {category}: {len(urls)} san pham")

    def close(self):
        if self.driver:
            self.driver.quit()
            print("Da dong browser")

def main():
    crawler = MultiCategoryLazadaCrawler()

    try:
        print("Bat dau crawl multi-category Lazada...")

        # Crawl tat ca categories
        # max_pages_per_category: so trang moi category
        # max_products_per_page: so san pham moi trang
        products = crawler.crawl_all_categories(max_pages_per_category=3, max_products_per_page=15)

        if products:
            crawler.save_to_files(products)
        else:
            print("Khong crawl duoc san pham nao")

    except KeyboardInterrupt:
        print("\nDung crawling theo yeu cau nguoi dung")
    except Exception as e:
        print(f"Loi trong qua trinh crawling: {e}")
    finally:
        crawler.close()

if __name__ == "__main__":
    main()