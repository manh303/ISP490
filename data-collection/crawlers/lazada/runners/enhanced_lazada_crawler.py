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

class EnhancedLazadaCrawler:
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
            print("[OK] Chrome driver khoi tao thanh cong")
        except Exception as e:
            print(f"[ERROR] Loi khoi tao Chrome driver: {e}")
            raise

    def random_delay(self, min_delay=1, max_delay=3):
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def scroll_page(self, scroll_pause_time=2, max_scrolls=5):
        print(f"[SCROLL] Dang cuon trang de load san pham...")
        for i in range(max_scrolls):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause_time)

    def extract_product_details(self, product_url: str) -> Dict[str, Any]:
        """Trích xuất shop_name và reviews từ product detail page"""
        detail_data = {
            "shop_name": "",
            "shop_rating": "",
            "shop_followers": "",
            "reviews": [],
            "review_summary": {
                "total_reviews": 0,
                "average_rating": 0.0,
                "rating_distribution": {}
            }
        }

        try:
            print(f"[DETAIL] Getting product details from: {product_url[:80]}...")

            # Navigate to product detail page
            self.driver.get(product_url)
            self.random_delay(2, 4)

            # Wait for page to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                print("[WARN] Product detail page timeout")
                return detail_data

            # Extract shop information
            self.extract_shop_info(detail_data)

            # Extract reviews
            self.extract_reviews(detail_data)

            return detail_data

        except Exception as e:
            print(f"[ERROR] Error extracting product details: {e}")
            return detail_data

    def extract_shop_info(self, detail_data: Dict[str, Any]):
        """Extract shop information from product detail page"""
        try:
            # Shop name từ selector: .seller-name-v2__detail-name
            shop_name_selectors = [
                ".seller-name-v2__detail-name",
                ".seller-name-retail-v2 a",
                "[class*='seller-name']",
                "[class*='shop-name']"
            ]

            for selector in shop_name_selectors:
                try:
                    shop_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    shop_name = shop_element.text.strip()
                    if shop_name and len(shop_name) > 2:
                        detail_data["shop_name"] = shop_name
                        print(f" Found shop name: {shop_name}")
                        break
                except NoSuchElementException:
                    continue

            # Shop rating từ selector: .ratings-tag (Seller Ratings X%)
            try:
                rating_elements = self.driver.find_elements(By.CSS_SELECTOR, ".ratings-tag")
                for elem in rating_elements:
                    text = elem.text.strip()
                    if "Seller Ratings" in text:
                        rating_match = re.search(r'(\d+)%', text)
                        if rating_match:
                            detail_data["shop_rating"] = rating_match.group(1) + "%"
                            print(f" Found shop rating: {detail_data['shop_rating']}")
                            break
            except NoSuchElementException:
                pass

            # Shop badges/achievements
            try:
                badge_elements = self.driver.find_elements(By.CSS_SELECTOR, ".selling-tag-text")
                badges = []
                for elem in badge_elements:
                    badge_text = elem.text.strip()
                    if badge_text:
                        badges.append(badge_text)

                if badges:
                    detail_data["shop_badges"] = badges
                    print(f" Found shop badges: {', '.join(badges)}")
            except NoSuchElementException:
                pass

        except Exception as e:
            print(f"[ERROR] Error extracting shop info: {e}")

    def extract_reviews(self, detail_data: Dict[str, Any]):
        """Extract reviews from product detail page"""
        try:
            # Scroll down to load reviews section
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self.random_delay(2, 3)

            # Look for reviews container
            reviews_container_selectors = [
                ".mod-reviews",
                "[class*='review']",
                "[class*='comment']"
            ]

            reviews_container = None
            for selector in reviews_container_selectors:
                try:
                    reviews_container = self.driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except NoSuchElementException:
                    continue

            if not reviews_container:
                print("[WARN] Reviews container not found")
                return

            # Extract individual reviews
            review_items = reviews_container.find_elements(By.CSS_SELECTOR, ".item")
            reviews = []

            for i, item in enumerate(review_items[:10]):  # Limit to first 10 reviews
                try:
                    review_data = {
                        "reviewer": "",
                        "rating": 0,
                        "time": "",
                        "content": "",
                        "sku_info": "",
                        "helpful_count": 0
                    }

                    # Extract reviewer name
                    try:
                        reviewer_elem = item.find_element(By.CSS_SELECTOR, ".reviewer")
                        review_data["reviewer"] = reviewer_elem.text.strip()
                    except NoSuchElementException:
                        pass

                    # Extract review time
                    try:
                        time_elem = item.find_element(By.CSS_SELECTOR, ".time")
                        review_data["time"] = time_elem.text.strip()
                    except NoSuchElementException:
                        pass

                    # Extract rating (count stars)
                    try:
                        star_container = item.find_element(By.CSS_SELECTOR, ".container-star")
                        stars = star_container.find_elements(By.CSS_SELECTOR, ".star")
                        review_data["rating"] = len(stars)
                    except NoSuchElementException:
                        pass

                    # Extract review content
                    try:
                        content_elem = item.find_element(By.CSS_SELECTOR, ".item-content-main-content-reviews-item span")
                        review_data["content"] = content_elem.text.strip()
                    except NoSuchElementException:
                        pass

                    # Extract SKU info (color, size, etc.)
                    try:
                        sku_items = item.find_elements(By.CSS_SELECTOR, ".skuInfo-item")
                        sku_info = []
                        for sku_item in sku_items:
                            sku_text = sku_item.text.strip()
                            if sku_text:
                                sku_info.append(sku_text)
                        review_data["sku_info"] = "; ".join(sku_info)
                    except NoSuchElementException:
                        pass

                    # Extract helpful count
                    try:
                        helpful_elem = item.find_element(By.CSS_SELECTOR, ".item-content-like-content-text")
                        helpful_text = helpful_elem.text.strip()
                        helpful_match = re.search(r'Helpful\((\d+)\)', helpful_text)
                        if helpful_match:
                            review_data["helpful_count"] = int(helpful_match.group(1))
                    except NoSuchElementException:
                        pass

                    if review_data["content"] or review_data["rating"] > 0:
                        reviews.append(review_data)
                        print(f"📝 Review {i+1}: {review_data['rating']}⭐ - {review_data['content'][:50]}...")

                except Exception as e:
                    print(f"[ERROR] Error extracting review {i+1}: {e}")

            detail_data["reviews"] = reviews
            detail_data["review_summary"]["total_reviews"] = len(reviews)

            # Calculate average rating
            if reviews:
                ratings = [r["rating"] for r in reviews if r["rating"] > 0]
                if ratings:
                    detail_data["review_summary"]["average_rating"] = sum(ratings) / len(ratings)

            print(f"[INFO] Extracted {len(reviews)} reviews with avg rating {detail_data['review_summary']['average_rating']:.1f}")

        except Exception as e:
            print(f"[ERROR] Error extracting reviews: {e}")

    def extract_product_data(self, product_element, category) -> Optional[Dict[str, Any]]:
        """Extract product data from listing page (fixed shop_name issue)"""
        try:
            product_data = {
                "category": category,
                "title": "",
                "url": "",
                "image_url": "",  # Changed from 'image' to 'image_url'
                "price_text": "",
                "price": None,
                "original_price_text": "",
                "original_price": None,
                "discount": "",
                "rating": None,
                "review_count": "",
                "sold_count": "",  # Separated from review_count
                "location": "",
                "shop_name": "",  # Will be filled from detail page
                "shop_rating": "",
                "shop_badges": [],
                "reviews": [],
                "review_summary": {}
            }

            # Extract product URL and title
            try:
                link_element = product_element.find_element(By.CSS_SELECTOR, "div.RfADt > a[href]")
                href = link_element.get_attribute("href")
                if href:
                    if href.startswith("//"):
                        href = "https:" + href
                    elif href.startswith("/"):
                        href = "https://www.lazada.vn" + href
                    product_data["url"] = href

                    title_text = link_element.text.strip()
                    if title_text:
                        product_data["title"] = title_text
            except NoSuchElementException:
                pass

            # Fallback title from img alt
            if not product_data["title"]:
                try:
                    img_element = product_element.find_element(By.CSS_SELECTOR, "img[alt]")
                    alt_text = img_element.get_attribute("alt")
                    if alt_text:
                        product_data["title"] = alt_text.strip()
                except NoSuchElementException:
                    pass

            # Extract image URL (not base64)
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

                        # Only take actual URLs, not base64
                        if img_src and not img_src.startswith("data:") and "lazcdn.com" in img_src:
                            product_data["image_url"] = img_src
                            break
                    except NoSuchElementException:
                        continue

                # Try data-src for lazy loading
                if not product_data["image_url"]:
                    try:
                        img_element = product_element.find_element(By.CSS_SELECTOR, "img[data-src*='lazcdn.com']")
                        data_src = img_element.get_attribute("data-src")
                        if data_src:
                            product_data["image_url"] = data_src
                    except NoSuchElementException:
                        pass

            except NoSuchElementException:
                pass

            # Extract price
            try:
                price_element = product_element.find_element(By.CSS_SELECTOR, "span.ooOxS")
                price_text = price_element.text.strip()
                if price_text:
                    product_data["price_text"] = price_text
                    product_data["price"] = self.normalize_price(price_text)
            except NoSuchElementException:
                # Try alternative price selectors
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

            # Extract location
            try:
                location_element = product_element.find_element(By.CSS_SELECTOR, "span.oa6ri")
                product_data["location"] = location_element.text.strip()
            except NoSuchElementException:
                pass

            # Extract discount
            try:
                discount_element = product_element.find_element(By.CSS_SELECTOR, ".ic-dynamic-badge")
                product_data["discount"] = discount_element.text.strip()
            except NoSuchElementException:
                pass

            # Extract rating (count filled stars)
            try:
                rating_container = product_element.find_element(By.CSS_SELECTOR, ".mdmmT._32vUv")
                filled_stars = rating_container.find_elements(By.CSS_SELECTOR, "i._9-ogB.Dy1nx")
                star_count = len(filled_stars)
                if star_count > 0:
                    product_data["rating"] = float(star_count)
            except NoSuchElementException:
                pass

            # Extract review count and sold count (FIXED)
            try:
                # Review count from span.qzqFw (usually in parentheses)
                try:
                    qzqfw_element = product_element.find_element(By.CSS_SELECTOR, "span.qzqFw")
                    qzqfw_text = qzqfw_element.text.strip()
                    if qzqfw_text:
                        # Extract number from parentheses: "(123)" -> "123"
                        review_match = re.search(r'\((\d+)\)', qzqfw_text)
                        if review_match:
                            product_data["review_count"] = review_match.group(1)
                        elif qzqfw_text.isdigit():
                            product_data["review_count"] = qzqfw_text
                except NoSuchElementException:
                    pass

                # Sold count (separate from review count)
                if not product_data["review_count"]:
                    # Look for sold count patterns
                    all_text = product_element.text
                    sold_match = re.search(r'(\d+(?:\.\d+)?[kK]?)\s*(?:đã bán|sold)', all_text, re.IGNORECASE)
                    if sold_match:
                        sold_str = sold_match.group(1)
                        if sold_str.lower().endswith('k'):
                            sold_num = float(sold_str[:-1]) * 1000
                            product_data["sold_count"] = str(int(sold_num))
                        else:
                            product_data["sold_count"] = sold_str

            except Exception as e:
                print(f"[ERROR] Error getting review/sold count: {e}")

            # Return basic product data (shop details will be added later)
            if product_data["title"] and product_data["url"]:
                return product_data
            else:
                return None

        except Exception as e:
            print(f"[ERROR] Error extracting product data: {e}")
            return None

    def normalize_price(self, price_text: str) -> Optional[int]:
        if not price_text:
            return None

        digits = re.sub(r'[^\d]', '', price_text)
        if digits:
            try:
                return int(digits)
            except ValueError:
                return None
        return None

    def build_paginated_url(self, base_url: str, page: int) -> str:
        if page == 1:
            return base_url

        if '?' in base_url:
            return f"{base_url}&page={page}"
        else:
            return f"{base_url}?page={page}"

    def get_products_from_page(self, category_name: str, category_url: str, page_num=1, max_products=40, get_details=True) -> List[Dict[str, Any]]:
        """Get products from listing page with optional detail extraction"""
        url = self.build_paginated_url(category_url, page_num)

        print(f"\n[CRAWL] Crawling {category_name} - Page {page_num}: {url}")

        try:
            self.driver.get(url)
            self.random_delay(2, 4)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                print("[WARN] Page load timeout")
                return []

            self.scroll_page()

            products = []

            # Find product containers
            try:
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.Bm3ON[data-qa-locator="product-item"]')
                print(f" Found {len(product_elements)} product containers")

                for i, element in enumerate(product_elements[:max_products]):
                    print(f"\n Processing product {i+1}/{min(len(product_elements), max_products)}...")

                    # Extract basic product data
                    product_data = self.extract_product_data(element, category_name)

                    if product_data:
                        print(f"[OK] Basic data: {product_data['title'][:50]}... - {product_data['price_text']}")

                        # Extract detailed information from product page
                        if get_details and product_data["url"]:
                            print(f"[DETAIL] Getting detailed info...")
                            detail_data = self.extract_product_details(product_data["url"])

                            # Merge detail data into product data
                            product_data.update(detail_data)

                            # Random delay between product details
                            self.random_delay(1, 3)

                        products.append(product_data)

                        print(f"[OK] [{i+1}] Complete: {product_data['title'][:40]}... | Shop: {product_data.get('shop_name', 'N/A')} | Reviews: {len(product_data.get('reviews', []))}")
                    else:
                        print(f"[ERROR] [{i+1}] Failed to extract product data")

            except Exception as e:
                print(f"[ERROR] Error finding product containers: {e}")

            print(f"\n[INFO] Page {page_num}: Extracted {len(products)} products")
            return products

        except Exception as e:
            print(f"[ERROR] Error crawling page {page_num}: {e}")
            return []

    def crawl_category(self, category_name: str, category_url: str, max_pages=60, max_products_per_page=40, get_details=True) -> List[Dict[str, Any]]:
        """Crawl a specific category"""
        print(f"\n === CRAWLING {category_name.upper()} ===")
        all_products = []

        for page in range(1, max_pages + 1):
            try:
                products = self.get_products_from_page(
                    category_name,
                    category_url,
                    page,
                    max_products_per_page,
                    get_details
                )
                all_products.extend(products)

                if page < max_pages:
                    print(f"⏸️ Resting between pages...")
                    self.random_delay(3, 6)

            except Exception as e:
                print(f"[ERROR] Error crawling page {page}: {e}")
                continue

        print(f"\n[SUCCESS] === {category_name}: Total {len(all_products)} products ===")
        return all_products

    def save_to_files(self, products: List[Dict[str, Any]], filename_prefix="enhanced_lazada"):
        """Save products to multiple file formats"""
        if not products:
            print("[ERROR] No data to save")
            return

        output_dir = Path("../../outputs")
        output_dir.mkdir(exist_ok=True)

        timestamp = int(time.time())

        # 1. Save enhanced JSON with all details
        json_file = output_dir / f"{filename_prefix}_complete_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        # 2. Save CSV with main fields
        csv_file = output_dir / f"{filename_prefix}_main_{timestamp}.csv"
        if products:
            main_fields = [
                'category', 'title', 'url', 'image_url', 'price_text', 'price',
                'rating', 'review_count', 'sold_count', 'location', 'shop_name',
                'shop_rating', 'discount'
            ]

            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=main_fields)
                writer.writeheader()
                for product in products:
                    row = {field: product.get(field, '') for field in main_fields}
                    writer.writerow(row)

        # 3. Save reviews separately
        reviews_file = output_dir / f"{filename_prefix}_reviews_{timestamp}.json"
        all_reviews = []
        for product in products:
            for review in product.get('reviews', []):
                review_with_product = {
                    'product_title': product.get('title', ''),
                    'product_url': product.get('url', ''),
                    'shop_name': product.get('shop_name', ''),
                    **review
                }
                all_reviews.append(review_with_product)

        if all_reviews:
            with open(reviews_file, 'w', encoding='utf-8') as f:
                json.dump(all_reviews, f, ensure_ascii=False, indent=2)

        print(f"\n[SAVE] Files saved:")
        print(f"   Complete JSON: {json_file}")
        print(f"  [CSV] Main CSV: {csv_file}")
        print(f"   Reviews JSON: {reviews_file}")
        print(f"   Total products: {len(products)}")
        print(f"   Total reviews: {len(all_reviews)}")

        # Statistics
        shop_names = [p.get('shop_name', '') for p in products if p.get('shop_name')]
        unique_shops = len(set(shop_names))
        print(f"   Unique shops: {unique_shops}")

    def close(self):
        if self.driver:
            self.driver.quit()
            print("[CLOSE] Browser closed")

def main():
    """Main function to run the enhanced crawler"""
    crawler = EnhancedLazadaCrawler()

    try:
        print(" Starting Enhanced Lazada Crawler...")
        print(" Features:")
        print("  [OK] Fixed shop_name extraction from product details")
        print("  [OK] Extract reviews from product pages")
        print("  [OK] Image URLs only (no base64)")
        print("  [OK] Separate review_count and sold_count")
        print("")

        # Test with one category first
        category_name = "smartphones"
        category_url = crawler.categories[category_name]

        print(f" Testing with {category_name} category...")

        # Crawl with detailed information
        products = crawler.crawl_category(
            category_name,
            category_url,
            max_pages=2,  # Start with 2 pages for testing
            max_products_per_page=5,  # 5 products per page for testing
            get_details=True  # Get shop details and reviews
        )

        if products:
            crawler.save_to_files(products, f"enhanced_lazada_{category_name}")

            # Print sample data
            print(f"\n[RESULTS] Sample Results:")
            for i, product in enumerate(products[:3]):
                print(f"\n Product {i+1}:")
                print(f"  [PRODUCT] Title: {product.get('title', 'N/A')[:60]}...")
                print(f"   Price: {product.get('price_text', 'N/A')}")
                print(f"   Shop: {product.get('shop_name', 'N/A')}")
                print(f"   Shop Rating: {product.get('shop_rating', 'N/A')}")
                print(f"   Reviews: {len(product.get('reviews', []))}")
                print(f"   URL: {product.get('url', 'N/A')[:60]}...")

        else:
            print("[ERROR] No products crawled")

    except KeyboardInterrupt:
        print("\n Stopped by user")
    except Exception as e:
        print(f"[ERROR] Error during crawling: {e}")
    finally:
        crawler.close()

if __name__ == "__main__":
    main()