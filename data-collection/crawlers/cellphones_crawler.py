#!/usr/bin/env python3
import json
import time
import random
import re
from typing import List, Dict, Optional
from urllib.parse import urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from base_crawler import BaseCrawler


class CellphonesCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(
            source_name="cellphones",
            base_url="https://cellphones.com.vn",
            delay_range=(0.5, 1.2)  # nhịp nhanh hơn một chút
        )

        # Chỉ giữ các danh mục chính hay dùng; có thể mở rộng sau
        self.categories = {
            "mobile": "/mobile.html",
            "tablet": "/tablet.html",
            "laptop": "/laptop.html",
            "phu-kien": "/phu-kien.html",
            "man-hinh": "/man-hinh.html",
            "tivi": "/tivi.html"
        }

        self.stats = {
            "total_products": 0,
            "categories_processed": 0,
            "pages_crawled": 0,
            "start_time": None
        }

    # =====================
    # CATEGORY PAGE (LIST)
    # =====================
    def get_category_url(self, category: str) -> str:
        path = self.categories.get(category, "/mobile.html")
        return f"{self.base_url}{path}"

    def get_product_urls_from_category(self, category: str, max_clicks: int = 60) -> List[str]:
        """
        Chỉ dùng cơ chế 'Xem thêm' cuối trang:
        - Scroll đến cuối -> bấm nút -> đợi số lượng item tăng -> lặp
        - Dừng khi không còn nút hoặc không tăng thêm item sau N lần thử.
        """
        urls: List[str] = []
        with self.setup_driver(headless=True) as driver:
            url = self.get_category_url(category)
            if not self.can_crawl(url):
                self.logger.warning(f"Robots.txt disallows crawling: {url}")
                return []

            self.logger.info(f"[CellphoneS] Crawl category by LOAD MORE only: {category} -> {url}")
            driver.get(url)

            # Chờ block sản phẩm xuất hiện
            product_card_selectors = [
                ".product-info-container a.product__link",
                ".product-info-container .product__link",
                ".product-item a",
                "a[href*='.html'][href*='cellphones.com.vn']"
            ]
            all_cards = self._wait_any_selector_and_collect(driver, product_card_selectors, min_count=8, timeout=20)

            # Vòng lặp bấm 'Xem thêm'
            no_growth_rounds = 0
            clicks = 0
            last_count = len(all_cards)

            while clicks < max_clicks:
                # Thử bấm nút 'Xem thêm'
                clicked = self._click_load_more(driver)

                # Nếu không bấm được, cố scroll sâu thêm 2 lần và thử lại
                if not clicked:
                    self._smooth_scroll_bottom(driver, rounds=2)
                    clicked = self._click_load_more(driver)

                # Nếu vẫn không có, coi như hết dữ liệu
                if not clicked:
                    self.logger.info("[LoadMore] Button not found or disabled -> stop.")
                    break

                clicks += 1

                # Đợi số lượng item tăng
                increased = self._wait_cards_increase(driver, product_card_selectors, last_count, timeout=15)
                all_cards = self._safe_collect_cards(driver, product_card_selectors)
                current_count = len(all_cards)
                self.logger.info(f"[LoadMore] click={clicks} -> items: {last_count} -> {current_count}")

                if increased and current_count > last_count:
                    last_count = current_count
                    no_growth_rounds = 0
                else:
                    no_growth_rounds += 1
                    if no_growth_rounds >= 3:
                        self.logger.info("[LoadMore] No growth after 3 rounds -> stop.")
                        break

                # Nhịp giữa các lần click
                time.sleep(random.uniform(0.4, 0.9))

            # Thu link từ toàn bộ thẻ sản phẩm hiện có
            urls = self._extract_product_urls_from_cards(driver, product_card_selectors)

        # Lọc trùng, hợp lệ
        dedup = []
        seen = set()
        for u in urls:
            if self._is_valid_product_url(u) and u not in seen:
                dedup.append(u)
                seen.add(u)

        self.logger.info(f"[CellphoneS] Total product URLs (unique): {len(dedup)}")
        return dedup

    def _click_load_more(self, driver) -> bool:
        """
        Tìm và click nút 'Xem thêm' với nhiều selector; thử JS click; xử lý overlay.
        """
        # Dọn overlay (nếu có)
        self._dismiss_popups(driver)

        selectors = [
            ".btn-show-more",
            ".button__show-more-product",
            ".load-more-btn",
            "a.btn-show-more",
        ]
        xpaths = [
            "//button[contains(translate(., 'XEM THÊM', 'xem thêm'), 'xem thêm')]",
            "//a[contains(translate(., 'XEM THÊM', 'xem thêm'), 'xem thêm')]",
        ]

        # Ưu tiên CSS trước
        for css in selectors:
            btns = driver.find_elements(By.CSS_SELECTOR, css)
            for b in btns[:1]:
                try:
                    if b.is_displayed() and b.is_enabled():
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                        time.sleep(0.2)
                        try:
                            b.click()
                        except Exception:
                            driver.execute_script("arguments[0].click();", b)
                        time.sleep(0.6)
                        return True
                except StaleElementReferenceException:
                    continue

        # Thử XPath
        for xp in xpaths:
            btns = driver.find_elements(By.XPATH, xp)
            for b in btns[:1]:
                try:
                    if b.is_displayed() and b.is_enabled():
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                        time.sleep(0.2)
                        try:
                            b.click()
                        except Exception:
                            driver.execute_script("arguments[0].click();", b)
                        time.sleep(0.6)
                        return True
                except StaleElementReferenceException:
                    continue

        return False

    def _dismiss_popups(self, driver):
        """Đóng các popup/consent thường gặp (nếu có)."""
        candidates = [
            ".modal-footer .btn, .modal-footer button",
            "button[aria-label='Close']",
            ".btn-close, .close"
        ]
        for css in candidates:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, css)
                for el in els[:2]:
                    if el.is_displayed():
                        driver.execute_script("arguments[0].click();", el)
                        time.sleep(0.2)
            except Exception:
                continue

    def _smooth_scroll_bottom(self, driver, rounds=1):
        for _ in range(rounds):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(0.2, 0.4))

    def _wait_any_selector_and_collect(self, driver, selectors: List[str], min_count=1, timeout=15):
        end = time.time() + timeout
        while time.time() < end:
            cards = self._safe_collect_cards(driver, selectors)
            if len(cards) >= min_count:
                return cards
            self._smooth_scroll_bottom(driver, rounds=1)
        return self._safe_collect_cards(driver, selectors)

    def _safe_collect_cards(self, driver, selectors: List[str]):
        for css in selectors:
            els = driver.find_elements(By.CSS_SELECTOR, css)
            if els and len(els) >= 4:  # ưu tiên selector “đúng” trả về nhiều item
                return els
        # fallback: chọn selector trả về nhiều nhất
        best = []
        for css in selectors:
            els = driver.find_elements(By.CSS_SELECTOR, css)
            if len(els) > len(best):
                best = els
        return best

    def _wait_cards_increase(self, driver, selectors: List[str], last_count: int, timeout=12) -> bool:
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: len(self._safe_collect_cards(d, selectors)) > last_count
            )
            return True
        except TimeoutException:
            return False

    def _extract_product_urls_from_cards(self, driver, selectors: List[str]) -> List[str]:
        hrefs = []
        cards = self._safe_collect_cards(driver, selectors)
        for el in cards:
            try:
                href = el.get_attribute("href")
                if href:
                    hrefs.append(href.strip())
                    continue
                # Nếu selector đang ở <div> bao ngoài, thử tìm <a> con
                a = el.find_element(By.XPATH, ".//a[contains(@href,'.html')]")
                href2 = a.get_attribute("href")
                if href2:
                    hrefs.append(href2.strip())
            except Exception:
                continue
        return hrefs

    def _is_valid_product_url(self, url: str) -> bool:
        if not url or ".html" not in url:
            return False
        if "cellphones.com.vn" not in url:
            return False
        # tránh URL danh mục
        ends = ("/mobile.html", "/tablet.html", "/laptop.html", "/phu-kien.html")
        return not any(url.endswith(x) for x in ends)

    # =====================
    # PRODUCT DETAIL PAGE
    # =====================
    def extract_product_data(self, product_url: str) -> Optional[Dict]:
        if not self.can_crawl(product_url):
            self.logger.warning(f"Robots.txt disallows crawling: {product_url}")
            return None

        with self.setup_driver(headless=True) as driver:
            try:
                self.logger.info(f"[CellphoneS] Extract product: {product_url}")
                driver.get(product_url)
                time.sleep(1.2)  # để JS/JSON-LD sẵn sàng

                # Ưu tiên đọc JSON-LD trước (nhanh, ít “nhiễu”)
                data_ld = self._extract_json_ld(driver)

                # Các trường mặc định
                product_id = self._extract_product_id_from_url(product_url)
                product_name = data_ld.get("name") or self._extract_product_name(driver)
                brand = (data_ld.get("brand") or {}).get("name") if isinstance(data_ld.get("brand"), dict) else data_ld.get("brand")
                category = data_ld.get("@type") if data_ld else self._extract_category(driver)
                image_urls = data_ld.get("image") if isinstance(data_ld.get("image"), list) else ([data_ld.get("image")] if data_ld.get("image") else [])
                description = data_ld.get("description") or self._extract_description(driver)

                offer = data_ld.get("offers") if isinstance(data_ld.get("offers"), dict) else None
                price_current = self._to_int(offer.get("price")) if offer else None
                price_original = None  # sẽ cố đọc thêm bên dưới

                aggr = data_ld.get("aggregateRating") if isinstance(data_ld.get("aggregateRating"), dict) else None
                rating_avg = float(aggr.get("ratingValue")) if aggr and aggr.get("ratingValue") else None
                rating_count = int(float(aggr.get("reviewCount"))) if aggr and aggr.get("reviewCount") else None

                # Fallback nếu thiếu:
                if not product_name or product_name == "N/A":
                    product_name = self._extract_product_name(driver)
                if not brand:
                    brand = self._extract_brand(driver)
                if not category:
                    category = self._extract_category(driver)
                if not image_urls:
                    image_urls = self._extract_images(driver)
                if not description or description == "N/A":
                    description = self._extract_description(driver)
                if price_current is None:
                    price_current, price_original = self._extract_prices(driver)
                else:
                    # cố tìm original
                    _, price_original = self._extract_prices(driver)

                discount_percent = self._calculate_discount(price_current, price_original)

                seller_name, seller_type = ("CellphoneS", "Retail")

                reviews, detailed_description = self._extract_reviews_and_detailed_info(driver)
                if detailed_description and detailed_description != "N/A":
                    if not description or description == "N/A":
                        description = detailed_description
                    else:
                        # nối ngắn gọn
                        description = f"{description} | {detailed_description}"

                product_data = self.create_product_data(
                    product_id=product_id,
                    url=product_url,
                    product_name=product_name or "N/A",
                    brand=brand or "N/A",
                    category=category or "Điện tử",
                    description=description or "N/A",
                    image_urls=image_urls[:5] if image_urls else [],
                    price_current=price_current,
                    price_original=price_original,
                    discount_percent=discount_percent,
                    rating_avg=rating_avg,
                    rating_count=rating_count,
                    sold_count=None,
                    seller_name=seller_name,
                    seller_type=seller_type
                )

                if reviews:
                    product_data["reviews"] = reviews
                    product_data["review_count"] = len(reviews)

                return product_data

            except Exception as e:
                self.logger.error(f"[CellphoneS] Extract error: {e}")
                return None

    def _extract_json_ld(self, driver) -> Dict:
        """
        Lấy script[type="application/ld+json"] rồi chọn node dạng Product
        """
        try:
            scripts = driver.find_elements(By.CSS_SELECTOR, "script[type='application/ld+json']")
            best = {}
            for s in scripts:
                try:
                    txt = s.get_attribute("textContent")
                    if not txt:
                        continue
                    data = json.loads(txt)
                    # Có trang nhúng mảng JSON-LD
                    candidates = data if isinstance(data, list) else [data]
                    for node in candidates:
                        if isinstance(node, dict) and node.get("@type", "").lower() == "product":
                            # Ưu tiên node này
                            best = node
                            break
                    if best:
                        break
                except Exception:
                    continue
            return best or {}
        except Exception:
            return {}

    def _extract_product_id_from_url(self, url: str) -> str:
        try:
            m = re.search(r"-(\d+)\.html", url)
            if m:
                return m.group(1)
            slug = urlparse(url).path.split("/")[-1].replace(".html", "")
            return slug
        except Exception:
            return "unknown"

    def _extract_product_name(self, driver) -> str:
        for css in ["h1.product__name", "h1.title", ".product-title h1", "h1"]:
            el = self.safe_find_element(driver, By.CSS_SELECTOR, css)
            if el:
                t = self.extract_text_safe(el)
                if t and len(t) > 2:
                    return t.strip()
        return "N/A"

    def _extract_brand(self, driver) -> str:
        for css in [".product-brand", ".brand-name", ".manufacturer"]:
            el = self.safe_find_element(driver, By.CSS_SELECTOR, css)
            if el:
                t = self.extract_text_safe(el)
                if t and t.lower() not in ["cellphones", "cps"]:
                    return t.strip()
        # đoán từ tên
        el = self.safe_find_element(driver, By.CSS_SELECTOR, "h1")
        if el:
            name = self.extract_text_safe(el)
            brands = ["iPhone", "Apple", "Samsung", "Xiaomi", "Oppo", "Vivo", "Huawei", "Nokia", "Realme", "OnePlus", "ASUS", "Acer", "LG", "Sony"]
            for b in brands:
                if b.lower() in name.lower():
                    return b
        return "N/A"

    def _extract_category(self, driver) -> str:
        for css in [".breadcrumb a:nth-last-child(2)", ".breadcrumbs a:last-child", ".navigation a:last-child"]:
            el = self.safe_find_element(driver, By.CSS_SELECTOR, css)
            if el:
                t = self.extract_text_safe(el)
                if t and t.lower() not in ["cellphones", "trang chủ", "home"]:
                    return t.strip()
        return "Điện tử"

    def _extract_description(self, driver) -> str:
        blocks = []
        for css in [
            ".product-description", ".product-info-detail", ".product-summary",
            ".short-description", ".product-content", ".product-detail-content",
            ".product-overview", ".content-detail", "[class*='description']",
            "[class*='overview']"
        ]:
            els = driver.find_elements(By.CSS_SELECTOR, css)
            for el in els[:2]:
                t = self.extract_text_safe(el)
                if t and len(t) > 20:
                    blocks.append(t.strip()[:600])
        # specs ngắn
        for css in [".product-specifications", ".product-features", ".tech-specs", ".product-attributes", "[class*='spec']"]:
            els = driver.find_elements(By.CSS_SELECTOR, css)
            for el in els[:1]:
                t = self.extract_text_safe(el)
                if t and len(t) > 20:
                    blocks.append(t.strip()[:300])
        return " | ".join(blocks[:3]) if blocks else "N/A"

    def _extract_images(self, driver) -> List[str]:
        imgs = []
        for css in [
            ".product__image img", ".product-gallery img", ".product-image img",
            ".gallery img", ".slider img", ".product-photos img", "[class*='product-img']",
            "[class*='gallery'] img"
        ]:
            els = driver.find_elements(By.CSS_SELECTOR, css)
            for el in els:
                for attr in ["src", "data-src", "data-lazy", "data-original"]:
                    v = (el.get_attribute(attr) or "").strip()
                    if v and (v.startswith("http") or v.startswith("/")):
                        if v.startswith("/"):
                            v = f"{self.base_url}{v}"
                        if v not in imgs:
                            imgs.append(v)
        return imgs[:5]

    def _extract_prices(self, driver) -> tuple:
        curr_css = [
            ".product__price--show",
            ".box-info__box-price .product__price--show",
            ".price-current", ".final-price", ".special-price",
            "[class*='price-show']", "[class*='current-price']"
        ]
        orig_css = [
            ".product__price--through",
            ".box-info__box-price .product__price--through",
            ".price-old", ".original-price", ".regular-price",
            "[class*='price-through']", "[class*='price-old']"
        ]

        def read_first(css_list):
            for css in css_list:
                el = self.safe_find_element(driver, By.CSS_SELECTOR, css)
                if el:
                    v = self.clean_price(self.extract_text_safe(el))
                    if v:
                        return v
            return None

        return read_first(curr_css), read_first(orig_css)

    def _calculate_discount(self, current: Optional[int], original: Optional[int]) -> Optional[float]:
        if current and original and original > current:
            return round(((original - current) / original) * 100, 2)
        return None

    def _extract_reviews_and_detailed_info(self, driver) -> tuple:
        reviews, detailed = [], ""
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.8)

        for css in [".review-item", ".customer-review", ".user-review", ".comment-item", "[class*='review']"]:
            els = driver.find_elements(By.CSS_SELECTOR, css)[:5]
            for el in els:
                t = self.extract_text_safe(el)
                if t and len(t) > 20:
                    reviews.append(t.strip()[:250])
            if reviews:
                break

        for css in [
            ".product-detail-description", ".product-overview-content", ".product-info-content",
            ".description-content", ".product-content-detail", ".specification-content",
            "[class*='detail-content']", "[class*='overview-content']"
        ]:
            el = self.safe_find_element(driver, By.CSS_SELECTOR, css)
            if el:
                t = self.extract_text_safe(el)
                if t and len(t) > 50:
                    detailed = t.strip()[:1200]
                    break

        return reviews, detailed

    def _to_int(self, s):
        try:
            if s is None:
                return None
            return int(re.sub(r"[^\d]", "", str(s)))
        except Exception:
            return None

    # =====================
    # PUBLIC APIS
    # =====================
    def crawl_category(self, category: str, max_clicks: int = 60, max_products: int = 300) -> List[Dict]:
        self.logger.info(f"Start crawl category: {category}")
        urls = self.get_product_urls_from_category(category, max_clicks=max_clicks)

        if len(urls) > max_products:
            urls = urls[:max_products]
            self.logger.info(f"Limit to {max_products} URLs")

        data = []
        for i, u in enumerate(urls, 1):
            self.logger.info(f"[{i}/{len(urls)}] {u}")
            pd = self.extract_product_data(u)
            if pd:
                data.append(pd)
            self.random_delay()
        self.logger.info(f"Done category {category}: {len(data)} products")
        return data

    def crawl_all_categories_mass(self, max_clicks_per_category=50, max_products_per_category=300):
        print("=" * 60)
        print("CELLPHONES MASS CRAWLER (LOAD MORE ONLY)")
        print("=" * 60)
        self.stats["start_time"] = time.time()

        all_products = []
        for i, (key, path) in enumerate(self.categories.items(), 1):
            print(f"\n[{i}/{len(self.categories)}] Category: {key}")
            try:
                items = self.crawl_category(key, max_clicks=max_clicks_per_category, max_products=max_products_per_category)
                all_products.extend(items)
                self.stats["categories_processed"] += 1
                if items:
                    self.save_category_progress(items, key)
                print(f"  -> {len(items)} items (total: {len(all_products)})")
                time.sleep(random.uniform(1.5, 3))
            except Exception as e:
                print(f"  ERROR {key}: {e}")

        self.stats["total_products"] = len(all_products)
        self.save_final_results(all_products)
        self.print_mass_summary()
        return all_products

    # =====================
    # SAVE & SUMMARY (giữ nguyên logic cũ)
    # =====================
    def save_category_progress(self, products, category_name):
        import os
        filename = f"cellphones_progress_{category_name}_{int(time.time())}.json"
        data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
        os.makedirs(data_dir, exist_ok=True)
        filepath = os.path.join(data_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
        print(f"  Progress saved: {filename}")

    def save_final_results(self, all_products):
        import os
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
        os.makedirs(data_dir, exist_ok=True)

        main_filename = f"cellphones_mass_results_{ts}.json"
        main_filepath = os.path.join(data_dir, main_filename)
        with open(main_filepath, "w", encoding="utf-8") as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)

        duration = time.time() - self.stats["start_time"]
        summary = {
            "crawl_info": {
                "crawl_date": datetime.now().isoformat(),
                "total_products": len(all_products),
                "categories_processed": self.stats["categories_processed"],
                "duration_minutes": duration / 60,
                "products_per_min": len(all_products) / (duration / 60) if duration else 0,
            }
        }
        summary_filename = f"cellphones_mass_summary_{ts}.json"
        summary_filepath = os.path.join(data_dir, summary_filename)
        with open(summary_filepath, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print("\nSaved:")
        print(f"  Main:    {main_filename}")
        print(f"  Summary: {summary_filename}")

    def print_mass_summary(self):
        duration = time.time() - self.stats["start_time"]
        print("\n" + "=" * 60)
        print("CELLPHONES MASS CRAWL COMPLETED")
        print("=" * 60)
        print(f"Duration: {duration/60:.1f} minutes")
        print(f"Total products: {self.stats['total_products']:,}")
        print(f"Categories: {self.stats['categories_processed']}")

def main():
    import sys

    crawler = CellphonesCrawler()
    mode = "test"
    if len(sys.argv) > 1 and sys.argv[1] in ["test", "mass"]:
        mode = sys.argv[1]

    try:
        if mode == "test":
            print("CELLPHONES TEST (LOAD MORE ONLY)")
            products = crawler.crawl_category("mobile", max_clicks=60, max_products=200)
            if products:
                filename = f"cellphones_test_{int(time.time())}.jsonl"
                crawler.save_data(products, filename)
                print(f"Saved {len(products)} items -> {filename}")
            else:
                print("No products collected.")
        else:
            print("CELLPHONES MASS (LOAD MORE ONLY)")
            products = crawler.crawl_all_categories_mass(
                max_clicks_per_category=60,
                max_products_per_category=300
            )
            print(f"\nTotal: {len(products):,}")

    except KeyboardInterrupt:
        print("\nInterrupted.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        crawler.cleanup()


if __name__ == "__main__":
    main()
