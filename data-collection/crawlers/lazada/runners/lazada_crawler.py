import time
import json
import csv
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
import random
import os, tempfile, time, random
import undetected_chromedriver as uc

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class FixedLazadaCrawler:
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
        """
        Cấu hình Chrome anti-bot cho Lazada:
        - undetected-chromedriver
        - spoof UA/locale/platform
        - chặn ảnh/font để nhẹ & giảm dấu vết
        - stealth JS che dấu automation
        - hỗ trợ proxy & non-headless qua env
        Env hỗ trợ:
        LAZADA_UA            : override UA
        LAZADA_HTTP_PROXY    : http(s)://user:pass@host:port
        LAZADA_HEADLESS      : "0" để chạy non-headless (nếu có Xvfb), mặc định headless
        LAZADA_CHROME_MAJOR  : ép major version (vd "120") nếu cần
        CHROME_BIN           : path chromium/chrome trong container
        CHROMEDRIVER_PATH    : path chromedriver (không bắt buộc với uc)
        """
        import os, tempfile, random, json
        import undetected_chromedriver as uc
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

        # --- Options & flags an toàn trong container ---
        opts = Options()
        headless = os.environ.get("LAZADA_HEADLESS", "1") != "0"
        if headless:
            # "old-headless" tương thích anti-bot tốt hơn một số site
            opts.add_argument("--headless=chrome")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-software-rasterizer")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-features=Translate,OptimizationHints,AutomationControlled")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--mute-audio")
        opts.add_argument("--no-zygote")
        opts.add_argument("--window-size=1280,900")
        # Ngôn ngữ & UA
        ua = os.environ.get(
            "LAZADA_UA",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        opts.add_argument("--lang=vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7")
        opts.add_argument(f"--user-agent={ua}")

        # Hồ sơ/Cache tạm để tránh bị "dính" fingerprint giữa các lần
        tmp = tempfile.mkdtemp(prefix="uc-profile-")
        opts.add_argument(f"--user-data-dir={tmp}/profile")
        opts.add_argument(f"--disk-cache-dir={tmp}/cache")

        # Proxy (nếu có)
        proxy = os.environ.get("LAZADA_HTTP_PROXY")
        if proxy:
            opts.add_argument(f"--proxy-server={proxy}")

        # Trỏ tới binary Chromium trong container (nếu có)
        chrome_bin = os.environ.get("CHROME_BIN", "/usr/bin/chromium-browser")
        if os.path.exists(chrome_bin):
            # uc v3: dùng browser_executable_path
            browser_executable_path = chrome_bin
        else:
            browser_executable_path = None

        # Khai báo capabilities: giảm đợi page-load
        caps = DesiredCapabilities.CHROME.copy()
        caps["pageLoadStrategy"] = "eager"

        # Tham số uc
        version_main = os.environ.get("LAZADA_CHROME_MAJOR")
        uc_kwargs = dict(
            options=opts,
            desired_capabilities=caps,
            headless=headless,
            suppress_welcome=True,
            use_subprocess=True,
        )
        if browser_executable_path:
            uc_kwargs["browser_executable_path"] = browser_executable_path
        if version_main:
            uc_kwargs["version_main"] = int(version_main)

        # Khởi tạo driver (uc sẽ tự quản driver nếu không chỉ định)
        self.driver = uc.Chrome(**uc_kwargs)

        # Random viewport một chút
        self.driver.set_window_size(
            random.randint(1200, 1420), random.randint(800, 1020)
        )

        # --- CDP tweaks: spoof UA/lang/platform + chặn tài nguyên nặng ---
        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
            # chặn ảnh/font/icon để nhẹ & bớt footprint
            self.driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": [
                "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
                "*.woff", "*.woff2", "*.ttf", "*.otf", "*.ico"
            ]})
            # override UA + ngôn ngữ + platform Win32
            self.driver.execute_cdp_cmd("Network.setUserAgentOverride", {
                "userAgent": ua,
                "platform": "Win32",
                "acceptLanguage": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"
            })
        except Exception:
            pass

        # --- Stealth JS: che dấu webdriver, plugins, languages, WebGL vendor ---
        stealth_js = r"""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'language',  { get: () => 'vi-VN' });
        Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN','vi','en-US','en'] });
        Object.defineProperty(navigator, 'platform',  { get: () => 'Win32' });
        Object.defineProperty(navigator, 'vendor',    { get: () => 'Google Inc.' });
        // plugins length > 0
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });

        // WebGL vendor/renderer
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function (param) {
        if (param === 37445) { return 'Google Inc. (Intel)'; } // UNMASKED_VENDOR_WEBGL
        if (param === 37446) { return 'ANGLE (Intel, Intel(R) UHD Graphics, D3D11)'; } // UNMASKED_RENDERER_WEBGL
        return getParameter.call(this, param);
        };

        // permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(parameters)
        );
        """
        try:
            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": stealth_js}
            )
        except Exception:
            pass

        print("[AntiBot] Driver ready - headless=%s, UA=%s" % (headless, ua))


    # ====== THÊM các helper này vào trong class (ngay dưới setup_driver) ======
    def is_blocked(self, html: str) -> bool:
        if not html:
            return True
        low = html.lower()
        patterns = [
            "are you human", "security verification", "captcha",
            "access denied", "forbidden",
            "tạm thời không thể truy cập", "xác minh bảo mật"
        ]
        return any(p in low for p in patterns)

    def humanize(self):
        """Giả lập thao tác nhẹ để giảm dấu hiệu bot."""
        try:
            import random, time
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(self.driver).move_by_offset(
                random.randint(1, 7), random.randint(1, 5)
            ).perform()
            self.driver.execute_script(
                "window.scrollTo(0, Math.floor(Math.random()*300)+100);"
            )
            time.sleep(random.uniform(0.6, 1.6))
        except Exception:
            pass

    def get_with_retry(self, url: str, max_tries: int = 5, sleep_min=1.5, sleep_max=3.5) -> str:
        """Đi tới URL với backoff; nếu bị chặn thử lại sau khi reset dấu vết nhẹ."""
        import time, random
        for i in range(1, max_tries + 1):
            self.driver.get(url)
            time.sleep(random.uniform(sleep_min, sleep_max))
            self.humanize()
            html = ""
            try:
                html = self.driver.page_source
            except Exception:
                pass
            if not self.is_blocked(html):
                return html

            # Bị chặn → dọn cookie & đổi một chút rồi thử lại
            try:
                self.driver.delete_all_cookies()
            except Exception:
                pass
            self.driver.set_window_size(
                random.randint(1200, 1420), random.randint(800, 1020)
            )
            # backoff tăng dần
            time.sleep(random.uniform(3.0, 6.0) + i * random.uniform(0.8, 1.6))
        raise RuntimeError("Bị anti-bot chặn liên tục khi GET: %s" % url)

    def random_delay(self, min_delay=1, max_delay=3):
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def scroll_page(self, scroll_pause_time=2, max_scrolls=5):
        print(f"Dang cuon trang de load san pham...")
        for i in range(max_scrolls):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause_time)
            print(f"   Da cuon lan {i+1}/{max_scrolls}")

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
                # Thu cac selector khac nhau cho anh
                img_selectors = [
                    "img[type='product']",
                    "img[alt*='']",  # Anh co alt text
                    ".picture-wrapper img",
                    "img[src*='lazcdn.com']",  # Anh tu Lazada CDN
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
                            # Chi dung base64 neu khong co URL nao khac
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
                    print(f"   Found price: {price_text.encode('ascii', 'ignore').decode('ascii')}")
            except NoSuchElementException:
                print("   No price found with span.ooOxS")
                pass

            # Neu khong tim thay gia, thu tim cac selector khac
            if not product_data["price_text"]:
                price_selectors = [
                    "span[class*='price']",
                    "div[class*='price']",
                    "[class*='Currency']",
                    "*:contains('₫')"  # Fallback
                ]

                for selector in price_selectors:
                    try:
                        if selector == "*:contains('₫')":
                            # Tim tat ca element chua ky tu dong tien
                            all_elements = product_element.find_elements(By.CSS_SELECTOR, "*")
                            for elem in all_elements:
                                text = elem.text.strip()
                                if '₫' in text and any(char.isdigit() for char in text):
                                    product_data["price_text"] = text
                                    product_data["price"] = self.normalize_price(text)
                                    print(f"   Found price fallback: {text.encode('ascii', 'ignore').decode('ascii')}")
                                    break
                        else:
                            price_element = product_element.find_element(By.CSS_SELECTOR, selector)
                            price_text = price_element.text.strip()
                            if price_text and any(char.isdigit() for char in price_text):
                                product_data["price_text"] = price_text
                                product_data["price"] = self.normalize_price(price_text)
                                print(f"   Found price with {selector}: {price_text}")
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
                # Tim rating container cua Lazada
                rating_container = None
                rating_selectors = [
                    ".mdmmT._32vUv",  # Container rating chinh xac
                    "div:has(i._9-ogB)",  # Container co chua cac sao
                    "*:has(.qzqFw)"  # Container co chua review count
                ]

                for selector in rating_selectors:
                    try:
                        rating_container = product_element.find_element(By.CSS_SELECTOR, selector)
                        break
                    except NoSuchElementException:
                        continue

                if rating_container:
                    # Dem so sao day (class Dy1nx)
                    try:
                        filled_stars = rating_container.find_elements(By.CSS_SELECTOR, "i._9-ogB.Dy1nx")
                        star_count = len(filled_stars)
                        if star_count > 0:
                            product_data["rating"] = float(star_count)
                            print(f"   Found rating by stars: {star_count}")
                    except NoSuchElementException:
                        pass

                # Neu chua co rating, thu tim pattern text
                if not product_data["rating"]:
                    # Tim trong cac element co text rating
                    rating_text_selectors = [
                        ".qzqFw",  # Review count element
                        "[class*='rating']",
                        "[class*='star']",
                        "*[title*='star']"
                    ]

                    for selector in rating_text_selectors:
                        try:
                            rating_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                            for elem in rating_elements:
                                text = elem.text.strip()
                                if text:
                                    # Tim pattern rating nhu 4.5 hoac 4,8
                                    rating_match = re.search(r'\b(\d[.,]\d)\b', text)
                                    if rating_match:
                                        rating_str = rating_match.group(1).replace(',', '.')
                                        product_data["rating"] = float(rating_str)
                                        print(f"   Found rating in text: {rating_str}")
                                        break
                        except NoSuchElementException:
                            continue
                        if product_data["rating"]:
                            break

                # Fallback: tim trong tat ca text
                if not product_data["rating"]:
                    all_text = product_element.text
                    rating_match = re.search(r'\b(\d[.,]\d)\b', all_text)
                    if rating_match:
                        rating_str = rating_match.group(1).replace(',', '.')
                        try:
                            rating_val = float(rating_str)
                            if 0 <= rating_val <= 5:  # Rating thuong tu 0-5
                                product_data["rating"] = rating_val
                                print(f"   Found rating in all text: {rating_str}")
                        except ValueError:
                            pass

            except Exception as e:
                print(f"   Error getting rating: {e}")

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
                            print(f"   Found review count from qzqFw: {review_match.group(1)}")
                        # Hoac chi la so: "123"
                        elif qzqfw_text.isdigit():
                            product_data["review_count"] = qzqfw_text
                            print(f"   Found review count: {qzqfw_text}")
                except NoSuchElementException:
                    pass

                # Neu chua co review count, tim bang cach khac
                if not product_data["review_count"]:
                    # Cac selector va pattern co the chua so luong ban/review
                    review_selectors = [
                        "[class*='review']",
                        "[class*='sold']",
                        "[class*='ban']",
                        ".qzqFw + *",  # Element sau rating
                        "*[class*='Review']"
                    ]

                    for selector in review_selectors:
                        try:
                            review_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                            for elem in review_elements:
                                text = elem.text.strip()
                                if text:
                                    # Tim pattern so luong: "1234 đã bán", "567 reviews", v.v.
                                    sold_match = re.search(r'(\d+(?:\.\d+)?[kK]?)\s*(?:đã bán|sold|ban|reviews?|đánh giá)', text, re.IGNORECASE)
                                    if sold_match:
                                        sold_str = sold_match.group(1)
                                        # Xu ly 'k' suffix (1.2k = 1200)
                                        if sold_str.lower().endswith('k'):
                                            sold_num = float(sold_str[:-1]) * 1000
                                            product_data["review_count"] = str(int(sold_num))
                                        else:
                                            product_data["review_count"] = sold_str
                                        print(f"   Found sold/review count: {sold_str}")
                                        break
                        except NoSuchElementException:
                            continue
                        if product_data["review_count"]:
                            break

                # Neu van khong tim thay, tim trong tat ca text
                if not product_data["review_count"]:
                    all_text = product_element.text
                    # Tim pattern trong ngoac truoc
                    review_match = re.search(r'\((\d+)\)', all_text)
                    if review_match:
                        product_data["review_count"] = review_match.group(1)
                        print(f"   Found review count in parentheses: {review_match.group(1)}")
                    else:
                        # Tim pattern "X đã bán"
                        sold_match = re.search(r'(\d+(?:\.\d+)?[kK]?)\s*(?:đã bán|sold|ban|reviews?|đánh giá)', all_text, re.IGNORECASE)
                        if sold_match:
                            sold_str = sold_match.group(1)
                            if sold_str.lower().endswith('k'):
                                sold_num = float(sold_str[:-1]) * 1000
                                product_data["review_count"] = str(int(sold_num))
                            else:
                                product_data["review_count"] = sold_str
                            print(f"   Found sold/review count in text: {sold_str}")

            except Exception as e:
                print(f"   Error getting review/sold count: {e}")

            # Lay shop_name
            try:
                # Cac selector co the chua ten shop
                shop_selectors = [
                    "[class*='shop']",
                    "[class*='store']",
                    "[class*='seller']",
                    "[class*='mall']",
                    ".seller-name",
                    "*[title*='shop']",
                    "*[title*='store']",
                    "a[href*='/shop/']",  # Link den shop
                    # Tim trong cac element nho xung quanh location
                    "span.oa6ri ~ *",  # Element sau location
                    "span.oa6ri + *"   # Element lien ke sau location
                ]

                for selector in shop_selectors:
                    try:
                        shop_elements = product_element.find_elements(By.CSS_SELECTOR, selector)
                        for elem in shop_elements:
                            text = elem.text.strip()
                            if text and 5 <= len(text) <= 100:  # Ten shop khong qua ngan hoac dai
                                # Loai bo cac text khong phai shop name
                                skip_words = ['voucher', 'giam', 'gia', '₫', 'price', 'discount',
                                            'shipping', 'free', 'sale', 'hot', 'new', 'reviews',
                                            'đã bán', 'sold', 'vietnam', 'hà nội', 'hcm', 'tp']
                                if not any(skip_word in text.lower() for skip_word in skip_words):
                                    # Kiem tra khong phai gia tien
                                    if not re.search(r'\d+.*₫|\d+.*đ', text):
                                        product_data["shop_name"] = text
                                        print(f"   Found shop name: {text}")
                                        break
                    except NoSuchElementException:
                        continue
                    if product_data["shop_name"]:
                        break

                # Neu van chua co shop name, tim trong tat ca text cua container
                if not product_data["shop_name"]:
                    try:
                        # Lay tat ca text tu cac element nho trong container
                        all_elements = product_element.find_elements(By.CSS_SELECTOR, "span, div, a")
                        for elem in all_elements:
                            text = elem.text.strip()
                            if text and 5 <= len(text) <= 50:  # Ten shop ngắn hon
                                # Kiem tra pattern ten shop (chu cai + so, khong co ky tu dac biet)
                                if re.match(r'^[a-zA-Z0-9À-ỹ\s]+$', text):  # Chi chu va so
                                    skip_words = ['voucher', 'giam', 'gia', 'shipping', 'free', 'sale']
                                    if not any(skip_word in text.lower() for skip_word in skip_words):
                                        if not re.search(r'\d+.*₫|\d+.*đ', text):
                                            if text not in [product_data.get('title', ''), product_data.get('location', '')]:
                                                product_data["shop_name"] = text
                                                print(f"   Found shop name in text: {text}")
                                                break
                    except Exception:
                        pass

            except Exception as e:
                print(f"   Error getting shop name: {e}")

            # Chi tra ve neu co thong tin co ban
            if product_data["title"] and product_data["url"]:
                return product_data
            else:
                print(f"   Product rejected - title: {bool(product_data['title'])}, url: {bool(product_data['url'])}")
                return None

        except Exception as e:
            print(f"Loi khi trich xuat du lieu san pham: {e}")
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

    def get_products_from_page(self, category_name: str, category_url: str, page_num=1, max_products=50) -> List[Dict[str, Any]]:
        url = self.build_paginated_url(category_url, page_num)

        print(f"Dang crawl {category_name} - trang {page_num}: {url}")

        try:
            self.driver.get(url)
            self.random_delay(2, 4)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                print("Timeout cho trang load")
                return []

            self.scroll_page()

            products = []

            # Tim product containers voi selector chinh xac
            try:
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.Bm3ON[data-qa-locator="product-item"]')
                print(f"   Tim thay {len(product_elements)} product containers")

                for i, element in enumerate(product_elements[:max_products]):
                    product_data = self.extract_product_data(element, category_name)
                    if product_data:
                        products.append(product_data)
                        price_display = product_data['price_text'].encode('ascii', 'ignore').decode('ascii')
                        print(f"   [{i+1}] Extracted: {product_data['title'][:50]}... - {price_display}")
                    else:
                        print(f"   [{i+1}] Failed to extract product data")

            except Exception as e:
                print(f"   Loi tim product containers: {e}")

            print(f"Trang {page_num}: Da trich xuat {len(products)} san pham")
            return products

        except Exception as e:
            print(f"Loi khi crawl trang {page_num}: {e}")
            return []

    def crawl_category(self, category_name: str, category_url: str, max_pages=60, max_products_per_page=30) -> List[Dict[str, Any]]:
        print(f"\n=== Crawling {category_name.upper()} ===")
        all_products = []

        for page in range(1, max_pages + 1):
            try:
                products = self.get_products_from_page(category_name, category_url, page, max_products_per_page)
                all_products.extend(products)

                if page < max_pages:
                    print(f"Nghi giua cac trang...")
                    self.random_delay(3, 6)

            except Exception as e:
                print(f"Loi crawl trang {page}: {e}")
                continue

        print(f"=== {category_name}: Tong cong {len(all_products)} san pham ===")
        return all_products

    def crawl_all_categories(self, max_pages_per_category=5, max_products_per_page=30):
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

        # Loai bo duplicate
        unique_products = []
        seen_urls = set()

        for product in all_data:
            url = product.get('url', '')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_products.append(product)

        print(f"\nTong cong: {len(unique_products)} san pham unique")
        return unique_products

    def save_to_files(self, products: List[Dict[str, Any]], filename_prefix="fixed_lazada_multi_category"):
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
    crawler = FixedLazadaCrawler()

    try:
        print("Bat dau crawl multi-category Lazada...")

        # Crawl tat ca categories
        # max_pages_per_category: so trang moi category
        # max_products_per_page: so san pham moi trang
        products = crawler.crawl_all_categories(max_pages_per_category=60, max_products_per_page=40)

        if products:
            crawler.save_to_files(products)

            # In thong ke
            print(f"\nThong ke chi tiet:")
            print(f"   Tong san pham: {len(products)}")

            # Thong ke gia
            prices = [p['price'] for p in products if p.get('price')]
            if prices:
                print(f"   Gia thap nhat: {min(prices):,} VND")
                print(f"   Gia cao nhat: {max(prices):,} VND")
                print(f"   Gia trung binh: {sum(prices)//len(prices):,} VND")

            # Hien thi vai san pham mau
            print(f"\nVai san pham mau:")
            for i, product in enumerate(products[:5]):
                print(f"   {i+1}. [{product.get('category', 'N/A')}] {product.get('title', 'N/A')[:60]}...")
                price_text = product.get('price_text', 'N/A')
                if price_text != 'N/A':
                    price_text = price_text.encode('ascii', 'ignore').decode('ascii')
                print(f"      Gia: {price_text}")
                print(f"      URL: {product.get('url', 'N/A')[:80]}...")
                print()
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