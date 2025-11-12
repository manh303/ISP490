import time
import json
import csv
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
import random
import os, tempfile
import undetected_chromedriver as uc

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains

class AntiBotLazadaCrawler:
    def __init__(self):
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

    def get_random_user_agent(self):
        """Lay user agent ngau nhien."""
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        return random.choice(agents)

    def setup_driver(self):
        """Setup Chrome driver với antibot techniques mạnh."""
        opts = Options()
        
        # Basic stealth options
        headless = os.environ.get("LAZADA_HEADLESS", "1") != "0"
        if headless:
            opts.add_argument('--headless=new')
        
        opts.add_argument('--no-sandbox')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('--disable-blink-features=AutomationControlled')
        opts.add_argument('--disable-extensions')
        opts.add_argument('--disable-plugins')
        opts.add_argument('--disable-web-security')
        opts.add_argument('--disable-features=VizDisplayCompositor')
        opts.add_argument('--disable-ipc-flooding-protection')
        opts.add_argument('--disable-background-networking')
        opts.add_argument('--mute-audio')
        opts.add_argument('--no-zygote')
        
        # Window and viewport
        opts.add_argument('--window-size=1920,1080')
        opts.add_argument('--start-maximized')
        
        # Random user agent
        ua = self.get_random_user_agent()
        opts.add_argument(f'--user-agent={ua}')
        opts.add_argument('--lang=vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7')
        
        # Proxy support
        proxy = os.environ.get("LAZADA_HTTP_PROXY")
        if proxy:
            opts.add_argument(f'--proxy-server={proxy}')
        
        # Temp profile
        tmp = tempfile.mkdtemp(prefix="uc-profile-")
        opts.add_argument(f"--user-data-dir={tmp}/profile")
        opts.add_argument(f"--disk-cache-dir={tmp}/cache")
        
        # Advanced stealth
        opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        opts.add_experimental_option('useAutomationExtension', False)
        
        # Prefs for better stealth
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,
                "geolocation": 2,
            },
            "profile.managed_default_content_settings": {
                "images": 2  # Block images for speed
            }
        }
        opts.add_experimental_option("prefs", prefs)
        
        # Chrome binary - Windows path
        chrome_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
        ]
        chrome_bin = None
        for path in chrome_paths:
            if os.path.exists(path):
                chrome_bin = path
                break
        if chrome_bin:
            opts.binary_location = chrome_bin
        
        # UC kwargs - simplified
        try:
            self.driver = uc.Chrome(
                options=opts,
                headless=headless,
                version_main=None  # Let UC auto-detect
            )
        except Exception as e:
            print(f"UC Chrome failed: {e}, trying regular Chrome...")
            # Fallback to regular Chrome
            from selenium.webdriver.chrome.service import Service
            service = Service()
            self.driver = webdriver.Chrome(service=service, options=opts)
        
        # Random viewport
        self.driver.set_window_size(
            random.randint(1200, 1920), 
            random.randint(800, 1080)
        )
        
        # Advanced stealth scripts
        stealth_script = """
        // Remove webdriver traces
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['vi-VN', 'vi', 'en-US', 'en']});
        Object.defineProperty(navigator, 'language', {get: () => 'vi-VN'});
        Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
        Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 0});
        
        // Mock chrome runtime
        window.chrome = {runtime: {}};
        
        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' 
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters)
        );
        
        // WebGL fingerprint
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParameter(parameter);
        };
        """
        
        try:
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': stealth_script})
        except Exception:
            pass
            
        print(f"[AntiBot] Driver ready - headless={headless}, UA={ua[:50]}...")

    def is_blocked(self, html: str) -> bool:
        """Kiểm tra xem trang có bị chặn không."""
        if not html or len(html) < 1000:
            return True
        
        low = html.lower()
        block_patterns = [
            "are you human", "security verification", "captcha", "recaptcha",
            "access denied", "forbidden", "blocked", "bot detected",
            "tạm thời không thể truy cập", "xác minh bảo mật", "chặn truy cập",
            "cloudflare", "ddos protection", "checking your browser",
            "please wait", "loading", "redirecting", "verify you are human"
        ]
        
        if any(pattern in low for pattern in block_patterns):
            return True
            
        # Kiểm tra có content thực tế không
        content_indicators = [
            "lazada", "product", "price", "shop", "cart", "buy",
            "sản phẩm", "giá", "mua", "cửa hàng"
        ]
        
        if not any(indicator in low for indicator in content_indicators):
            return True
            
        return False

    def humanize(self):
        """Giả lập hành vi người dùng thực."""
        try:
            # Random mouse movements
            for _ in range(random.randint(2, 4)):
                x_offset = random.randint(-50, 50)
                y_offset = random.randint(-30, 30)
                ActionChains(self.driver).move_by_offset(x_offset, y_offset).perform()
                time.sleep(random.uniform(0.1, 0.3))
            
            # Random scrolling pattern
            scroll_actions = [
                "window.scrollTo(0, Math.floor(Math.random()*500)+200);",
                "window.scrollBy(0, Math.floor(Math.random()*300)+100);",
                "window.scrollTo(0, 0);",  # Back to top sometimes
            ]
            
            for _ in range(random.randint(1, 3)):
                action = random.choice(scroll_actions)
                self.driver.execute_script(action)
                time.sleep(random.uniform(0.5, 1.2))
            
            # Random page interactions
            if random.random() < 0.3:  # 30% chance
                try:
                    self.driver.execute_script(
                        "var elements = document.querySelectorAll('div, span, a');"
                        "if(elements.length > 0) {"
                        "  var randomEl = elements[Math.floor(Math.random() * Math.min(elements.length, 10))];"
                        "  if(randomEl) randomEl.focus();"
                        "}"
                    )
                except:
                    pass
            
            time.sleep(random.uniform(1.0, 2.5))
            
        except Exception as e:
            print(f"[Humanize] Error: {e}")
            time.sleep(random.uniform(0.5, 1.0))

    def get_with_retry(self, url, max_tries=8):
        """Load trang với retry và anti-detection."""
        for attempt in range(1, max_tries + 1):
            try:
                print(f"[Attempt {attempt}/{max_tries}] Loading: {url}")
                
                # Clear previous state
                try:
                    self.driver.delete_all_cookies()
                    self.driver.execute_script("window.localStorage.clear();")
                    self.driver.execute_script("window.sessionStorage.clear();")
                except:
                    pass
                
                # Navigate with timeout
                self.driver.set_page_load_timeout(30)
                self.driver.get(url)
                
                # Wait and humanize
                time.sleep(random.uniform(2, 4))
                self.humanize()
                
                # Check if page loaded successfully
                html = self.driver.page_source
                if self.is_blocked(html):
                    print(f"[Blocked] Attempt {attempt} - detected anti-bot")
                    if attempt < max_tries:
                        cooldown = random.uniform(10, 20) + attempt * 5
                        print(f"[Cooldown] Waiting {cooldown:.1f}s before retry...")
                        time.sleep(cooldown)
                    continue
                
                # Success
                print(f"[Success] Page loaded successfully")
                return html
                
            except Exception as e:
                print(f"[Error] Attempt {attempt}: {str(e)}")
                if attempt < max_tries:
                    time.sleep(random.uniform(5, 10))
                continue
        
        raise RuntimeError(f"Failed to load page after {max_tries} attempts: {url}")

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

            # Lay link san pham
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

            # Lay gia
            try:
                price_element = product_element.find_element(By.CSS_SELECTOR, "span.ooOxS")
                price_text = price_element.text.strip()
                if price_text:
                    product_data["price_text"] = price_text
                    product_data["price"] = self.normalize_price(price_text)
            except NoSuchElementException:
                pass

            # Chi tra ve neu co thong tin co ban
            if product_data["title"] and product_data["url"]:
                return product_data
            else:
                return None

        except Exception as e:
            print(f"Loi khi trich xuat du lieu san pham: {e}")
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

    def get_products_from_page(self, category_name: str, category_url: str, page_num=1, max_products=50) -> List[Dict[str, Any]]:
        url = self.build_paginated_url(category_url, page_num)
        print(f"Dang crawl {category_name} - trang {page_num}: {url}")

        try:
            # Dùng retry với humanize để giảm block
            html = self.get_with_retry(url)
            self.random_delay(3, 6)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                print("Timeout cho trang load")
                return []

            self.scroll_page()
            products = []

            # Tim product containers
            try:
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.Bm3ON[data-qa-locator="product-item"]')
                print(f"   Tim thay {len(product_elements)} product containers")

                for i, element in enumerate(product_elements[:max_products]):
                    product_data = self.extract_product_data(element, category_name)
                    if product_data:
                        products.append(product_data)
                        print(f"   [{i+1}] Extracted: {product_data['title'][:50]}...")
                    else:
                        print(f"   [{i+1}] Failed to extract product data")

            except Exception as e:
                print(f"   Loi tim product containers: {e}")

            print(f"Trang {page_num}: Da trich xuat {len(products)} san pham")
            return products

        except Exception as e:
            print(f"Loi khi crawl trang {page_num}: {e}")
            return []

    def crawl_category(self, category_name: str, category_url: str, max_pages=3, max_products_per_page=30) -> List[Dict[str, Any]]:
        print(f"\n=== Crawling {category_name.upper()} ===")
        all_products = []

        for page in range(1, max_pages + 1):
            try:
                products = self.get_products_from_page(category_name, category_url, page, max_products_per_page)
                all_products.extend(products)

                if page < max_pages:
                    print(f"Nghi giua cac trang...")
                    self.random_delay(5, 10)

            except Exception as e:
                print(f"Loi crawl trang {page}: {e}")
                continue

        print(f"=== {category_name}: Tong cong {len(all_products)} san pham ===")
        return all_products

    def save_to_files(self, products: List[Dict[str, Any]], filename_prefix="antibot_lazada"):
        if not products:
            print("Khong co du lieu de luu")
            return

        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # Save JSON
        json_file = output_dir / f"{filename_prefix}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        # Save CSV
        csv_file = output_dir / f"{filename_prefix}.csv"
        if products:
            fieldnames = products[0].keys()
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(products)

        print(f"\nDa luu du lieu:")
        print(f"  JSON: {json_file}")
        print(f"  CSV: {csv_file}")
        print(f"  Tong san pham: {len(products)}")

    def close(self):
        if self.driver:
            self.driver.quit()
            print("Da dong browser")

def main():
    crawler = AntiBotLazadaCrawler()

    try:
        print("Bat dau crawl Lazada voi antibot...")
        
        # Test với 1 category trước
        products = crawler.crawl_category("smartphones", crawler.categories["smartphones"], max_pages=2, max_products_per_page=20)

        if products:
            crawler.save_to_files(products)
            print(f"\nThong ke:")
            print(f"   Tong san pham: {len(products)}")
            
            # Hiển thị vài sản phẩm mẫu
            for i, product in enumerate(products[:3]):
                print(f"   {i+1}. {product.get('title', 'N/A')[:60]}...")
                print(f"      Gia: {product.get('price_text', 'N/A')}")
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