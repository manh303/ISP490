#!/usr/bin/env python3
"""
Lazada Crawler with Login - Bypass anti-bot bằng session đã login
"""
import json
import time
import random
import os
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
PROFILE_DIR = os.environ.get("LAZADA_PROFILE_DIR", "/tmp/profiles/lazada")
LOG_PREFIX = "[Lazada-Login]"

class LazadaLoginCrawler:
    def __init__(self):
        self.categories = {
            "smartphones": "https://www.lazada.vn/dien-thoai-may-tinh-bang/",
            "laptops": "https://www.lazada.vn/may-tinh-xach-tay/",
            "tablets": "https://www.lazada.vn/may-tinh-bang/",
            "smartwatches": "https://www.lazada.vn/dong-ho-thong-minh/",
            "headphones": "https://www.lazada.vn/thiet-bi-am-thanh/"
        }

    def manual_login(self, page):
        """Hướng dẫn login thủ công"""
        print(f"{LOG_PREFIX} Opening Lazada login page...")
        print(f"{LOG_PREFIX} Please login manually in the browser window")
        print(f"{LOG_PREFIX} After login, press Enter to continue...")
        
        page.goto("https://www.lazada.vn/customer/account/login", wait_until='networkidle')
        
        # Wait for manual login
        input("Press Enter after you have logged in...")
        
        # Save cookies
        cookies = page.context.cookies()
        Path(PROFILE_DIR).mkdir(parents=True, exist_ok=True)
        with open(f"{PROFILE_DIR}/cookies.json", 'w') as f:
            json.dump(cookies, f)
        
        print(f"{LOG_PREFIX} Cookies saved!")

    def load_cookies(self, context):
        """Load saved cookies"""
        cookie_file = f"{PROFILE_DIR}/cookies.json"
        if os.path.exists(cookie_file):
            with open(cookie_file, 'r') as f:
                cookies = json.load(f)
                context.add_cookies(cookies)
            print(f"{LOG_PREFIX} Cookies loaded")
            return True
        return False

    def extract_products(self, page) -> List[Dict[str, Any]]:
        """Extract products từ page"""
        try:
            time.sleep(random.uniform(3, 5))
            
            # Scroll
            for _ in range(3):
                page.evaluate(f'window.scrollBy(0, {random.randint(300, 700)})')
                time.sleep(random.uniform(1, 2))
            
            content = page.content()
            
            # Extract từ JSON
            patterns = [
                r'window\.pageData\s*=\s*(\{.+?\});',
                r'__INITIAL_STATE__\s*=\s*(\{.+?\});'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    try:
                        data = json.loads(match.group(1))
                        items = data.get('mods', {}).get('listItems', [])
                        if items:
                            print(f"{LOG_PREFIX} Found {len(items)} products")
                            return items
                    except:
                        continue
            
            # Fallback: DOM extraction
            elements = page.query_selector_all('[data-qa-locator="product-item"]')
            if not elements:
                elements = page.query_selector_all('.Bm3ON')
            
            print(f"{LOG_PREFIX} Found {len(elements)} product elements")
            
            products = []
            for elem in elements[:40]:
                try:
                    product = {}
                    
                    link = elem.query_selector('a[href]')
                    if link:
                        product['productUrl'] = link.get_attribute('href')
                        product['name'] = link.get_attribute('title') or ''
                    
                    price_elem = elem.query_selector('.ooOxS')
                    if price_elem:
                        product['price'] = price_elem.inner_text()
                    
                    if product.get('productUrl'):
                        match = re.search(r'-i(\d+)', product['productUrl'])
                        product['itemId'] = match.group(1) if match else ''
                    
                    if product.get('name'):
                        products.append(product)
                except:
                    continue
            
            return products
            
        except Exception as e:
            print(f"{LOG_PREFIX} Extract error: {e}")
            return []

    def transform_product(self, item: Dict, category: str, page: int) -> Dict[str, Any]:
        """Transform sang format chuẩn"""
        price_text = item.get('price', '0')
        price = int(''.join(filter(str.isdigit, str(price_text)))) if price_text else 0
        
        product_url = item.get('productUrl', '')
        if product_url and product_url.startswith('//'):
            product_url = 'https:' + product_url
        
        return {
            "source": "lazada",
            "category": category,
            "product_id": str(item.get('itemId', '')),
            "product_name": item.get('name', ''),
            "price_current": price,
            "price_original": price,
            "discount_percent": 0,
            "rating_avg": 0.0,
            "review_count": 0,
            "brand": "",
            "seller_name": "",
            "url": product_url,
            "image_urls": [],
            "crawl_date": datetime.now().isoformat(),
            "page_number": page
        }

    def crawl_category(self, browser, category: str, url: str, max_pages: int = 2) -> List[Dict[str, Any]]:
        """Crawl category"""
        print(f"{LOG_PREFIX} Crawling: {category}")
        
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        # Load cookies
        has_cookies = self.load_cookies(context)
        
        page = context.new_page()
        
        # Manual login if no cookies
        if not has_cookies:
            self.manual_login(page)
        
        all_products = []
        
        try:
            for page_num in range(1, max_pages + 1):
                page_url = f"{url}?page={page_num}"
                print(f"{LOG_PREFIX} Page {page_num}: {page_url}")
                
                page.goto(page_url, wait_until='networkidle', timeout=60000)
                
                items = self.extract_products(page)
                
                if not items:
                    break
                
                for item in items:
                    product = self.transform_product(item, category, page_num)
                    if product.get('product_name'):
                        all_products.append(product)
                
                time.sleep(random.uniform(3, 5))
                
        except Exception as e:
            print(f"{LOG_PREFIX} Error: {e}")
        finally:
            context.close()
        
        print(f"{LOG_PREFIX} Category '{category}': {len(all_products)} products")
        return all_products

    def save_jsonl(self, products: List[Dict[str, Any]], category: str):
        """Save to JSONL"""
        output_dir = OUTPUT_DIR
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except:
            output_dir = "/tmp"
        
        today = datetime.now().strftime("%Y-%m-%d")
        date_dir = Path(output_dir) / "lazada" / f"date={today}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"lazada_{category}_{timestamp}.jsonl"
        filepath = date_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False) + '\n')
        
        print(f"{LOG_PREFIX} Saved {len(products)} to {filepath}")

    def run(self, max_pages=2):
        """Run crawler"""
        if not PLAYWRIGHT_AVAILABLE:
            print(f"{LOG_PREFIX} Playwright not available")
            return
        
        print(f"{LOG_PREFIX} Starting...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,  # Non-headless for manual login
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            try:
                for category, url in self.categories.items():
                    products = self.crawl_category(browser, category, url, max_pages)
                    
                    if products:
                        self.save_jsonl(products, category)
                    
                    time.sleep(random.uniform(2, 4))
                
                print(f"{LOG_PREFIX} Completed!")
            finally:
                browser.close()

def main():
    crawler = LazadaLoginCrawler()
    crawler.run(max_pages=2)

if __name__ == "__main__":
    main()
