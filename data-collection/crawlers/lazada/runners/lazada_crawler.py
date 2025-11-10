#!/usr/bin/env python3
"""
Lazada Selenium Crawler - Anti-bot bypass
"""
import time
import json
import re
import random
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        SELENIUM_AVAILABLE = True
        uc = None
    except ImportError:
        SELENIUM_AVAILABLE = False
        uc = None

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
LOG_PREFIX = "[Lazada]"

class LazadaCrawler:
    def __init__(self):
        self.categories = {
            "smartphones": "https://www.lazada.vn/tag/mobiles/?q=mobiles",
            "laptops": "https://www.lazada.vn/tag/laptops/?q=laptops",
            "tablets": "https://www.lazada.vn/tag/tablets/?q=tablets",
            "smartwatches": "https://www.lazada.vn/tag/smartwatch/?q=smartwatch",
            "headphones": "https://www.lazada.vn/tag/headphones/?q=headphones"
        }
        self.driver = None
        if SELENIUM_AVAILABLE:
            self.setup_driver()

    def setup_driver(self):
        try:
            if uc:
                # Undetected ChromeDriver - bypass anti-bot
                options = uc.ChromeOptions()
                options.add_argument("--headless=new")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument("--window-size=1920,1080")
                
                self.driver = uc.Chrome(options=options, version_main=None)
            else:
                # Fallback to regular Selenium with stealth
                from selenium.webdriver.chrome.options import Options
                options = Options()
                options.add_argument("--headless=new")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument("--window-size=1920,1080")
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                
                self.driver = webdriver.Chrome(options=options)
                # Hide webdriver property
                self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                    'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
                })
            
            self.driver.set_page_load_timeout(45)
            print(f"{LOG_PREFIX} Chrome driver ready (stealth mode)")
        except Exception as e:
            print(f"{LOG_PREFIX} Error creating driver: {e}")
            self.driver = None

    def extract_product(self, element, category) -> Optional[Dict[str, Any]]:
        try:
            data = {"source": "lazada", "category": category}
            
            # URL & Title
            try:
                link = element.find_element(By.CSS_SELECTOR, "div.RfADt > a[href]")
                href = link.get_attribute("href")
                if href:
                    if href.startswith("//"):
                        href = "https:" + href
                    data["url"] = href
                    data["product_name"] = link.text.strip() or link.get_attribute("title") or ""
                    
                    # Extract product_id from URL
                    match = re.search(r'-i(\d+)', href)
                    data["product_id"] = match.group(1) if match else ""
            except:
                return None
            
            # Price
            try:
                price_elem = element.find_element(By.CSS_SELECTOR, "span.ooOxS")
                price_text = price_elem.text.strip()
                data["price_current"] = int(re.sub(r'[^\d]', '', price_text)) if price_text else 0
            except:
                data["price_current"] = 0
            
            # Rating
            try:
                stars = element.find_elements(By.CSS_SELECTOR, "i._9-ogB.Dy1nx")
                data["rating_avg"] = float(len(stars)) if stars else 0.0
            except:
                data["rating_avg"] = 0.0
            
            # Review count
            try:
                review_elem = element.find_element(By.CSS_SELECTOR, "span.qzqFw")
                review_text = review_elem.text.strip()
                match = re.search(r'\((\d+)\)', review_text)
                data["review_count"] = int(match.group(1)) if match else 0
            except:
                data["review_count"] = 0
            
            # Image
            try:
                img = element.find_element(By.CSS_SELECTOR, "img[src]")
                data["image_urls"] = [img.get_attribute("src")]
            except:
                data["image_urls"] = []
            
            # Additional fields
            data["price_original"] = 0
            data["discount_percent"] = 0
            data["brand"] = ""
            data["seller_name"] = ""
            data["crawl_date"] = datetime.now().isoformat()
            data["page_number"] = 1
            
            return data if data.get("product_name") and data.get("url") else None
        except:
            return None

    def crawl_page(self, category: str, url: str, page: int) -> List[Dict[str, Any]]:
        page_url = f"{url}&page={page}" if '?' in url else f"{url}?page={page}"
        
        try:
            self.driver.get(page_url)
            time.sleep(random.uniform(3, 6))  # Longer wait
            
            # Human-like scrolling
            for _ in range(3):
                scroll_height = random.randint(300, 800)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_height});")
                time.sleep(random.uniform(0.5, 1.5))
            
            time.sleep(random.uniform(2, 3))
            
            # Wait for products to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.Bm3ON'))
                )
            except TimeoutException:
                print(f"{LOG_PREFIX} Timeout waiting for products on page {page}")
            
            # Extract products
            elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.Bm3ON[data-qa-locator="product-item"]')
            print(f"{LOG_PREFIX} Page {page}: Found {len(elements)} products")
            
            products = []
            for elem in elements[:40]:
                product = self.extract_product(elem, category)
                if product:
                    product["page_number"] = page
                    products.append(product)
                time.sleep(random.uniform(0.1, 0.3))  # Small delay between extractions
            
            return products
        except Exception as e:
            print(f"{LOG_PREFIX} Error on page {page}: {e}")
            return []

    def crawl_category(self, category: str, url: str, max_pages: int = 60):
        print(f"{LOG_PREFIX} Crawling: {category} (max {max_pages} pages)")
        
        all_products = []
        consecutive_empty = 0
        
        for page in range(1, max_pages + 1):
            products = self.crawl_page(category, url, page)
            
            if products:
                all_products.extend(products)
                consecutive_empty = 0
            else:
                consecutive_empty += 1
                if consecutive_empty >= 3:  # Stop after 3 empty pages
                    print(f"{LOG_PREFIX} Stopping after {consecutive_empty} empty pages")
                    break
            
            # Random delay between pages (3-8 seconds)
            time.sleep(random.uniform(3, 8))
        
        print(f"{LOG_PREFIX} Category '{category}': {len(all_products)} products")
        return all_products

    def save_jsonl(self, products: List[Dict[str, Any]], category: str):
        output_dir = OUTPUT_DIR
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except:
            output_dir = "/tmp"
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"lazada_{category}_{timestamp}.jsonl"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False) + '\n')
        
        print(f"{LOG_PREFIX} Saved {len(products)} products to {filepath}")

    def run(self, max_pages=60):
        if not self.driver:
            print(f"{LOG_PREFIX} No driver available")
            return
        
        print(f"{LOG_PREFIX} Starting Lazada crawler...")
        
        try:
            for category, url in self.categories.items():
                products = self.crawl_category(category, url, max_pages)
                
                if products:
                    self.save_jsonl(products, category)
                
                time.sleep(random.uniform(3, 5))
            
            print(f"{LOG_PREFIX} Completed!")
        finally:
            if self.driver:
                self.driver.quit()

def main():
    crawler = LazadaCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
