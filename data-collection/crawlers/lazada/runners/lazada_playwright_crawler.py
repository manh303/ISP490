#!/usr/bin/env python3
"""
Lazada Playwright Crawler - Bypass anti-bot với Playwright stealth
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
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
LOG_PREFIX = "[Lazada-PW]"

class LazadaPlaywrightCrawler:
    def __init__(self):
        self.categories = {
            "smartphones": "https://www.lazada.vn/dien-thoai-may-tinh-bang/",
            "laptops": "https://www.lazada.vn/may-tinh-xach-tay/",
            "tablets": "https://www.lazada.vn/may-tinh-bang/",
            "smartwatches": "https://www.lazada.vn/dong-ho-thong-minh/",
            "headphones": "https://www.lazada.vn/thiet-bi-am-thanh/"
        }

    def extract_products_from_page(self, page) -> List[Dict[str, Any]]:
        """Extract products từ page HTML"""
        try:
            # Wait for page load
            time.sleep(random.uniform(5, 8))
            
            # Try multiple selectors
            selectors = [
                '[data-qa-locator="product-item"]',
                '.Bm3ON',
                '[data-item-id]',
                '.gridItem'
            ]
            
            for selector in selectors:
                try:
                    page.wait_for_selector(selector, timeout=5000)
                    break
                except:
                    continue
            
            # Screenshot for debug
            try:
                page.screenshot(path='/tmp/lazada_debug.png')
            except:
                pass
            
            # Get page content
            content = page.content()
            
            # Save HTML for debug
            try:
                with open('/tmp/lazada_page.html', 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"{LOG_PREFIX} Saved HTML to /tmp/lazada_page.html")
            except:
                pass
            
            # Extract từ script tag chứa window.pageData
            patterns = [
                r'window\.pageData\s*=\s*(\{.+?\});',
                r'__INITIAL_STATE__\s*=\s*(\{.+?\});',
                r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\});'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    try:
                        data = json.loads(match.group(1))
                        items = data.get('mods', {}).get('listItems', [])
                        if not items:
                            items = data.get('listItems', [])
                        if not items:
                            items = data.get('items', [])
                        if items:
                            print(f"{LOG_PREFIX} Found {len(items)} products from JSON")
                            return items
                    except Exception as e:
                        print(f"{LOG_PREFIX} JSON parse error: {e}")
                        continue
            
            # Fallback: Extract từ DOM
            products = []
            
            # Try multiple selectors
            for selector in ['[data-qa-locator="product-item"]', '.Bm3ON', '[data-item-id]', '.gridItem']:
                elements = page.query_selector_all(selector)
                if elements:
                    print(f"{LOG_PREFIX} Found {len(elements)} elements with selector: {selector}")
                    break
            
            if not elements:
                print(f"{LOG_PREFIX} No product elements found with any selector")
                return []
            
            for elem in elements[:40]:
                try:
                    product = {}
                    
                    # Title & URL
                    link = elem.query_selector('a[href]')
                    if link:
                        product['productUrl'] = link.get_attribute('href')
                        product['name'] = link.get_attribute('title') or elem.query_selector('.RfADt')
                        if product['name'] and hasattr(product['name'], 'inner_text'):
                            product['name'] = product['name'].inner_text()
                    
                    # Price
                    price_elem = elem.query_selector('.ooOxS')
                    if price_elem:
                        product['price'] = price_elem.inner_text()
                    
                    # Rating
                    stars = elem.query_selector_all('i._9-ogB.Dy1nx')
                    product['ratingScore'] = str(len(stars)) if stars else '0'
                    
                    # Reviews
                    review_elem = elem.query_selector('.qzqFw')
                    if review_elem:
                        review_text = review_elem.inner_text()
                        match = re.search(r'\((\d+)\)', review_text)
                        product['review'] = match.group(1) if match else '0'
                    
                    # Image
                    img = elem.query_selector('img[src]')
                    if img:
                        product['image'] = img.get_attribute('src')
                    
                    # Item ID
                    if product.get('productUrl'):
                        match = re.search(r'-i(\d+)', product['productUrl'])
                        product['itemId'] = match.group(1) if match else ''
                    
                    if product.get('name'):
                        products.append(product)
                        
                except Exception as e:
                    continue
            
            return products
            
        except Exception as e:
            print(f"{LOG_PREFIX} Extract error: {e}")
            return []

    def transform_product(self, item: Dict, category: str, page: int) -> Dict[str, Any]:
        """Transform sang format chuẩn"""
        price_text = item.get('price', '0')
        price = int(''.join(filter(str.isdigit, str(price_text)))) if price_text else 0
        
        original_price_text = item.get('originalPrice', '0')
        original_price = int(''.join(filter(str.isdigit, str(original_price_text)))) if original_price_text else 0
        
        discount = item.get('discount', '0%').replace('%', '').replace('-', '')
        discount_percent = int(discount) if discount.isdigit() else 0
        
        rating_score = item.get('ratingScore', '0')
        rating_avg = float(rating_score) if rating_score else 0.0
        
        review_count = item.get('review', 0)
        if isinstance(review_count, str):
            review_count = int(''.join(filter(str.isdigit, review_count))) if review_count else 0
        
        product_url = item.get('productUrl', '')
        if product_url and product_url.startswith('//'):
            product_url = 'https:' + product_url
        
        image = item.get('image', '')
        if image and image.startswith('//'):
            image = 'https:' + image
        
        return {
            "source": "lazada",
            "category": category,
            "product_id": str(item.get('itemId', '')),
            "product_name": item.get('name', ''),
            "price_current": price,
            "price_original": original_price if original_price > 0 else price,
            "discount_percent": discount_percent,
            "rating_avg": rating_avg,
            "review_count": review_count,
            "brand": item.get('brandName', ''),
            "seller_name": item.get('sellerName', ''),
            "url": product_url,
            "image_urls": [image] if image else [],
            "crawl_date": datetime.now().isoformat(),
            "page_number": page
        }

    def crawl_category(self, browser, category: str, url: str, max_pages: int = 3) -> List[Dict[str, Any]]:
        """Crawl một category"""
        print(f"{LOG_PREFIX} Crawling: {category}")
        
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        all_products = []
        
        try:
            for page_num in range(1, max_pages + 1):
                page_url = f"{url}?page={page_num}"
                
                print(f"{LOG_PREFIX} Page {page_num}: {page_url}")
                page.goto(page_url, wait_until='networkidle', timeout=60000)
                
                # Wait longer
                time.sleep(random.uniform(5, 8))
                
                # Random scroll
                for _ in range(5):
                    page.evaluate(f'window.scrollBy(0, {random.randint(200, 600)})')
                    time.sleep(random.uniform(1, 2))
                
                time.sleep(random.uniform(3, 5))
                
                # Extract products
                items = self.extract_products_from_page(page)
                
                if not items:
                    print(f"{LOG_PREFIX} No products on page {page_num}")
                    break
                
                for item in items:
                    product = self.transform_product(item, category, page_num)
                    if product.get('product_name'):
                        all_products.append(product)
                
                time.sleep(random.uniform(3, 6))
                
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

    def run(self, max_pages=3):
        """Run crawler"""
        if not PLAYWRIGHT_AVAILABLE:
            print(f"{LOG_PREFIX} Playwright not available")
            return
        
        print(f"{LOG_PREFIX} Starting...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
            
            try:
                for category, url in self.categories.items():
                    products = self.crawl_category(browser, category, url, max_pages)
                    
                    if products:
                        self.save_jsonl(products, category)
                    
                    time.sleep(random.uniform(3, 5))
                
                print(f"{LOG_PREFIX} Completed!")
            finally:
                browser.close()

def main():
    crawler = LazadaPlaywrightCrawler()
    crawler.run(max_pages=3)

if __name__ == "__main__":
    main()
