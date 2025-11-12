#!/usr/bin/env python3
"""
Lazada Reviews Crawler with Cookies (Same approach as products crawler)
"""
import json
import time
import random
import os
import re
import uuid
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
COOKIE_FILE = "/tmp/profiles/lazada/cookies.json"
LOG_PREFIX = "[Lazada-Reviews]"

class LazadaReviewsCrawler:
    def __init__(self):
        self.categories = {
            "smartphones": "https://www.lazada.vn/tag/mobiles/?q=mobiles",
            "laptops": "https://www.lazada.vn/tag/laptops/?q=laptops",
            "tablets": "https://www.lazada.vn/tag/tablets/?q=tablets"
        }
        self.max_products_per_category = 5
        self.max_reviews_per_product = 5

    def extract_product_urls(self, page) -> List[Dict[str, str]]:
        """Extract product URLs from listing page (same as products crawler)"""
        try:
            time.sleep(random.uniform(3, 5))
            
            for _ in range(5):
                page.evaluate(f'window.scrollBy(0, {random.randint(300, 700)})')
                time.sleep(random.uniform(1, 2))
            
            # Try JSON data first
            content = page.content()
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
                            products = []
                            for item in items[:self.max_products_per_category]:
                                url = item.get('productUrl', '')
                                if url:
                                    if url.startswith('//'):
                                        url = 'https:' + url
                                    products.append({
                                        'url': url,
                                        'name': item.get('name', 'Unknown'),
                                        'id': item.get('itemId', '')
                                    })
                            print(f"{LOG_PREFIX} Found {len(products)} products from JSON")
                            return products
                    except:
                        continue
            
            # Fallback to DOM extraction
            elements = page.query_selector_all('div.Bm3ON[data-qa-locator="product-item"]')
            print(f"{LOG_PREFIX} Found {len(elements)} product elements")
            
            products = []
            for elem in elements[:self.max_products_per_category]:
                try:
                    link = elem.query_selector('div.RfADt > a[href]')
                    if link:
                        href = link.get_attribute('href')
                        if href:
                            if href.startswith('//'):
                                href = 'https:' + href
                            elif href.startswith('/'):
                                href = 'https://www.lazada.vn' + href
                            
                            name = link.inner_text().strip() or link.get_attribute('title') or 'Unknown'
                            
                            match = re.search(r'-i(\d+)', href)
                            product_id = match.group(1) if match else ''
                            
                            products.append({
                                'url': href,
                                'name': name,
                                'id': product_id
                            })
                except:
                    continue
            
            return products
            
        except Exception as e:
            print(f"{LOG_PREFIX} Extract error: {e}")
            return []

    def extract_reviews_from_page(self, page, product_url: str, product_id: str, product_name: str) -> List[Dict[str, Any]]:
        """Extract reviews from product detail page"""
        reviews = []
        
        try:
            print(f"{LOG_PREFIX} Loading product: {product_name[:50]}...")
            page.goto(product_url, wait_until='domcontentloaded', timeout=90000)
            page.wait_for_timeout(5000)
            
            # Scroll to reviews section
            for _ in range(3):
                page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                page.wait_for_timeout(2000)
            
            # Extract reviews using selectors from product_detail.html
            review_items = page.query_selector_all('div.mod-reviews div.item')
            print(f"{LOG_PREFIX} Found {len(review_items)} review items")
            
            for i, item in enumerate(review_items[:self.max_reviews_per_product]):
                try:
                    # Reviewer name
                    reviewer_name = "Anonymous"
                    try:
                        reviewer_elem = item.query_selector('span.reviewer')
                        if reviewer_elem:
                            reviewer_name = reviewer_elem.inner_text().strip()
                    except:
                        pass
                    
                    # Review date
                    review_date = None
                    try:
                        time_elem = item.query_selector('span.time')
                        if time_elem:
                            review_date = time_elem.inner_text().strip()
                    except:
                        pass
                    
                    # Rating (count stars)
                    rating = 0
                    try:
                        stars = item.query_selector_all('div.container-star img.star')
                        rating = len(stars)
                    except:
                        pass
                    
                    # Review text
                    review_text = ""
                    try:
                        review_elem = item.query_selector('div.item-content-main-content-reviews-item span')
                        if review_elem:
                            review_text = review_elem.inner_text().strip()
                    except:
                        pass
                    
                    # SKU info
                    sku_info = ""
                    try:
                        sku_elems = item.query_selector_all('div.skuInfo-item')
                        sku_parts = []
                        for sku in sku_elems:
                            sku_parts.append(sku.inner_text().strip())
                        sku_info = ", ".join(sku_parts)
                    except:
                        pass
                    
                    # Helpful count
                    helpful_count = 0
                    try:
                        helpful_elem = item.query_selector('span.item-content-like-content-text')
                        if helpful_elem:
                            helpful_text = helpful_elem.inner_text()
                            match = re.search(r'Helpful\((\d+)\)', helpful_text)
                            if match:
                                helpful_count = int(match.group(1))
                    except:
                        pass
                    
                    if review_text:
                        review_data = {
                            'review_id': f"lazada_rev_{uuid.uuid4().hex[:8]}",
                            'product_id': product_id,
                            'product_name': product_name,
                            'product_url': product_url,
                            'reviewer_name': reviewer_name,
                            'review_date': review_date,
                            'rating': rating,
                            'review_text': review_text[:500],
                            'sku_info': sku_info,
                            'helpful_count': helpful_count,
                            'crawl_timestamp': datetime.now().isoformat()
                        }
                        reviews.append(review_data)
                        print(f"{LOG_PREFIX}   Review {i+1}: {rating}★ - {review_text[:50]}...")
                
                except Exception as e:
                    print(f"{LOG_PREFIX} Failed to extract review {i+1}: {e}")
                    continue
            
            print(f"{LOG_PREFIX} Extracted {len(reviews)} reviews")
            
        except Exception as e:
            print(f"{LOG_PREFIX} Error extracting reviews: {e}")
        
        return reviews

    def crawl_category_reviews(self, browser, category: str, url: str, max_pages: int = 2) -> List[Dict[str, Any]]:
        """Crawl reviews for a category (same pattern as products crawler)"""
        print(f"{LOG_PREFIX} Crawling: {category}")
        
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        if os.path.exists(COOKIE_FILE):
            with open(COOKIE_FILE, 'r') as f:
                cookies = json.load(f)
                context.add_cookies(cookies)
            print(f"{LOG_PREFIX} Cookies loaded")
        else:
            print(f"{LOG_PREFIX} WARNING: No cookies found")
        
        page = context.new_page()
        all_reviews = []
        
        try:
            for page_num in range(1, max_pages + 1):
                if '?' in url:
                    page_url = f"{url}&page={page_num}"
                else:
                    page_url = f"{url}?page={page_num}"
                print(f"{LOG_PREFIX} Page {page_num}")
                
                try:
                    page.goto(page_url, wait_until='domcontentloaded', timeout=90000)
                    page.wait_for_timeout(3000)
                except Exception as e:
                    print(f"{LOG_PREFIX} Page load timeout, retrying...")
                    page.goto(page_url, wait_until='load', timeout=120000)
                
                products = self.extract_product_urls(page)
                
                if not products:
                    print(f"{LOG_PREFIX} No products on page {page_num}")
                    break
                
                # Extract reviews from each product
                for i, product in enumerate(products):
                    try:
                        reviews = self.extract_reviews_from_page(
                            page,
                            product['url'],
                            product['id'],
                            product['name']
                        )
                        
                        for review in reviews:
                            review['category'] = category
                            review['page_number'] = page_num
                        
                        all_reviews.extend(reviews)
                        time.sleep(random.uniform(2, 4))
                        
                    except Exception as e:
                        print(f"{LOG_PREFIX} Failed product {i+1}: {e}")
                        continue
                
                time.sleep(random.uniform(3, 5))
                
        except Exception as e:
            print(f"{LOG_PREFIX} Error: {e}")
        finally:
            context.close()
        
        print(f"{LOG_PREFIX} Category '{category}': {len(all_reviews)} reviews")
        return all_reviews

    def save_jsonl(self, reviews: List[Dict[str, Any]], category: str):
        """Save reviews to JSONL file"""
        output_dir = OUTPUT_DIR
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except:
            output_dir = "/tmp"
        
        today = datetime.now().strftime("%Y-%m-%d")
        date_dir = Path(output_dir) / "lazada_reviews" / f"date={today}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"lazada_reviews_{category}_{timestamp}.jsonl"
        filepath = date_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for review in reviews:
                f.write(json.dumps(review, ensure_ascii=False) + '\n')
        
        print(f"{LOG_PREFIX} Saved {len(reviews)} to {filepath}")

    def run(self, max_pages=2):
        """Run the reviews crawler"""
        if not PLAYWRIGHT_AVAILABLE:
            print(f"{LOG_PREFIX} Playwright not available")
            return
        
        if not os.path.exists(COOKIE_FILE):
            print(f"{LOG_PREFIX} WARNING: Cookie file not found: {COOKIE_FILE}")
            print(f"{LOG_PREFIX} Will try without cookies...")
        
        print(f"{LOG_PREFIX} Starting reviews crawler...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            try:
                for category, url in self.categories.items():
                    reviews = self.crawl_category_reviews(browser, category, url, max_pages)
                    
                    if reviews:
                        self.save_jsonl(reviews, category)
                    
                    time.sleep(random.uniform(2, 4))
                
                print(f"{LOG_PREFIX} Completed!")
            finally:
                browser.close()

def main():
    crawler = LazadaReviewsCrawler()
    crawler.run(max_pages=1)  # Limit to 1 page for testing

if __name__ == "__main__":
    main()
