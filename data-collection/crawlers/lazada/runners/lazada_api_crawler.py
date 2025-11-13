#!/usr/bin/env python3
"""
Lazada API Crawler - Bypass anti-bot bằng cách dùng API
"""
import requests
import json
import time
import random
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
LOG_PREFIX = "[Lazada-API]"

class LazadaAPICrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Referer': 'https://www.lazada.vn/'
        })
        
        self.categories = {
            "smartphones": {"q": "điện thoại", "cat_id": ""},
            "laptops": {"q": "laptop", "cat_id": ""},
            "tablets": {"q": "máy tính bảng", "cat_id": ""},
            "smartwatches": {"q": "đồng hồ thông minh", "cat_id": ""},
            "headphones": {"q": "tai nghe", "cat_id": ""}
        }

    def fetch_page(self, query: str, page: int) -> List[Dict[str, Any]]:
        """Fetch products từ Lazada mobile API"""
        url = "https://www.lazada.vn/api/search"
        
        params = {
            'q': query,
            'page': page,
            'pageSize': 40,
            'sort': 'popularity'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)',
            'Accept': 'application/json, text/plain, */*',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': f'https://www.lazada.vn/catalog/?q={query}'
        }
        
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=15)
            
            if response.status_code != 200:
                print(f"{LOG_PREFIX} HTTP {response.status_code}")
                # Fallback: Try scraping HTML
                return self._scrape_html_fallback(query, page)
            
            try:
                data = response.json()
                items = data.get('mods', {}).get('listItems', [])
                
                if not items:
                    items = data.get('items', [])
                
                if items:
                    print(f"{LOG_PREFIX} Page {page}: {len(items)} products")
                    return items
            except:
                pass
            
            # Fallback to HTML scraping
            return self._scrape_html_fallback(query, page)
            
        except Exception as e:
            print(f"{LOG_PREFIX} Error: {e}")
            return self._scrape_html_fallback(query, page)
    
    def _scrape_html_fallback(self, query: str, page: int) -> List[Dict[str, Any]]:
        """Fallback: Parse HTML response"""
        try:
            url = f"https://www.lazada.vn/catalog/?q={query}&page={page}"
            response = self.session.get(url, timeout=15)
            
            if response.status_code != 200:
                return []
            
            html = response.text
            
            # Extract JSON data từ script tag
            import re
            match = re.search(r'window\.pageData\s*=\s*(\{.+?\});', html, re.DOTALL)
            if not match:
                print(f"{LOG_PREFIX} Cannot find pageData in HTML")
                return []
            
            data = json.loads(match.group(1))
            items = data.get('mods', {}).get('listItems', [])
            
            if items:
                print(f"{LOG_PREFIX} HTML fallback page {page}: {len(items)} products")
            
            return items
            
        except Exception as e:
            print(f"{LOG_PREFIX} HTML fallback error: {e}")
            return []

    def transform_product(self, item: Dict, category: str, page: int) -> Dict[str, Any]:
        """Transform Lazada API response sang format chuẩn"""
        
        # Extract giá
        price_text = item.get('price', '0')
        price = int(''.join(filter(str.isdigit, str(price_text)))) if price_text else 0
        
        original_price_text = item.get('originalPrice', '0')
        original_price = int(''.join(filter(str.isdigit, str(original_price_text)))) if original_price_text else 0
        
        # Discount
        discount = item.get('discount', '0%').replace('%', '').replace('-', '')
        discount_percent = int(discount) if discount.isdigit() else 0
        
        # Rating
        rating_score = item.get('ratingScore')
        rating_avg = float(rating_score) if rating_score else 0.0
        
        # Review count
        review_count = item.get('review', 0)
        if isinstance(review_count, str):
            review_count = int(''.join(filter(str.isdigit, review_count))) if review_count else 0
        
        # URL
        product_url = item.get('productUrl', '')
        if product_url and product_url.startswith('//'):
            product_url = 'https:' + product_url
        
        # Image
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

    def crawl_category(self, category: str, config: Dict, max_pages: int = 3) -> List[Dict[str, Any]]:
        """Crawl một category"""
        print(f"{LOG_PREFIX} Crawling: {category} (max {max_pages} pages)")
        
        query = config['q']
        all_products = []
        
        for page in range(1, max_pages + 1):
            items = self.fetch_page(query, page)
            
            if not items:
                break
            
            for item in items:
                product = self.transform_product(item, category, page)
                if product.get('product_name'):
                    all_products.append(product)
            
            time.sleep(random.uniform(1, 3))
        
        print(f"{LOG_PREFIX} Category '{category}': {len(all_products)} products")
        return all_products

    def save_jsonl(self, products: List[Dict[str, Any]], category: str):
        """Save products to JSONL"""
        output_dir = OUTPUT_DIR
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except:
            output_dir = "/tmp"
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        today = datetime.now().strftime("%Y-%m-%d")
        date_dir = Path(output_dir) / "lazada" / f"date={today}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"lazada_{category}_{timestamp}.jsonl"
        filepath = date_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False) + '\n')
        
        print(f"{LOG_PREFIX} Saved {len(products)} products to {filepath}")

    def run(self, max_pages=3):
        """Run crawler cho tất cả categories"""
        print(f"{LOG_PREFIX} Starting Lazada API crawler...")
        
        for category, config in self.categories.items():
            products = self.crawl_category(category, config, max_pages)
            
            if products:
                self.save_jsonl(products, category)
            
            time.sleep(random.uniform(2, 4))
        
        print(f"{LOG_PREFIX} Completed!")

def main():
    crawler = LazadaAPICrawler()
    crawler.run(max_pages=3)

if __name__ == "__main__":
    main()
