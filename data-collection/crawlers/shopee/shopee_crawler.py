#!/usr/bin/env python3
"""
Shopee API Crawler - Thay thế Lazada
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
LOG_PREFIX = "[Shopee]"

class ShopeeCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://shopee.vn/'
        })
        
        self.categories = {
            "smartphones": {"cat": 11036132, "name": "Điện Thoại"},
            "laptops": {"cat": 11035954, "name": "Laptop"},
            "tablets": {"cat": 11036279, "name": "Máy Tính Bảng"},
            "smartwatches": {"cat": 11035639, "name": "Đồng Hồ Thông Minh"},
            "headphones": {"cat": 11036188, "name": "Tai Nghe"}
        }

    def fetch_page(self, cat_id: int, page: int) -> List[Dict[str, Any]]:
        url = "https://shopee.vn/api/v4/search/search_items"
        
        params = {
            'by': 'relevancy',
            'limit': 60,
            'match_id': cat_id,
            'newest': page * 60,
            'order': 'desc',
            'page_type': 'search',
            'scenario': 'PAGE_CATEGORY',
            'version': 2
        }
        
        try:
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code != 200:
                print(f"{LOG_PREFIX} HTTP {response.status_code}")
                return []
            
            data = response.json()
            items = data.get('items', [])
            
            if items:
                print(f"{LOG_PREFIX} Page {page}: {len(items)} products")
            
            return items
            
        except Exception as e:
            print(f"{LOG_PREFIX} Error: {e}")
            return []

    def transform_product(self, item: Dict, category: str, page: int) -> Dict[str, Any]:
        item_basic = item.get('item_basic', {})
        
        price = item_basic.get('price', 0) // 100000
        raw_discount = item_basic.get('raw_discount', 0)
        discount_percent = int(raw_discount) if raw_discount else 0
        price_original = int(price / (1 - discount_percent/100)) if discount_percent > 0 else price
        
        rating = item_basic.get('item_rating', {}).get('rating_star', 0)
        rating_avg = float(rating) if rating else 0.0
        
        review_count = item_basic.get('item_rating', {}).get('rating_count', [0])[0]
        
        image_hash = item_basic.get('image', '')
        image_url = f"https://cf.shopee.vn/file/{image_hash}" if image_hash else ""
        
        item_id = item_basic.get('itemid', '')
        shop_id = item_basic.get('shopid', '')
        product_url = f"https://shopee.vn/product/{shop_id}/{item_id}" if item_id and shop_id else ""
        
        return {
            "source": "shopee",
            "category": category,
            "product_id": str(item_id),
            "product_name": item_basic.get('name', ''),
            "price_current": price,
            "price_original": price_original,
            "discount_percent": discount_percent,
            "rating_avg": rating_avg,
            "review_count": review_count,
            "brand": item_basic.get('brand', ''),
            "seller_name": item_basic.get('shop_name', ''),
            "url": product_url,
            "image_urls": [image_url] if image_url else [],
            "crawl_date": datetime.now().isoformat(),
            "page_number": page
        }

    def crawl_category(self, category: str, config: Dict, max_pages: int = 2) -> List[Dict[str, Any]]:
        print(f"{LOG_PREFIX} Crawling: {category}")
        
        cat_id = config['cat']
        all_products = []
        
        for page in range(max_pages):
            items = self.fetch_page(cat_id, page)
            
            if not items:
                break
            
            for item in items:
                product = self.transform_product(item, category, page + 1)
                if product.get('product_name'):
                    all_products.append(product)
            
            time.sleep(random.uniform(1, 2))
        
        print(f"{LOG_PREFIX} Category '{category}': {len(all_products)} products")
        return all_products

    def save_jsonl(self, products: List[Dict[str, Any]], category: str):
        output_dir = OUTPUT_DIR
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except:
            output_dir = "/tmp"
        
        today = datetime.now().strftime("%Y-%m-%d")
        date_dir = Path(output_dir) / "shopee" / f"date={today}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"shopee_{category}_{timestamp}.jsonl"
        filepath = date_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False) + '\n')
        
        print(f"{LOG_PREFIX} Saved {len(products)} to {filepath}")

    def run(self, max_pages=2):
        print(f"{LOG_PREFIX} Starting...")
        
        for category, config in self.categories.items():
            products = self.crawl_category(category, config, max_pages)
            
            if products:
                self.save_jsonl(products, category)
            
            time.sleep(random.uniform(2, 3))
        
        print(f"{LOG_PREFIX} Completed!")

def main():
    crawler = ShopeeCrawler()
    crawler.run(max_pages=2)

if __name__ == "__main__":
    main()
