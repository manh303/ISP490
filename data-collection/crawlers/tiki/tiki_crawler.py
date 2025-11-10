#!/usr/bin/env python3
"""
Tiki API Crawler - Kế thừa từ mass crawler
"""
import requests
import json
import time
import random
import os
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
LOG_PREFIX = "[Tiki]"

class TikiCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
        })
        
        self.categories = [
            'điện thoại',
            'laptop',
            'máy tính bảng',
            'đồng hồ thông minh',
            'tai nghe',
            'máy ảnh',
            'loa bluetooth',
            'màn hình máy tính',
            'chuột máy tính',
            'bàn phím',
            'tivi smart',
            'máy in'
        ]

    def crawl_category(self, category: str, max_pages: int = 60):
        """Crawl Tiki category using API"""
        print(f"{LOG_PREFIX} Crawling: {category} (max {max_pages} pages)")
        
        all_products = []
        
        for page in range(1, max_pages + 1):
            try:
                url = "https://tiki.vn/api/v2/products"
                params = {
                    'limit': 40,
                    'include': 'advertisement',
                    'aggregations': 2,
                    'q': category,
                    'page': page
                }
                
                response = self.session.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    products = data.get('data', [])
                    
                    if not products:
                        print(f"{LOG_PREFIX} No products on page {page}")
                        break
                    
                    print(f"{LOG_PREFIX} Page {page}: {len(products)} products")
                    
                    for product in products:
                        try:
                            processed = {
                                "source": "tiki",
                                "category": category,
                                "product_id": str(product.get('id', '')),
                                "product_name": product.get('name', ''),
                                "price_current": product.get('price', 0),
                                "price_original": product.get('list_price', 0),
                                "discount_percent": product.get('discount_rate', 0),
                                "rating_avg": product.get('rating_average', 0),
                                "review_count": product.get('review_count', 0),
                                "brand": product.get('brand_name', ''),
                                "seller_name": product.get('seller_name', ''),
                                "url": f"https://tiki.vn/{product.get('url_path', '')}",
                                "image_urls": [product.get('thumbnail_url', '')],
                                "crawl_date": datetime.now().isoformat(),
                                "page_number": page
                            }
                            all_products.append(processed)
                        except Exception as e:
                            print(f"{LOG_PREFIX} Error processing product: {e}")
                    
                    time.sleep(random.uniform(2, 4))
                else:
                    print(f"{LOG_PREFIX} HTTP {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"{LOG_PREFIX} Error on page {page}: {e}")
                break
        
        print(f"{LOG_PREFIX} Category '{category}': {len(all_products)} products")
        return all_products

    def save_jsonl(self, products, category: str):
        """Save to JSONL"""
        output_dir = OUTPUT_DIR
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except:
            output_dir = "/tmp"
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tiki_{category.replace(' ', '_')}_{timestamp}.jsonl"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False) + '\n')
        
        print(f"{LOG_PREFIX} Saved {len(products)} products to {filepath}")

    def run(self, max_pages=60):
        """Run crawler"""
        print(f"{LOG_PREFIX} Starting Tiki API crawler...")
        
        all_products = []
        
        for category in self.categories:
            products = self.crawl_category(category, max_pages=max_pages)
            
            if products:
                all_products.extend(products)
                self.save_jsonl(products, category)
            
            time.sleep(random.uniform(3, 5))
        
        print(f"{LOG_PREFIX} Total: {len(all_products)} products")
        print(f"{LOG_PREFIX} Completed!")

def main():
    crawler = TikiCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
