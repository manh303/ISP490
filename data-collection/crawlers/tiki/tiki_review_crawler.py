#!/usr/bin/env python3
"""
Tiki Review Crawler - Crawl reviews from Tiki API
"""
import requests
import json
import time
import random
import os
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
LOG_PREFIX = "[Tiki-Reviews]"
CHECKPOINT_FILE = Path(OUTPUT_DIR) / "tiki_reviews_checkpoint.json"

class TikiReviewCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
        })
        self.checkpoint = self.load_checkpoint()

    def load_checkpoint(self):
        """Load crawled product IDs"""
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return set(json.load(f))
        return set()

    def save_checkpoint(self, product_id):
        """Save progress"""
        self.checkpoint.add(product_id)
        CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(list(self.checkpoint), f)

    def get_product_ids_from_jsonl(self):
        """Read product IDs from existing Tiki JSONL files"""
        product_ids = []
        tiki_dir = Path(OUTPUT_DIR) / "tiki"
        
        if not tiki_dir.exists():
            print(f"{LOG_PREFIX} No Tiki data found. Run tiki_crawler.py first!")
            return []
        
        for jsonl_file in tiki_dir.rglob("*.jsonl"):
            try:
                with open(jsonl_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        product = json.loads(line)
                        product_id = product.get('product_id')
                        if product_id:
                            product_ids.append(product_id)
            except Exception as e:
                print(f"{LOG_PREFIX} Error reading {jsonl_file}: {e}")
        
        return list(set(product_ids))

    def crawl_product_reviews(self, product_id: str, max_reviews: int = 100):
        """Crawl reviews for a product"""
        all_reviews = []
        page = 1
        
        while len(all_reviews) < max_reviews:
            try:
                url = "https://tiki.vn/api/v2/reviews"
                params = {
                    'product_id': product_id,
                    'limit': 20,
                    'page': page,
                    'sort': 'score|desc,id|desc,stars|all'
                }
                
                response = self.session.get(url, params=params, timeout=15)
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                reviews = data.get('data', [])
                
                if not reviews:
                    break
                
                for review in reviews:
                    try:
                        processed = {
                            "source": "tiki",
                            "product_id": product_id,
                            "review_id": str(review.get('id', '')),
                            "reviewer_name": review.get('created_by', {}).get('name', 'Anonymous'),
                            "rating": review.get('rating', 0),
                            "title": review.get('title', ''),
                            "content": review.get('content', ''),
                            "review_time": review.get('created_at', ''),
                            "helpful_count": review.get('thank_count', 0),
                            "images": review.get('images', []),
                            "crawl_date": datetime.now().isoformat()
                        }
                        all_reviews.append(processed)
                    except Exception as e:
                        print(f"{LOG_PREFIX} Error processing review: {e}")
                
                if len(reviews) < 20:
                    break
                
                page += 1
                time.sleep(random.uniform(0.5, 1))
                
            except Exception as e:
                print(f"{LOG_PREFIX} Error on page {page}: {e}")
                break
        
        return all_reviews[:max_reviews]

    def save_jsonl(self, reviews):
        """Save reviews to JSONL with date partition"""
        output_dir = OUTPUT_DIR
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except:
            output_dir = "/tmp"
        
        today = datetime.now().strftime("%Y-%m-%d")
        date_dir = Path(output_dir) / "tiki_reviews" / f"date={today}"
        date_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tiki_reviews_{timestamp}.jsonl"
        filepath = date_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for review in reviews:
                f.write(json.dumps(review, ensure_ascii=False) + '\n')
        
        print(f"{LOG_PREFIX} Saved {len(reviews)} reviews to {filepath}")

    def run(self, max_products: int = 100, max_reviews_per_product: int = 50):
        """Run review crawler"""
        print(f"{LOG_PREFIX} Starting Tiki Review crawler...")
        
        product_ids = self.get_product_ids_from_jsonl()
        
        if not product_ids:
            print(f"{LOG_PREFIX} No products found!")
            return
        
        print(f"{LOG_PREFIX} Found {len(product_ids)} products")
        product_ids = product_ids[:max_products]
        
        all_reviews = []
        
        for i, product_id in enumerate(product_ids, 1):
            if product_id in self.checkpoint:
                print(f"{LOG_PREFIX} [{i}/{len(product_ids)}] Skip {product_id} (done)")
                continue
            
            print(f"{LOG_PREFIX} [{i}/{len(product_ids)}] Product {product_id}")
            
            reviews = self.crawl_product_reviews(product_id, max_reviews_per_product)
            
            if reviews:
                print(f"{LOG_PREFIX} Got {len(reviews)} reviews")
                all_reviews.extend(reviews)
            else:
                print(f"{LOG_PREFIX} No reviews")
            
            self.save_checkpoint(product_id)
            time.sleep(random.uniform(1, 2))
            
            # Save every 500 reviews
            if len(all_reviews) >= 500:
                self.save_jsonl(all_reviews)
                all_reviews = []
        
        # Save remaining
        if all_reviews:
            self.save_jsonl(all_reviews)
        
        print(f"{LOG_PREFIX} Completed!")

def main():
    crawler = TikiReviewCrawler()
    crawler.run(max_products=100, max_reviews_per_product=50)

if __name__ == "__main__":
    main()
