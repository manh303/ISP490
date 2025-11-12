#!/usr/bin/env python3
"""
Lazada Mock Crawler - Generate realistic mock data
"""
import json
import random
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
LOG_PREFIX = "[Lazada-Mock]"

class LazadaMockCrawler:
    def __init__(self):
        self.categories = {
            "smartphones": ["iPhone", "Samsung Galaxy", "Xiaomi", "OPPO", "Vivo", "Realme"],
            "laptops": ["Dell", "HP", "Lenovo", "Asus", "Acer", "MSI"],
            "tablets": ["iPad", "Samsung Tab", "Huawei MatePad", "Lenovo Tab"],
            "smartwatches": ["Apple Watch", "Samsung Galaxy Watch", "Xiaomi Mi Band", "Huawei Watch"],
            "headphones": ["AirPods", "Sony", "JBL", "Samsung Buds", "Beats"]
        }
        
        self.models = {
            "smartphones": ["15 Pro Max", "S24 Ultra", "14 Pro", "Note 13", "A54", "12T"],
            "laptops": ["XPS 15", "Pavilion", "ThinkPad", "VivoBook", "Aspire", "Gaming"],
            "tablets": ["Pro 12.9", "S9", "Air", "M11", "P11"],
            "smartwatches": ["Series 9", "6 Classic", "Band 8", "GT 4"],
            "headphones": ["Pro 2", "WH-1000XM5", "Tune", "Buds 2", "Studio"]
        }

    def generate_product(self, category: str, page: int, idx: int) -> Dict[str, Any]:
        """Generate một product mock"""
        brand = random.choice(self.categories[category])
        model = random.choice(self.models[category])
        
        base_price = {
            "smartphones": random.randint(3000000, 30000000),
            "laptops": random.randint(10000000, 40000000),
            "tablets": random.randint(5000000, 25000000),
            "smartwatches": random.randint(2000000, 15000000),
            "headphones": random.randint(500000, 8000000)
        }[category]
        
        discount = random.choice([0, 5, 10, 15, 20, 25, 30])
        price_current = int(base_price * (1 - discount/100))
        
        product_id = f"lz{random.randint(100000000, 999999999)}"
        
        return {
            "source": "lazada",
            "category": category,
            "product_id": product_id,
            "product_name": f"{brand} {model}",
            "price_current": price_current,
            "price_original": base_price,
            "discount_percent": discount,
            "rating_avg": round(random.uniform(3.5, 5.0), 1),
            "review_count": random.randint(10, 5000),
            "brand": brand,
            "seller_name": random.choice(["Lazada Official", "Tech Store", "Mobile World", "Gadget Shop"]),
            "url": f"https://www.lazada.vn/products/{product_id}.html",
            "image_urls": [f"https://img.lazcdn.com/g/p/{product_id}.jpg"],
            "crawl_date": datetime.now().isoformat(),
            "page_number": page
        }

    def crawl_category(self, category: str, max_pages: int = 3) -> List[Dict[str, Any]]:
        """Generate products cho category"""
        print(f"{LOG_PREFIX} Generating: {category} ({max_pages} pages)")
        
        all_products = []
        products_per_page = 40
        
        for page in range(1, max_pages + 1):
            for idx in range(products_per_page):
                product = self.generate_product(category, page, idx)
                all_products.append(product)
        
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
        """Run mock crawler"""
        print(f"{LOG_PREFIX} Starting mock data generation...")
        
        for category in self.categories.keys():
            products = self.crawl_category(category, max_pages)
            
            if products:
                self.save_jsonl(products, category)
        
        print(f"{LOG_PREFIX} Completed!")

def main():
    crawler = LazadaMockCrawler()
    crawler.run(max_pages=3)

if __name__ == "__main__":
    main()
