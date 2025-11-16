#!/usr/bin/env python3
"""Mass crawler for multiple categories and pages - Windows console compatible"""

import requests
import json
import time
import random
import os
import hashlib
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from urllib.parse import urljoin


class MassCrawler:
    def __init__(self):
        self.session = requests.Session()

        # --- Thư mục output để MinIO / Airflow đọc ---
        # Sẽ trùng với CRAWLER_OUTPUT_DIR trong Airflow (ví dụ: /app/data/outputs)
        self.output_dir = Path(os.getenv("CRAWLER_OUTPUT_DIR", "/app/data/outputs"))

        # Browser headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

        # Multiple categories for Tiki
        self.tiki_categories = [
            'điện thoại',
            'laptop',
            'máy tính bảng',
            'dồng hồ thông minh',
            'tai nghe',
            'máy ảnh',
            'loa bluetooth',
            'màn hình máy tính',
            'chuột máy tính',
            'bàn phím máy tính',
            'tivi smart',
            'máy in'
        ]

        # CellphoneS categories (chưa dùng, để sẵn)
        self.cellphones_categories = {
            'dien-thoai': 'smartphone',
            'laptop': 'laptop',
            'tablet': 'tablet',
            'dong-ho-thong-minh': 'smartwatch',
            'tai-nghe': 'headphone'
        }

        # Combined categories for mass crawling (use tiki_categories as main)
        self.categories = self.tiki_categories

        self.stats = {
            'total_products': 0,
            'tiki_products': 0,
            'cellphones_products': 0,
            'categories_processed': 0,
            'pages_crawled': 0,
            'start_time': None
        }

    # vẫn giữ cho tương lai nếu cần
    def generate_global_id(self, platform, product_id):
        """Generate global product ID"""
        return hashlib.sha256(f"{platform}_{product_id}".encode()).hexdigest()

    # -----------------------------
    #  LƯU DỮ LIỆU DẠNG JSONL → MINIO BUFFER
    # -----------------------------
    @staticmethod
    def _slugify(text: str) -> str:
        text = text.lower()
        return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")

    def save_to_minio_buffer(self, products, category: str):
        """
        Thay cho save_to_db:
        - Ghi products ra file .jsonl trong CRAWLER_OUTPUT_DIR
        - Để Airflow upload lên MinIO sau này
        """
        if not products:
            return

        date_str = datetime.now().strftime("%Y-%m-%d")
        category_slug = self._slugify(category)

        # ví dụ: /app/data/outputs/tiki/date=2025-11-15/dien_thoai.jsonl
        out_dir = self.output_dir / "tiki" / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)

        file_path = out_dir / f"{category_slug}.jsonl"

        with file_path.open("w", encoding="utf-8") as f:
            for p in products:
                f.write(json.dumps(p, ensure_ascii=False) + "\n")

        print(
            f"      Saved {len(products)} products for category '{category}' "
            f"to {file_path} (MinIO buffer)"
        )

    # -----------------------------
    #  CRAWL TIKI
    # -----------------------------
    def crawl_tiki_category_paginated(self, category, max_pages=50):
        """Crawl Tiki category with multiple pages"""
        print(f"\n>>> Crawling category: {category} (up to {max_pages} pages)")

        all_products = []

        for page in range(1, max_pages + 1):
            print(f"    Page {page}/{max_pages}...")

            try:
                url = "https://tiki.vn/api/v2/products"
                params = {
                    'limit': 40,  # 40 products per page
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
                        print(f"      No products on page {page}, stopping")
                        break

                    print(f"      Found {len(products)} products")

                    # Process each product
                    for product in products:
                        try:
                            processed = {
                                "source": "tiki_mass_crawl",
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
                            print(f"      Error processing product: {e}")
                            continue

                    self.stats['pages_crawled'] += 1

                    # Random delay between pages
                    time.sleep(random.uniform(2, 4))

                else:
                    print(f"      HTTP Error {response.status_code}")
                    break

            except Exception as e:
                print(f"      Exception on page {page}: {e}")
                break

        print(f"    Category '{category}' total: {len(all_products)} products")
        return all_products

    # -----------------------------
    #  MASS CRAWL
    # -----------------------------
    def run_mass_crawl(self):
        """Run crawl tiki across all categories"""
        print("=" * 60)
        print("CRAWLER TIKI STARTING")
        print("=" * 60)
        print(f"Categories to crawl: {len(self.categories)}")
        print(f"Expected products: {len(self.categories) * 15 * 40}")  # categories * pages * products_per_page
        print()

        self.stats['start_time'] = time.time()
        all_products = []

        # Crawl each category
        for i, category in enumerate(self.categories, 1):
            print(f"\n[{i}/{len(self.categories)}] Processing: {category}")

            try:
                category_products = self.crawl_tiki_category_paginated(category, max_pages=50)
                all_products.extend(category_products)
                self.stats['categories_processed'] += 1

                # --- thay vì save_to_db, lưu ra file cho MinIO ---
                if category_products:
                    self.save_to_minio_buffer(category_products, category)

                print(f"    Running total: {len(all_products)} products")

                # Break between categories
                print("    Waiting before next category...")
                time.sleep(random.uniform(5, 8))

            except Exception as e:
                print(f"    ERROR in category {category}: {e}")
                continue

        # Không còn db_conn nên không close

        self.print_summary(all_products)
        return all_products

    # -----------------------------
    #  SUMMARY
    # -----------------------------
    def get_breakdown(self, products):
        """Get breakdown by category"""
        breakdown = {}
        for product in products:
            category = product.get('category', 'unknown')
            breakdown[category] = breakdown.get(category, 0) + 1
        return breakdown

    def print_summary(self, products):
        """Print final summary"""
        duration = time.time() - self.stats['start_time']

        print("\n" + "=" * 60)
        print("CRAWL TIKI COMPLETED!")
        print("=" * 60)
        print(f"Duration: {duration/60:.1f} minutes")
        print(f"Total products: {len(products):,}")
        print(f"Categories: {self.stats['categories_processed']}")
        print(f"Pages: {self.stats['pages_crawled']}")
        print(f"Speed: {len(products)/(duration/60):.1f} products/minute")

        # Category breakdown
        breakdown = self.get_breakdown(products)
        print(f"\nBreakdown by category:")
        for category, count in sorted(breakdown.items(), key=lambda x: x[1], reverse=True):
            print(f"  {category}: {count:,} products")

        # Sample products
        print(f"\nSample products:")
        for i, product in enumerate(products[:5], 1):
            name = product.get('product_name', 'N/A')[:50]
            price = product.get('price_current', 'N/A')
            print(
                f"  {i}. {name}... | {price:,} VND"
                if isinstance(price, int) else
                f"  {i}. {name}... | {price}"
            )

        print()
        print(f"Data saved for MinIO upload under: {self.output_dir / 'tiki'}")


def main():
    """Run the mass crawler"""
    crawler = MassCrawler()

    print("MASS MULTI-CATEGORY CRAWLER")
    print("This will crawl multiple categories with pagination")
    print("Expected to collect thousands of products")
    print()

    # Auto-start for demo
    try:
        products = crawler.run_mass_crawl()

        print(f"\nSUCCESS! Collected {len(products):,} total products")
        print("Check MinIO buffer folder (CRAWLER_OUTPUT_DIR/tiki/date=YYYY-MM-DD) for results")

    except KeyboardInterrupt:
        print("\nCrawl interrupted by user")
    except Exception as e:
        print(f"\nCrawl error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
