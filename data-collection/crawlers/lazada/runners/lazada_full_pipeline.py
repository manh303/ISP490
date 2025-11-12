#!/usr/bin/env python3
"""
Lazada Full Pipeline: Get product URLs then crawl reviews
"""
import json
import time
import random
import os
import re
from pathlib import Path
from datetime import datetime
from typing import List

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

from lazada_reviews_final import LazadaReviewsCrawler

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/tmp/data/outputs")
LOG_PREFIX = "[Lazada-Pipeline]"

def get_product_urls_from_category(category_url: str, max_products: int = 10) -> List[str]:
    """Get product URLs from category page"""
    print(f"{LOG_PREFIX} Getting products from: {category_url}")
    
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    
    driver = uc.Chrome(options=options)
    product_urls = []
    
    try:
        driver.get(category_url)
        print(f"{LOG_PREFIX} Waiting for page to load...")
        time.sleep(10)
        
        # Scroll to load products
        for i in range(5):
            driver.execute_script(f'window.scrollTo(0, {(i+1)*300})')
            time.sleep(1.5)
        
        # Extract product URLs using JavaScript
        js_script = """
        var links = document.querySelectorAll('a[href*="/products/"]');
        var urls = [];
        var seen = new Set();
        for (var i = 0; i < links.length; i++) {
            var url = links[i].href;
            if (url && url.includes('/products/i') && !seen.has(url)) {
                seen.add(url);
                urls.push(url);
            }
        }
        return urls;
        """
        
        urls = driver.execute_script(js_script) or []
        product_urls = urls[:max_products]
        
        print(f"{LOG_PREFIX} Found {len(product_urls)} product URLs")
        
    except Exception as e:
        print(f"{LOG_PREFIX} Error: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass
    
    return product_urls

def main():
    """Main pipeline"""
    if not SELENIUM_AVAILABLE:
        print(f"{LOG_PREFIX} Selenium not available")
        return
    
    print("=" * 60)
    print("LAZADA FULL PIPELINE: Products + Reviews")
    print("=" * 60)
    
    # Step 1: Get product URLs from categories
    categories = {
        "smartphones": "https://www.lazada.vn/dien-thoai-di-dong/",
        "laptops": "https://www.lazada.vn/may-tinh-xach-tay/",
    }
    
    all_product_urls = []
    
    for category, url in categories.items():
        print(f"\n{LOG_PREFIX} Category: {category}")
        urls = get_product_urls_from_category(url, max_products=5)
        all_product_urls.extend(urls)
        time.sleep(3)
    
    print(f"\n{LOG_PREFIX} Total products to crawl: {len(all_product_urls)}")
    
    # Save product URLs
    today = datetime.now().strftime("%Y-%m-%d")
    urls_file = Path(OUTPUT_DIR) / "lazada_product_urls" / f"date={today}" / f"urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    urls_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(urls_file, 'w', encoding='utf-8') as f:
        json.dump(all_product_urls, f, indent=2, ensure_ascii=False)
    print(f"{LOG_PREFIX} Saved URLs to: {urls_file}")
    
    # Step 2: Crawl reviews from product URLs
    if all_product_urls:
        print(f"\n{LOG_PREFIX} Starting reviews crawl...")
        crawler = LazadaReviewsCrawler()
        crawler.run(all_product_urls)
    else:
        print(f"{LOG_PREFIX} No product URLs found!")
    
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED!")
    print("=" * 60)

if __name__ == "__main__":
    main()
