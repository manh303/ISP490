#!/usr/bin/env python3
"""
Debug FPTShop URL extraction specifically
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'crawlers'))

from fptshop_crawler import FPTShopCrawler

def test_url_extraction():
    print("🔍 DEBUGGING FPTSHOP URL EXTRACTION")
    print("=" * 50)

    crawler = FPTShopCrawler()

    try:
        # Test just URL extraction for dien-thoai category
        print("Testing URL extraction for dien-thoai category...")

        # Get URLs from first page only
        urls = crawler.get_product_urls_from_category("dien-thoai", max_pages=1)

        print(f"📊 Found {len(urls)} URLs:")
        print("-" * 30)

        for i, url in enumerate(urls[:10], 1):  # Show first 10 URLs
            print(f"{i:2d}. {url}")

        if len(urls) > 10:
            print(f"    ... and {len(urls) - 10} more URLs")

        # Quick validation
        valid_urls = [url for url in urls if '/dien-thoai/' in url and 'fptshop.com.vn' in url]
        print(f"\n✅ Valid product URLs: {len(valid_urls)}/{len(urls)}")

        if len(urls) == 0:
            print("❌ No URLs found - selectors may be incorrect")
        elif len(valid_urls) < len(urls) * 0.8:
            print("⚠️  Many invalid URLs found - may need selector refinement")
        else:
            print("🎉 URL extraction looks good!")

    except Exception as e:
        print(f"❌ Error during URL extraction: {e}")
        import traceback
        traceback.print_exc()
    finally:
        crawler.cleanup()

if __name__ == "__main__":
    test_url_extraction()