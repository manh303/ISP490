#!/usr/bin/env python3
"""Quick test of anti-bot crawler"""

from lazada_antibot_crawler import LazadaAntiBotCrawler

def quick_test():
    print("=== QUICK ANTI-BOT TEST ===")

    crawler = LazadaAntiBotCrawler(mode="fast")

    try:
        # Test just 1 page
        products = crawler.crawl_category("mobiles", max_pages=1)

        print(f"\nRESULTS:")
        print(f"  Products found: {len(products)}")
        print(f"  Anti-bot blocks: {crawler.stats['anti_bot_blocks']}")
        print(f"  Requests made: {crawler.stats['requests_made']}")

        if products:
            print(f"\nSample products:")
            for i, product in enumerate(products[:3], 1):
                print(f"  {i}. {product['title']}")
                print(f"     URL: {product['url']}")

            return True
        else:
            print("No products found!")
            return False

    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    success = quick_test()
    print(f"\nTest {'PASSED' if success else 'FAILED'}")