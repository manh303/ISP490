#!/usr/bin/env python3
"""
Script để chạy Enhanced Lazada Crawler với các tùy chọn khác nhau
"""

import sys
import argparse
from enhanced_lazada_crawler import EnhancedLazadaCrawler

def main():
    parser = argparse.ArgumentParser(description='Enhanced Lazada Crawler')
    parser.add_argument('--category', type=str, help='Category to crawl (e.g., smartphones, laptops)')
    parser.add_argument('--all-categories', action='store_true', help='Crawl all categories')
    parser.add_argument('--pages', type=int, default=3, help='Max pages per category (default: 3)')
    parser.add_argument('--products-per-page', type=int, default=10, help='Max products per page (default: 10)')
    parser.add_argument('--no-details', action='store_true', help='Skip product detail extraction (faster)')
    parser.add_argument('--output-prefix', type=str, default='enhanced_lazada', help='Output filename prefix')

    args = parser.parse_args()

    crawler = EnhancedLazadaCrawler()

    try:
        print("[CRAWLER] Enhanced Lazada Crawler")
        print("=" * 50)
        print(f"[CONFIG] Settings:")
        print(f"  [PAGES] Max pages per category: {args.pages}")
        print(f"  [PRODUCTS] Max products per page: {args.products_per_page}")
        print(f"  [DETAILS] Extract details: {not args.no_details}")
        print(f"  [OUTPUT] Output prefix: {args.output_prefix}")
        print("")

        get_details = not args.no_details

        if args.all_categories:
            print("[ALL] Crawling ALL categories...")
            all_products = []

            for cat_name, cat_url in crawler.categories.items():
                print(f"\n[CATEGORY] Processing category: {cat_name}")

                products = crawler.crawl_category(
                    cat_name,
                    cat_url,
                    max_pages=args.pages,
                    max_products_per_page=args.products_per_page,
                    get_details=get_details
                )

                all_products.extend(products)

                # Rest between categories
                if cat_name != list(crawler.categories.keys())[-1]:
                    print("[PAUSE] Resting between categories...")
                    crawler.random_delay(5, 10)

            if all_products:
                crawler.save_to_files(all_products, f"{args.output_prefix}_all_categories")
                print(f"\n[SUCCESS] Total products crawled: {len(all_products)}")
            else:
                print("[ERROR] No products crawled")

        elif args.category:
            if args.category in crawler.categories:
                cat_url = crawler.categories[args.category]
                print(f"[TARGET] Crawling category: {args.category}")

                products = crawler.crawl_category(
                    args.category,
                    cat_url,
                    max_pages=args.pages,
                    max_products_per_page=args.products_per_page,
                    get_details=get_details
                )

                if products:
                    crawler.save_to_files(products, f"{args.output_prefix}_{args.category}")
                    print(f"\n[SUCCESS] Products crawled: {len(products)}")
                else:
                    print("[ERROR] No products crawled")
            else:
                print(f"[ERROR] Category '{args.category}' not found!")
                print(f"Available categories: {', '.join(crawler.categories.keys())}")
                sys.exit(1)

        else:
            print("[ERROR] Please specify --category <name> or --all-categories")
            print(f"Available categories: {', '.join(crawler.categories.keys())}")
            parser.print_help()
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n[STOP] Stopped by user")
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        sys.exit(1)
    finally:
        crawler.close()

if __name__ == "__main__":
    main()