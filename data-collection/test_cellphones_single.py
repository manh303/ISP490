#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'crawlers'))

from cellphones_crawler import CellphonesCrawler
import json

def test_single_product():
    """Test crawler với 1 sản phẩm cụ thể"""
    crawler = CellphonesCrawler()

    # Test URL được cung cấp
    test_url = "https://cellphones.com.vn/iphone-17-pro.html"

    print(f"Testing CellphoneS crawler with: {test_url}")
    print("=" * 50)

    try:
        result = crawler.extract_product_data(test_url)

        if result:
            print("✅ CRAWL THÀNH CÔNG!")
            print(f"📱 Tên sản phẩm: {result.get('product_name', 'N/A')}")
            print(f"💰 Giá hiện tại: {result.get('price_current', 'N/A'):,} VND" if result.get('price_current') else "💰 Giá hiện tại: N/A")
            print(f"💸 Giá gốc: {result.get('price_original', 'N/A'):,} VND" if result.get('price_original') else "💸 Giá gốc: N/A")
            print(f"⭐ Đánh giá: {result.get('rating_avg', 'N/A')} ({result.get('rating_count', 'N/A')} reviews)")
            print(f"🏪 Thương hiệu: {result.get('brand', 'N/A')}")
            print(f"📂 Danh mục: {result.get('category', 'N/A')}")
            print(f"🖼️ Số hình ảnh: {len(result.get('image_urls', []))}")

            # Mô tả
            description = result.get('description', 'N/A')
            if description and description != 'N/A':
                print(f"📝 Mô tả: {description[:200]}{'...' if len(description) > 200 else ''}")
            else:
                print("📝 Mô tả: Không có")

            # Reviews
            reviews = result.get('reviews', [])
            if reviews:
                print(f"💬 Reviews: {len(reviews)} đánh giá")
                print(f"   Đánh giá đầu tiên: {reviews[0][:150]}{'...' if len(reviews[0]) > 150 else ''}")
            else:
                print("💬 Reviews: Không lấy được")

            # Save to file for inspection
            filename = f"cellphones_single_test_{int(__import__('time').time())}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"\n💾 Dữ liệu đã lưu vào: {filename}")

        else:
            print("❌ CRAWL THẤT BẠI - Không lấy được dữ liệu")

    except Exception as e:
        print(f"❌ LỖI: {e}")
        import traceback
        traceback.print_exc()

    finally:
        crawler.cleanup()

if __name__ == "__main__":
    test_single_product()