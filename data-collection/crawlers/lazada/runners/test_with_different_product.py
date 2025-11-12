#!/usr/bin/env python3
"""Test with different products that likely have reviews"""
from lazada_reviews_final import LazadaReviewsCrawler

print("Testing with multiple products")
print("=" * 60)

# Try multiple products - some should have reviews
test_urls = [
    "https://www.lazada.vn/products/i2974492628.html",  # Original
    "https://www.lazada.vn/products/i3068844913.html",  # Try another
    "https://www.lazada.vn/products/i2974492628.html",  # Xiaomi
]

crawler = LazadaReviewsCrawler()
crawler.max_reviews_per_product = 5

for i, url in enumerate(test_urls, 1):
    print(f"\n{'='*60}")
    print(f"Testing product {i}/{len(test_urls)}")
    print(f"URL: {url}")
    print('='*60)
    
    reviews = crawler.extract_reviews_from_product(url)
    
    if reviews:
        print(f"\n✅ SUCCESS! Found {len(reviews)} reviews")
        for j, review in enumerate(reviews[:3], 1):
            print(f"\nReview {j}:")
            print(f"  Rating: {review['rating']}★")
            print(f"  Text: {review['review_text'][:80]}...")
        break
    else:
        print(f"\n⚠️ No reviews found for this product")
        print("Trying next product...")

print("\n" + "=" * 60)
print("Test completed!")
