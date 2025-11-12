#!/usr/bin/env python3
"""Quick test for final reviews crawler"""
from lazada_reviews_final import LazadaReviewsCrawler

print("Testing Lazada Reviews Crawler (Final Version)")
print("=" * 60)

# Test with one product
test_url = "https://www.lazada.vn/products/i2974492628.html"

crawler = LazadaReviewsCrawler()
crawler.max_reviews_per_product = 5  # Limit for testing

print(f"\nTesting with: {test_url}")
crawler.run([test_url])

print("\n" + "=" * 60)
print("Test completed!")
print("Check output in: ../../../../../data/outputs/lazada_reviews/")
