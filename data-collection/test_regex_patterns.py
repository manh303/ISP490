import re

def test_rating_patterns():
    # Test cac pattern rating
    test_texts = [
        "Rating: 4.5 out of 5",
        "4,8 stars",
        "Rating 4.2",
        "4.7/5.0",
        "5.0 ⭐",
        "3,9",
        "Đánh giá: 4.6",
        "No rating"
    ]

    print("=== Test Rating Patterns ===")
    for text in test_texts:
        rating_match = re.search(r'\b(\d[.,]\d)\b', text)
        if rating_match:
            rating_str = rating_match.group(1).replace(',', '.')
            try:
                rating_val = float(rating_str)
                if 0 <= rating_val <= 5:
                    print(f" '{text}' -> Rating: {rating_val}")
                else:
                    print(f" '{text}' -> Invalid rating: {rating_val}")
            except ValueError:
                print(f" '{text}' -> Parse error")
        else:
            print(f" '{text}' -> No rating found")

def test_sold_patterns():
    # Test cac pattern sold/review count
    test_texts = [
        "1234 đã bán",
        "567 reviews",
        "2.5k sold",
        "150 đánh giá",
        "10k+ đã ban",
        "999 SOLD",
        "1.2K reviews",
        "5 ban",
        "No sales data"
    ]

    print("\n=== Test Sold/Review Patterns ===")
    for text in test_texts:
        sold_match = re.search(r'(\d+(?:\.\d+)?[kK]?)\s*(?:đã bán|sold|ban|reviews?|đánh giá)', text, re.IGNORECASE)
        if sold_match:
            sold_str = sold_match.group(1)
            if sold_str.lower().endswith('k'):
                sold_num = float(sold_str[:-1]) * 1000
                result = str(int(sold_num))
            else:
                result = sold_str
            print(f" '{text}' -> Sold/Review count: {result}")
        else:
            print(f" '{text}' -> No sold/review count found")

if __name__ == "__main__":
    test_rating_patterns()
    test_sold_patterns()