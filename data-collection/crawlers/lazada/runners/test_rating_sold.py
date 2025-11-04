from lazada_crawler import FixedLazadaCrawler

def test_rating_sold():
    crawler = FixedLazadaCrawler()

    try:
        # Test chi 1 trang, 5 san pham
        products = crawler.crawl_multiple_pages(max_pages=1, max_products_per_page=5)

        if products:
            print(f"\nKet qua test rating va sold count:")
            print(f"So san pham: {len(products)}")

            for i, product in enumerate(products):
                print(f"\n--- San pham {i+1} ---")
                print(f"Title: {product.get('title', 'N/A')[:60]}...")

                price_text = product.get('price_text', 'N/A')
                if price_text != 'N/A':
                    price_text = price_text.encode('ascii', 'ignore').decode('ascii')
                print(f"Price: {price_text}")

                print(f"Rating: {product.get('rating', 'N/A')}")
                print(f"Review/Sold Count: {product.get('review_count', 'N/A')}")
                print(f"Location: {product.get('location', 'N/A')}")
                print(f"Discount: {product.get('discount', 'N/A')}")

            # Luu voi ten khac
            crawler.save_to_files(products, "test_rating_sold")
        else:
            print("Khong crawl duoc san pham nao")

    except Exception as e:
        print(f"Loi: {e}")
    finally:
        crawler.close()

if __name__ == "__main__":
    test_rating_sold()