import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

def debug_rating_sold():
    chrome_options = Options()
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get("https://www.lazada.vn/dien-thoai-di-dong/")
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        # Tim product containers
        product_elements = driver.find_elements(By.CSS_SELECTOR, 'div.Bm3ON[data-qa-locator="product-item"]')
        print(f"Found {len(product_elements)} product containers")

        for i, product in enumerate(product_elements[:5]):
            print(f"\n--- Product {i+1} ---")

            # Get all text content and look for patterns
            try:
                all_text = product.text
                print(f"All text:\n{all_text}")

                # Look for numbers that might be ratings (4.5, 4,8 etc)
                import re
                ratings = re.findall(r'\b\d[.,]\d\b', all_text)
                if ratings:
                    print(f"Potential ratings: {ratings}")

                # Look for sold patterns
                sold_patterns = re.findall(r'(\d+\+?\s*(?:đã bán|sold|ban|da ban))|(?:(?:đã bán|sold|ban|da ban)\s*\d+)', all_text, re.IGNORECASE)
                if sold_patterns:
                    print(f"Potential sold patterns: {sold_patterns}")

                # Look for specific elements that might contain rating/sold
                potential_elements = product.find_elements(By.CSS_SELECTOR, "span, div")
                for elem in potential_elements:
                    text = elem.text.strip()
                    if text and (any(char.isdigit() for char in text)):
                        if '.' in text or ',' in text:
                            # Could be rating
                            try:
                                if len(text) < 10 and ('.' in text or ',' in text):
                                    print(f"  Potential rating element: '{text}' - Class: {elem.get_attribute('class')}")
                            except:
                                pass
                        elif 'bán' in text.lower() or 'sold' in text.lower():
                            print(f"  Potential sold element: '{text}' - Class: {elem.get_attribute('class')}")

            except Exception as e:
                print(f"Error processing product {i+1}: {e}")

        # Save one product container HTML for inspection
        if product_elements:
            with open("debug_product_rating_sold.html", "w", encoding="utf-8") as f:
                f.write(product_elements[0].get_attribute('outerHTML'))
            print("\nSaved first product HTML to debug_product_rating_sold.html")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    debug_rating_sold()