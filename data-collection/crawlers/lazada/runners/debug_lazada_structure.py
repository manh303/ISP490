import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

def debug_lazada_structure():
    """Debug current Lazada page structure để tìm correct selectors"""

    chrome_options = Options()
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        url = "https://www.lazada.vn/dien-thoai-di-dong/"
        print(f"Dang mo: {url}")

        driver.get(url)
        time.sleep(5)  # Wait for page load

        # Scroll để load lazy content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        # Lấy page source và parse với BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Tìm các elements chứa sản phẩm
        print("\nTim kiem product containers...")

        # Tìm các containers có thể chứa sản phẩm
        possible_containers = [
            soup.find_all(attrs={"data-qa-locator": lambda x: x and "product" in x.lower()}),
            soup.find_all(attrs={"data-spm": True}),
            soup.find_all("div", class_=lambda x: x and "item" in x.lower()),
            soup.find_all("div", class_=lambda x: x and "product" in x.lower()),
            soup.find_all("a", href=lambda x: x and "/products/" in x)
        ]

        for i, containers in enumerate(possible_containers):
            if containers:
                print(f"\n--- Kieu container {i+1}: {len(containers)} elements ---")

                # Phân tích container đầu tiên
                first_container = containers[0]
                print(f"Tag: {first_container.name}")
                print(f"Classes: {first_container.get('class', [])}")
                print(f"Data attributes: {[(k, v) for k, v in first_container.attrs.items() if k.startswith('data-')]}")

                # Tìm price trong container này
                print("\n Tim price selectors:")
                price_elements = [
                    first_container.find_all(string=lambda text: text and any(c in text for c in ['₫', 'đ', 'VND']) if text else False),
                    first_container.find_all(class_=lambda x: x and any(word in x.lower() for word in ['price', 'currency'])),
                    first_container.find_all(attrs={"data-testid": lambda x: x and "price" in x.lower() if x else False})
                ]

                for j, price_group in enumerate(price_elements):
                    if price_group:
                        print(f"  Nhóm {j+1}: {len(price_group)} price elements")
                        for price_elem in price_group[:3]:  # Chỉ hiển thị 3 đầu
                            if hasattr(price_elem, 'get'):
                                print(f"    - Tag: {price_elem.name}, Class: {price_elem.get('class')}, Text: '{price_elem.get_text().strip()}'")
                            else:
                                print(f"    - Text node: '{price_elem.strip()}'")

                # Lưu HTML của container đầu tiên để debug
                if i == 1:  # data-spm containers thường chứa sản phẩm
                    with open("data-collection/debug_product_container.html", "w", encoding="utf-8") as f:
                        f.write(str(first_container.prettify()))
                    print(" Đã lưu HTML container vào debug_product_container.html")

        # Tìm tất cả text có chứa ký hiệu tiền tệ
        print("\n Tat ca text chua tien te:")
        price_texts = soup.find_all(string=lambda text: text and any(c in text for c in ['₫', 'đ', 'VND', 'vnđ']) if text else False)
        for i, price_text in enumerate(price_texts[:10]):  # Chỉ hiển thị 10 đầu
            parent = price_text.parent
            print(f"{i+1}. '{price_text.strip()}' - Parent: {parent.name} {parent.get('class', [])}")

        # Lưu toàn bộ HTML để debug
        with open("data-collection/debug_lazada_full.html", "w", encoding="utf-8") as f:
            f.write(soup.prettify())
        print("\n Đã lưu full HTML vào debug_lazada_full.html")

    except Exception as e:
        print(f" Loi: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    debug_lazada_structure()