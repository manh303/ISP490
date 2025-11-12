import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

class SimpleLazadaCrawler:
    def __init__(self):
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Setup Chrome với antibot cơ bản."""
        opts = Options()
        
        # Basic options
        opts.add_argument('--no-sandbox')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('--disable-blink-features=AutomationControlled')
        opts.add_argument('--disable-extensions')
        opts.add_argument('--window-size=1920,1080')
        
        # User agent
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        opts.add_argument(f'--user-agent={ua}')
        
        # Stealth
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=opts)
        
        # Hide webdriver
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print("[Simple] Driver ready")

    def humanize(self):
        """Giả lập hành vi người dùng."""
        try:
            # Random scroll
            self.driver.execute_script(f"window.scrollTo(0, {random.randint(100, 500)});")
            time.sleep(random.uniform(1, 3))
            
            # Random mouse move
            ActionChains(self.driver).move_by_offset(
                random.randint(-10, 10), random.randint(-10, 10)
            ).perform()
            
        except Exception:
            pass

    def get_page(self, url, max_retries=3):
        """Load trang với retry."""
        for attempt in range(max_retries):
            try:
                print(f"[Attempt {attempt+1}] Loading: {url}")
                self.driver.get(url)
                time.sleep(random.uniform(2, 4))
                self.humanize()
                
                # Check if loaded
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                html = self.driver.page_source
                if len(html) > 5000 and "lazada" in html.lower():
                    print("[Success] Page loaded")
                    return True
                    
            except Exception as e:
                print(f"[Error] Attempt {attempt+1}: {e}")
                time.sleep(random.uniform(3, 6))
                
        return False

    def scroll_and_wait(self):
        """Cuộn trang để load sản phẩm."""
        print("Scrolling to load products...")
        for i in range(3):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            print(f"   Scroll {i+1}/3")

    def extract_products(self):
        """Trích xuất sản phẩm cơ bản."""
        products = []
        
        try:
            # Tìm product containers
            elements = self.driver.find_elements(By.CSS_SELECTOR, 'div[data-qa-locator="product-item"]')
            print(f"Found {len(elements)} product containers")
            
            for i, elem in enumerate(elements[:10]):  # Chỉ lấy 10 sản phẩm đầu
                try:
                    # Title và URL
                    link = elem.find_element(By.CSS_SELECTOR, "a[href]")
                    title = link.text.strip()
                    url = link.get_attribute("href")
                    
                    # Price
                    try:
                        price_elem = elem.find_element(By.CSS_SELECTOR, "span.ooOxS")
                        price = price_elem.text.strip()
                    except:
                        price = "N/A"
                    
                    if title and url:
                        products.append({
                            "title": title,
                            "url": url,
                            "price": price
                        })
                        print(f"   [{i+1}] {title[:50]}... - {price}")
                        
                except Exception as e:
                    print(f"   [{i+1}] Error extracting: {e}")
                    
        except Exception as e:
            print(f"Error finding products: {e}")
            
        return products

    def test_crawl(self):
        """Test crawl một trang."""
        url = "https://www.lazada.vn/tag/mobiles/?q=mobiles"
        
        if self.get_page(url):
            self.scroll_and_wait()
            products = self.extract_products()
            
            print(f"\nExtracted {len(products)} products:")
            for i, p in enumerate(products[:3]):
                print(f"{i+1}. {p['title'][:60]}...")
                print(f"   Price: {p['price']}")
                print(f"   URL: {p['url'][:80]}...")
                print()
                
            return products
        else:
            print("Failed to load page")
            return []

    def close(self):
        if self.driver:
            self.driver.quit()

def main():
    crawler = SimpleLazadaCrawler()
    
    try:
        products = crawler.test_crawl()
        print(f"\nTotal products: {len(products)}")
        
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        crawler.close()

if __name__ == "__main__":
    main()