import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import pandas as pd
import time
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LazadaCrawler:
    def __init__(self):
        """Initialize undetected ChromeDriver"""
        logger.info("Setting up undetected ChromeDriver...")
        self.driver = uc.Chrome()
        self.wait_time = 8
        
    def get_product_listings(self, url):
        """Get product listings from a category page"""
        try:
            logger.info(f"Accessing URL: {url}")
            self.driver.get(url)
            time.sleep(self.wait_time)  # Wait for page load
            
            # Gentle scroll
            for i in range(3):
                self.driver.execute_script(f"window.scrollTo(0, {(i+1)*500});")
                time.sleep(2)
            
            # Get products using working selector
            products = []
            elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")
            logger.info(f"Found {len(elements)} products")
            
            for elem in elements:
                try:
                    # Extract name using working selector
                    name_elem = elem.find_element(By.CSS_SELECTOR, 'a[title]')
                    title = name_elem.get_attribute('title') or name_elem.text.strip()

                    # Extract price using working selector
                    price_elem = elem.find_element(By.CSS_SELECTOR, '.ooOxS')
                    price = price_elem.text.strip()

                    # Extract URL
                    url_elem = elem.find_element(By.CSS_SELECTOR, 'a[href*="/products/"]')
                    link = url_elem.get_attribute('href')
                    if link.startswith('//'):
                        link = 'https:' + link

                    # Extract rating count
                    rating = "No rating"
                    try:
                        rating_elem = elem.find_element(By.CSS_SELECTOR, '.qzqFw')
                        rating = rating_elem.text.strip()
                    except:
                        pass

                    # Extract image
                    image = ""
                    try:
                        img_elem = elem.find_element(By.CSS_SELECTOR, "img")
                        image = img_elem.get_attribute('src')
                    except:
                        pass

                    product = {
                        'title': title,
                        'price': price,
                        'link': link,
                        'image': image,
                        'rating': rating,
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    products.append(product)
                except Exception as e:
                    logger.error(f"Error extracting product data: {str(e)}")
                    continue
                    
            return products
            
        except Exception as e:
            logger.error(f"Error accessing page {url}: {str(e)}")
            return []

    def crawl_category(self, category_url, num_pages=3):
        """Crawl multiple pages of a category"""
        all_products = []
        
        for page in range(1, num_pages + 1):
            url = f"{category_url}?page={page}"
            logger.info(f"Crawling page {page}/{num_pages}")
            
            products = self.get_product_listings(url)
            all_products.extend(products)
            
            time.sleep(2)  # Delay between pages
            
        return all_products
        
    def save_to_csv(self, products, filename):
        """Save products to CSV file"""
        if not products:
            logger.warning("No products to save!")
            return
            
        df = pd.DataFrame(products)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"Saved {len(products)} products to {filename}")
        
    def close(self):
        """Close the browser properly"""
        try:
            self.driver.quit()
            logger.info("Browser closed successfully")
        except:
            pass

def main():
    crawler = LazadaCrawler()
    try:
        # Crawl mobile phones category
        products = crawler.crawl_category(
            "https://www.lazada.vn/dien-thoai-di-dong",
            num_pages=10
        )
        
        if products:
            # Create output directory if it doesn't exist
            output_dir = Path("data")
            output_dir.mkdir(exist_ok=True)
            
            # Save results
            crawler.save_to_csv(
                products,
                output_dir / f"lazada_phones_{time.strftime('%Y%m%d_%H%M%S')}.csv"
            )
    finally:
        crawler.close()

if __name__ == "__main__":
    main()