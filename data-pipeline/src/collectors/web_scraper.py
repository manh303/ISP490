import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from pathlib import Path
from loguru import logger
from utils.config import Config

class WebScraper:
    def __init__(self):
        self.config = Config()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def scrape_sample_ecommerce(self, num_pages=5):
        """
        Scrape sample data từ public ecommerce sites
        (Chỉ demo - trong thực tế cần tuân thủ robots.txt)
        """
        products_data = []
        
        # Demo với fake data generator để tránh vi phạm ToS
        for i in range(num_pages * 20):  # 20 products per page
            product = self._generate_sample_product(i)
            products_data.append(product)
            time.sleep(0.1)  # Be nice to servers
        
        df = pd.DataFrame(products_data)
        
        # Save to raw data
        save_path = Path(self.config.RAW_DATA_PATH) / "scraped_data" / "sample_products.csv"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(save_path, index=False)
        
        logger.info(f"Scraped {len(products_data)} products, saved to {save_path}")
        return df
    
    def _generate_sample_product(self, index):
        """Generate sample product data để test"""
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
        brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
        
        return {
            'product_id': f'PROD_{index:06d}',
            'name': f'Sample Product {index}',
            'category': random.choice(categories),
            'brand': random.choice(brands),
            'price': round(random.uniform(10, 500), 2),
            'rating': round(random.uniform(3.0, 5.0), 1),
            'num_reviews': random.randint(0, 1000),
            'availability': random.choice(['In Stock', 'Out of Stock']),
            'scraped_date': pd.Timestamp.now()
        }