# File: src/collectors/multi_source_scraper.py
import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import random

class MultiSourceScraper:
    def __init__(self):
        self.sources = {
            'fake_store_api': 'https://fakestoreapi.com/products',
            'dummyjson_products': 'https://dummyjson.com/products', 
            'jsonplaceholder': 'https://jsonplaceholder.typicode.com/posts'
        }
    
    def scrape_fake_store_api(self):
        """Scrape Fake Store API for product data"""
        try:
            response = requests.get(self.sources['fake_store_api'])
            products = response.json()
            
            # Convert to DataFrame
            df = pd.DataFrame(products)
            df['source'] = 'fake_store_api'
            df['scraped_at'] = pd.Timestamp.now()
            
            return df
        except Exception as e:
            print(f"Error scraping Fake Store API: {e}")
            return pd.DataFrame()
    
    def scrape_dummyjson_products(self):
        """Scrape DummyJSON for product data"""
        try:
            all_products = []
            
            # DummyJSON has pagination
            for page in range(1, 10):  # Get 10 pages
                url = f"{self.sources['dummyjson_products']}?limit=100&skip={page*100}"
                response = requests.get(url)
                data = response.json()
                
                if 'products' in data:
                    all_products.extend(data['products'])
                
                time.sleep(0.5)  # Be respectful
            
            df = pd.DataFrame(all_products)
            df['source'] = 'dummyjson'
            df['scraped_at'] = pd.Timestamp.now()
            
            return df
        except Exception as e:
            print(f"Error scraping DummyJSON: {e}")
            return pd.DataFrame()