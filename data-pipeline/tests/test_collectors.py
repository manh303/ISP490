import pytest
import pandas as pd
from src.collectors.kaggle_collector import KaggleCollector
from src.collectors.web_scraper import WebScraper

class TestKaggleCollector:
    def setup_method(self):
        self.collector = KaggleCollector()
    
    def test_initialization(self):
        assert self.collector.config is not None
    
    def test_load_csv_functionality(self):
        # Test với sample CSV
        sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'price': [10.0, 20.0, 30.0]
        })
        
        # Save temporary file
        temp_path = '/tmp/test_sample.csv'
        sample_data.to_csv(temp_path, index=False)
        
        # Test load
        loaded_df = self.collector.load_csv_file(temp_path)
        assert loaded_df is not None
        assert len(loaded_df) == 3

class TestWebScraper:
    def setup_method(self):
        self.scraper = WebScraper()
    
    def test_sample_product_generation(self):
        product = self.scraper._generate_sample_product(1)
        assert 'product_id' in product
        assert 'name' in product
        assert 'price' in product
        assert product['price'] > 0