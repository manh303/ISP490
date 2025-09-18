# Web scraping với requests + BeautifulSoup
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin

class EcommerceScraper:
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def scrape_quotes_to_scrape(self):
        """Demo scraping từ quotes.toscrape.com"""
        url = "http://quotes.toscrape.com"
        response = self.session.get(url)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        quotes = []
        
        for quote in soup.find_all('div', class_='quote'):
            quotes.append({
                'text': quote.find('span', class_='text').text,
                'author': quote.find('small', class_='author').text,
                'tags': [tag.text for tag in quote.find_all('a', class_='tag')]
            })
        
        return pd.DataFrame(quotes)
    
    def scrape_books_to_scrape(self):
        """Scrape books data for e-commerce analysis"""
        base_url = "http://books.toscrape.com"
        books = []
        
        for page in range(1, 6):  # Scrape 5 pages
            url = f"{base_url}/catalogue/page-{page}.html"
            response = self.session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            for book in soup.find_all('article', class_='product_pod'):
                title = book.find('h3').find('a')['title']
                price = book.find('p', class_='price_color').text
                rating = book.find('p', class_='star-rating')['class'][1]
                availability = book.find('p', class_='instock availability').text.strip()
                
                books.append({
                    'title': title,
                    'price': price,
                    'rating': rating,
                    'availability': availability,
                    'scraped_at': datetime.now().isoformat()
                })
            
            time.sleep(1)  # Respectful scraping
        
        return pd.DataFrame(books)

# Usage
scraper = EcommerceScraper()
books_data = scraper.scrape_books_to_scrape()
print(f"📚 Scraped {len(books_data)} books")