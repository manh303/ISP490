# File: src/collectors/api_collectors.py
import requests
import pandas as pd
import time

class APICollector:
    def __init__(self):
        self.apis = {
            'restcountries': 'https://restcountries.com/v3.1/all',
            'exchangerates': 'https://api.exchangerate-api.com/v4/latest/USD',
            'worldbank': 'https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json',
        }
    
    def get_economic_data(self):
        """Get economic indicators for market analysis"""
        try:
            # GDP data from World Bank
            response = requests.get(self.apis['worldbank'])
            data = response.json()
            
            if len(data) > 1:
                gdp_data = data[1]  # Skip metadata
                df = pd.DataFrame(gdp_data)
                df['data_source'] = 'worldbank_api'
                return df
                
        except Exception as e:
            print(f"Error fetching economic data: {e}")
            return pd.DataFrame()
    
    def get_currency_data(self):
        """Get exchange rates for international analysis"""
        try:
            response = requests.get(self.apis['exchangerates'])
            data = response.json()
            
            rates_df = pd.DataFrame(list(data['rates'].items()), 
                                   columns=['currency', 'rate'])
            rates_df['base_currency'] = data['base']
            rates_df['date'] = data['date']
            rates_df['data_source'] = 'exchangerates_api'
            
            return rates_df
            
        except Exception as e:
            print(f"Error fetching currency data: {e}")
            return pd.DataFrame()