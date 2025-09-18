# File: src/generators/synthetic_data_generator.py
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

class SyntheticEcommerceGenerator:
    def __init__(self, scale_factor=10):
        self.fake = Faker()
        self.scale_factor = scale_factor  # Multiply base data by this factor
        
    def generate_customers(self, base_count=100000):
        """Generate synthetic customer data"""
        total_customers = base_count * self.scale_factor
        
        customers = []
        for i in range(total_customers):
            customers.append({
                'customer_id': f'CUST_{i:08d}',
                'customer_name': self.fake.name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'address': self.fake.address(),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'country': self.fake.country(),
                'zip_code': self.fake.zipcode(),
                'registration_date': self.fake.date_between(start_date='-2y', end_date='today'),
                'customer_segment': random.choice(['VIP', 'Regular', 'Occasional', 'New']),
                'lifetime_value': round(random.uniform(50, 5000), 2),
                'source': 'synthetic_generation'
            })
        
        return pd.DataFrame(customers)
    
    def generate_products(self, base_count=50000):
        """Generate synthetic product catalog"""
        total_products = base_count * self.scale_factor
        
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 
                     'Sports', 'Toys', 'Health & Beauty', 'Automotive']
        
        products = []
        for i in range(total_products):
            category = random.choice(categories)
            products.append({
                'product_id': f'PROD_{i:08d}',
                'product_name': f'{category} Product {i}',
                'category': category,
                'subcategory': f'{category} Sub-{random.randint(1, 5)}',
                'brand': f'Brand_{random.randint(1, 100)}',
                'price': round(random.uniform(10, 1000), 2),
                'cost': round(random.uniform(5, 500), 2),
                'weight': round(random.uniform(0.1, 10), 2),
                'dimensions': f'{random.randint(10,100)}x{random.randint(10,100)}x{random.randint(10,100)}',
                'stock_quantity': random.randint(0, 1000),
                'rating': round(random.uniform(1, 5), 1),
                'num_reviews': random.randint(0, 10000),
                'created_date': self.fake.date_between(start_date='-3y', end_date='today'),
                'source': 'synthetic_generation'
            })
        
        return pd.DataFrame(products)
    
    def generate_transactions(self, customers_df, products_df, transactions_per_customer=5):
        """Generate synthetic transaction data"""
        
        transactions = []
        
        for _, customer in customers_df.iterrows():
            num_orders = np.random.poisson(transactions_per_customer)
            
            for order_num in range(num_orders):
                order_date = self.fake.date_between(
                    start_date=customer['registration_date'], 
                    end_date='today'
                )
                
                # Random products in this order
                num_items = np.random.poisson(3) + 1  # At least 1 item
                order_products = products_df.sample(n=min(num_items, len(products_df)))
                
                order_id = f"ORDER_{customer['customer_id']}_{order_num:04d}"
                order_total = 0
                
                for _, product in order_products.iterrows():
                    quantity = random.randint(1, 5)
                    item_total = product['price'] * quantity
                    order_total += item_total
                    
                    transactions.append({
                        'order_id': order_id,
                        'customer_id': customer['customer_id'],
                        'product_id': product['product_id'],
                        'order_date': order_date,
                        'quantity': quantity,
                        'unit_price': product['price'],
                        'item_total': round(item_total, 2),
                        'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
                        'shipping_cost': round(random.uniform(5, 25), 2),
                        'order_status': random.choice(['completed', 'shipped', 'delivered', 'cancelled']),
                        'source': 'synthetic_generation'
                    })
        
        return pd.DataFrame(transactions)