#!/usr/bin/env python3
"""
Simple Kafka Consumer for E-commerce Analytics
Reads data from Kafka topics and processes for analytics
"""

import json
import time
import pandas as pd
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer
from collections import defaultdict, Counter
import threading

class EcommerceKafkaProcessor:
    def __init__(self):
        self.stats = defaultdict(int)
        self.running = True
        self.batch_size = 100

        # PostgreSQL connection
        try:
            self.pg_conn = psycopg2.connect(
                host="localhost",
                database="ecommerce_analytics",
                user="postgres",
                password="postgres",
                port="5432"
            )
            self.pg_cursor = self.pg_conn.cursor()
            print("OK Connected to PostgreSQL")
            self.setup_tables()
        except Exception as e:
            print(f"ERROR PostgreSQL connection failed: {e}")
            self.pg_conn = None

    def setup_tables(self):
        """Create analytics tables if they don't exist"""
        try:
            # User events analytics
            self.pg_cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_events_analytics (
                    id SERIAL PRIMARY KEY,
                    window_start TIMESTAMP,
                    event_type VARCHAR(50),
                    category VARCHAR(100),
                    event_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Transaction analytics
            self.pg_cursor.execute("""
                CREATE TABLE IF NOT EXISTS transaction_analytics (
                    id SERIAL PRIMARY KEY,
                    window_start TIMESTAMP,
                    payment_method VARCHAR(50),
                    transaction_count INTEGER,
                    total_revenue DECIMAL(15,2),
                    avg_transaction DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Product analytics
            self.pg_cursor.execute("""
                CREATE TABLE IF NOT EXISTS product_analytics (
                    id SERIAL PRIMARY KEY,
                    source VARCHAR(50),
                    category VARCHAR(100),
                    product_count INTEGER,
                    avg_price DECIMAL(10,2),
                    max_price DECIMAL(10,2),
                    min_price DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            self.pg_conn.commit()
            print("OK Analytics tables created/verified")

        except Exception as e:
            print(f"ERROR Error creating tables: {e}")

    def process_user_events(self):
        """Process user events stream"""
        consumer = KafkaConsumer(
            'user_events',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='user_events_processor'
        )

        print("Starting user events processor...")
        event_buffer = []

        for message in consumer:
            if not self.running:
                break

            try:
                data = message.value
                event_buffer.append(data)

                # Process batch
                if len(event_buffer) >= self.batch_size:
                    self.analyze_user_events(event_buffer)
                    event_buffer = []

                self.stats['user_events_processed'] += 1

            except Exception as e:
                print(f"Error processing user event: {e}")

        consumer.close()

    def analyze_user_events(self, events):
        """Analyze user events batch"""
        if not events:
            return

        # Group by event type and category
        analysis = defaultdict(lambda: defaultdict(int))

        for event in events:
            event_type = event.get('event_type', 'unknown')
            category = event.get('category', 'unknown')
            analysis[event_type][category] += 1

        # Save to PostgreSQL
        if self.pg_conn:
            try:
                window_start = datetime.now().replace(second=0, microsecond=0)

                for event_type, categories in analysis.items():
                    for category, count in categories.items():
                        self.pg_cursor.execute("""
                            INSERT INTO user_events_analytics
                            (window_start, event_type, category, event_count)
                            VALUES (%s, %s, %s, %s)
                        """, (window_start, event_type, category, count))

                self.pg_conn.commit()
                print(f"Saved user events analysis: {len(events)} events")

            except Exception as e:
                print(f"ERROR Error saving user events: {e}")

    def process_transactions(self):
        """Process transactions stream"""
        consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='transactions_processor'
        )

        print("Starting transactions processor...")
        transaction_buffer = []

        for message in consumer:
            if not self.running:
                break

            try:
                data = message.value
                transaction_buffer.append(data)

                # Process batch
                if len(transaction_buffer) >= self.batch_size:
                    self.analyze_transactions(transaction_buffer)
                    transaction_buffer = []

                self.stats['transactions_processed'] += 1

            except Exception as e:
                print(f"Error processing transaction: {e}")

        consumer.close()

    def analyze_transactions(self, transactions):
        """Analyze transactions batch"""
        if not transactions:
            return

        # Group by payment method
        analysis = defaultdict(lambda: {'count': 0, 'amounts': []})

        for txn in transactions:
            payment_method = txn.get('payment_method', 'unknown')
            amount = float(txn.get('amount', 0))

            analysis[payment_method]['count'] += 1
            analysis[payment_method]['amounts'].append(amount)

        # Calculate metrics and save to PostgreSQL
        if self.pg_conn:
            try:
                window_start = datetime.now().replace(second=0, microsecond=0)

                for payment_method, data in analysis.items():
                    amounts = data['amounts']
                    if amounts:
                        total_revenue = sum(amounts)
                        avg_transaction = total_revenue / len(amounts)

                        self.pg_cursor.execute("""
                            INSERT INTO transaction_analytics
                            (window_start, payment_method, transaction_count, total_revenue, avg_transaction)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (window_start, payment_method, data['count'], total_revenue, avg_transaction))

                self.pg_conn.commit()
                print(f"Saved transaction analysis: {len(transactions)} transactions")

            except Exception as e:
                print(f"ERROR Error saving transactions: {e}")

    def show_stats(self):
        """Display processing statistics"""
        while self.running:
            time.sleep(30)  # Show stats every 30 seconds
            print(f"""
KAFKA PROCESSING STATS
========================
User Events Processed: {self.stats['user_events_processed']:,}
Transactions Processed: {self.stats['transactions_processed']:,}
Time: {datetime.now().strftime('%H:%M:%S')}
            """)

    def run(self):
        """Start all processors"""
        print("Starting E-commerce Kafka Data Processor...")

        # Start processors in separate threads
        threads = []

        # User events processor
        user_thread = threading.Thread(target=self.process_user_events)
        user_thread.daemon = True
        threads.append(user_thread)
        user_thread.start()

        # Transactions processor
        txn_thread = threading.Thread(target=self.process_transactions)
        txn_thread.daemon = True
        threads.append(txn_thread)
        txn_thread.start()

        # Stats display
        stats_thread = threading.Thread(target=self.show_stats)
        stats_thread.daemon = True
        threads.append(stats_thread)
        stats_thread.start()

        try:
            print("All processors started! Processing real-time data...")
            print("Data being analyzed and saved to PostgreSQL")
            print("Press Ctrl+C to stop...")

            # Keep main thread alive
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\nStopping processors...")
            self.running = False

        finally:
            if self.pg_conn:
                self.pg_conn.close()

def main():
    processor = EcommerceKafkaProcessor()
    processor.run()

if __name__ == "__main__":
    main()