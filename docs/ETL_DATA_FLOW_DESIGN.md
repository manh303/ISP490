# Thiết Kế ETL và Data Flow cho E-commerce DSS

## 🎯 Tổng Quan ETL Pipeline

Hệ thống ETL được thiết kế để xử lý dữ liệu e-commerce Việt Nam từ nhiều nguồn khác nhau, đảm bảo chất lượng dữ liệu và hiệu suất cao.

## 🏗️ Kiến Trúc ETL Tổng Thể

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           ETL ARCHITECTURE                             │
└─────────────────────────────────────────────────────────────────────────┘

   Data Sources          Staging Layer        Core DW           Data Marts
   ┌─────────────┐      ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
   │ Lazada API  │─────▶│   Raw Data  │───▶│ Fact Tables │───▶│ Sales Mart  │
   │ Shopee API  │      │   Storage   │    │   (OLTP)    │    │Customer Mart│
   │ Tiki API    │      │ (MongoDB)   │    │             │    │Product Mart │
   │ Sendo API   │      │             │    │ Dimension   │    │             │
   │ FPTShop     │      │ Data        │    │ Tables      │    │ Marketing   │
   │ CellphoneS  │      │ Quality     │    │ (SCD)       │    │ Mart        │
   │             │      │ Checks      │    │             │    │             │
   │ PostgreSQL  │      │             │    │ PostgreSQL  │    │ OLAP Cubes  │
   │ Real-time   │      │ Transformation   │             │    │             │
   │ Kafka       │      │ Business Logic   │             │    │             │
   └─────────────┘      └─────────────┘    └─────────────┘    └─────────────┘
        │                       │                  │                  │
        │                       │                  │                  │
   [Extract Phase]      [Transform Phase]    [Load Phase]      [Serve Phase]
```

## 📊 Data Sources Analysis

### 1. External API Sources
```yaml
external_apis:
  lazada:
    endpoint: "https://api.lazada.vn/rest"
    rate_limit: "1000 requests/hour"
    data_format: "JSON"
    authentication: "OAuth 2.0"
    refresh_frequency: "Every 15 minutes"

  shopee:
    endpoint: "https://partner.shopeemobile.com/api/v2"
    rate_limit: "600 requests/hour"
    data_format: "JSON"
    authentication: "API Key + Secret"
    refresh_frequency: "Every 20 minutes"

  tiki:
    endpoint: "https://api.tiki.vn/integration"
    rate_limit: "800 requests/hour"
    data_format: "JSON"
    authentication: "Bearer Token"
    refresh_frequency: "Every 10 minutes"

  sendo:
    endpoint: "https://open.sendo.vn/api"
    rate_limit: "500 requests/hour"
    data_format: "JSON"
    authentication: "API Key"
    refresh_frequency: "Every 30 minutes"
```

### 2. Internal Database Sources
```yaml
internal_databases:
  postgresql_oltp:
    host: "postgres:5432"
    database: "ecommerce_dss"
    tables:
      - orders
      - customers
      - products
      - transactions
    sync_method: "CDC (Change Data Capture)"
    sync_frequency: "Real-time"

  mongodb_logs:
    host: "mongodb:27017"
    database: "ecommerce_logs"
    collections:
      - user_activities
      - system_events
      - error_logs
    sync_method: "Incremental"
    sync_frequency: "Every 5 minutes"
```

### 3. Real-time Streaming Sources
```yaml
kafka_topics:
  sales_events:
    format: "Avro"
    partitions: 6
    retention: "7 days"
    producer: "Sales microservice"

  customer_events:
    format: "JSON"
    partitions: 3
    retention: "30 days"
    producer: "Customer microservice"

  inventory_events:
    format: "Avro"
    partitions: 4
    retention: "14 days"
    producer: "Inventory microservice"
```

## 🔄 ETL Workflow Design

### Stage 1: Data Extraction (Extract)

#### 1.1 API Data Extraction
```python
# Vietnamese E-commerce API Extractor
import asyncio
import aiohttp
from datetime import datetime, timedelta
import pandas as pd

class VietnameseEcommerceExtractor:
    def __init__(self):
        self.platforms = {
            'lazada': {
                'base_url': 'https://api.lazada.vn/rest',
                'headers': {'Authorization': 'Bearer {token}'},
                'rate_limit': 1000
            },
            'shopee': {
                'base_url': 'https://partner.shopeemobile.com/api/v2',
                'headers': {'Content-Type': 'application/json'},
                'rate_limit': 600
            },
            'tiki': {
                'base_url': 'https://api.tiki.vn/integration',
                'headers': {'Authorization': 'Bearer {token}'},
                'rate_limit': 800
            }
        }

    async def extract_products(self, platform: str, start_date: datetime):
        """Extract product data from Vietnamese e-commerce platforms"""

        endpoint = f"{self.platforms[platform]['base_url']}/products"
        params = {
            'updated_after': start_date.isoformat(),
            'limit': 100,
            'fields': 'id,name,price,category,brand,stock,rating,reviews'
        }

        async with aiohttp.ClientSession() as session:
            products = []
            page = 1

            while True:
                params['page'] = page

                async with session.get(endpoint,
                                     headers=self.platforms[platform]['headers'],
                                     params=params) as response:

                    if response.status != 200:
                        break

                    data = await response.json()
                    batch_products = data.get('products', [])

                    if not batch_products:
                        break

                    # Vietnamese specific processing
                    for product in batch_products:
                        product['platform'] = platform
                        product['price_vnd'] = self._convert_to_vnd(product['price'])
                        product['vietnam_category'] = self._map_vietnam_category(
                            product['category'])
                        product['extracted_at'] = datetime.now()

                    products.extend(batch_products)
                    page += 1

                    # Rate limiting
                    await asyncio.sleep(3600 / self.platforms[platform]['rate_limit'])

            return products

    def _convert_to_vnd(self, price_usd: float, exchange_rate: float = 24000):
        """Convert USD to Vietnamese Dong"""
        return price_usd * exchange_rate

    def _map_vietnam_category(self, category: str) -> str:
        """Map international categories to Vietnamese market categories"""
        category_mapping = {
            'mobile_phones': 'Điện thoại di động',
            'laptops': 'Máy tính xách tay',
            'electronics': 'Thiết bị điện tử',
            'fashion': 'Thời trang',
            'home_garden': 'Nhà cửa & Đời sống'
        }
        return category_mapping.get(category.lower(), category)
```

#### 1.2 Database Change Data Capture
```python
# PostgreSQL CDC using Debezium-like approach
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

class PostgreSQLCDC:
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self.slot_name = 'vietnam_ecommerce_cdc'

    def setup_logical_replication(self):
        """Setup logical replication for change data capture"""

        conn = psycopg2.connect(**self.connection_params)
        cursor = conn.cursor()

        # Create replication slot
        cursor.execute(f"""
            SELECT pg_create_logical_replication_slot('{self.slot_name}', 'pgoutput')
        """)

        # Setup publication for tables
        cursor.execute("""
            CREATE PUBLICATION vietnam_ecommerce_pub
            FOR TABLE orders, customers, products, transactions
        """)

        conn.commit()
        cursor.close()
        conn.close()

    def stream_changes(self):
        """Stream database changes in real-time"""

        conn = LogicalReplicationConnection(**self.connection_params)
        cursor = conn.cursor()

        cursor.start_replication(
            slot_name=self.slot_name,
            decode=True,
            start_lsn=0
        )

        def process_change(msg):
            if msg.data_start:
                change_data = {
                    'table': msg.table,
                    'operation': msg.operation,  # INSERT, UPDATE, DELETE
                    'data': msg.payload,
                    'timestamp': msg.timestamp,
                    'lsn': msg.data_start
                }

                # Send to Kafka for further processing
                self.send_to_kafka(change_data)

        cursor.consume_stream(process_change)

    def send_to_kafka(self, change_data):
        """Send CDC events to Kafka"""
        from kafka import KafkaProducer
        import json

        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        topic = f"cdc_{change_data['table']}"
        producer.send(topic, change_data)
        producer.flush()
```

### Stage 2: Data Transformation (Transform)

#### 2.1 Data Cleansing and Validation
```python
import pandas as pd
import re
from typing import Dict, List, Optional

class VietnameseDataCleaner:
    def __init__(self):
        self.vietnam_provinces = [
            'Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Hai Phong',
            'Can Tho', 'Bien Hoa', 'Hue', 'Nha Trang', 'Buon Ma Thuot',
            'Quy Nhon', 'Vung Tau', 'Thai Nguyen', 'Thanh Hoa', 'Thai Binh'
            # ... thêm 63 tỉnh thành
        ]

        self.vietnam_phone_pattern = r'^\+84[0-9]{9,10}$'
        self.vietnam_postal_pattern = r'^[0-9]{6}$'

    def clean_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate Vietnamese customer data"""

        # Standardize Vietnamese names
        df['full_name'] = df['full_name'].apply(self._standardize_vietnamese_name)

        # Validate and clean phone numbers
        df['phone'] = df['phone'].apply(self._clean_vietnamese_phone)
        df['phone_valid'] = df['phone'].apply(self._validate_vietnamese_phone)

        # Standardize addresses
        df['city'] = df['city'].apply(self._standardize_city_name)
        df['region'] = df['city'].apply(self._get_vietnam_region)

        # Validate postal codes
        df['postal_code_valid'] = df['postal_code'].apply(self._validate_postal_code)

        # Clean income levels
        df['income_level'] = df['income_level'].apply(self._standardize_income_level)

        return df

    def _standardize_vietnamese_name(self, name: str) -> str:
        """Standardize Vietnamese names with proper capitalization"""
        if pd.isna(name):
            return None

        # Remove extra spaces and standardize
        name = re.sub(r'\s+', ' ', name.strip())

        # Proper case for Vietnamese names
        words = name.split()
        standardized_words = []

        for word in words:
            # Handle Vietnamese specific name patterns
            if word.lower() in ['nguyễn', 'trần', 'lê', 'phạm', 'huỳnh', 'hoàng', 'phan', 'vũ', 'võ', 'đặng']:
                standardized_words.append(word.capitalize())
            else:
                standardized_words.append(word.title())

        return ' '.join(standardized_words)

    def _clean_vietnamese_phone(self, phone: str) -> str:
        """Clean and format Vietnamese phone numbers"""
        if pd.isna(phone):
            return None

        # Remove all non-digits
        digits = re.sub(r'[^\d]', '', phone)

        # Handle different Vietnamese phone formats
        if digits.startswith('84'):
            return f"+{digits}"
        elif digits.startswith('0'):
            return f"+84{digits[1:]}"
        else:
            return f"+84{digits}"

    def _validate_vietnamese_phone(self, phone: str) -> bool:
        """Validate Vietnamese phone number format"""
        if pd.isna(phone):
            return False
        return bool(re.match(self.vietnam_phone_pattern, phone))

    def _standardize_city_name(self, city: str) -> str:
        """Standardize Vietnamese city names"""
        if pd.isna(city):
            return None

        city_mapping = {
            'hcm': 'Ho Chi Minh City',
            'tp hcm': 'Ho Chi Minh City',
            'saigon': 'Ho Chi Minh City',
            'sg': 'Ho Chi Minh City',
            'hn': 'Hanoi',
            'ha noi': 'Hanoi',
            'dn': 'Da Nang',
            'da nang': 'Da Nang',
            'hp': 'Hai Phong',
            'hai phong': 'Hai Phong'
        }

        city_clean = city.lower().strip()
        return city_mapping.get(city_clean, city.title())

    def _get_vietnam_region(self, city: str) -> str:
        """Map Vietnamese cities to regions (North, Central, South)"""
        if pd.isna(city):
            return None

        north_cities = ['Hanoi', 'Hai Phong', 'Quang Ninh', 'Lang Son', 'Cao Bang']
        central_cities = ['Da Nang', 'Hue', 'Quang Nam', 'Quang Ngai', 'Binh Dinh']
        south_cities = ['Ho Chi Minh City', 'Can Tho', 'Dong Nai', 'Binh Duong', 'Ba Ria-Vung Tau']

        if city in north_cities:
            return 'North'
        elif city in central_cities:
            return 'Central'
        elif city in south_cities:
            return 'South'
        else:
            return 'Unknown'

    def _validate_postal_code(self, postal_code: str) -> bool:
        """Validate Vietnamese postal code format"""
        if pd.isna(postal_code):
            return False
        return bool(re.match(self.vietnam_postal_pattern, str(postal_code)))

    def _standardize_income_level(self, income: str) -> str:
        """Standardize income level categories"""
        if pd.isna(income):
            return None

        income_mapping = {
            'low': 'Low',
            'middle': 'Middle',
            'high': 'High',
            'thấp': 'Low',
            'trung bình': 'Middle',
            'cao': 'High'
        }

        return income_mapping.get(income.lower().strip(), income)
```

#### 2.2 Business Logic Transformation
```python
class BusinessLogicTransformer:
    def __init__(self):
        self.vnd_exchange_rate = 24000  # USD to VND
        self.vietnam_tax_rate = 0.1  # 10% VAT

    def transform_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Vietnamese business logic transformations"""

        # Currency conversion
        df['total_amount_vnd'] = df['total_amount_usd'] * self.vnd_exchange_rate
        df['unit_price_vnd'] = df['unit_price_usd'] * self.vnd_exchange_rate

        # Calculate Vietnamese VAT
        df['vat_amount_vnd'] = df['subtotal_usd'] * self.vnd_exchange_rate * self.vietnam_tax_rate

        # Vietnamese payment method mapping
        df['payment_method_local'] = df['payment_method'].apply(self._map_payment_method)

        # Customer segment classification
        df['customer_segment_vietnam'] = df.apply(self._classify_vietnamese_customer, axis=1)

        # Seasonal business flags
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['is_tet_season'] = df['order_date'].apply(self._is_tet_season)
        df['is_shopping_festival'] = df['order_date'].apply(self._is_shopping_festival)

        # Platform commission calculation
        df['platform_commission'] = df.apply(self._calculate_platform_commission, axis=1)

        return df

    def _map_payment_method(self, method: str) -> str:
        """Map payment methods to Vietnamese standards"""
        payment_mapping = {
            'momo': 'E_Wallet_MoMo',
            'zalopay': 'E_Wallet_ZaloPay',
            'vnpay': 'E_Wallet_VNPay',
            'cod': 'COD',
            'cash_on_delivery': 'COD',
            'bank_transfer': 'Bank_Transfer',
            'credit_card': 'Credit_Card',
            'debit_card': 'Debit_Card'
        }
        return payment_mapping.get(method.lower(), method)

    def _classify_vietnamese_customer(self, row) -> str:
        """Classify customers based on Vietnamese market criteria"""
        monthly_spending_vnd = row['monthly_spending_usd'] * self.vnd_exchange_rate

        if monthly_spending_vnd >= 12000000:  # 12M VND
            return 'Premium'
        elif monthly_spending_vnd >= 4800000:  # 4.8M VND
            return 'High_Value'
        else:
            return 'Regular'

    def _is_tet_season(self, date) -> bool:
        """Check if date falls in Vietnamese Tet season"""
        # Tet usually falls between January 21 - February 20
        return (date.month == 1 and date.day >= 21) or (date.month == 2 and date.day <= 20)

    def _is_shopping_festival(self, date) -> bool:
        """Check if date falls in major Vietnamese shopping festivals"""
        # 9/9, 10/10, 11/11, 12/12 shopping festivals
        return (date.month == 9 and date.day == 9) or \
               (date.month == 10 and date.day == 10) or \
               (date.month == 11 and date.day == 11) or \
               (date.month == 12 and date.day == 12)

    def _calculate_platform_commission(self, row) -> float:
        """Calculate commission based on Vietnamese platform rates"""
        platform_rates = {
            'Lazada': 0.06,
            'Shopee': 0.055,
            'Tiki': 0.08,
            'Sendo': 0.05,
            'FPTShop': 0.04,
            'CellphoneS': 0.035
        }

        rate = platform_rates.get(row['platform'], 0.06)  # Default 6%
        return row['total_amount_usd'] * rate
```

### Stage 3: Data Loading (Load)

#### 3.1 Dimension Loading with SCD Type 2
```python
import psycopg2
from datetime import datetime
from typing import Dict, List

class DimensionLoader:
    def __init__(self, db_config: Dict):
        self.db_config = db_config

    def load_customer_dimension(self, customers_df: pd.DataFrame):
        """Load customer dimension with SCD Type 2 support"""

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        for _, customer in customers_df.iterrows():
            # Check if customer exists
            cursor.execute("""
                SELECT customer_key, customer_id, full_name, email, city,
                       customer_segment, income_level
                FROM dim_customer
                WHERE customer_id = %s AND is_current = TRUE
            """, (customer['customer_id'],))

            existing = cursor.fetchone()

            if existing:
                # Check for changes (SCD Type 2)
                if self._customer_has_changes(existing, customer):
                    # Close current record
                    cursor.execute("""
                        UPDATE dim_customer
                        SET expiry_date = %s, is_current = FALSE
                        WHERE customer_id = %s AND is_current = TRUE
                    """, (datetime.now().date(), customer['customer_id']))

                    # Insert new record
                    self._insert_new_customer_record(cursor, customer)
            else:
                # New customer
                self._insert_new_customer_record(cursor, customer)

        conn.commit()
        cursor.close()
        conn.close()

    def _customer_has_changes(self, existing, new_customer) -> bool:
        """Check if customer attributes have changed"""
        return (existing[2] != new_customer['full_name'] or
                existing[3] != new_customer['email'] or
                existing[4] != new_customer['city'] or
                existing[5] != new_customer['customer_segment'] or
                existing[6] != new_customer['income_level'])

    def _insert_new_customer_record(self, cursor, customer):
        """Insert new customer record"""
        cursor.execute("""
            INSERT INTO dim_customer (
                customer_id, full_name, email, phone, date_of_birth, age,
                gender, registration_date, city, state, region, country,
                postal_code, customer_segment, income_level, monthly_spending_usd,
                preferred_device, preferred_payment_method, shopping_frequency,
                vietnam_region_preference, effective_date, expiry_date, is_current
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            customer['customer_id'], customer['full_name'], customer['email'],
            customer['phone'], customer['date_of_birth'], customer['age'],
            customer['gender'], customer['registration_date'], customer['city'],
            customer['state'], customer['region'], customer['country'],
            customer['postal_code'], customer['customer_segment'],
            customer['income_level'], customer['monthly_spending_usd'],
            customer['preferred_device'], customer['preferred_payment_method'],
            customer['shopping_frequency'], customer['vietnam_region_preference'],
            datetime.now().date(), None, True
        ))
```

#### 3.2 Fact Table Loading
```python
class FactLoader:
    def __init__(self, db_config: Dict):
        self.db_config = db_config

    def load_sales_facts(self, sales_df: pd.DataFrame):
        """Load sales facts with dimension key lookups"""

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        # Prepare batch insert
        batch_data = []

        for _, sale in sales_df.iterrows():
            # Lookup dimension keys
            customer_key = self._lookup_customer_key(cursor, sale['customer_id'])
            product_key = self._lookup_product_key(cursor, sale['product_id'])
            time_key = self._lookup_time_key(cursor, sale['order_date'])
            geography_key = self._lookup_geography_key(cursor, sale['city'])
            platform_key = self._lookup_platform_key(cursor, sale['platform'])

            if all([customer_key, product_key, time_key, geography_key, platform_key]):
                batch_data.append((
                    sale['order_id'], customer_key, product_key, time_key,
                    geography_key, platform_key, sale['quantity'],
                    sale['unit_price_usd'], sale['unit_price_vnd'],
                    sale['total_amount_usd'], sale['total_amount_vnd'],
                    sale['discount_amount_usd'], sale['tax_amount_usd'],
                    sale['shipping_cost_usd'], sale['profit_margin_usd'],
                    sale['vietnam_promotion_applied'], sale['payment_method_local']
                ))

        # Batch insert for performance
        if batch_data:
            cursor.executemany("""
                INSERT INTO fact_sales (
                    order_id, customer_key, product_key, time_key,
                    geography_key, platform_key, quantity,
                    unit_price_usd, unit_price_vnd, total_amount_usd,
                    total_amount_vnd, discount_amount_usd, tax_amount_usd,
                    shipping_cost_usd, profit_margin_usd,
                    vietnam_promotion_applied, payment_method_local
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (order_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    total_amount_usd = EXCLUDED.total_amount_usd,
                    updated_at = CURRENT_TIMESTAMP
            """, batch_data)

        conn.commit()
        cursor.close()
        conn.close()

    def _lookup_customer_key(self, cursor, customer_id: str) -> int:
        """Lookup customer key from dimension table"""
        cursor.execute("""
            SELECT customer_key FROM dim_customer
            WHERE customer_id = %s AND is_current = TRUE
        """, (customer_id,))
        result = cursor.fetchone()
        return result[0] if result else None

    def _lookup_product_key(self, cursor, product_id: str) -> int:
        """Lookup product key from dimension table"""
        cursor.execute("""
            SELECT product_key FROM dim_product
            WHERE product_id = %s AND is_current = TRUE
        """, (product_id,))
        result = cursor.fetchone()
        return result[0] if result else None

    def _lookup_time_key(self, cursor, date_value) -> int:
        """Lookup time key from time dimension"""
        cursor.execute("""
            SELECT time_key FROM dim_time
            WHERE date_value = %s
        """, (date_value.date(),))
        result = cursor.fetchone()
        return result[0] if result else None
```

## ⚡ Real-time Streaming ETL

### Kafka Streaming Processing
```python
from kafka import KafkaConsumer, KafkaProducer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class StreamingETL:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Vietnam_Ecommerce_Streaming_ETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

    def process_sales_stream(self):
        """Process real-time sales events from Kafka"""

        # Define schema for sales events
        sales_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price_usd", DoubleType(), True),
            StructField("total_amount_usd", DoubleType(), True),
            StructField("platform", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("city", StringType(), True)
        ])

        # Read from Kafka
        sales_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "sales_events") \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON and apply transformations
        processed_stream = sales_stream \
            .select(from_json(col("value").cast("string"), sales_schema).alias("data")) \
            .select("data.*") \
            .withColumn("total_amount_vnd", col("total_amount_usd") * 24000) \
            .withColumn("unit_price_vnd", col("unit_price_usd") * 24000) \
            .withColumn("region",
                       when(col("city").isin(["Hanoi", "Hai Phong"]), "North")
                       .when(col("city").isin(["Da Nang", "Hue"]), "Central")
                       .when(col("city").isin(["Ho Chi Minh City", "Can Tho"]), "South")
                       .otherwise("Unknown")) \
            .withColumn("payment_method_local",
                       when(col("payment_method") == "momo", "E_Wallet_MoMo")
                       .when(col("payment_method") == "zalopay", "E_Wallet_ZaloPay")
                       .when(col("payment_method") == "cod", "COD")
                       .otherwise(col("payment_method"))) \
            .withColumn("is_tet_season",
                       when((month(col("order_timestamp")) == 1) & (dayofmonth(col("order_timestamp")) >= 21) |
                            (month(col("order_timestamp")) == 2) & (dayofmonth(col("order_timestamp")) <= 20), True)
                       .otherwise(False)) \
            .withColumn("processed_timestamp", current_timestamp())

        # Write to multiple sinks
        # 1. Write to PostgreSQL for immediate OLTP queries
        postgres_query = processed_stream \
            .writeStream \
            .outputMode("append") \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/ecommerce_dss") \
            .option("dbtable", "fact_sales_realtime") \
            .option("user", "dss_user") \
            .option("password", "dss_password_123") \
            .option("driver", "org.postgresql.Driver") \
            .trigger(processingTime='30 seconds') \
            .start()

        # 2. Write to Kafka for downstream processing
        kafka_query = processed_stream \
            .select(to_json(struct("*")).alias("value")) \
            .writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", "sales_processed") \
            .trigger(processingTime='10 seconds') \
            .start()

        # 3. Write aggregated data to Redis for caching
        redis_query = processed_stream \
            .groupBy(
                window(col("order_timestamp"), "5 minutes"),
                col("platform"),
                col("region")
            ) \
            .agg(
                count("*").alias("order_count"),
                sum("total_amount_vnd").alias("total_revenue_vnd"),
                avg("total_amount_vnd").alias("avg_order_value_vnd")
            ) \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .trigger(processingTime='1 minute') \
            .start()

        return [postgres_query, kafka_query, redis_query]
```

## 🔧 Data Quality Monitoring

### Automated Data Quality Checks
```python
class DataQualityMonitor:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.quality_thresholds = {
            'completeness_min': 0.95,  # 95% minimum completeness
            'accuracy_min': 0.99,      # 99% minimum accuracy
            'duplicate_max': 0.01,     # Max 1% duplicates
            'freshness_max_hours': 2   # Data must be < 2 hours old
        }

    def run_quality_checks(self) -> Dict:
        """Run comprehensive data quality checks"""

        results = {
            'timestamp': datetime.now(),
            'checks': {}
        }

        # Completeness checks
        results['checks']['completeness'] = self._check_completeness()

        # Accuracy checks
        results['checks']['accuracy'] = self._check_accuracy()

        # Duplicate checks
        results['checks']['duplicates'] = self._check_duplicates()

        # Freshness checks
        results['checks']['freshness'] = self._check_freshness()

        # Business rule checks
        results['checks']['business_rules'] = self._check_business_rules()

        # Overall quality score
        results['overall_score'] = self._calculate_overall_score(results['checks'])

        # Send alerts if quality is below threshold
        if results['overall_score'] < 0.95:
            self._send_quality_alert(results)

        return results

    def _check_completeness(self) -> Dict:
        """Check data completeness across key tables"""

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        completeness_results = {}

        # Fact table completeness
        cursor.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(customer_key) as customer_key_count,
                COUNT(product_key) as product_key_count,
                COUNT(total_amount_usd) as amount_count
            FROM fact_sales
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """)

        fact_stats = cursor.fetchone()
        completeness_results['fact_sales'] = {
            'customer_key_completeness': fact_stats[1] / fact_stats[0] if fact_stats[0] > 0 else 0,
            'product_key_completeness': fact_stats[2] / fact_stats[0] if fact_stats[0] > 0 else 0,
            'amount_completeness': fact_stats[3] / fact_stats[0] if fact_stats[0] > 0 else 0
        }

        # Customer dimension completeness
        cursor.execute("""
            SELECT
                COUNT(*) as total_customers,
                COUNT(email) as email_count,
                COUNT(phone) as phone_count,
                COUNT(city) as city_count
            FROM dim_customer
            WHERE is_current = TRUE
        """)

        customer_stats = cursor.fetchone()
        completeness_results['dim_customer'] = {
            'email_completeness': customer_stats[1] / customer_stats[0] if customer_stats[0] > 0 else 0,
            'phone_completeness': customer_stats[2] / customer_stats[0] if customer_stats[0] > 0 else 0,
            'city_completeness': customer_stats[3] / customer_stats[0] if customer_stats[0] > 0 else 0
        }

        cursor.close()
        conn.close()

        return completeness_results

    def _check_accuracy(self) -> Dict:
        """Check data accuracy using business rules"""

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        accuracy_results = {}

        # Vietnamese phone number format validation
        cursor.execute("""
            SELECT
                COUNT(*) as total_customers,
                COUNT(CASE WHEN phone ~ '^\+84[0-9]{9,10}$' THEN 1 END) as valid_phone_count
            FROM dim_customer
            WHERE is_current = TRUE AND phone IS NOT NULL
        """)

        phone_stats = cursor.fetchone()
        accuracy_results['phone_format'] = {
            'accuracy_rate': phone_stats[1] / phone_stats[0] if phone_stats[0] > 0 else 0,
            'total_checked': phone_stats[0],
            'valid_count': phone_stats[1]
        }

        # Price consistency (VND = USD * exchange_rate)
        cursor.execute("""
            SELECT
                COUNT(*) as total_sales,
                COUNT(CASE
                    WHEN ABS(unit_price_vnd - (unit_price_usd * 24000)) < 100
                    THEN 1
                END) as consistent_price_count
            FROM fact_sales
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """)

        price_stats = cursor.fetchone()
        accuracy_results['price_consistency'] = {
            'accuracy_rate': price_stats[1] / price_stats[0] if price_stats[0] > 0 else 0,
            'total_checked': price_stats[0],
            'consistent_count': price_stats[1]
        }

        cursor.close()
        conn.close()

        return accuracy_results

    def _send_quality_alert(self, results: Dict):
        """Send data quality alerts via multiple channels"""

        from kafka import KafkaProducer
        import smtplib
        from email.mime.text import MIMEText

        alert_message = f"""
        DATA QUALITY ALERT

        Overall Quality Score: {results['overall_score']:.2%}
        Timestamp: {results['timestamp']}

        Issues Detected:
        """

        for check_name, check_results in results['checks'].items():
            if isinstance(check_results, dict):
                for metric, value in check_results.items():
                    if isinstance(value, dict) and 'accuracy_rate' in value:
                        if value['accuracy_rate'] < 0.95:
                            alert_message += f"- {check_name}.{metric}: {value['accuracy_rate']:.2%}\n"

        # Send to Kafka for real-time monitoring
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        producer.send('data_quality_alerts', {
            'alert_type': 'quality_degradation',
            'severity': 'high' if results['overall_score'] < 0.90 else 'medium',
            'message': alert_message,
            'timestamp': results['timestamp'].isoformat(),
            'details': results
        })

        producer.flush()
```

## 📋 Orchestration với Apache Airflow

### Main ETL DAG
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'vietnam-ecommerce-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['admin@vietnam-ecommerce.com']
}

dag = DAG(
    'vietnam_ecommerce_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for Vietnamese e-commerce data',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False,
    max_active_runs=1,
    tags=['vietnam', 'ecommerce', 'etl', 'analytics']
)

# Data extraction tasks
extract_lazada = PythonOperator(
    task_id='extract_lazada_data',
    python_callable=extract_platform_data,
    op_kwargs={'platform': 'lazada'},
    dag=dag
)

extract_shopee = PythonOperator(
    task_id='extract_shopee_data',
    python_callable=extract_platform_data,
    op_kwargs={'platform': 'shopee'},
    dag=dag
)

extract_tiki = PythonOperator(
    task_id='extract_tiki_data',
    python_callable=extract_platform_data,
    op_kwargs={'platform': 'tiki'},
    dag=dag
)

# Data quality checks
quality_check_raw = PythonOperator(
    task_id='quality_check_raw_data',
    python_callable=run_raw_data_quality_checks,
    dag=dag
)

# Data transformation
transform_customers = PythonOperator(
    task_id='transform_customer_data',
    python_callable=transform_customer_data,
    dag=dag
)

transform_products = PythonOperator(
    task_id='transform_product_data',
    python_callable=transform_product_data,
    dag=dag
)

transform_sales = PythonOperator(
    task_id='transform_sales_data',
    python_callable=transform_sales_data,
    dag=dag
)

# Data loading
load_dimensions = PythonOperator(
    task_id='load_dimension_tables',
    python_callable=load_all_dimensions,
    dag=dag
)

load_facts = PythonOperator(
    task_id='load_fact_tables',
    python_callable=load_sales_facts,
    dag=dag
)

# Post-load quality checks
quality_check_final = PythonOperator(
    task_id='quality_check_final_data',
    python_callable=run_final_quality_checks,
    dag=dag
)

# Refresh materialized views
refresh_views = BashOperator(
    task_id='refresh_materialized_views',
    bash_command='''
        psql -h postgres -U dss_user -d ecommerce_dss -c "
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_sales;
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_analytics;
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_product_performance;
        "
    ''',
    dag=dag
)

# Send completion notification
send_notification = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_etl_completion_notification,
    dag=dag
)

# Define task dependencies
[extract_lazada, extract_shopee, extract_tiki] >> quality_check_raw
quality_check_raw >> [transform_customers, transform_products, transform_sales]
[transform_customers, transform_products, transform_sales] >> load_dimensions
load_dimensions >> load_facts
load_facts >> quality_check_final
quality_check_final >> refresh_views
refresh_views >> send_notification
```

## 🎯 Performance Optimization Strategies

### 1. Parallel Processing
```python
import concurrent.futures
from multiprocessing import Pool

class ParallelETL:
    def __init__(self, max_workers=4):
        self.max_workers = max_workers

    def parallel_platform_extraction(self, platforms: List[str]):
        """Extract data from multiple platforms in parallel"""

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_platform = {
                executor.submit(self.extract_platform_data, platform): platform
                for platform in platforms
            }

            results = {}
            for future in concurrent.futures.as_completed(future_to_platform):
                platform = future_to_platform[future]
                try:
                    data = future.result()
                    results[platform] = data
                    print(f"Successfully extracted data from {platform}")
                except Exception as exc:
                    print(f"Platform {platform} generated an exception: {exc}")
                    results[platform] = None

            return results

    def parallel_data_processing(self, data_chunks: List[pd.DataFrame]):
        """Process data chunks in parallel"""

        with Pool(processes=self.max_workers) as pool:
            processed_chunks = pool.map(self.process_data_chunk, data_chunks)

        return pd.concat(processed_chunks, ignore_index=True)
```

### 2. Incremental Loading Strategy
```python
class IncrementalLoader:
    def __init__(self, db_config):
        self.db_config = db_config

    def get_last_processed_timestamp(self, table_name: str) -> datetime:
        """Get the last processed timestamp for incremental loading"""

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT COALESCE(MAX(created_at), '1900-01-01'::timestamp)
            FROM {table_name}
        """)

        last_timestamp = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        return last_timestamp

    def load_incremental_data(self, source_table: str, target_table: str):
        """Load only new/modified data since last run"""

        last_timestamp = self.get_last_processed_timestamp(target_table)

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        # Extract only changed records
        cursor.execute(f"""
            INSERT INTO {target_table}
            SELECT * FROM {source_table}
            WHERE last_modified > %s
            ON CONFLICT (id) DO UPDATE SET
                -- Update all columns except id and created_at
                last_modified = EXCLUDED.last_modified
        """, (last_timestamp,))

        conn.commit()
        cursor.close()
        conn.close()
```

## 📊 Monitoring và Alerting

### ETL Pipeline Monitoring
```python
class ETLMonitor:
    def __init__(self):
        self.metrics = {
            'extraction_time': {},
            'transformation_time': {},
            'loading_time': {},
            'data_volume': {},
            'error_count': {}
        }

    def monitor_extraction_performance(self, platform: str, start_time: datetime, end_time: datetime, record_count: int):
        """Monitor extraction performance metrics"""

        duration = (end_time - start_time).total_seconds()
        throughput = record_count / duration if duration > 0 else 0

        self.metrics['extraction_time'][platform] = duration

        # Send to Prometheus
        self.send_metric_to_prometheus('etl_extraction_duration_seconds', duration, {'platform': platform})
        self.send_metric_to_prometheus('etl_extraction_throughput_records_per_second', throughput, {'platform': platform})

        # Alert if performance degrades
        if duration > 300:  # 5 minutes threshold
            self.send_alert(f"Extraction performance degraded for {platform}: {duration:.2f} seconds")

    def send_metric_to_prometheus(self, metric_name: str, value: float, labels: Dict):
        """Send metrics to Prometheus for monitoring"""

        import requests

        # Format metric for Prometheus pushgateway
        metric_line = f'{metric_name}{{{",".join([f"{k}=\\"{v}\\"" for k, v in labels.items()])}}} {value}'

        try:
            response = requests.post(
                'http://prometheus-pushgateway:9091/metrics/job/vietnam_ecommerce_etl',
                data=metric_line,
                headers={'Content-Type': 'text/plain'}
            )
            response.raise_for_status()
        except Exception as e:
            print(f"Failed to send metric to Prometheus: {e}")

    def send_alert(self, message: str, severity: str = 'warning'):
        """Send alert to multiple channels"""

        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'message': message,
            'severity': severity,
            'source': 'vietnam_ecommerce_etl'
        }

        # Send to Kafka for real-time alerts
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        producer.send('etl_alerts', alert_data)
        producer.flush()

        # Send to Slack/Teams webhook (example)
        # self.send_to_slack(message, severity)
```

---

## 🎯 Kết Luận

Thiết kế ETL này cung cấp:

1. **Khả năng mở rộng**: Hỗ trợ processing song song và streaming real-time
2. **Chất lượng dữ liệu**: Kiểm tra tự động và monitoring
3. **Tối ưu cho Việt Nam**: Xử lý đặc thù thị trường Việt Nam
4. **Hiệu suất cao**: Sử dụng batch processing và incremental loading
5. **Monitoring toàn diện**: Tracking metrics và alerting

Hệ thống này đảm bảo dữ liệu chính xác, kịp thời và phù hợp cho ra quyết định kinh doanh trong thị trường e-commerce Việt Nam.