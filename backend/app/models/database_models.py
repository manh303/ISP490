#!/usr/bin/env python3
"""
Database Models and Schemas for Big Data Streaming Platform
Supports both PostgreSQL (processed data) and MongoDB (raw streaming data)
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from decimal import Decimal
import enum

# SQLAlchemy for PostgreSQL
from sqlalchemy import (
    Column, Integer, String, DateTime, Float, Boolean, Text, 
    JSON, ForeignKey, Index, UniqueConstraint, CheckConstraint,
    DECIMAL, BigInteger, SmallInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
import uuid

# Pydantic for API schemas
from pydantic import BaseModel, Field, validator, root_validator
from pydantic.types import UUID4, EmailStr

# MongoDB Document Models (using Motor/PyMongo)
from typing_extensions import Annotated

# ====================================
# SQLALCHEMY BASE & POSTGRESQL MODELS
# ====================================
Base = declarative_base()

class TimestampMixin:
    """Mixin for created_at and updated_at timestamps"""
    created_at = Column(DateTime(timezone=True), default=datetime.now, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.now, onupdate=datetime.now, nullable=False)

class UUIDMixin:
    """Mixin for UUID primary key"""
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

# ====================================
# ENUMS
# ====================================
class OrderStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"

class CustomerTier(str, enum.Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"

class AlertSeverity(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

# ====================================
# POSTGRESQL TABLES (Processed Data)
# ====================================
class Customer(Base, UUIDMixin, TimestampMixin):
    """Customer information (processed and cleaned)"""
    __tablename__ = "customers"
    
    customer_id = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    phone = Column(String(20))
    date_of_birth = Column(DateTime(timezone=True))
    
    # Demographics
    gender = Column(String(20))
    country = Column(String(100), nullable=False, index=True)
    state = Column(String(100))
    city = Column(String(100))
    postal_code = Column(String(20))
    
    # Business metrics
    customer_tier = Column(String(20), default=CustomerTier.BRONZE.value)
    total_orders = Column(Integer, default=0)
    total_spent = Column(DECIMAL(12, 2), default=0)
    average_order_value = Column(DECIMAL(10, 2), default=0)
    last_order_date = Column(DateTime(timezone=True))
    
    # Behavioral data
    marketing_consent = Column(Boolean, default=False)
    preferred_category = Column(String(100))
    acquisition_channel = Column(String(100))
    
    # Relationships
    orders = relationship("Order", back_populates="customer")
    
    __table_args__ = (
        Index('ix_customer_tier_country', 'customer_tier', 'country'),
        Index('ix_customer_spending', 'total_spent', 'customer_tier'),
    )

class Product(Base, UUIDMixin, TimestampMixin):
    """Product catalog (processed)"""
    __tablename__ = "products"
    
    product_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    category = Column(String(100), nullable=False, index=True)
    subcategory = Column(String(100))
    brand = Column(String(100), index=True)
    
    # Pricing
    price = Column(DECIMAL(10, 2), nullable=False)
    cost = Column(DECIMAL(10, 2))
    margin_percent = Column(Float)
    
    # Inventory
    stock_quantity = Column(Integer, default=0)
    reorder_level = Column(Integer, default=10)
    supplier_id = Column(String(50))
    
    # Attributes
    weight = Column(Float)
    dimensions = Column(JSON)  # {"length": x, "width": y, "height": z}
    color = Column(String(50))
    size = Column(String(50))
    
    # Business metrics
    total_sold = Column(Integer, default=0)
    total_revenue = Column(DECIMAL(12, 2), default=0)
    avg_rating = Column(Float)
    review_count = Column(Integer, default=0)
    
    # Status
    is_active = Column(Boolean, default=True)
    is_featured = Column(Boolean, default=False)
    
    # Relationships
    order_items = relationship("OrderItem", back_populates="product")
    
    __table_args__ = (
        Index('ix_product_category_brand', 'category', 'brand'),
        Index('ix_product_price_range', 'price', 'category'),
        CheckConstraint('price > 0', name='check_positive_price'),
    )

class Order(Base, UUIDMixin, TimestampMixin):
    """Order information (processed)"""
    __tablename__ = "orders"
    
    order_id = Column(String(50), unique=True, nullable=False, index=True)
    customer_uuid = Column(UUID(as_uuid=True), ForeignKey('customers.id'), nullable=False)
    
    # Order details
    order_date = Column(DateTime(timezone=True), nullable=False, index=True)
    status = Column(String(20), default=OrderStatus.PENDING.value, nullable=False)
    
    # Financial
    subtotal = Column(DECIMAL(10, 2), nullable=False)
    tax_amount = Column(DECIMAL(10, 2), default=0)
    shipping_cost = Column(DECIMAL(8, 2), default=0)
    discount_amount = Column(DECIMAL(8, 2), default=0)
    total_amount = Column(DECIMAL(10, 2), nullable=False)
    
    # Shipping
    shipping_address = Column(JSON)  # Full address object
    shipping_method = Column(String(50))
    tracking_number = Column(String(100))
    estimated_delivery = Column(DateTime(timezone=True))
    actual_delivery = Column(DateTime(timezone=True))
    
    # Metadata
    order_source = Column(String(50))  # web, mobile, api, etc.
    payment_method = Column(String(50))
    currency = Column(String(3), default='USD')
    
    # Relationships
    customer = relationship("Customer", back_populates="orders")
    order_items = relationship("OrderItem", back_populates="order")
    
    __table_args__ = (
        Index('ix_order_date_status', 'order_date', 'status'),
        Index('ix_order_total_date', 'total_amount', 'order_date'),
        CheckConstraint('total_amount > 0', name='check_positive_total'),
    )

class OrderItem(Base, UUIDMixin, TimestampMixin):
    """Individual items within orders"""
    __tablename__ = "order_items"
    
    order_uuid = Column(UUID(as_uuid=True), ForeignKey('orders.id'), nullable=False)
    product_uuid = Column(UUID(as_uuid=True), ForeignKey('products.id'), nullable=False)
    
    # Item details
    quantity = Column(Integer, nullable=False)
    unit_price = Column(DECIMAL(10, 2), nullable=False)
    total_price = Column(DECIMAL(10, 2), nullable=False)
    discount_applied = Column(DECIMAL(8, 2), default=0)
    
    # Product snapshot (at time of order)
    product_name = Column(String(255))
    product_category = Column(String(100))
    
    # Relationships
    order = relationship("Order", back_populates="order_items")
    product = relationship("Product", back_populates="order_items")
    
    __table_args__ = (
        Index('ix_orderitem_order_product', 'order_uuid', 'product_uuid'),
        CheckConstraint('quantity > 0', name='check_positive_quantity'),
        CheckConstraint('unit_price > 0', name='check_positive_unit_price'),
    )

class AnalyticsSummary(Base, UUIDMixin, TimestampMixin):
    """Pre-aggregated analytics data for fast dashboard queries"""
    __tablename__ = "analytics_summary"
    
    # Time dimension
    time_bucket = Column(DateTime(timezone=True), nullable=False, index=True)
    granularity = Column(String(20), nullable=False)  # hour, day, week, month
    
    # Dimensions
    customer_id = Column(String(50), index=True)
    product_id = Column(String(50), index=True)
    category = Column(String(100), index=True)
    country = Column(String(100), index=True)
    order_source = Column(String(50))
    
    # Metrics
    order_count = Column(Integer, default=0)
    revenue = Column(DECIMAL(12, 2), default=0)
    order_value = Column(DECIMAL(10, 2))
    quantity_sold = Column(Integer, default=0)
    unique_customers = Column(Integer, default=0)
    new_customers = Column(Integer, default=0)
    
    # Advanced metrics
    avg_order_value = Column(DECIMAL(10, 2))
    customer_lifetime_value = Column(DECIMAL(12, 2))
    conversion_rate = Column(Float)
    
    __table_args__ = (
        Index('ix_analytics_time_granularity', 'time_bucket', 'granularity'),
        Index('ix_analytics_category_time', 'category', 'time_bucket'),
        UniqueConstraint('time_bucket', 'granularity', 'customer_id', 'product_id', 
                        name='uq_analytics_summary'),
    )

class MLModelPrediction(Base, UUIDMixin, TimestampMixin):
    """ML model predictions tracking"""
    __tablename__ = "ml_predictions"
    
    model_name = Column(String(100), nullable=False, index=True)
    model_version = Column(String(50))
    
    # Input/Output
    input_features = Column(JSONB)
    prediction = Column(JSONB)
    confidence_score = Column(Float)
    
    # Metadata
    prediction_type = Column(String(50))  # classification, regression, etc.
    execution_time_ms = Column(Integer)
    api_endpoint = Column(String(200))
    
    # Business context
    customer_id = Column(String(50), index=True)
    order_id = Column(String(50))
    business_context = Column(JSONB)
    
    __table_args__ = (
        Index('ix_ml_model_time', 'model_name', 'created_at'),
        Index('ix_ml_customer_predictions', 'customer_id', 'created_at'),
    )

class SystemAlert(Base, UUIDMixin, TimestampMixin):
    """System alerts and notifications"""
    __tablename__ = "system_alerts"
    
    alert_type = Column(String(50), nullable=False, index=True)
    severity = Column(String(20), default=AlertSeverity.LOW.value, nullable=False)
    title = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)
    
    # Context
    component = Column(String(100))  # kafka, postgres, model, etc.
    source = Column(String(100))     # specific service/job
    metadata = Column(JSONB)
    
    # Status
    is_active = Column(Boolean, default=True)
    acknowledged_at = Column(DateTime(timezone=True))
    acknowledged_by = Column(String(100))
    resolved_at = Column(DateTime(timezone=True))
    
    __table_args__ = (
        Index('ix_alert_severity_active', 'severity', 'is_active'),
        Index('ix_alert_type_time', 'alert_type', 'created_at'),
    )

# ====================================
# PYDANTIC SCHEMAS FOR API
# ====================================
class CustomerBase(BaseModel):
    """Base customer schema"""
    customer_id: str = Field(..., max_length=50)
    email: EmailStr
    first_name: str = Field(..., max_length=100)
    last_name: str = Field(..., max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    country: str = Field(..., max_length=100)
    state: Optional[str] = Field(None, max_length=100)
    city: Optional[str] = Field(None, max_length=100)
    
class CustomerCreate(CustomerBase):
    """Schema for creating customers"""
    marketing_consent: bool = False
    acquisition_channel: Optional[str] = None

class CustomerUpdate(BaseModel):
    """Schema for updating customers"""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    marketing_consent: Optional[bool] = None
    preferred_category: Optional[str] = None

class CustomerResponse(CustomerBase):
    """Schema for customer responses"""
    id: UUID4
    customer_tier: str
    total_orders: int
    total_spent: Decimal
    average_order_value: Decimal
    last_order_date: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class ProductBase(BaseModel):
    """Base product schema"""
    product_id: str = Field(..., max_length=50)
    name: str = Field(..., max_length=255)
    description: Optional[str] = None
    category: str = Field(..., max_length=100)
    brand: str = Field(..., max_length=100)
    price: Decimal = Field(..., gt=0)
    
class ProductCreate(ProductBase):
    """Schema for creating products"""
    stock_quantity: int = Field(default=0, ge=0)
    is_active: bool = True

class ProductResponse(ProductBase):
    """Schema for product responses"""
    id: UUID4
    stock_quantity: int
    total_sold: int
    total_revenue: Decimal
    avg_rating: Optional[float]
    review_count: int
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class OrderBase(BaseModel):
    """Base order schema"""
    order_id: str = Field(..., max_length=50)
    customer_id: str = Field(..., max_length=50)
    order_date: datetime
    status: OrderStatus = OrderStatus.PENDING
    subtotal: Decimal = Field(..., gt=0)
    total_amount: Decimal = Field(..., gt=0)

class OrderCreate(OrderBase):
    """Schema for creating orders"""
    items: List[Dict[str, Any]] = Field(..., min_items=1)
    shipping_address: Dict[str, Any]
    payment_method: str

class OrderResponse(OrderBase):
    """Schema for order responses"""
    id: UUID4
    tax_amount: Decimal
    shipping_cost: Decimal
    discount_amount: Decimal
    shipping_method: Optional[str]
    tracking_number: Optional[str]
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class AnalyticsRequest(BaseModel):
    """Schema for analytics requests"""
    metrics: List[str] = Field(..., min_items=1)
    dimensions: List[str] = Field(default=[])
    time_range: str = Field(..., regex=r"^(1h|6h|24h|7d|30d)$")
    granularity: str = Field(default="hour", regex=r"^(hour|day|week|month)$")
    filters: Optional[Dict[str, Any]] = None

class AnalyticsResponse(BaseModel):
    """Schema for analytics responses"""
    metrics: List[str]
    dimensions: List[str]
    time_range: str
    granularity: str
    data_points: int
    results: List[Dict[str, Any]]
    query_timestamp: datetime

# ====================================
# MONGODB DOCUMENT SCHEMAS
# ====================================
class StreamingDataDocument(BaseModel):
    """Schema for raw streaming data in MongoDB"""
    _id: Optional[str] = None
    source_topic: str
    timestamp: datetime = Field(default_factory=datetime.now)
    raw_data: Dict[str, Any]
    processed: bool = False
    processing_errors: Optional[List[str]] = None
    
    # Kafka metadata
    kafka_offset: Optional[int] = None
    kafka_partition: Optional[int] = None
    kafka_key: Optional[str] = None
    
    # Processing metadata
    received_at: datetime = Field(default_factory=datetime.now)
    processed_at: Optional[datetime] = None
    processor_version: Optional[str] = None

class MetricsDocument(BaseModel):
    """Schema for metrics collection in MongoDB"""
    _id: Optional[str] = None
    metric_name: str
    metric_type: str  # gauge, counter, histogram
    value: float
    tags: Dict[str, str] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.now)
    
    # Aggregation info
    window_size: Optional[str] = None  # 1m, 5m, 1h, etc.
    aggregation_method: Optional[str] = None  # avg, sum, max, min

class AlertDocument(BaseModel):
    """Schema for alerts in MongoDB"""
    _id: Optional[str] = None
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    alert_type: str
    severity: AlertSeverity
    title: str
    message: str
    
    # Context
    component: str
    source: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # Timing
    triggered_at: datetime = Field(default_factory=datetime.now)
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    
    # Status
    is_active: bool = True
    escalated: bool = False

# ====================================
# UTILITY FUNCTIONS
# ====================================
def create_tables(engine):
    """Create all PostgreSQL tables"""
    Base.metadata.create_all(bind=engine)

def get_customer_by_id(db: Session, customer_id: str) -> Optional[Customer]:
    """Get customer by customer_id"""
    return db.query(Customer).filter(Customer.customer_id == customer_id).first()

def get_recent_orders(db: Session, limit: int = 100) -> List[Order]:
    """Get recent orders"""
    return db.query(Order).order_by(Order.order_date.desc()).limit(limit).all()

def get_analytics_summary(
    db: Session, 
    start_date: datetime, 
    end_date: datetime,
    granularity: str = "hour"
) -> List[AnalyticsSummary]:
    """Get analytics summary for date range"""
    return db.query(AnalyticsSummary)\
        .filter(
            AnalyticsSummary.time_bucket >= start_date,
            AnalyticsSummary.time_bucket <= end_date,
            AnalyticsSummary.granularity == granularity
        )\
        .order_by(AnalyticsSummary.time_bucket)\
        .all()

# ====================================
# MONGODB COLLECTIONS SETUP
# ====================================
MONGODB_COLLECTIONS = {
    "streaming_data": StreamingDataDocument,
    "products_stream": StreamingDataDocument,
    "customers_stream": StreamingDataDocument,
    "orders_stream": StreamingDataDocument,
    "transactions_stream": StreamingDataDocument,
    "metrics": MetricsDocument,
    "alerts": AlertDocument,
    "ml_predictions": dict,  # For ML prediction storage
    "system_logs": dict,     # For system logging
}

# Collection indexes for MongoDB
MONGODB_INDEXES = {
    "streaming_data": [
        [("timestamp", -1)],
        [("source_topic", 1), ("timestamp", -1)],
        [("processed", 1), ("timestamp", -1)]
    ],
    "metrics": [
        [("metric_name", 1), ("timestamp", -1)],
        [("metric_type", 1), ("timestamp", -1)],
        [("tags.component", 1), ("timestamp", -1)]
    ],
    "alerts": [
        [("severity", 1), ("is_active", 1)],
        [("component", 1), ("triggered_at", -1)],
        [("alert_type", 1), ("triggered_at", -1)]
    ]
}