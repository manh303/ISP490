// MongoDB initialization script
db = db.getSiblingDB('ecommerce_dss');

// Create collections
db.createCollection('raw_products');
db.createCollection('processed_products');
db.createCollection('customer_interactions');
db.createCollection('ml_predictions');

// Create indexes
db.raw_products.createIndex({ "product_id": 1 }, { unique: true });
db.raw_products.createIndex({ "category": 1 });
db.raw_products.createIndex({ "scraped_date": 1 });

db.processed_products.createIndex({ "product_id": 1 }, { unique: true });
db.customer_interactions.createIndex({ "user_id": 1, "product_id": 1 });
db.ml_predictions.createIndex({ "model_type": 1, "created_at": 1 });

// Insert sample data
db.raw_products.insertOne({
    product_id: "SAMPLE_001",
    name: "Sample Product",
    category: "Electronics",
    price: 99.99,
    rating: 4.5,
    scraped_date: new Date()
});

print("MongoDB initialized successfully");