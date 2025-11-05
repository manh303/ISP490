"""
Data Schemas for E-commerce Processing
Định nghĩa schemas cho các loại dữ liệu khác nhau
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, ArrayType, MapType, BooleanType, LongType
)


class EcommerceSchemas:
    """Collection of schemas for e-commerce data."""

    @staticmethod
    def get_tiki_product_schema():
        """Schema cho Tiki products."""
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
            StructField("short_url", StringType(), True),
            StructField("price", StructType([
                StructField("current", DoubleType(), True),
                StructField("original", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("discount_rate", DoubleType(), True)
            ]), True),
            StructField("rating", StructType([
                StructField("average", DoubleType(), True),
                StructField("count", IntegerType(), True),
                StructField("distribution", MapType(StringType(), IntegerType()), True)
            ]), True),
            StructField("seller", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("rating", DoubleType(), True)
            ]), True),
            StructField("category", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("path", ArrayType(StringType()), True)
            ]), True),
            StructField("brand", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("slug", StringType(), True)
            ]), True),
            StructField("images", ArrayType(StructType([
                StructField("base_url", StringType(), True),
                StructField("is_gallery", BooleanType(), True),
                StructField("label", StringType(), True)
            ])), True),
            StructField("specifications", ArrayType(StructType([
                StructField("name", StringType(), True),
                StructField("attributes", ArrayType(StructType([
                    StructField("code", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("value", StringType(), True)
                ])), True)
            ])), True),
            StructField("quantity_sold", StructType([
                StructField("value", IntegerType(), True),
                StructField("text", StringType(), True)
            ]), True),
            StructField("description", StringType(), True),
            StructField("short_description", StringType(), True),
            StructField("stock_item", StructType([
                StructField("qty", IntegerType(), True),
                StructField("max_sale_qty", IntegerType(), True)
            ]), True),
            StructField("badges", ArrayType(StructType([
                StructField("code", StringType(), True),
                StructField("text", StringType(), True)
            ])), True),
            StructField("crawled_at", TimestampType(), True),
            StructField("source", StringType(), True)
        ])

    @staticmethod
    def get_lazada_product_schema():
        """Schema cho Lazada products."""
        return StructType([
            StructField("itemId", StringType(), True),
            StructField("name", StringType(), True),
            StructField("productUrl", StringType(), True),
            StructField("image", StringType(), True),
            StructField("images", ArrayType(StringType()), True),
            StructField("priceShow", StringType(), True),
            StructField("originalPrice", StringType(), True),
            StructField("discount", StringType(), True),
            StructField("location", StringType(), True),
            StructField("ratingScore", DoubleType(), True),
            StructField("review", IntegerType(), True),
            StructField("itemSoldCntShow", StringType(), True),
            StructField("shopId", StringType(), True),
            StructField("shopName", StringType(), True),
            StructField("isOfficialStore", BooleanType(), True),
            StructField("categoryName", StringType(), True),
            StructField("categoryId", StringType(), True),
            StructField("brandName", StringType(), True),
            StructField("labels", ArrayType(StructType([
                StructField("text", StringType(), True),
                StructField("type", StringType(), True)
            ])), True),
            StructField("promotions", ArrayType(StructType([
                StructField("promotionMsg", StringType(), True),
                StructField("startTime", StringType(), True),
                StructField("endTime", StringType(), True)
            ])), True),
            StructField("crawled_at", TimestampType(), True),
            StructField("source", StringType(), True)
        ])

    @staticmethod
    def get_fptshop_product_schema():
        """Schema cho FPTShop products."""
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
            StructField("image", StringType(), True),
            StructField("price", StructType([
                StructField("current", DoubleType(), True),
                StructField("original", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("formatted_current", StringType(), True),
                StructField("formatted_original", StringType(), True)
            ]), True),
            StructField("promotion", StructType([
                StructField("text", StringType(), True),
                StructField("percentage", DoubleType(), True)
            ]), True),
            StructField("rating", StructType([
                StructField("score", DoubleType(), True),
                StructField("count", IntegerType(), True)
            ]), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("specifications", MapType(StringType(), StringType()), True),
            StructField("features", ArrayType(StringType()), True),
            StructField("availability", StructType([
                StructField("in_stock", BooleanType(), True),
                StructField("quantity", IntegerType(), True)
            ]), True),
            StructField("crawled_at", TimestampType(), True),
            StructField("source", StringType(), True)
        ])

    @staticmethod
    def get_cellphones_product_schema():
        """Schema cho CellphoneS products."""
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
            StructField("image_url", StringType(), True),
            StructField("price", StructType([
                StructField("current", DoubleType(), True),
                StructField("original", DoubleType(), True),
                StructField("currency", StringType(), True)
            ]), True),
            StructField("discount", StructType([
                StructField("percentage", DoubleType(), True),
                StructField("amount", DoubleType(), True),
                StructField("label", StringType(), True)
            ]), True),
            StructField("rating", StructType([
                StructField("score", DoubleType(), True),
                StructField("count", IntegerType(), True)
            ]), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("model", StringType(), True),
            StructField("variants", ArrayType(StructType([
                StructField("name", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("available", BooleanType(), True)
            ])), True),
            StructField("promotions", ArrayType(StringType()), True),
            StructField("installment", StructType([
                StructField("available", BooleanType(), True),
                StructField("months", IntegerType(), True),
                StructField("monthly_payment", DoubleType(), True)
            ]), True),
            StructField("crawled_at", TimestampType(), True),
            StructField("source", StringType(), True)
        ])

    @staticmethod
    def get_unified_product_schema():
        """Unified schema cho tất cả platforms."""
        return StructType([
            # Core identification
            StructField("id", StringType(), True),
            StructField("external_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
            StructField("platform", StringType(), True),

            # Pricing
            StructField("current_price", DoubleType(), True),
            StructField("original_price", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("discount_percentage", DoubleType(), True),
            StructField("discount_amount", DoubleType(), True),

            # Rating and reviews
            StructField("rating_score", DoubleType(), True),
            StructField("rating_count", IntegerType(), True),

            # Product classification
            StructField("category", StringType(), True),
            StructField("category_path", ArrayType(StringType()), True),
            StructField("brand", StringType(), True),
            StructField("model", StringType(), True),

            # Sales information
            StructField("sold_count", IntegerType(), True),
            StructField("sold_text", StringType(), True),
            StructField("availability", BooleanType(), True),
            StructField("stock_quantity", IntegerType(), True),

            # Seller information
            StructField("seller_id", StringType(), True),
            StructField("seller_name", StringType(), True),
            StructField("seller_rating", DoubleType(), True),
            StructField("is_official_store", BooleanType(), True),

            # Content
            StructField("description", StringType(), True),
            StructField("short_description", StringType(), True),
            StructField("images", ArrayType(StringType()), True),
            StructField("main_image", StringType(), True),

            # Features and specifications
            StructField("specifications", MapType(StringType(), StringType()), True),
            StructField("features", ArrayType(StringType()), True),
            StructField("tags", ArrayType(StringType()), True),

            # Promotions and offers
            StructField("promotions", ArrayType(StringType()), True),
            StructField("badges", ArrayType(StringType()), True),

            # Metadata
            StructField("crawled_at", TimestampType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("source", StringType(), True),
            StructField("data_version", StringType(), True)
        ])

    @staticmethod
    def get_category_schema():
        """Schema cho category data."""
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("slug", StringType(), True),
            StructField("parent_id", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("path", ArrayType(StringType()), True),
            StructField("product_count", IntegerType(), True),
            StructField("platform", StringType(), True),
            StructField("url", StringType(), True),
            StructField("image", StringType(), True),
            StructField("description", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])

    @staticmethod
    def get_review_schema():
        """Schema cho review data."""
        return StructType([
            StructField("id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("rating", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("content", StringType(), True),
            StructField("pros", ArrayType(StringType()), True),
            StructField("cons", ArrayType(StringType()), True),
            StructField("verified_purchase", BooleanType(), True),
            StructField("helpful_count", IntegerType(), True),
            StructField("reply", StructType([
                StructField("content", StringType(), True),
                StructField("author", StringType(), True),
                StructField("timestamp", TimestampType(), True)
            ]), True),
            StructField("images", ArrayType(StringType()), True),
            StructField("platform", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])

    @staticmethod
    def get_analytics_schema():
        """Schema cho analytics results."""
        return StructType([
            StructField("metric_name", StringType(), True),
            StructField("metric_value", DoubleType(), True),
            StructField("metric_type", StringType(), True),
            StructField("dimension", StringType(), True),
            StructField("dimension_value", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("category", StringType(), True),
            StructField("calculation_date", TimestampType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])

    @staticmethod
    def get_ml_features_schema():
        """Schema cho ML features."""
        return StructType([
            StructField("product_id", StringType(), True),
            StructField("platform", StringType(), True),

            # Basic features
            StructField("price_current", DoubleType(), True),
            StructField("price_original", DoubleType(), True),
            StructField("discount_percentage", DoubleType(), True),
            StructField("rating_score", DoubleType(), True),
            StructField("rating_count", IntegerType(), True),
            StructField("sold_count", IntegerType(), True),

            # Text features
            StructField("name_length", IntegerType(), True),
            StructField("name_word_count", IntegerType(), True),
            StructField("description_length", IntegerType(), True),

            # Category features
            StructField("category_avg_price", DoubleType(), True),
            StructField("category_product_count", IntegerType(), True),
            StructField("price_vs_category_avg", DoubleType(), True),

            # Brand features
            StructField("brand_avg_price", DoubleType(), True),
            StructField("brand_avg_rating", DoubleType(), True),
            StructField("brand_product_count", IntegerType(), True),

            # Platform features
            StructField("platform_avg_price", DoubleType(), True),
            StructField("platform_product_count", IntegerType(), True),

            # Derived features
            StructField("popularity_score", DoubleType(), True),
            StructField("value_score", DoubleType(), True),
            StructField("price_volatility", DoubleType(), True),

            # Time features
            StructField("crawl_hour", IntegerType(), True),
            StructField("crawl_day_of_week", IntegerType(), True),
            StructField("crawl_month", IntegerType(), True),

            # Target variables (for ML)
            StructField("price_category", StringType(), True),
            StructField("rating_category", StringType(), True),
            StructField("popularity_category", StringType(), True),

            StructField("created_at", TimestampType(), True)
        ])


def get_schema_for_platform(platform: str) -> StructType:
    """Get schema for specific platform."""
    schema_map = {
        "tiki": EcommerceSchemas.get_tiki_product_schema(),
        "lazada": EcommerceSchemas.get_lazada_product_schema(),
        "fptshop": EcommerceSchemas.get_fptshop_product_schema(),
        "cellphones": EcommerceSchemas.get_cellphones_product_schema(),
        "unified": EcommerceSchemas.get_unified_product_schema()
    }

    return schema_map.get(platform.lower(), EcommerceSchemas.get_unified_product_schema())