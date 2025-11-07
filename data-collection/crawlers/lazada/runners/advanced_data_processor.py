#!/usr/bin/env python3
"""
Advanced Data Processor for Enhanced Lazada Crawler
Xử lý và phân tích dữ liệu crawled với nhiều tính năng mạnh mẽ
"""

import json
import csv
import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
import statistics

class AdvancedDataProcessor:
    def __init__(self, data_file: str = None):
        """
        Initialize data processor

        Args:
            data_file: Path to JSON file with crawled data
        """
        self.data = []
        self.processed_data = {}
        self.analytics = {}

        if data_file:
            self.load_data(data_file)

    def load_data(self, data_file: str):
        """Load data from JSON file"""
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            print(f"✅ Loaded {len(self.data)} products from {data_file}")
        except Exception as e:
            print(f"❌ Error loading data: {e}")

    def clean_and_standardize_data(self) -> Dict[str, Any]:
        """
        Làm sạch và chuẩn hóa dữ liệu
        """
        print("🧹 Cleaning and standardizing data...")

        cleaned_data = []
        stats = {
            'total_products': len(self.data),
            'valid_products': 0,
            'missing_price': 0,
            'missing_shop': 0,
            'missing_reviews': 0,
            'invalid_data': 0
        }

        for product in self.data:
            try:
                cleaned_product = self._clean_single_product(product)
                if cleaned_product:
                    cleaned_data.append(cleaned_product)
                    stats['valid_products'] += 1
                else:
                    stats['invalid_data'] += 1

            except Exception as e:
                print(f"❌ Error cleaning product: {e}")
                stats['invalid_data'] += 1

        # Count missing data
        for product in cleaned_data:
            if not product.get('price'):
                stats['missing_price'] += 1
            if not product.get('shop_name'):
                stats['missing_shop'] += 1
            if not product.get('reviews'):
                stats['missing_reviews'] += 1

        self.processed_data['cleaned'] = cleaned_data
        self.processed_data['cleaning_stats'] = stats

        print(f"✅ Cleaned data: {stats['valid_products']}/{stats['total_products']} valid products")
        return cleaned_data

    def _clean_single_product(self, product: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clean individual product data"""

        # Required fields
        if not product.get('title') or not product.get('url'):
            return None

        cleaned = {
            # Basic info
            'product_id': self._generate_product_id(product),
            'title': self._clean_text(product.get('title', '')),
            'category': product.get('category', 'unknown'),
            'url': product.get('url', ''),
            'image_url': product.get('image_url', ''),

            # Price info
            'price': self._standardize_price(product.get('price')),
            'price_text': product.get('price_text', ''),
            'original_price': self._standardize_price(product.get('original_price')),
            'discount_text': product.get('discount', ''),
            'discount_percentage': self._extract_discount_percentage(product.get('discount', '')),

            # Ratings & Reviews
            'rating': self._standardize_rating(product.get('rating')),
            'review_count': self._standardize_count(product.get('review_count', '')),
            'sold_count': self._standardize_count(product.get('sold_count', '')),

            # Location & Shop
            'location': self._standardize_location(product.get('location', '')),
            'shop_name': self._clean_text(product.get('shop_name', '')),
            'shop_rating': self._extract_shop_rating(product.get('shop_rating', '')),
            'shop_badges': product.get('shop_badges', []),

            # Reviews
            'reviews': self._clean_reviews(product.get('reviews', [])),
            'review_summary': self._calculate_review_summary(product.get('reviews', [])),

            # Metadata
            'crawled_at': datetime.now().isoformat(),
            'data_quality_score': 0  # Will be calculated
        }

        # Calculate data quality score
        cleaned['data_quality_score'] = self._calculate_quality_score(cleaned)

        return cleaned

    def _generate_product_id(self, product: Dict[str, Any]) -> str:
        """Generate unique product ID"""
        import hashlib

        # Use title + url for unique ID
        content = f"{product.get('title', '')}_{product.get('url', '')}"
        hash_value = hashlib.md5(content.encode('utf-8')).hexdigest()[:12]
        return f"LAZADA_{hash_value.upper()}"

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""

        # Remove extra whitespace and normalize
        cleaned = re.sub(r'\s+', ' ', str(text).strip())
        # Remove emojis (optional)
        cleaned = re.sub(r'[^\w\s\-.,()%ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠàáâãèéêìíòóôõùúăđĩũơƯĂẠẢẤẦẨẪẬẮẰẲẴẶẸẺẽỀềểưăạảấầẩẫậắằẳẵặẹẻẽềềểếỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪễệỉịọỏốồổỗộớờởỡợụủứừỬỮỰỲỴÝỶỸửữựỳỵýỷỹ]', ' ', cleaned)
        return cleaned.strip()

    def _standardize_price(self, price) -> Optional[int]:
        """Standardize price to VND integer"""
        if not price:
            return None

        if isinstance(price, (int, float)):
            return int(price)

        # Extract numbers from text
        price_str = re.sub(r'[^\d]', '', str(price))
        if price_str:
            try:
                return int(price_str)
            except ValueError:
                return None
        return None

    def _standardize_rating(self, rating) -> Optional[float]:
        """Standardize rating to float"""
        if not rating:
            return None

        try:
            rating_val = float(rating)
            return min(max(rating_val, 0), 5)  # Clamp between 0-5
        except (ValueError, TypeError):
            return None

    def _standardize_count(self, count_str: str) -> int:
        """Standardize count strings to integers"""
        if not count_str:
            return 0

        count_str = str(count_str).lower().strip()

        # Handle 'k' suffix
        if count_str.endswith('k'):
            try:
                return int(float(count_str[:-1]) * 1000)
            except ValueError:
                return 0

        # Extract numbers
        numbers = re.findall(r'\d+', count_str)
        if numbers:
            try:
                return int(numbers[0])
            except ValueError:
                return 0

        return 0

    def _standardize_location(self, location: str) -> str:
        """Standardize location names"""
        if not location:
            return "Không xác định"

        location = str(location).strip()

        # Common location mappings
        location_map = {
            'hà nội': 'Hà Nội',
            'ha noi': 'Hà Nội',
            'hanoi': 'Hà Nội',
            'ho chi minh': 'Hồ Chí Minh',
            'hồ chí minh': 'Hồ Chí Minh',
            'hcm': 'Hồ Chí Minh',
            'saigon': 'Hồ Chí Minh',
            'tp.hcm': 'Hồ Chí Minh',
            'hai phong': 'Hải Phòng',
            'hải phòng': 'Hải Phòng',
            'da nang': 'Đà Nẵng',
            'đà nẵng': 'Đà Nẵng',
            'can tho': 'Cần Thơ',
            'cần thơ': 'Cần Thơ'
        }

        location_lower = location.lower()
        for key, value in location_map.items():
            if key in location_lower:
                return value

        return location.title()

    def _extract_discount_percentage(self, discount_text: str) -> Optional[int]:
        """Extract discount percentage from text"""
        if not discount_text:
            return None

        # Find percentage pattern
        match = re.search(r'(\d+)%', discount_text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None

        return None

    def _extract_shop_rating(self, shop_rating: str) -> Optional[int]:
        """Extract shop rating percentage"""
        if not shop_rating:
            return None

        match = re.search(r'(\d+)%', shop_rating)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None

        return None

    def _clean_reviews(self, reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean reviews data"""
        cleaned_reviews = []

        for review in reviews:
            if not isinstance(review, dict):
                continue

            cleaned_review = {
                'reviewer': self._clean_text(review.get('reviewer', '')),
                'rating': self._standardize_rating(review.get('rating')),
                'time': review.get('time', ''),
                'content': self._clean_text(review.get('content', '')),
                'sku_info': self._clean_text(review.get('sku_info', '')),
                'helpful_count': self._standardize_count(str(review.get('helpful_count', 0))),
                'review_length': len(review.get('content', '')),
                'has_sku_info': bool(review.get('sku_info'))
            }

            # Only keep reviews with content or rating
            if cleaned_review['content'] or cleaned_review['rating']:
                cleaned_reviews.append(cleaned_review)

        return cleaned_reviews

    def _calculate_review_summary(self, reviews: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate review summary statistics"""
        if not reviews:
            return {
                'total_reviews': 0,
                'average_rating': 0.0,
                'rating_distribution': {},
                'sentiment_score': 0.0
            }

        ratings = [r.get('rating', 0) for r in reviews if r.get('rating')]

        # Rating distribution
        rating_dist = Counter(ratings)

        return {
            'total_reviews': len(reviews),
            'average_rating': np.mean(ratings) if ratings else 0.0,
            'rating_distribution': dict(rating_dist),
            'sentiment_score': self._calculate_sentiment_score(reviews)
        }

    def _calculate_sentiment_score(self, reviews: List[Dict[str, Any]]) -> float:
        """Simple sentiment analysis based on keywords"""
        positive_words = ['tốt', 'good', 'great', 'excellent', 'amazing', 'perfect', 'love', 'recommend', 'best']
        negative_words = ['bad', 'terrible', 'awful', 'worst', 'hate', 'poor', 'disappointing', 'dở', 'tệ']

        total_score = 0
        total_reviews = 0

        for review in reviews:
            content = review.get('content', '').lower()
            if not content:
                continue

            positive_count = sum(1 for word in positive_words if word in content)
            negative_count = sum(1 for word in negative_words if word in content)

            if positive_count > 0 or negative_count > 0:
                score = (positive_count - negative_count) / (positive_count + negative_count)
                total_score += score
                total_reviews += 1

        return total_score / total_reviews if total_reviews > 0 else 0.0

    def _calculate_quality_score(self, product: Dict[str, Any]) -> int:
        """Calculate data quality score (0-100)"""
        score = 0

        # Basic info (30 points)
        if product.get('title'): score += 10
        if product.get('price'): score += 10
        if product.get('image_url'): score += 10

        # Shop info (20 points)
        if product.get('shop_name'): score += 10
        if product.get('shop_rating'): score += 10

        # Reviews (30 points)
        reviews = product.get('reviews', [])
        if reviews:
            score += 15
            if len(reviews) >= 5: score += 10
            if product.get('review_summary', {}).get('average_rating', 0) > 0: score += 5

        # Additional info (20 points)
        if product.get('rating'): score += 5
        if product.get('location'): score += 5
        if product.get('review_count', 0) > 0: score += 5
        if product.get('discount_percentage'): score += 5

        return min(score, 100)

    def analyze_data(self) -> Dict[str, Any]:
        """
        Phân tích dữ liệu toàn diện
        """
        print("📊 Analyzing data...")

        if not self.processed_data.get('cleaned'):
            self.clean_and_standardize_data()

        data = self.processed_data['cleaned']
        df = pd.DataFrame(data)

        analytics = {
            'overview': self._analyze_overview(df),
            'price_analysis': self._analyze_prices(df),
            'category_analysis': self._analyze_categories(df),
            'shop_analysis': self._analyze_shops(df),
            'location_analysis': self._analyze_locations(df),
            'review_analysis': self._analyze_reviews(df),
            'quality_analysis': self._analyze_quality(df)
        }

        self.analytics = analytics
        return analytics

    def _analyze_overview(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Overall data analysis"""
        return {
            'total_products': len(df),
            'unique_shops': df['shop_name'].nunique(),
            'unique_categories': df['category'].nunique(),
            'unique_locations': df['location'].nunique(),
            'products_with_reviews': len(df[df['review_summary'].apply(lambda x: x.get('total_reviews', 0) > 0)]),
            'products_with_rating': len(df[df['rating'].notna()]),
            'products_with_price': len(df[df['price'].notna()]),
            'average_quality_score': df['data_quality_score'].mean()
        }

    def _analyze_prices(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Price analysis"""
        prices = df[df['price'].notna()]['price']

        if len(prices) == 0:
            return {'error': 'No price data available'}

        return {
            'count': len(prices),
            'mean': int(prices.mean()),
            'median': int(prices.median()),
            'min': int(prices.min()),
            'max': int(prices.max()),
            'std': int(prices.std()),
            'quartiles': {
                'q1': int(prices.quantile(0.25)),
                'q2': int(prices.quantile(0.5)),
                'q3': int(prices.quantile(0.75))
            },
            'price_ranges': {
                'under_1m': len(prices[prices < 1000000]),
                '1m_5m': len(prices[(prices >= 1000000) & (prices < 5000000)]),
                '5m_10m': len(prices[(prices >= 5000000) & (prices < 10000000)]),
                '10m_20m': len(prices[(prices >= 10000000) & (prices < 20000000)]),
                'over_20m': len(prices[prices >= 20000000])
            }
        }

    def _analyze_categories(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Category analysis"""
        category_stats = df.groupby('category').agg({
            'price': ['count', 'mean', 'median'],
            'rating': 'mean',
            'review_count': 'mean',
            'data_quality_score': 'mean'
        }).round(2)

        return {
            'category_distribution': df['category'].value_counts().to_dict(),
            'category_stats': category_stats.to_dict(),
            'most_expensive_category': df.groupby('category')['price'].mean().idxmax() if len(df[df['price'].notna()]) > 0 else None,
            'highest_rated_category': df.groupby('category')['rating'].mean().idxmax() if len(df[df['rating'].notna()]) > 0 else None
        }

    def _analyze_shops(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Shop analysis"""
        shop_data = df[df['shop_name'] != ''].copy()

        if len(shop_data) == 0:
            return {'error': 'No shop data available'}

        shop_stats = shop_data.groupby('shop_name').agg({
            'price': ['count', 'mean'],
            'rating': 'mean',
            'shop_rating': 'first',
            'data_quality_score': 'mean'
        }).round(2)

        # Top shops by various metrics
        top_shops = {
            'most_products': shop_stats['price']['count'].nlargest(10).to_dict(),
            'highest_shop_rating': shop_data.groupby('shop_name')['shop_rating'].first().nlargest(10).to_dict(),
            'highest_product_rating': shop_stats['rating']['mean'].nlargest(10).to_dict()
        }

        return {
            'total_shops': len(shop_stats),
            'top_shops': top_shops,
            'shop_rating_distribution': shop_data['shop_rating'].value_counts().to_dict(),
            'average_products_per_shop': shop_stats['price']['count'].mean()
        }

    def _analyze_locations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Location analysis"""
        location_stats = df.groupby('location').agg({
            'price': ['count', 'mean'],
            'rating': 'mean',
            'shop_name': 'nunique'
        }).round(2)

        return {
            'location_distribution': df['location'].value_counts().to_dict(),
            'location_stats': location_stats.to_dict(),
            'most_expensive_location': df.groupby('location')['price'].mean().idxmax() if len(df[df['price'].notna()]) > 0 else None
        }

    def _analyze_reviews(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Review analysis"""
        # Extract all reviews
        all_reviews = []
        for _, row in df.iterrows():
            reviews = row.get('reviews', [])
            for review in reviews:
                review_data = dict(review)
                review_data['product_category'] = row['category']
                review_data['product_price'] = row['price']
                all_reviews.append(review_data)

        if not all_reviews:
            return {'error': 'No reviews available'}

        reviews_df = pd.DataFrame(all_reviews)

        return {
            'total_reviews': len(all_reviews),
            'average_rating': reviews_df['rating'].mean() if 'rating' in reviews_df.columns else 0,
            'rating_distribution': reviews_df['rating'].value_counts().to_dict() if 'rating' in reviews_df.columns else {},
            'reviews_by_category': reviews_df['product_category'].value_counts().to_dict() if 'product_category' in reviews_df.columns else {},
            'average_review_length': reviews_df['review_length'].mean() if 'review_length' in reviews_df.columns else 0,
            'reviews_with_sku_info': len(reviews_df[reviews_df.get('has_sku_info', False)]) if 'has_sku_info' in reviews_df.columns else 0
        }

    def _analyze_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Data quality analysis"""
        return {
            'quality_score_distribution': {
                'excellent_90_100': len(df[df['data_quality_score'] >= 90]),
                'good_70_89': len(df[(df['data_quality_score'] >= 70) & (df['data_quality_score'] < 90)]),
                'fair_50_69': len(df[(df['data_quality_score'] >= 50) & (df['data_quality_score'] < 70)]),
                'poor_below_50': len(df[df['data_quality_score'] < 50])
            },
            'average_quality_score': df['data_quality_score'].mean(),
            'missing_data_analysis': {
                'missing_price': len(df[df['price'].isna()]),
                'missing_rating': len(df[df['rating'].isna()]),
                'missing_shop_name': len(df[df['shop_name'] == '']),
                'missing_reviews': len(df[df['review_summary'].apply(lambda x: x.get('total_reviews', 0) == 0)])
            }
        }

    def generate_insights(self) -> Dict[str, Any]:
        """
        Tạo insights và recommendations
        """
        print("💡 Generating insights...")

        if not self.analytics:
            self.analyze_data()

        insights = {
            'market_insights': self._generate_market_insights(),
            'price_insights': self._generate_price_insights(),
            'quality_insights': self._generate_quality_insights(),
            'shop_insights': self._generate_shop_insights(),
            'recommendations': self._generate_recommendations()
        }

        return insights

    def _generate_market_insights(self) -> List[str]:
        """Generate market insights"""
        insights = []

        overview = self.analytics.get('overview', {})
        category_analysis = self.analytics.get('category_analysis', {})

        # Market size insight
        total_products = overview.get('total_products', 0)
        insights.append(f"📊 Market có {total_products:,} sản phẩm từ {overview.get('unique_shops', 0)} shops khác nhau")

        # Category insights
        if 'category_distribution' in category_analysis:
            top_category = max(category_analysis['category_distribution'].items(), key=lambda x: x[1])
            insights.append(f"📱 Category phổ biến nhất: {top_category[0]} với {top_category[1]} sản phẩm")

        # Coverage insights
        review_coverage = (overview.get('products_with_reviews', 0) / total_products) * 100 if total_products > 0 else 0
        insights.append(f"💬 {review_coverage:.1f}% sản phẩm có reviews")

        return insights

    def _generate_price_insights(self) -> List[str]:
        """Generate price insights"""
        insights = []

        price_analysis = self.analytics.get('price_analysis', {})

        if 'mean' in price_analysis:
            avg_price = price_analysis['mean']
            median_price = price_analysis['median']
            insights.append(f"💰 Giá trung bình: {avg_price:,} VND, Giá median: {median_price:,} VND")

        if 'price_ranges' in price_analysis:
            ranges = price_analysis['price_ranges']
            most_common_range = max(ranges.items(), key=lambda x: x[1])
            insights.append(f"💸 Phân khúc giá phổ biến nhất: {most_common_range[0]} với {most_common_range[1]} sản phẩm")

        return insights

    def _generate_quality_insights(self) -> List[str]:
        """Generate data quality insights"""
        insights = []

        quality_analysis = self.analytics.get('quality_analysis', {})

        if 'average_quality_score' in quality_analysis:
            avg_quality = quality_analysis['average_quality_score']
            insights.append(f"📈 Điểm chất lượng dữ liệu trung bình: {avg_quality:.1f}/100")

        if 'quality_score_distribution' in quality_analysis:
            dist = quality_analysis['quality_score_distribution']
            excellent = dist.get('excellent_90_100', 0)
            total = sum(dist.values())
            excellent_pct = (excellent / total) * 100 if total > 0 else 0
            insights.append(f"⭐ {excellent_pct:.1f}% sản phẩm có chất lượng dữ liệu excellent (90-100 điểm)")

        return insights

    def _generate_shop_insights(self) -> List[str]:
        """Generate shop insights"""
        insights = []

        shop_analysis = self.analytics.get('shop_analysis', {})

        if 'total_shops' in shop_analysis:
            total_shops = shop_analysis['total_shops']
            avg_products = shop_analysis.get('average_products_per_shop', 0)
            insights.append(f"🏪 {total_shops} shops với trung bình {avg_products:.1f} sản phẩm/shop")

        if 'top_shops' in shop_analysis:
            top_by_products = shop_analysis['top_shops'].get('most_products', {})
            if top_by_products:
                top_shop = max(top_by_products.items(), key=lambda x: x[1])
                insights.append(f"🏆 Shop có nhiều sản phẩm nhất: {top_shop[0]} với {top_shop[1]} sản phẩm")

        return insights

    def _generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []

        quality_analysis = self.analytics.get('quality_analysis', {})

        # Data quality recommendations
        if 'missing_data_analysis' in quality_analysis:
            missing = quality_analysis['missing_data_analysis']

            if missing.get('missing_price', 0) > 0:
                recommendations.append(f"🔧 Cần cải thiện crawler để lấy price cho {missing['missing_price']} sản phẩm")

            if missing.get('missing_shop_name', 0) > 0:
                recommendations.append(f"🔧 Cần cải thiện logic lấy shop_name cho {missing['missing_shop_name']} sản phẩm")

        # Coverage recommendations
        overview = self.analytics.get('overview', {})
        review_coverage = (overview.get('products_with_reviews', 0) / overview.get('total_products', 1)) * 100

        if review_coverage < 50:
            recommendations.append("📈 Nên tăng cường crawl reviews để có insights tốt hơn về sentiment")

        return recommendations

    def export_analysis_report(self, output_dir: str = "analysis_output"):
        """
        Export comprehensive analysis report
        """
        print("📄 Exporting analysis report...")

        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 1. Export analytics JSON
        analytics_file = output_path / f"analytics_report_{timestamp}.json"
        with open(analytics_file, 'w', encoding='utf-8') as f:
            json.dump(self.analytics, f, ensure_ascii=False, indent=2)

        # 2. Export insights
        insights = self.generate_insights()
        insights_file = output_path / f"insights_report_{timestamp}.json"
        with open(insights_file, 'w', encoding='utf-8') as f:
            json.dump(insights, f, ensure_ascii=False, indent=2)

        # 3. Export cleaned data
        if self.processed_data.get('cleaned'):
            cleaned_file = output_path / f"cleaned_data_{timestamp}.json"
            with open(cleaned_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_data['cleaned'], f, ensure_ascii=False, indent=2)

        # 4. Export summary CSV
        self._export_summary_csv(output_path, timestamp)

        print(f"✅ Analysis reports exported to {output_path}")
        print(f"📊 Files created:")
        print(f"  - {analytics_file.name}")
        print(f"  - {insights_file.name}")
        if self.processed_data.get('cleaned'):
            print(f"  - cleaned_data_{timestamp}.json")
        print(f"  - summary_report_{timestamp}.csv")

    def _export_summary_csv(self, output_path: Path, timestamp: str):
        """Export summary CSV report"""
        if not self.processed_data.get('cleaned'):
            return

        df = pd.DataFrame(self.processed_data['cleaned'])

        # Select key columns for summary
        summary_columns = [
            'product_id', 'title', 'category', 'price', 'price_text',
            'rating', 'review_count', 'sold_count', 'location', 'shop_name',
            'shop_rating', 'discount_percentage', 'data_quality_score'
        ]

        # Filter columns that exist
        available_columns = [col for col in summary_columns if col in df.columns]
        summary_df = df[available_columns]

        summary_file = output_path / f"summary_report_{timestamp}.csv"
        summary_df.to_csv(summary_file, index=False, encoding='utf-8')

def main():
    """Main function to demonstrate usage"""

    # Example usage
    print("🚀 Advanced Data Processor for Lazada Crawler")
    print("=" * 60)

    # Initialize processor
    processor = AdvancedDataProcessor()

    # Look for latest data file
    data_files = list(Path("../../outputs").glob("enhanced_lazada_*.json"))
    if not data_files:
        print("❌ No data files found in ../../outputs/")
        print("💡 Run enhanced_lazada_crawler.py first to generate data")
        return

    # Use the latest file
    latest_file = max(data_files, key=lambda x: x.stat().st_mtime)
    print(f"📂 Loading data from: {latest_file}")

    # Load and process data
    processor.load_data(str(latest_file))

    # Clean and analyze
    cleaned_data = processor.clean_and_standardize_data()
    analytics = processor.analyze_data()
    insights = processor.generate_insights()

    # Print key insights
    print("\n💡 KEY INSIGHTS:")
    print("-" * 40)

    for category, insight_list in insights.items():
        if isinstance(insight_list, list) and insight_list:
            print(f"\n{category.upper()}:")
            for insight in insight_list[:3]:  # Show top 3
                print(f"  {insight}")

    # Export reports
    processor.export_analysis_report()

    print("\n✅ Analysis complete!")

if __name__ == "__main__":
    main()