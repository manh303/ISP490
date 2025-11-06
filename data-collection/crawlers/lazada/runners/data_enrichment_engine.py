#!/usr/bin/env python3
"""
Data Enrichment Engine for Lazada Products
Làm giàu dữ liệu với các tính năng AI/ML và external data sources
"""

import json
import re
import requests
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import numpy as np
import pandas as pd
from collections import defaultdict, Counter
import threading
import queue

class DataEnrichmentEngine:
    def __init__(self):
        """Initialize data enrichment engine"""
        self.enriched_data = []
        self.enrichment_stats = {
            'products_processed': 0,
            'enrichments_added': 0,
            'errors': 0
        }

        # Product categorization rules
        self.detailed_categories = self._load_detailed_categories()
        self.brand_patterns = self._load_brand_patterns()
        self.spec_extractors = self._load_spec_extractors()

    def _load_detailed_categories(self) -> Dict[str, Dict[str, Any]]:
        """Load detailed category classification rules"""
        return {
            'smartphones': {
                'keywords': ['iphone', 'samsung', 'xiaomi', 'oppo', 'vivo', 'realme', 'phone', 'điện thoại'],
                'brands': ['apple', 'samsung', 'xiaomi', 'oppo', 'vivo', 'realme', 'huawei', 'nokia'],
                'subcategories': {
                    'flagship': ['pro max', 'ultra', 'note', 'plus'],
                    'mid_range': ['lite', 'a series', 'redmi'],
                    'budget': ['entry', 'go', 'basic']
                }
            },
            'laptops': {
                'keywords': ['laptop', 'notebook', 'macbook', 'thinkpad', 'gaming'],
                'brands': ['apple', 'dell', 'hp', 'lenovo', 'asus', 'acer', 'msi'],
                'subcategories': {
                    'gaming': ['gaming', 'rog', 'predator', 'legion'],
                    'business': ['thinkpad', 'probook', 'latitude'],
                    'ultrabook': ['macbook', 'zenbook', 'spectre', 'xps']
                }
            },
            'tablets': {
                'keywords': ['tablet', 'ipad', 'máy tính bảng'],
                'brands': ['apple', 'samsung', 'xiaomi', 'huawei'],
                'subcategories': {
                    'premium': ['pro', 'air'],
                    'standard': ['basic', 'wifi'],
                    'mini': ['mini', 'compact']
                }
            },
            'accessories': {
                'keywords': ['case', 'charger', 'cable', 'adapter', 'stand'],
                'brands': ['anker', 'belkin', 'ugreen'],
                'subcategories': {
                    'protection': ['case', 'screen protector', 'ốp lưng'],
                    'charging': ['charger', 'cable', 'wireless'],
                    'audio': ['headphone', 'earphone', 'speaker']
                }
            }
        }

    def _load_brand_patterns(self) -> Dict[str, List[str]]:
        """Load brand detection patterns"""
        return {
            'apple': ['iphone', 'ipad', 'macbook', 'apple'],
            'samsung': ['samsung', 'galaxy'],
            'xiaomi': ['xiaomi', 'redmi', 'poco', 'mi'],
            'oppo': ['oppo', 'find', 'reno'],
            'vivo': ['vivo', 'y series', 'v series'],
            'realme': ['realme'],
            'huawei': ['huawei', 'honor', 'mate'],
            'dell': ['dell', 'alienware', 'inspiron', 'xps'],
            'hp': ['hp', 'pavilion', 'envy', 'omen'],
            'asus': ['asus', 'rog', 'zenbook', 'vivobook'],
            'lenovo': ['lenovo', 'thinkpad', 'ideapad', 'legion'],
            'acer': ['acer', 'predator', 'aspire', 'swift'],
            'msi': ['msi', 'gaming']
        }

    def _load_spec_extractors(self) -> Dict[str, Dict[str, str]]:
        """Load specification extraction patterns"""
        return {
            'memory': {
                'ram': r'(\d+)\s*GB\s*RAM|RAM\s*(\d+)\s*GB|(\d+)\s*GB\s*/',
                'storage': r'(\d+)\s*GB(?!\s*RAM)|(\d+)\s*TB|(\d+)\s*GB\s*SSD|(\d+)\s*GB\s*HDD'
            },
            'display': {
                'screen_size': r'(\d+(?:\.\d+)?)\s*inch|(\d+(?:\.\d+)?)"',
                'resolution': r'(\d+)\s*x\s*(\d+)|(\d+)K|4K|HD|FHD'
            },
            'camera': {
                'main_camera': r'(\d+)\s*MP|(\d+)\s*megapixel',
                'camera_count': r'(\d+)\s*camera|(\d+)\s*lens'
            },
            'battery': {
                'capacity': r'(\d+)\s*mAh|(\d+)\s*mah',
                'charging': r'(\d+)W|fast\s*charging|wireless\s*charging'
            },
            'processor': {
                'cpu': r'(snapdragon|mediatek|exynos|intel|amd|m1|m2)',
                'cores': r'(\d+)\s*core|octa|quad|hexa'
            }
        }

    def enrich_product_data(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Main function to enrich product data
        """
        print("🔧 Starting data enrichment process...")

        enriched_products = []
        total_products = len(products)

        for i, product in enumerate(products):
            try:
                print(f"🔧 Enriching product {i+1}/{total_products}: {product.get('title', 'Unknown')[:50]}...")

                enriched_product = self._enrich_single_product(product)
                enriched_products.append(enriched_product)

                self.enrichment_stats['products_processed'] += 1

                # Progress update
                if (i + 1) % 10 == 0:
                    print(f"✅ Processed {i+1}/{total_products} products")

            except Exception as e:
                print(f"❌ Error enriching product {i+1}: {e}")
                enriched_products.append(product)  # Keep original if enrichment fails
                self.enrichment_stats['errors'] += 1

        self.enriched_data = enriched_products
        print(f"✅ Enrichment complete: {len(enriched_products)} products processed")

        return enriched_products

    def _enrich_single_product(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single product with additional data"""

        enriched = dict(product)  # Copy original product

        # 1. Enhanced categorization
        enriched.update(self._enhance_categorization(product))

        # 2. Brand detection
        enriched.update(self._detect_brand(product))

        # 3. Specification extraction
        enriched.update(self._extract_specifications(product))

        # 4. Price analysis
        enriched.update(self._analyze_price_value(product))

        # 5. Review sentiment analysis
        enriched.update(self._analyze_review_sentiment(product))

        # 6. Competitive analysis
        enriched.update(self._perform_competitive_analysis(product))

        # 7. Market positioning
        enriched.update(self._analyze_market_position(product))

        # 8. Quality indicators
        enriched.update(self._calculate_quality_indicators(product))

        return enriched

    def _enhance_categorization(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced product categorization"""
        title = product.get('title', '').lower()
        category = product.get('category', '').lower()

        enhanced_cat = {
            'detailed_category': category,
            'subcategory': 'unknown',
            'product_type': 'unknown',
            'category_confidence': 0.0
        }

        best_match_score = 0
        best_category = None

        # Match against detailed categories
        for cat_name, cat_info in self.detailed_categories.items():
            score = 0

            # Check keywords
            for keyword in cat_info['keywords']:
                if keyword in title:
                    score += 1

            # Check brands
            for brand in cat_info['brands']:
                if brand in title:
                    score += 0.5

            if score > best_match_score:
                best_match_score = score
                best_category = cat_name

        if best_category:
            enhanced_cat['detailed_category'] = best_category
            enhanced_cat['category_confidence'] = min(best_match_score / 3, 1.0)

            # Find subcategory
            cat_info = self.detailed_categories[best_category]
            for subcat_name, subcat_keywords in cat_info['subcategories'].items():
                for keyword in subcat_keywords:
                    if keyword in title:
                        enhanced_cat['subcategory'] = subcat_name
                        break

        self.enrichment_stats['enrichments_added'] += 1
        return enhanced_cat

    def _detect_brand(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Detect product brand"""
        title = product.get('title', '').lower()

        brand_info = {
            'brand': 'unknown',
            'brand_confidence': 0.0,
            'brand_tier': 'unknown'
        }

        # Brand tier mapping
        brand_tiers = {
            'premium': ['apple', 'samsung'],
            'mainstream': ['xiaomi', 'oppo', 'vivo', 'dell', 'hp'],
            'budget': ['realme', 'poco'],
            'luxury': ['alienware', 'rog']
        }

        best_brand = None
        best_score = 0

        for brand, patterns in self.brand_patterns.items():
            score = 0
            for pattern in patterns:
                if pattern in title:
                    score += len(pattern) / len(title)  # Weighted by pattern length

            if score > best_score:
                best_score = score
                best_brand = brand

        if best_brand:
            brand_info['brand'] = best_brand
            brand_info['brand_confidence'] = min(best_score * 10, 1.0)

            # Determine brand tier
            for tier, brands in brand_tiers.items():
                if best_brand in brands:
                    brand_info['brand_tier'] = tier
                    break

        self.enrichment_stats['enrichments_added'] += 1
        return brand_info

    def _extract_specifications(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Extract technical specifications from title and description"""
        title = product.get('title', '')

        specs = {
            'specifications': {},
            'spec_completeness': 0.0
        }

        total_spec_categories = len(self.spec_extractors)
        found_specs = 0

        for spec_category, patterns in self.spec_extractors.items():
            category_specs = {}

            for spec_name, pattern in patterns.items():
                matches = re.findall(pattern, title, re.IGNORECASE)
                if matches:
                    # Extract first non-empty match
                    for match in matches:
                        if isinstance(match, tuple):
                            value = next((m for m in match if m), None)
                        else:
                            value = match

                        if value:
                            category_specs[spec_name] = value
                            break

            if category_specs:
                specs['specifications'][spec_category] = category_specs
                found_specs += 1

        specs['spec_completeness'] = found_specs / total_spec_categories if total_spec_categories > 0 else 0

        self.enrichment_stats['enrichments_added'] += 1
        return specs

    def _analyze_price_value(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze price and value proposition"""
        price = product.get('price')
        if not price:
            return {'price_analysis': {}}

        analysis = {
            'price_analysis': {
                'price_tier': 'unknown',
                'value_score': 0.0,
                'price_per_spec': {}
            }
        }

        # Determine price tier based on category
        category = product.get('detailed_category', product.get('category', ''))

        price_tiers = {
            'smartphones': {
                'budget': (0, 5000000),
                'mid_range': (5000000, 15000000),
                'premium': (15000000, 30000000),
                'flagship': (30000000, float('inf'))
            },
            'laptops': {
                'budget': (0, 15000000),
                'mid_range': (15000000, 30000000),
                'premium': (30000000, 50000000),
                'workstation': (50000000, float('inf'))
            },
            'tablets': {
                'budget': (0, 8000000),
                'mid_range': (8000000, 20000000),
                'premium': (20000000, float('inf'))
            }
        }

        if category in price_tiers:
            for tier, (min_price, max_price) in price_tiers[category].items():
                if min_price <= price < max_price:
                    analysis['price_analysis']['price_tier'] = tier
                    break

        # Calculate value score based on specs and reviews
        value_factors = []

        # Factor 1: Specifications completeness
        spec_completeness = product.get('spec_completeness', 0)
        value_factors.append(spec_completeness * 0.3)

        # Factor 2: Review rating
        rating = product.get('rating', 0)
        if rating:
            value_factors.append((rating / 5) * 0.4)

        # Factor 3: Review count (popularity indicator)
        review_count = product.get('review_count', 0)
        if isinstance(review_count, str):
            review_count = int(re.sub(r'\D', '', review_count)) if review_count.isdigit() else 0

        if review_count > 0:
            # Normalize review count (logarithmic scale)
            normalized_reviews = min(np.log10(review_count + 1) / 3, 1)
            value_factors.append(normalized_reviews * 0.3)

        if value_factors:
            analysis['price_analysis']['value_score'] = sum(value_factors)

        self.enrichment_stats['enrichments_added'] += 1
        return analysis

    def _analyze_review_sentiment(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced review sentiment analysis"""
        reviews = product.get('reviews', [])

        sentiment_analysis = {
            'sentiment_analysis': {
                'overall_sentiment': 'neutral',
                'sentiment_score': 0.0,
                'positive_aspects': [],
                'negative_aspects': [],
                'keyword_sentiment': {}
            }
        }

        if not reviews:
            return sentiment_analysis

        # Vietnamese sentiment keywords
        positive_keywords = {
            'quality': ['chất lượng tốt', 'quality', 'good', 'excellent', 'great', 'amazing', 'perfect', 'tuyệt vời', 'xuất sắc'],
            'performance': ['nhanh', 'mượt', 'smooth', 'fast', 'powerful', 'mạnh mẽ'],
            'design': ['đẹp', 'beautiful', 'nice design', 'elegant', 'sang trọng'],
            'value': ['worth', 'giá tốt', 'reasonable', 'affordable', 'đáng tiền'],
            'shipping': ['nhanh', 'fast delivery', 'good service', 'dịch vụ tốt']
        }

        negative_keywords = {
            'quality': ['kém', 'poor', 'bad quality', 'tệ', 'dở'],
            'performance': ['chậm', 'slow', 'lag', 'giật lag', 'weak'],
            'design': ['xấu', 'ugly', 'poor design', 'không đẹp'],
            'value': ['đắt', 'expensive', 'overpriced', 'không đáng'],
            'shipping': ['chậm', 'slow delivery', 'poor service', 'dịch vụ kém']
        }

        aspect_scores = defaultdict(list)
        overall_scores = []

        for review in reviews:
            content = review.get('content', '').lower()
            rating = review.get('rating', 0)

            if not content:
                continue

            # Rating-based sentiment
            if rating > 0:
                overall_scores.append((rating - 3) / 2)  # Normalize to -1 to 1

            # Keyword-based sentiment analysis
            for aspect, keywords in positive_keywords.items():
                for keyword in keywords:
                    if keyword in content:
                        aspect_scores[aspect].append(1)

            for aspect, keywords in negative_keywords.items():
                for keyword in keywords:
                    if keyword in content:
                        aspect_scores[aspect].append(-1)

        # Calculate overall sentiment
        if overall_scores:
            avg_sentiment = np.mean(overall_scores)
            sentiment_analysis['sentiment_analysis']['sentiment_score'] = avg_sentiment

            if avg_sentiment > 0.3:
                sentiment_analysis['sentiment_analysis']['overall_sentiment'] = 'positive'
            elif avg_sentiment < -0.3:
                sentiment_analysis['sentiment_analysis']['overall_sentiment'] = 'negative'
            else:
                sentiment_analysis['sentiment_analysis']['overall_sentiment'] = 'neutral'

        # Analyze aspects
        for aspect, scores in aspect_scores.items():
            if scores:
                avg_score = np.mean(scores)
                sentiment_analysis['sentiment_analysis']['keyword_sentiment'][aspect] = avg_score

                if avg_score > 0.5:
                    sentiment_analysis['sentiment_analysis']['positive_aspects'].append(aspect)
                elif avg_score < -0.5:
                    sentiment_analysis['sentiment_analysis']['negative_aspects'].append(aspect)

        self.enrichment_stats['enrichments_added'] += 1
        return sentiment_analysis

    def _perform_competitive_analysis(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Perform basic competitive analysis"""

        competitive_analysis = {
            'competitive_analysis': {
                'market_position': 'unknown',
                'competitive_advantages': [],
                'competitive_disadvantages': [],
                'uniqueness_score': 0.0
            }
        }

        # This would typically compare against other products
        # For now, we'll use internal metrics

        price = product.get('price', 0)
        rating = product.get('rating', 0)
        review_count = product.get('review_count', 0)
        spec_completeness = product.get('spec_completeness', 0)

        # Calculate uniqueness score based on available data
        uniqueness_factors = []

        # Specification richness
        if spec_completeness > 0.7:
            uniqueness_factors.append(0.3)
            competitive_analysis['competitive_analysis']['competitive_advantages'].append('rich_specifications')

        # High rating
        if rating > 4.0:
            uniqueness_factors.append(0.4)
            competitive_analysis['competitive_analysis']['competitive_advantages'].append('high_rating')

        # Good review volume
        if isinstance(review_count, str):
            review_count = int(re.sub(r'\D', '', review_count)) if review_count.isdigit() else 0

        if review_count > 50:
            uniqueness_factors.append(0.3)
            competitive_analysis['competitive_analysis']['competitive_advantages'].append('popular_product')

        competitive_analysis['competitive_analysis']['uniqueness_score'] = sum(uniqueness_factors)

        self.enrichment_stats['enrichments_added'] += 1
        return competitive_analysis

    def _analyze_market_position(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze market position and trends"""

        market_analysis = {
            'market_analysis': {
                'target_segment': 'unknown',
                'market_trends': [],
                'seasonality': 'unknown',
                'demand_indicators': {}
            }
        }

        category = product.get('detailed_category', '')
        price_tier = product.get('price_analysis', {}).get('price_tier', '')
        brand_tier = product.get('brand_tier', '')

        # Determine target segment
        if brand_tier == 'premium' and price_tier in ['premium', 'flagship']:
            market_analysis['market_analysis']['target_segment'] = 'premium_users'
        elif price_tier == 'budget':
            market_analysis['market_analysis']['target_segment'] = 'budget_conscious'
        elif price_tier == 'mid_range':
            market_analysis['market_analysis']['target_segment'] = 'mainstream_users'

        # Analyze demand indicators
        review_count = product.get('review_count', 0)
        if isinstance(review_count, str):
            review_count = int(re.sub(r'\D', '', review_count)) if review_count.isdigit() else 0

        sold_count = product.get('sold_count', 0)
        if isinstance(sold_count, str):
            sold_count = int(re.sub(r'\D', '', sold_count)) if sold_count.isdigit() else 0

        market_analysis['market_analysis']['demand_indicators'] = {
            'review_volume': 'high' if review_count > 100 else 'medium' if review_count > 20 else 'low',
            'sales_volume': 'high' if sold_count > 1000 else 'medium' if sold_count > 100 else 'low'
        }

        # Identify trends based on specifications
        specs = product.get('specifications', {})
        trends = []

        if 'memory' in specs:
            ram = specs['memory'].get('ram', '')
            if '8' in ram or '12' in ram or '16' in ram:
                trends.append('high_memory_trend')

        if '5g' in product.get('title', '').lower():
            trends.append('5g_connectivity')

        market_analysis['market_analysis']['market_trends'] = trends

        self.enrichment_stats['enrichments_added'] += 1
        return market_analysis

    def _calculate_quality_indicators(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate various quality indicators"""

        quality_indicators = {
            'quality_indicators': {
                'data_completeness': 0.0,
                'information_richness': 0.0,
                'credibility_score': 0.0,
                'engagement_score': 0.0
            }
        }

        # Data completeness (how many fields are filled)
        required_fields = ['title', 'price', 'rating', 'shop_name', 'location']
        filled_fields = sum(1 for field in required_fields if product.get(field))
        quality_indicators['quality_indicators']['data_completeness'] = filled_fields / len(required_fields)

        # Information richness (specifications, reviews, etc.)
        richness_factors = []

        if product.get('specifications'):
            richness_factors.append(0.3)

        reviews = product.get('reviews', [])
        if len(reviews) > 5:
            richness_factors.append(0.3)

        if product.get('shop_rating'):
            richness_factors.append(0.2)

        if product.get('image_url'):
            richness_factors.append(0.2)

        quality_indicators['quality_indicators']['information_richness'] = sum(richness_factors)

        # Credibility score (shop rating, review consistency, etc.)
        credibility_factors = []

        shop_rating = product.get('shop_rating', 0)
        if shop_rating > 90:
            credibility_factors.append(0.4)

        product_rating = product.get('rating', 0)
        if product_rating > 4.0:
            credibility_factors.append(0.3)

        review_count = product.get('review_count', 0)
        if isinstance(review_count, str):
            review_count = int(re.sub(r'\D', '', review_count)) if review_count.isdigit() else 0

        if review_count > 20:
            credibility_factors.append(0.3)

        quality_indicators['quality_indicators']['credibility_score'] = sum(credibility_factors)

        # Engagement score (reviews, ratings interaction)
        engagement_factors = []

        if reviews:
            avg_helpful = np.mean([r.get('helpful_count', 0) for r in reviews])
            if avg_helpful > 0:
                engagement_factors.append(0.5)

        if review_count > 0:
            engagement_factors.append(min(np.log10(review_count + 1) / 2, 0.5))

        quality_indicators['quality_indicators']['engagement_score'] = sum(engagement_factors)

        self.enrichment_stats['enrichments_added'] += 1
        return quality_indicators

    def generate_enrichment_summary(self) -> Dict[str, Any]:
        """Generate summary of enrichment process"""

        if not self.enriched_data:
            return {'error': 'No enriched data available'}

        summary = {
            'enrichment_stats': self.enrichment_stats.copy(),
            'enrichment_coverage': {},
            'top_insights': []
        }

        # Calculate enrichment coverage
        total_products = len(self.enriched_data)

        coverage_metrics = {
            'brand_detected': 0,
            'specifications_extracted': 0,
            'sentiment_analyzed': 0,
            'competitive_analysis': 0,
            'market_position': 0
        }

        for product in self.enriched_data:
            if product.get('brand') != 'unknown':
                coverage_metrics['brand_detected'] += 1

            if product.get('specifications'):
                coverage_metrics['specifications_extracted'] += 1

            if product.get('sentiment_analysis'):
                coverage_metrics['sentiment_analyzed'] += 1

            if product.get('competitive_analysis'):
                coverage_metrics['competitive_analysis'] += 1

            if product.get('market_analysis'):
                coverage_metrics['market_position'] += 1

        # Convert to percentages
        for metric, count in coverage_metrics.items():
            summary['enrichment_coverage'][metric] = (count / total_products) * 100 if total_products > 0 else 0

        # Generate top insights
        insights = []

        # Brand distribution
        brands = [p.get('brand', 'unknown') for p in self.enriched_data if p.get('brand') != 'unknown']
        if brands:
            top_brand = Counter(brands).most_common(1)[0]
            insights.append(f"Most popular brand: {top_brand[0]} ({top_brand[1]} products)")

        # Price tier distribution
        price_tiers = [p.get('price_analysis', {}).get('price_tier', 'unknown') for p in self.enriched_data]
        price_tiers = [tier for tier in price_tiers if tier != 'unknown']
        if price_tiers:
            top_tier = Counter(price_tiers).most_common(1)[0]
            insights.append(f"Most common price tier: {top_tier[0]} ({top_tier[1]} products)")

        # Sentiment overview
        sentiments = [p.get('sentiment_analysis', {}).get('overall_sentiment', 'neutral') for p in self.enriched_data]
        sentiment_dist = Counter(sentiments)
        if sentiment_dist:
            insights.append(f"Sentiment distribution: {dict(sentiment_dist)}")

        summary['top_insights'] = insights

        return summary

    def export_enriched_data(self, output_dir: str = "enriched_output", filename_prefix: str = "enriched_lazada"):
        """Export enriched data to various formats"""

        if not self.enriched_data:
            print("❌ No enriched data to export")
            return

        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 1. Export complete enriched JSON
        enriched_file = output_path / f"{filename_prefix}_enriched_{timestamp}.json"
        with open(enriched_file, 'w', encoding='utf-8') as f:
            json.dump(self.enriched_data, f, ensure_ascii=False, indent=2)

        # 2. Export enrichment summary
        summary = self.generate_enrichment_summary()
        summary_file = output_path / f"{filename_prefix}_enrichment_summary_{timestamp}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        # 3. Export simplified CSV for analysis
        self._export_enriched_csv(output_path, filename_prefix, timestamp)

        print(f"✅ Enriched data exported:")
        print(f"  📄 Complete JSON: {enriched_file}")
        print(f"  📊 Summary: {summary_file}")
        print(f"  📈 Analysis CSV: {filename_prefix}_analysis_{timestamp}.csv")

    def _export_enriched_csv(self, output_path: Path, filename_prefix: str, timestamp: str):
        """Export enriched data as CSV for analysis"""

        analysis_data = []

        for product in self.enriched_data:
            row = {
                'product_id': product.get('product_id', ''),
                'title': product.get('title', ''),
                'category': product.get('detailed_category', product.get('category', '')),
                'subcategory': product.get('subcategory', ''),
                'brand': product.get('brand', ''),
                'brand_tier': product.get('brand_tier', ''),
                'price': product.get('price', 0),
                'price_tier': product.get('price_analysis', {}).get('price_tier', ''),
                'value_score': product.get('price_analysis', {}).get('value_score', 0),
                'rating': product.get('rating', 0),
                'review_count': product.get('review_count', 0),
                'shop_name': product.get('shop_name', ''),
                'shop_rating': product.get('shop_rating', 0),
                'location': product.get('location', ''),
                'sentiment_score': product.get('sentiment_analysis', {}).get('sentiment_score', 0),
                'overall_sentiment': product.get('sentiment_analysis', {}).get('overall_sentiment', ''),
                'spec_completeness': product.get('spec_completeness', 0),
                'data_completeness': product.get('quality_indicators', {}).get('data_completeness', 0),
                'credibility_score': product.get('quality_indicators', {}).get('credibility_score', 0),
                'uniqueness_score': product.get('competitive_analysis', {}).get('uniqueness_score', 0),
                'target_segment': product.get('market_analysis', {}).get('target_segment', '')
            }
            analysis_data.append(row)

        # Export to CSV
        df = pd.DataFrame(analysis_data)
        csv_file = output_path / f"{filename_prefix}_analysis_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')

def main():
    """Main function to demonstrate usage"""

    print("🚀 Data Enrichment Engine for Lazada Products")
    print("=" * 60)

    # Look for latest enhanced data file
    data_files = list(Path("../../outputs").glob("enhanced_lazada_*.json"))
    if not data_files:
        print("❌ No enhanced data files found in ../../outputs/")
        print("💡 Run enhanced_lazada_crawler.py first to generate data")
        return

    # Use the latest file
    latest_file = max(data_files, key=lambda x: x.stat().st_mtime)
    print(f"📂 Loading data from: {latest_file}")

    # Load data
    with open(latest_file, 'r', encoding='utf-8') as f:
        products = json.load(f)

    print(f"📦 Loaded {len(products)} products for enrichment")

    # Initialize enrichment engine
    engine = DataEnrichmentEngine()

    # Enrich data
    enriched_products = engine.enrich_product_data(products[:20])  # Test with first 20 products

    # Generate and display summary
    summary = engine.generate_enrichment_summary()

    print("\n📊 ENRICHMENT SUMMARY:")
    print("-" * 40)
    print(f"✅ Products processed: {summary['enrichment_stats']['products_processed']}")
    print(f"🔧 Enrichments added: {summary['enrichment_stats']['enrichments_added']}")
    print(f"❌ Errors: {summary['enrichment_stats']['errors']}")

    print("\n📈 ENRICHMENT COVERAGE:")
    for metric, percentage in summary['enrichment_coverage'].items():
        print(f"  {metric}: {percentage:.1f}%")

    print("\n💡 TOP INSIGHTS:")
    for insight in summary['top_insights']:
        print(f"  • {insight}")

    # Export enriched data
    engine.export_enriched_data()

    print("\n✅ Data enrichment complete!")

if __name__ == "__main__":
    main()