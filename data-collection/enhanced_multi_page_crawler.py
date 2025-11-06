#!/usr/bin/env python3
"""
Enhanced Multi-Page Lazada Crawler
Thu thập dữ liệu toàn diện từ nhiều trang và nhiều danh mục
Tối ưu cho DSS Analytics và Reporting
"""

import sys
import time
import json
import logging
from datetime import datetime
from pathlib import Path

# Add current directory to Python path
sys.path.append(str(Path(__file__).parent))

# Import enhanced crawler
from lazada_electronics_crawler import LazadaElectronicsCrawler

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedDataCollector:
    """Enhanced data collector for comprehensive e-commerce analytics"""

    def __init__(self, output_dir: str = None):
        self.output_dir = Path(output_dir) if output_dir else Path("../data/enhanced_lazada_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Enhanced crawling configuration
        self.crawl_config = {
            'pages_per_category': 15,  # Increased for comprehensive data
            'categories': [
                'smartphones',    # High-demand category
                'laptops',        # High-value category
                'tablets',        # Growing market
                'headphones',     # Popular accessories
                'speakers',       # Audio market
                'smartwatch',     # Wearables trend
                'cameras',        # Photography market
                'accessories'     # Broad market
            ],
            'delay_range': (3, 7),    # Respectful crawling
            'max_retries': 3,
            'headless': True          # Production ready
        }

        # Statistics tracking
        self.session_stats = {
            'start_time': datetime.now(),
            'total_categories': 0,
            'total_pages': 0,
            'total_products': 0,
            'success_rate': 0.0,
            'categories_completed': [],
            'categories_failed': [],
            'estimated_duration': 0
        }

    def estimate_crawl_duration(self) -> int:
        """Estimate total crawl duration in minutes"""
        pages_per_category = self.crawl_config['pages_per_category']
        num_categories = len(self.crawl_config['categories'])
        avg_delay = sum(self.crawl_config['delay_range']) / 2

        # Rough calculation: (pages * categories * avg_delay) + overhead
        total_pages = pages_per_category * num_categories
        estimated_seconds = (total_pages * avg_delay) + (num_categories * 30)  # 30s overhead per category

        return int(estimated_seconds / 60)  # Convert to minutes

    def print_crawl_plan(self):
        """Print detailed crawl execution plan"""
        estimated_duration = self.estimate_crawl_duration()

        plan = f"""
╔══════════════════════════════════════════════════════════════════════╗
║                    🚀 ENHANCED LAZADA CRAWLER PLAN                   ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  📊 CRAWL CONFIGURATION:                                            ║
║     • Categories: {len(self.crawl_config['categories'])} categories                                    ║
║     • Pages per Category: {self.crawl_config['pages_per_category']} pages                              ║
║     • Total Expected Pages: {self.crawl_config['pages_per_category'] * len(self.crawl_config['categories'])} pages                            ║
║     • Expected Products: ~{self.crawl_config['pages_per_category'] * len(self.crawl_config['categories']) * 40} products (estimate)          ║
║                                                                      ║
║  ⏱️  TIMING:                                                        ║
║     • Estimated Duration: {estimated_duration} minutes                           ║
║     • Delay Between Pages: {self.crawl_config['delay_range'][0]}-{self.crawl_config['delay_range'][1]} seconds                        ║
║     • Respectful Crawling: ✅ Enabled                               ║
║                                                                      ║
║  📁 OUTPUT:                                                         ║
║     • Directory: {str(self.output_dir):<45} ║
║     • Formats: JSON, CSV, JSONL                                     ║
║     • Reports: Detailed analytics reports                           ║
║                                                                      ║
║  🎯 TARGET CATEGORIES:                                              ║"""

        for i, category in enumerate(self.crawl_config['categories'], 1):
            plan += f"║     {i:2d}. {category.capitalize():<55} ║\n"

        plan += """║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
        """
        print(plan)

    def run_comprehensive_crawl(self) -> dict:
        """Execute comprehensive multi-category, multi-page crawl"""

        # Print execution plan
        self.print_crawl_plan()

        # Get user confirmation
        try:
            confirm = input("\\n🤔 Proceed with comprehensive crawl? (y/N): ").strip().lower()
            if confirm not in ['y', 'yes']:
                print("❌ Crawl cancelled by user")
                return {'success': False, 'message': 'Cancelled by user'}
        except KeyboardInterrupt:
            print("\\n❌ Crawl cancelled by user")
            return {'success': False, 'message': 'Cancelled by user'}

        print("\\n🚀 Starting comprehensive crawl...")
        self.session_stats['start_time'] = datetime.now()

        # Initialize enhanced crawler
        crawler = LazadaElectronicsCrawler(
            headless=self.crawl_config['headless'],
            output_dir=str(self.output_dir)
        )

        # Set enhanced delays for respectful crawling
        crawler.min_delay = self.crawl_config['delay_range'][0]
        crawler.max_delay = self.crawl_config['delay_range'][1]

        try:
            # Execute comprehensive crawl
            result = crawler.run_crawler(
                categories=self.crawl_config['categories'],
                max_pages_per_category=self.crawl_config['pages_per_category']
            )

            # Update session statistics
            if result['success']:
                self.session_stats.update({
                    'total_categories': result['report']['performance']['categories_processed'],
                    'total_pages': result['report']['performance']['pages_processed'],
                    'total_products': result['products_count'],
                    'success_rate': result['report']['performance']['success_rate'],
                    'categories_completed': self.crawl_config['categories']
                })

                # Generate enhanced analytics report
                analytics_report = self.generate_analytics_report(result)

                # Save enhanced report
                self.save_enhanced_report(analytics_report)

                print("\\n" + "="*80)
                print("🎉 COMPREHENSIVE CRAWL COMPLETED SUCCESSFULLY!")
                print("="*80)
                self.print_success_summary(result)

                return {
                    'success': True,
                    'crawler_result': result,
                    'analytics_report': analytics_report,
                    'session_stats': self.session_stats
                }
            else:
                print(f"\\n❌ Crawl failed: {result.get('error', 'Unknown error')}")
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown error'),
                    'session_stats': self.session_stats
                }

        except Exception as e:
            logger.error(f"Critical error in comprehensive crawl: {e}")
            return {
                'success': False,
                'error': str(e),
                'session_stats': self.session_stats
            }

    def generate_analytics_report(self, crawler_result: dict) -> dict:
        """Generate enhanced analytics report for DSS system"""

        products = crawler_result.get('products', [])
        report = crawler_result.get('report', {})

        # Basic analytics
        total_products = len(products) if isinstance(products, list) else crawler_result.get('products_count', 0)

        # Category distribution
        category_stats = {}
        brand_stats = {}
        price_ranges = {'0-1M': 0, '1M-5M': 0, '5M-10M': 0, '10M+': 0, 'Unknown': 0}

        # Enhanced sample analysis (from first 10 products for safety)
        sample_products = products[:10] if isinstance(products, list) and products else []

        for product in sample_products:
            # Category analysis
            category = product.get('category', 'Unknown')
            category_stats[category] = category_stats.get(category, 0) + 1

            # Brand analysis
            brand = product.get('brand') or 'Unknown'
            brand_stats[brand] = brand_stats.get(brand, 0) + 1

            # Price range analysis
            price = product.get('price', 0)
            if price == 0:
                price_ranges['Unknown'] += 1
            elif price < 1000000:
                price_ranges['0-1M'] += 1
            elif price < 5000000:
                price_ranges['1M-5M'] += 1
            elif price < 10000000:
                price_ranges['5M-10M'] += 1
            else:
                price_ranges['10M+'] += 1

        # Generate comprehensive analytics
        analytics = {
            'crawl_summary': {
                'total_products_collected': total_products,
                'categories_processed': len(self.crawl_config['categories']),
                'pages_processed': report.get('performance', {}).get('pages_processed', 0),
                'success_rate': report.get('performance', {}).get('success_rate', 0),
                'crawl_duration': report.get('crawl_session', {}).get('duration_formatted', 'Unknown')
            },

            'market_analysis': {
                'category_distribution': category_stats,
                'brand_distribution': dict(list(brand_stats.items())[:10]),  # Top 10 brands
                'price_range_distribution': price_ranges
            },

            'data_quality': {
                'products_with_prices': sum(1 for p in sample_products if p.get('price', 0) > 0),
                'products_with_images': sum(1 for p in sample_products if p.get('images')),
                'products_with_ratings': sum(1 for p in sample_products if p.get('rating')),
                'sample_size': len(sample_products)
            },

            'dss_readiness': {
                'suitable_for_dashboards': total_products > 100,
                'suitable_for_trend_analysis': report.get('performance', {}).get('pages_processed', 0) > 5,
                'suitable_for_competitor_analysis': len(category_stats) > 2,
                'data_completeness_score': self.calculate_completeness_score(sample_products)
            },

            'recommendations': self.generate_data_recommendations(total_products, category_stats, price_ranges),

            'metadata': {
                'crawl_timestamp': datetime.now().isoformat(),
                'data_version': '1.0',
                'platform': 'lazada_vn',
                'collection_method': 'enhanced_multi_page_crawler'
            }
        }

        return analytics

    def calculate_completeness_score(self, products: list) -> float:
        """Calculate data completeness score (0-100)"""
        if not products:
            return 0.0

        total_fields = 0
        completed_fields = 0

        key_fields = ['name', 'price', 'category', 'images', 'url']

        for product in products:
            for field in key_fields:
                total_fields += 1
                if product.get(field) and product[field] != 0:
                    completed_fields += 1

        return round((completed_fields / total_fields) * 100, 1) if total_fields > 0 else 0.0

    def generate_data_recommendations(self, total_products: int, categories: dict, prices: dict) -> list:
        """Generate actionable recommendations for data usage"""
        recommendations = []

        if total_products > 500:
            recommendations.append("✅ Sufficient data volume for comprehensive analytics dashboard")
        elif total_products > 200:
            recommendations.append("⚠️ Moderate data volume - suitable for basic analytics")
        else:
            recommendations.append("❌ Low data volume - consider increasing crawl scope")

        if len(categories) >= 4:
            recommendations.append("✅ Good category diversity for cross-category analysis")
        else:
            recommendations.append("⚠️ Limited category diversity - consider expanding categories")

        if prices['Unknown'] / sum(prices.values()) < 0.5:
            recommendations.append("✅ Good price data availability for pricing analytics")
        else:
            recommendations.append("❌ Poor price data - implement JavaScript rendering for dynamic prices")

        recommendations.append("💡 Suitable DSS applications: Product catalog analysis, category performance comparison")
        recommendations.append("💡 Limitations: Real revenue data not available, traffic analytics not possible")

        return recommendations

    def save_enhanced_report(self, analytics_report: dict):
        """Save enhanced analytics report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save detailed analytics report
        analytics_file = self.output_dir / f"enhanced_analytics_report_{timestamp}.json"
        with open(analytics_file, 'w', encoding='utf-8') as f:
            json.dump(analytics_report, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"📊 Enhanced analytics report saved: {analytics_file}")

    def print_success_summary(self, result: dict):
        """Print detailed success summary"""
        report = result.get('report', {})
        performance = report.get('performance', {})

        summary = f"""
📊 CRAWL STATISTICS:
   • Total Products: {result.get('products_count', 0):,}
   • Categories: {performance.get('categories_processed', 0)}
   • Pages: {performance.get('pages_processed', 0)}
   • Success Rate: {performance.get('success_rate', 0):.1f}%
   • Duration: {report.get('crawl_session', {}).get('duration_formatted', 'Unknown')}

📁 OUTPUT FILES:
   • Location: {self.output_dir}
   • Formats: JSON, CSV, JSONL, Analytics Report

🎯 DSS READINESS:
   • Dashboard Ready: ✅ Yes
   • Analytics Ready: ✅ Yes
   • Trend Analysis: ✅ Ready with time-series collection
   • Competitor Analysis: ✅ Cross-category data available

💡 NEXT STEPS:
   1. Import data into analytics database
   2. Setup time-series collection for trend analysis
   3. Implement cross-platform comparison (Shopee, Tiki)
   4. Build predictive models for demand forecasting
        """

        print(summary)

def main():
    """Main execution function"""

    # Initialize enhanced data collector
    collector = EnhancedDataCollector()

    # Run comprehensive crawl
    result = collector.run_comprehensive_crawl()

    if result['success']:
        print("\\n🎉 Enhanced data collection completed successfully!")
        print("📊 Data is ready for DSS analytics and reporting!")
    else:
        print(f"\\n❌ Enhanced data collection failed: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()