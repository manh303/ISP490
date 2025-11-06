#!/usr/bin/env python3
"""
Complete Data Pipeline Runner
Chạy toàn bộ pipeline từ crawling → processing → enrichment → analysis
"""

import sys
import argparse
import time
from pathlib import Path
from datetime import datetime

def run_complete_pipeline(
    category: str = "smartphones",
    pages: int = 2,
    products_per_page: int = 10,
    get_details: bool = True,
    skip_crawling: bool = False,
    output_prefix: str = "complete_pipeline"
):
    """
    Run complete data pipeline
    """

    print("🚀 COMPLETE LAZADA DATA PIPELINE")
    print("=" * 60)
    print(f"📊 Configuration:")
    print(f"  📱 Category: {category}")
    print(f"  📄 Max pages: {pages}")
    print(f"  📦 Products per page: {products_per_page}")
    print(f"  🔍 Get details: {get_details}")
    print(f"  ⏭️  Skip crawling: {skip_crawling}")
    print("")

    pipeline_start = time.time()
    latest_data_file = None

    # ================================
    # STEP 1: DATA CRAWLING
    # ================================
    if not skip_crawling:
        print("🕷️  STEP 1: DATA CRAWLING")
        print("-" * 40)

        try:
            from enhanced_lazada_crawler import EnhancedLazadaCrawler

            crawler = EnhancedLazadaCrawler()

            print(f"🎯 Crawling {category} category...")

            # Crawl data
            products = crawler.crawl_category(
                category,
                crawler.categories.get(category, ''),
                max_pages=pages,
                max_products_per_page=products_per_page,
                get_details=get_details
            )

            if products:
                # Save crawled data
                timestamp = int(time.time())
                filename = f"{output_prefix}_crawled_{timestamp}"
                crawler.save_to_files(products, filename)

                # Find the latest file
                output_dir = Path("../../outputs")
                latest_files = list(output_dir.glob(f"{filename}_complete_*.json"))
                if latest_files:
                    latest_data_file = max(latest_files, key=lambda x: x.stat().st_mtime)

                print(f"✅ Crawling complete: {len(products)} products")

            else:
                print("❌ No products crawled")
                return False

            crawler.close()

        except Exception as e:
            print(f"❌ Crawling failed: {e}")
            return False

    else:
        # Find latest existing data file
        output_dir = Path("../../outputs")
        data_files = list(output_dir.glob("enhanced_lazada_*.json"))
        if not data_files:
            print("❌ No existing data files found. Run with crawling first.")
            return False

        latest_data_file = max(data_files, key=lambda x: x.stat().st_mtime)
        print(f"📂 Using existing data: {latest_data_file}")

    # ================================
    # STEP 2: DATA PROCESSING & ANALYSIS
    # ================================
    print("\n📊 STEP 2: DATA PROCESSING & ANALYSIS")
    print("-" * 40)

    try:
        from advanced_data_processor import AdvancedDataProcessor

        # Initialize processor
        processor = AdvancedDataProcessor(str(latest_data_file))

        # Clean and standardize data
        print("🧹 Cleaning and standardizing data...")
        cleaned_data = processor.clean_and_standardize_data()

        # Analyze data
        print("📈 Analyzing data...")
        analytics = processor.analyze_data()

        # Generate insights
        print("💡 Generating insights...")
        insights = processor.generate_insights()

        # Export analysis reports
        processor.export_analysis_report(f"analysis_output_{category}")

        print(f"✅ Analysis complete: {len(cleaned_data)} products processed")

    except Exception as e:
        print(f"❌ Data processing failed: {e}")
        return False

    # ================================
    # STEP 3: DATA ENRICHMENT
    # ================================
    print("\n🔧 STEP 3: DATA ENRICHMENT")
    print("-" * 40)

    try:
        from data_enrichment_engine import DataEnrichmentEngine

        # Initialize enrichment engine
        engine = DataEnrichmentEngine()

        # Use cleaned data from processor
        products_to_enrich = processor.processed_data.get('cleaned', [])

        if products_to_enrich:
            print(f"🔧 Enriching {len(products_to_enrich)} products...")

            # Limit to reasonable number for testing
            max_enrich = min(len(products_to_enrich), 50)
            enriched_products = engine.enrich_product_data(products_to_enrich[:max_enrich])

            # Export enriched data
            engine.export_enriched_data(f"enriched_output_{category}", f"enriched_{category}")

            # Generate enrichment summary
            summary = engine.generate_enrichment_summary()

            print(f"✅ Enrichment complete: {len(enriched_products)} products enriched")

        else:
            print("⚠️  No cleaned data available for enrichment")

    except Exception as e:
        print(f"❌ Data enrichment failed: {e}")
        return False

    # ================================
    # STEP 4: PIPELINE SUMMARY
    # ================================
    print("\n📋 STEP 4: PIPELINE SUMMARY")
    print("-" * 40)

    pipeline_end = time.time()
    total_time = pipeline_end - pipeline_start

    print(f"⏱️  Total pipeline time: {total_time:.1f} seconds")
    print(f"📊 Pipeline Results:")

    # Crawling results
    if not skip_crawling and products:
        print(f"  🕷️  Crawled: {len(products)} products")

        # Data quality summary
        with_shop = len([p for p in products if p.get('shop_name')])
        with_reviews = len([p for p in products if p.get('reviews')])

        print(f"  🏪 Products with shop info: {with_shop}/{len(products)} ({(with_shop/len(products)*100):.1f}%)")
        print(f"  💬 Products with reviews: {with_reviews}/{len(products)} ({(with_reviews/len(products)*100):.1f}%)")

    # Processing results
    if 'cleaned_data' in locals():
        print(f"  🧹 Processed: {len(cleaned_data)} products")

        # Quality metrics
        avg_quality = sum(p.get('data_quality_score', 0) for p in cleaned_data) / len(cleaned_data) if cleaned_data else 0
        print(f"  📈 Average data quality: {avg_quality:.1f}/100")

    # Enrichment results
    if 'enriched_products' in locals():
        print(f"  🔧 Enriched: {len(enriched_products)} products")

        # Enrichment coverage
        if 'summary' in locals():
            coverage = summary.get('enrichment_coverage', {})
            print(f"  🎯 Brand detection: {coverage.get('brand_detected', 0):.1f}%")
            print(f"  🎯 Spec extraction: {coverage.get('specifications_extracted', 0):.1f}%")

    # Output files summary
    print(f"\n📁 Generated Files:")

    output_dirs = ["../../outputs", f"analysis_output_{category}", f"enriched_output_{category}"]

    for output_dir in output_dirs:
        dir_path = Path(output_dir)
        if dir_path.exists():
            files = list(dir_path.glob("*"))
            recent_files = [f for f in files if (datetime.now().timestamp() - f.stat().st_mtime) < 3600]  # Last hour

            if recent_files:
                print(f"  📂 {output_dir}: {len(recent_files)} files")
                for file in recent_files[-3:]:  # Show last 3 files
                    print(f"    • {file.name}")

    print(f"\n✅ COMPLETE PIPELINE FINISHED SUCCESSFULLY!")
    print(f"🎉 Ready for integration with Spark standardization pipeline")

    return True

def main():
    """Main function with CLI interface"""

    parser = argparse.ArgumentParser(description='Complete Lazada Data Pipeline')

    # Crawling options
    parser.add_argument('--category', type=str, default='smartphones',
                       help='Category to crawl (default: smartphones)')
    parser.add_argument('--pages', type=int, default=2,
                       help='Max pages per category (default: 2)')
    parser.add_argument('--products-per-page', type=int, default=10,
                       help='Max products per page (default: 10)')
    parser.add_argument('--no-details', action='store_true',
                       help='Skip product detail extraction (faster)')
    parser.add_argument('--skip-crawling', action='store_true',
                       help='Skip crawling, use existing data')

    # Output options
    parser.add_argument('--output-prefix', type=str, default='complete_pipeline',
                       help='Output filename prefix (default: complete_pipeline)')

    args = parser.parse_args()

    # Validate category
    from enhanced_lazada_crawler import EnhancedLazadaCrawler
    available_categories = list(EnhancedLazadaCrawler().categories.keys())

    if args.category not in available_categories:
        print(f"❌ Invalid category: {args.category}")
        print(f"Available categories: {', '.join(available_categories)}")
        sys.exit(1)

    # Run pipeline
    success = run_complete_pipeline(
        category=args.category,
        pages=args.pages,
        products_per_page=args.products_per_page,
        get_details=not args.no_details,
        skip_crawling=args.skip_crawling,
        output_prefix=args.output_prefix
    )

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()