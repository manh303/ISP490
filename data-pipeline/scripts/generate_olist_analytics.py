#!/usr/bin/env python3
"""
Generate business analytics from integrated Olist dataset
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
import numpy as np
from loguru import logger
from utils.database import DatabaseManager
from datetime import datetime

class OlistAnalyticsGenerator:
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    def load_integrated_data(self):
        """Load the integrated dataset"""
        try:
            df = self.db_manager.load_from_postgres("SELECT * FROM olist_integrated_dataset")
            logger.info(f"Loaded integrated dataset: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Failed to load integrated dataset: {e}")
            return None
    
    def generate_customer_analytics(self, df):
        """Generate customer analytics"""
        logger.info("Generating customer analytics...")
        
        customer_analytics = []
        
        # Customer segmentation based on RFM
        if all(col in df.columns for col in ['customer_id', 'recency_score', 'frequency_score', 'monetary_score']):
            
            # RFM segments
            rfm_segments = df.groupby(['recency_score', 'frequency_score', 'monetary_score']).size().reset_index(name='customer_count')
            
            customer_analytics.append({
                'metric_name': 'RFM_Segmentation',
                'metric_value': len(rfm_segments),
                'details': rfm_segments.to_dict('records')
            })
        
        # Customer lifetime value distribution
        if 'customer_total_spent' in df.columns:
            clv_stats = {
                'mean': df['customer_total_spent'].mean(),
                'median': df['customer_total_spent'].median(),
                'std': df['customer_total_spent'].std(),
                'percentiles': {
                    '25th': df['customer_total_spent'].quantile(0.25),
                    '75th': df['customer_total_spent'].quantile(0.75),
                    '95th': df['customer_total_spent'].quantile(0.95)
                }
            }
            
            customer_analytics.append({
                'metric_name': 'Customer_Lifetime_Value_Stats',
                'metric_value': clv_stats['mean'],
                'details': clv_stats
            })
        
        return customer_analytics
    
    def generate_product_analytics(self, df):
        """Generate product analytics"""
        logger.info("Generating product analytics...")
        
        product_analytics = []
        
        # Top selling categories
        if 'product_category_name' in df.columns:
            top_categories = df['product_category_name'].value_counts().head(10)
            
            product_analytics.append({
                'metric_name': 'Top_Selling_Categories',
                'metric_value': len(top_categories),
                'details': top_categories.to_dict()
            })
        
        # Average rating by category
        if all(col in df.columns for col in ['product_category_name', 'review_score']):
            avg_rating_by_category = df.groupby('product_category_name')['review_score'].mean().sort_values(ascending=False).head(10)
            
            product_analytics.append({
                'metric_name': 'Average_Rating_by_Category',
                'metric_value': avg_rating_by_category.mean(),
                'details': avg_rating_by_category.to_dict()
            })
        
        return product_analytics
    
    def generate_sales_analytics(self, df):
        """Generate sales analytics"""
        logger.info("Generating sales analytics...")
        
        sales_analytics = []
        
        # Monthly sales trends
        if all(col in df.columns for col in ['order_year', 'order_month', 'payment_value']):
            monthly_sales = df.groupby(['order_year', 'order_month'])['payment_value'].sum().reset_index()
            monthly_sales['year_month'] = monthly_sales['order_year'].astype(str) + '-' + monthly_sales['order_month'].astype(str).str.zfill(2)
            
            sales_analytics.append({
                'metric_name': 'Monthly_Sales_Trend',
                'metric_value': monthly_sales['payment_value'].sum(),
                'details': monthly_sales.to_dict('records')
            })
        
        # Payment method distribution
        if 'payment_type' in df.columns:
            payment_distribution = df['payment_type'].value_counts()
            
            sales_analytics.append({
                'metric_name': 'Payment_Method_Distribution',
                'metric_value': len(payment_distribution),
                'details': payment_distribution.to_dict()
            })
        
        return sales_analytics
    
    def generate_comprehensive_report(self):
        """Generate comprehensive business report"""
        
        logger.info("🚀 Generating Comprehensive Olist Analytics Report")
        logger.info("="*70)
        
        # Load data
        df = self.load_integrated_data()
        if df is None:
            return None
        
        # Generate analytics
        customer_analytics = self.generate_customer_analytics(df)
        product_analytics = self.generate_product_analytics(df)
        sales_analytics = self.generate_sales_analytics(df)
        
        # Compile report
        report = {
            'report_date': datetime.now(),
            'dataset_summary': {
                'total_records': len(df),
                'date_range': {
                    'start': df['order_purchase_timestamp'].min() if 'order_purchase_timestamp' in df.columns else None,
                    'end': df['order_purchase_timestamp'].max() if 'order_purchase_timestamp' in df.columns else None
                },
                'unique_customers': df['customer_id'].nunique() if 'customer_id' in df.columns else 0,
                'unique_products': df['product_id'].nunique() if 'product_id' in df.columns else 0
            },
            'customer_analytics': customer_analytics,
            'product_analytics': product_analytics,
            'sales_analytics': sales_analytics
        }
        
        # Save report
        try:
            # Save summary to database
            summary_df = pd.DataFrame([{
                'report_date': report['report_date'],
                'total_records': report['dataset_summary']['total_records'],
                'unique_customers': report['dataset_summary']['unique_customers'],
                'unique_products': report['dataset_summary']['unique_products'],
                'customer_metrics_count': len(customer_analytics),
                'product_metrics_count': len(product_analytics),
                'sales_metrics_count': len(sales_analytics)
            }])
            
            self.db_manager.save_to_postgres(summary_df, 'olist_analytics_reports')
            logger.success("Analytics report saved to database")
            
        except Exception as e:
            logger.error(f"Failed to save report: {e}")
        
        # Display key insights
        logger.info("\n📊 KEY BUSINESS INSIGHTS:")
        logger.info(f"• Total transactions analyzed: {report['dataset_summary']['total_records']:,}")
        logger.info(f"• Unique customers: {report['dataset_summary']['unique_customers']:,}")
        logger.info(f"• Unique products: {report['dataset_summary']['unique_products']:,}")
        
        if report['dataset_summary']['date_range']['start']:
            start_date = report['dataset_summary']['date_range']['start'].strftime('%Y-%m-%d')
            end_date = report['dataset_summary']['date_range']['end'].strftime('%Y-%m-%d')
            logger.info(f"• Analysis period: {start_date} to {end_date}")
        
        # Customer insights
        for metric in customer_analytics:
            if metric['metric_name'] == 'Customer_Lifetime_Value_Stats':
                logger.info(f"• Average customer value: R${metric['metric_value']:.2f}")
        
        # Product insights
        for metric in product_analytics:
            if metric['metric_name'] == 'Top_Selling_Categories':
                top_category = list(metric['details'].keys())[0]
                logger.info(f"• Top product category: {top_category}")
        
        return report

def main():
    """Main analytics execution"""
    
    generator = OlistAnalyticsGenerator()
    
    try:
        report = generator.generate_comprehensive_report()
        
        if report:
            logger.success("Analytics generation completed!")
            return True
        else:
            logger.error("Analytics generation failed!")
            return False
    
    except Exception as e:
        logger.error(f"Analytics execution failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)