#!/usr/bin/env python3
"""
Olist Dataset Batch Processor
Processes all Olist datasets through the Big Data pipeline
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from processors.olist_processor import OlistDatasetProcessor
from utils.database import DatabaseManager
import time

class OlistBatchProcessor:
    def __init__(self, max_workers=3):
        self.processor = OlistDatasetProcessor()
        self.db_manager = DatabaseManager()
        self.max_workers = max_workers
        
        # Define processing order (dependencies first)
        self.processing_order = [
            'olist_customers_dataset.csv',
            'olist_products_dataset.csv',
            'olist_orders_dataset.csv',
            'olist_order_items_dataset.csv',
            'olist_order_payments_dataset.csv',
            'olist_order_reviews_dataset.csv'
        ]
        
        self.base_path = Path('/app/data/raw/kaggle_datasets/brazilian-ecommerce')
    
    def get_dataset_files(self):
        """Get available dataset files"""
        available_files = {}
        
        for dataset_name in self.processing_order:
            file_path = self.base_path / dataset_name
            if file_path.exists():
                available_files[dataset_name] = str(file_path)
                logger.info(f"Found: {dataset_name}")
            else:
                logger.warning(f"Missing: {dataset_name}")
        
        return available_files
    
    def process_dataset_wrapper(self, dataset_info):
        """Wrapper for individual dataset processing (for parallel execution)"""
        dataset_name, file_path = dataset_info
        
        try:
            start_time = time.time()
            
            # Process dataset
            result = self.processor.process_individual_dataset(file_path, dataset_name)
            
            if result is None:
                return {
                    'dataset_name': dataset_name,
                    'status': 'failed',
                    'error': 'Processing returned None'
                }
            
            df_processed, stats = result
            
            # Save to staging
            table_name = self.processor.save_to_staging(df_processed, dataset_name)
            
            processing_time = time.time() - start_time
            
            return {
                'dataset_name': dataset_name,
                'status': 'success',
                'table_name': table_name,
                'stats': stats,
                'processing_time': processing_time
            }
        
        except Exception as e:
            logger.error(f"Error processing {dataset_name}: {e}")
            return {
                'dataset_name': dataset_name,
                'status': 'failed',
                'error': str(e)
            }
    
    def process_sequential(self, dataset_files):
        """Process datasets sequentially (safer for large datasets)"""
        results = []
        
        logger.info("Starting sequential processing...")
        
        for dataset_name in self.processing_order:
            if dataset_name in dataset_files:
                file_path = dataset_files[dataset_name]
                result = self.process_dataset_wrapper((dataset_name, file_path))
                results.append(result)
                
                # Log progress
                if result['status'] == 'success':
                    logger.success(f"✅ {dataset_name} processed successfully in {result['processing_time']:.1f}s")
                else:
                    logger.error(f"❌ {dataset_name} failed: {result['error']}")
        
        return results
    
    def process_parallel(self, dataset_files):
        """Process datasets in parallel (for independent datasets)"""
        results = []
        
        # Separate independent datasets that can be processed in parallel
        independent_datasets = [
            'olist_customers_dataset.csv',
            'olist_products_dataset.csv'
        ]
        
        dependent_datasets = [
            'olist_orders_dataset.csv',
            'olist_order_items_dataset.csv', 
            'olist_order_payments_dataset.csv',
            'olist_order_reviews_dataset.csv'
        ]
        
        logger.info("Starting parallel processing for independent datasets...")
        
        # Process independent datasets in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_dataset = {}
            
            for dataset_name in independent_datasets:
                if dataset_name in dataset_files:
                    file_path = dataset_files[dataset_name]
                    future = executor.submit(self.process_dataset_wrapper, (dataset_name, file_path))
                    future_to_dataset[future] = dataset_name
            
            for future in as_completed(future_to_dataset):
                result = future.result()
                results.append(result)
                
                if result['status'] == 'success':
                    logger.success(f"✅ {result['dataset_name']} processed in parallel")
                else:
                    logger.error(f"❌ {result['dataset_name']} failed in parallel")
        
        # Process dependent datasets sequentially
        logger.info("Processing dependent datasets sequentially...")
        for dataset_name in dependent_datasets:
            if dataset_name in dataset_files:
                file_path = dataset_files[dataset_name]
                result = self.process_dataset_wrapper((dataset_name, file_path))
                results.append(result)
        
        return results
    
    def generate_processing_report(self, results):
        """Generate comprehensive processing report"""
        
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']
        
        report = {
            'processing_summary': {
                'total_datasets': len(results),
                'successful': len(successful),
                'failed': len(failed),
                'success_rate': len(successful) / len(results) * 100 if results else 0
            },
            'dataset_details': results,
            'staging_tables': [r['table_name'] for r in successful if 'table_name' in r]
        }
        
        # Save report to database
        try:
            report_df = pd.DataFrame([{
                'batch_id': f"batch_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}",
                'total_datasets': report['processing_summary']['total_datasets'],
                'successful_datasets': report['processing_summary']['successful'],
                'failed_datasets': report['processing_summary']['failed'],
                'success_rate': report['processing_summary']['success_rate'],
                'processing_date': pd.Timestamp.now(),
                'staging_tables': str(report['staging_tables'])
            }])
            
            self.db_manager.save_to_postgres(report_df, 'olist_processing_batches')
            logger.info("Processing report saved to database")
        
        except Exception as e:
            logger.error(f"Failed to save processing report: {e}")
        
        return report
    
    def run_full_pipeline(self, parallel=False):
        """Run the complete Olist processing pipeline"""
        
        logger.info("🚀 Starting Olist Big Data Processing Pipeline")
        logger.info("="*80)
        
        start_time = time.time()
        
        # 1. Discover datasets
        dataset_files = self.get_dataset_files()
        logger.info(f"Found {len(dataset_files)} datasets to process")
        
        if not dataset_files:
            logger.error("No dataset files found!")
            return None
        
        # 2. Process datasets
        if parallel and len(dataset_files) > 1:
            results = self.process_parallel(dataset_files)
        else:
            results = self.process_sequential(dataset_files)
        
        # 3. Generate report
        report = self.generate_processing_report(results)
        
        total_time = time.time() - start_time
        
        # 4. Summary
        logger.info("="*80)
        logger.info("🎉 PROCESSING COMPLETE")
        logger.info(f"Total time: {total_time:.1f} seconds")
        logger.info(f"Datasets processed: {report['processing_summary']['successful']}/{report['processing_summary']['total_datasets']}")
        logger.info(f"Success rate: {report['processing_summary']['success_rate']:.1f}%")
        
        if report['processing_summary']['failed'] > 0:
            failed_datasets = [r['dataset_name'] for r in results if r['status'] == 'failed']
            logger.warning(f"Failed datasets: {failed_datasets}")
        
        logger.info(f"Staging tables created: {len(report['staging_tables'])}")
        for table in report['staging_tables']:
            logger.info(f"  - {table}")
        
        logger.info("\n🎯 Next step: Run integration pipeline to join staging tables")
        
        return report

def main():
    """Main execution function"""
    
    # Initialize processor
    processor = OlistBatchProcessor(max_workers=2)
    
    # Run pipeline
    try:
        report = processor.run_full_pipeline(parallel=True)
        
        if report and report['processing_summary']['success_rate'] > 0:
            logger.success("Pipeline completed successfully!")
            return True
        else:
            logger.error("Pipeline failed!")
            return False
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)