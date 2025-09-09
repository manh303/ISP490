#!/usr/bin/env python3
"""
Continuous Pipeline Runner - Chạy trong Docker container
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import time
from loguru import logger
from automated_pipeline import AutomatedPipeline

def main():
    """Main continuous pipeline runner"""
    logger.info("🚀 Starting Continuous Pipeline Runner...")
    
    pipeline = AutomatedPipeline()
    
    # Run initial setup
    logger.info("🔄 Running initial data pipeline...")
    success = pipeline.run_full_pipeline()
    
    if success:
        logger.success("✅ Initial pipeline completed successfully!")
    else:
        logger.error("❌ Initial pipeline failed!")
    
    # Keep container running và wait for scheduled tasks
    logger.info("⏰ Pipeline runner is active. Waiting for scheduled tasks...")
    logger.info("   - Full pipeline: Sundays at 2:00 AM")
    logger.info("   - Data updates: Daily at 6:00 AM")
    
    try:
        while True:
            # Health check every hour
            health = pipeline.get_pipeline_health()
            if health['status'] != 'healthy':
                logger.warning(f"⚠️ Pipeline health: {health['message']}")
            
            # Sleep for 1 hour
            time.sleep(3600)
            
    except KeyboardInterrupt:
        logger.info("🛑 Pipeline runner stopped by user")
    except Exception as e:
        logger.error(f"❌ Pipeline runner error: {e}")

if __name__ == "__main__":
    main()