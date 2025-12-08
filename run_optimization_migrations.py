#!/usr/bin/env python3
"""
Database Migration Runner for Performance Optimizations
========================================================
Runs SQL migration files to create indexes and product_metrics_global table.

Usage:
    python run_optimization_migrations.py [--rollback]

Environment Variables Required:
    DATABASE_URL - PostgreSQL connection string
"""

import os
import sys
import asyncio
import asyncpg
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MigrationRunner:
    """Runs SQL migration files with transaction support"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.migrations_dir = Path(__file__).parent / "backend" / "migrations"
        
    async def connect(self):
        """Create database connection"""
        try:
            self.conn = await asyncpg.connect(self.database_url)
            logger.info("✅ Connected to database successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            return False
    
    async def close(self):
        """Close database connection"""
        if hasattr(self, 'conn'):
            await self.conn.close()
            logger.info("Closed database connection")
    
    async def run_migration_file(self, filepath: Path, use_transaction: bool = True):
        """
        Run a single SQL migration file
        
        Args:
            filepath: Path to SQL file
            use_transaction: Whether to wrap in transaction (default True)
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Running migration: {filepath.name}")
        logger.info(f"{'='*60}")
        
        try:
            # Read SQL file
            with open(filepath, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Execute SQL
            start_time = datetime.now()
            
            if use_transaction:
                # Use transaction (rollback on error)
                async with self.conn.transaction():
                    await self.conn.execute(sql_content)
            else:
                # Without transaction (for operations that can't be in transaction)
                await self.conn.execute(sql_content)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ Migration completed successfully in {duration:.2f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            logger.error(f"File: {filepath}")
            return False
    
    async def verify_indexes(self):
        """Verify that indexes were created successfully"""
        logger.info("\n" + "="*60)
        logger.info("Verifying indexes...")
        logger.info("="*60)
        
        query = """
            SELECT 
                schemaname,
                tablename,
                indexname,
                pg_size_pretty(pg_relation_size(indexrelid)) AS size
            FROM pg_indexes
            JOIN pg_stat_user_indexes USING (schemaname, tablename, indexname)
            WHERE schemaname IN ('ml', 'dwh')
              AND indexname LIKE 'idx_%'
            ORDER BY schemaname, tablename, indexname;
        """
        
        try:
            rows = await self.conn.fetch(query)
            if rows:
                logger.info(f"Found {len(rows)} indexes:")
                for row in rows:
                    logger.info(f"  • {row['schemaname']}.{row['tablename']}.{row['indexname']} ({row['size']})")
                return True
            else:
                logger.warning("No indexes found!")
                return False
        except Exception as e:
            logger.error(f"Error verifying indexes: {e}")
            return False
    
    async def verify_product_metrics_table(self):
        """Verify product_metrics_global table was created and populated"""
        logger.info("\n" + "="*60)
        logger.info("Verifying product_metrics_global table...")
        logger.info("="*60)
        
        # Check table exists
        table_check = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'dwh' 
                AND table_name = 'product_metrics_global'
            );
        """
        
        try:
            exists = await self.conn.fetchval(table_check)
            if not exists:
                logger.error("❌ Table dwh.product_metrics_global does not exist!")
                return False
            
            logger.info("✅ Table exists")
            
            # Check data
            stats_query = """
                SELECT 
                    COUNT(*) as total_products,
                    COUNT(CASE WHEN avg_price > 0 THEN 1 END) as with_price,
                    COUNT(CASE WHEN total_orders > 0 THEN 1 END) as with_orders,
                    MAX(last_updated) as last_refresh
                FROM dwh.product_metrics_global;
            """
            
            stats = await self.conn.fetchrow(stats_query)
            logger.info(f"  • Total products: {stats['total_products']}")
            logger.info(f"  • With price data: {stats['with_price']}")
            logger.info(f"  • With orders data: {stats['with_orders']}")
            logger.info(f"  • Last refreshed: {stats['last_refresh']}")
            
            if stats['total_products'] == 0:
                logger.warning("⚠️  Table is empty - no products found")
                return False
            
            # Check refresh function exists
            func_check = """
                SELECT EXISTS (
                    SELECT FROM pg_proc p
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE n.nspname = 'dwh' 
                    AND p.proname = 'refresh_product_metrics_global'
                );
            """
            
            func_exists = await self.conn.fetchval(func_check)
            if func_exists:
                logger.info("✅ Refresh function exists")
            else:
                logger.warning("⚠️  Refresh function not found")
            
            return True
            
        except Exception as e:
            logger.error(f"Error verifying table: {e}")
            return False
    
    async def run_rollback(self):
        """Rollback migrations (drop indexes and table)"""
        logger.info("\n" + "="*60)
        logger.info("ROLLING BACK MIGRATIONS")
        logger.info("="*60)
        
        rollback_sql = """
            -- Drop indexes
            DROP INDEX CONCURRENTLY IF EXISTS ml.idx_fact_product_reco_src_sim_rank;
            DROP INDEX CONCURRENTLY IF EXISTS ml.idx_fact_product_reco_sim_desc;
            DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_product_product_key;
            DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_product_platform_category;
            DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_fact_product_daily_date_product;
            DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_fact_product_daily_metrics;
            DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_date_date_value;
            DROP INDEX CONCURRENTLY IF EXISTS dwh.idx_dim_platform_code;
            
            -- Drop table and function
            DROP TABLE IF EXISTS dwh.product_metrics_global CASCADE;
            DROP FUNCTION IF EXISTS dwh.refresh_product_metrics_global();
        """
        
        try:
            # Note: DROP INDEX CONCURRENTLY cannot be in transaction
            await self.conn.execute(rollback_sql)
            logger.info("✅ Rollback completed successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Rollback failed: {e}")
            return False
    
    async def run_all_migrations(self):
        """Run all migration files in order"""
        migrations = [
            ("create_performance_indexes.sql", True),
            ("create_product_metrics_table.sql", True),
        ]
        
        success_count = 0
        
        for filename, use_transaction in migrations:
            filepath = self.migrations_dir / filename
            
            if not filepath.exists():
                logger.error(f"❌ Migration file not found: {filepath}")
                continue
            
            if await self.run_migration_file(filepath, use_transaction):
                success_count += 1
            else:
                logger.error(f"Stopping migrations due to error in {filename}")
                return False
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Migrations completed: {success_count}/{len(migrations)}")
        logger.info(f"{'='*60}\n")
        
        # Verify migrations
        await self.verify_indexes()
        await self.verify_product_metrics_table()
        
        return success_count == len(migrations)


async def main():
    """Main entry point"""
    # Parse arguments
    rollback = "--rollback" in sys.argv
    
    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL","postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1")
    
    if not database_url:
        logger.error("❌ DATABASE_URL environment variable not set")
        logger.info("Usage: DATABASE_URL='postgresql://...' python run_optimization_migrations.py")
        sys.exit(1)
    
    # Create migration runner
    runner = MigrationRunner(database_url)
    
    # Connect to database
    if not await runner.connect():
        sys.exit(1)
    
    try:
        if rollback:
            # Run rollback
            success = await runner.run_rollback()
        else:
            # Run migrations
            logger.info("\n" + "="*60)
            logger.info("STARTING DATABASE OPTIMIZATION MIGRATIONS")
            logger.info("="*60)
            logger.info("This will create:")
            logger.info("  • Performance indexes on critical tables")
            logger.info("  • product_metrics_global materialized table")
            logger.info("  • Refresh function for daily updates")
            logger.info("="*60 + "\n")
            
            success = await runner.run_all_migrations()
        
        if success:
            logger.info("\n" + "🎉 " + "="*56)
            logger.info("✅ ALL MIGRATIONS COMPLETED SUCCESSFULLY!")
            logger.info("="*60)
            logger.info("\nNext steps:")
            logger.info("  1. Deploy updated backend code (dss_service.py)")
            logger.info("  2. Deploy Airflow DAG (refresh_product_metrics_dag.py)")
            logger.info("  3. Test DSS endpoints")
            logger.info("  4. Monitor query performance")
            logger.info("\nSee DEPLOYMENT_OPTIMIZATION.md for details.")
        else:
            logger.error("\n❌ MIGRATIONS FAILED - See errors above")
            sys.exit(1)
            
    finally:
        await runner.close()


if __name__ == "__main__":
    asyncio.run(main())
