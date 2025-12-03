import unittest
from unittest.mock import AsyncMock, MagicMock
from datetime import date
from app.services.analytics_service import AnalyticsService

class TestAnalystService(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """Setup mock database"""
        self.db = MagicMock()
        self.db.fetchrow = AsyncMock()
        self.service = AnalyticsService(self.db)
    
    async def test_get_overview_kpis_normal_range_returns_kpis(self):
        """Test get_overview_kpis with normal date range returns correct KPIs"""
        # Mock database to return KPI data
        fake_kpi_row = {
            'total_revenue': 1500000.50,
            'total_products': 250,
            'total_reviews': 3500,
            'avg_price': 450000.75,
            'avg_rating': 4.5,
            'category_name': 'Điện thoại & Phụ kiện'
        }
        self.db.fetchrow.return_value = fake_kpi_row
        
        # Call service method
        result = await self.service.get_overview_kpis(
            from_date=date(2025, 1, 1),
            to_date=date(2025, 1, 31),
            platform_code='LAZADA',
            category_key='123'
        )
        
        # Verify result mapping
        self.assertEqual(result.from_date, date(2025, 1, 1))
        self.assertEqual(result.to_date, date(2025, 1, 31))
        self.assertEqual(result.platform_code, 'LAZADA')
        self.assertEqual(result.category_key, '123')
        self.assertEqual(result.category_name, 'Điện thoại & Phụ kiện')
        self.assertEqual(result.total_revenue, 1500000.50)
        self.assertEqual(result.total_products, 250)
        self.assertEqual(result.total_reviews, 3500)
        self.assertAlmostEqual(result.avg_price, 450000.75, places=2)
        self.assertAlmostEqual(result.avg_rating, 4.5, places=1)
        
        # Verify database was called with correct SQL
        self.db.fetchrow.assert_awaited_once()
        call_args = self.db.fetchrow.call_args
        sql = call_args[0][0]
        params = call_args[0][1:]
        
        # Check SQL contains expected clauses
        self.assertIn('SELECT', sql)
        self.assertIn('total_revenue', sql)
        self.assertIn('total_products', sql)
        self.assertIn('avg_price', sql)
        self.assertIn('avg_rating', sql)
        self.assertIn('BETWEEN', sql)
        
        # Check parameters
        self.assertEqual(params[0], date(2025, 1, 1))
        self.assertEqual(params[1], date(2025, 1, 31))
        self.assertIn('LAZADA', params)
        self.assertIn(123, params)

    async def test_get_overview_kpis_no_data_returns_zero_kpis(self):
        """Test get_overview_kpis returns zero KPIs when no data found"""
        # Mock database to return row with NULL/zero values
        fake_empty_row = {
            'total_revenue': None,  # or 0
            'total_products': None,
            'total_reviews': None,
            'avg_price': None,
            'avg_rating': None,
            'category_name': None
        }
        self.db.fetchrow.return_value = fake_empty_row
        
        # Call service method
        result = await self.service.get_overview_kpis(
            from_date=date(2025, 2, 1),
            to_date=date(2025, 2, 28),
            platform_code='TIKI',
            category_key='456'
        )
        
        # Verify result has zero values
        self.assertIsNotNone(result)
        self.assertEqual(result.total_revenue, 0.0)
        self.assertEqual(result.total_products, 0)
        self.assertEqual(result.total_reviews, 0)
        self.assertIsNone(result.avg_price)  # _safe_float(None) = None
        self.assertIsNone(result.avg_rating)
        self.assertIsNone(result.category_name)
        
        # Verify database was called
        self.db.fetchrow.assert_awaited_once()

    
    async def test_get_top_products_returns_sorted_by_metric(self):
        """Test get_top_products returns products sorted by specified metric"""
        # Mock database to return list of products
        fake_products = [
            {
                'product_key': 'P001',
                'product_name': 'iPhone 15 Pro Max',
                'platform_code': 'LAZADA',
                'category_key': 100,
                'category_name': 'Điện thoại',
                'total_revenue': 5000000000,  # Highest revenue
                'total_reviews': 1500,
                'avg_rating': 4.8,
                'avg_price': 32000000
            },
            {
                'product_key': 'P002',
                'product_name': 'Samsung Galaxy S24',
                'platform_code': 'SHOPEE',
                'category_key': 100,
                'category_name': 'Điện thoại',
                'total_revenue': 3000000000,  # Second highest
                'total_reviews': 1200,
                'avg_rating': 4.6,
                'avg_price': 25000000
            },
            {
                'product_key': 'P003',
                'product_name': 'Xiaomi 14',
                'platform_code': 'TIKI',
                'category_key': 100,
                'category_name': 'Điện thoại',
                'total_revenue': 1500000000,  # Third
                'total_reviews': 800,
                'avg_rating': 4.5,
                'avg_price': 18000000
            }
        ]
        self.db.fetch = AsyncMock(return_value=fake_products)
        
        # Call service with revenue metric
        result = await self.service.get_top_products(
            from_date=date(2025, 1, 1),
            to_date=date(2025, 1, 31),
            metric='revenue',
            platform_code=None,
            category_key=None,
            limit=10
        )
        
        # Verify results are sorted by revenue (descending)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].product_key, 'P001')
        self.assertEqual(result[0].total_revenue, 5000000000)
        self.assertEqual(result[1].product_key, 'P002')
        self.assertEqual(result[1].total_revenue, 3000000000)
        self.assertEqual(result[2].product_key, 'P003')
        self.assertEqual(result[2].total_revenue, 1500000000)
        
        # Verify all fields are mapped correctly
        self.assertEqual(result[0].product_name, 'iPhone 15 Pro Max')
        self.assertEqual(result[0].platform_code, 'LAZADA')
        self.assertEqual(result[0].total_reviews, 1500)
        self.assertAlmostEqual(result[0].avg_rating, 4.8, places=1)
        
        # Verify database was called with correct SQL
        self.db.fetch.assert_awaited_once()
        call_args = self.db.fetch.call_args
        sql = call_args[0][0]
        
        # Check SQL contains ORDER BY revenue metric
        self.assertIn('ORDER BY', sql)
        self.assertIn('DESC', sql)
        self.assertIn('LIMIT', sql)
    
    async def test_get_top_products_empty_list(self):
        """Test get_top_products returns empty list when no products found"""
        # Mock database to return empty list
        self.db.fetch = AsyncMock(return_value=[])
        
        # Call service method
        result = await self.service.get_top_products(
            from_date=date(2025, 6, 1),
            to_date=date(2025, 6, 30),
            metric='revenue',
            platform_code='UNKNOWN_PLATFORM',
            category_key='999',
            limit=10
        )
        
        # Verify empty list is returned
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 0)
        
        # Verify database was called
        self.db.fetch.assert_awaited_once()

if __name__ == '__main__':
    unittest.main()