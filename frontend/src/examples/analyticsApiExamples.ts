/**
 * Analytics API Usage Examples
 * 
 * This file demonstrates how to use the analytics API functions
 * Similar to roleApiExamples.ts but for analytics endpoints
 */

import {
  // API Functions
  getTopRatedProducts,
  getRatingDistribution,
  getReviewTrends,
  getPriceVsRating,
  getCategoryPerformance,
  getSentimentDistribution,
  getPriceSegments,
  getPlatformComparison,
  getPlatformPriceComparison,
  getDashboardSummary,
  getAllAnalytics,
  getCategoryAnalytics,
  // Types
  type TopRatedProductsResponse,
  type RatingDistributionResponse,
  type ReviewTrendsResponse,
  type PriceVsRatingResponse,
  type CategoryPerformanceResponse,
  type SentimentDistributionResponse,
  type PriceSegmentsResponse,
  type PlatformComparisonResponse,
  type PlatformPriceComparisonResponse,
  type DashboardSummaryResponse,
} from '../services/analyticsApi';

/* ============================================
   EXAMPLE 1: Get Dashboard Summary
   ============================================ */
export const exampleGetDashboardSummary = async () => {
  try {
    const data: DashboardSummaryResponse = await getDashboardSummary();
    
    console.log('Dashboard Summary:', {
      totalProducts: data.summary.total_products,
      avgRating: data.summary.overall_avg_rating,
      totalReviews: data.summary.total_reviews,
      avgPrice: data.summary.avg_price,
      categories: data.summary.total_categories,
      highRatedProducts: data.summary.high_rated_products,
      popularProducts: data.summary.popular_products,
      platforms: data.summary.total_platforms,
      timestamp: data.timestamp,
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching dashboard summary:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 2: Get Top Rated Products
   ============================================ */
export const exampleGetTopRatedProducts = async () => {
  try {
    // Get top 10 products
    const data: TopRatedProductsResponse = await getTopRatedProducts({ limit: 10 });
    
    console.log('Top Rated Products:');
    console.log(`Chart Type: ${data.chart_type}`);
    console.log(`Title: ${data.title}`);
    
    data.data.forEach((product, index) => {
      console.log(`${index + 1}. ${product.product_name}`);
      console.log(`   Rating: ${product.rating_avg} ⭐`);
      console.log(`   Reviews: ${product.review_count}`);
      console.log(`   Price: ${product.price.toLocaleString('vi-VN')} VND`);
      console.log(`   Category: ${product.category}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching top rated products:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 3: Get Rating Distribution
   ============================================ */
export const exampleGetRatingDistribution = async (category?: string) => {
  try {
    const data: RatingDistributionResponse = await getRatingDistribution(
      category ? { category } : undefined
    );
    
    console.log(`Rating Distribution ${category ? `for ${category}` : '(All Categories)'}`);
    console.log(`Chart Type: ${data.chart_type}`);
    
    data.data.forEach((bucket) => {
      console.log(`Rating ${bucket.rating_bucket}: ${bucket.product_count} products`);
      console.log(`  Avg Price: ${bucket.avg_price.toLocaleString('vi-VN')} VND`);
      console.log(`  Total Reviews: ${bucket.total_reviews}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching rating distribution:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 4: Get Review Trends
   ============================================ */
export const exampleGetReviewTrends = async (days: number = 30) => {
  try {
    const data: ReviewTrendsResponse = await getReviewTrends({ days });
    
    console.log(`Review Trends - Last ${days} Days`);
    console.log(`Chart Type: ${data.chart_type}`);
    
    data.data.forEach((trend) => {
      console.log(`Date: ${trend.date}`);
      console.log(`  Products Reviewed: ${trend.products_reviewed}`);
      console.log(`  Avg Rating: ${trend?.avg_rating?.toFixed(2)}`);
      console.log(`  Total Reviews: ${trend.total_reviews}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching review trends:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 5: Get Price vs Rating
   ============================================ */
export const exampleGetPriceVsRating = async (category?: string) => {
  try {
    const data: PriceVsRatingResponse = await getPriceVsRating(
      category ? { category } : undefined
    );
    
    console.log(`Price vs Rating ${category ? `for ${category}` : '(All Categories)'}`);
    console.log(`Chart Type: ${data.chart_type} (Scatter Plot)`);
    console.log(`Total Products: ${data.data.length}`);
    
    // Show first 5 products
    data.data.slice(0, 5).forEach((product, index) => {
      console.log(`${index + 1}. ${product.product_name}`);
      console.log(`   Price: ${product.price.toLocaleString('vi-VN')} VND`);
      console.log(`   Rating: ${product.rating_avg}`);
      console.log(`   Reviews: ${product.review_count}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching price vs rating:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 6: Get Category Performance
   ============================================ */
export const exampleGetCategoryPerformance = async () => {
  try {
    const data: CategoryPerformanceResponse = await getCategoryPerformance();
    
    console.log('Category Performance Analysis');
    console.log(`Chart Type: ${data.chart_type}`);
    
    data.data.forEach((category) => {
      console.log(`\n${category.category.toUpperCase()}`);
      console.log(`  Products: ${category.product_count}`);
      console.log(`  Avg Rating: ${category.avg_rating.toFixed(2)}`);
      console.log(`  Avg Price: ${category.avg_price.toLocaleString('vi-VN')} VND`);
      console.log(`  Total Reviews: ${category.total_reviews}`);
      console.log(`  High Rated: ${category.high_rated_count}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching category performance:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 7: Get Sentiment Distribution
   ============================================ */
export const exampleGetSentimentDistribution = async () => {
  try {
    const data: SentimentDistributionResponse = await getSentimentDistribution();
    
    console.log('Sentiment Distribution');
    console.log(`Chart Type: ${data.chart_type} (Pie Chart)`);
    
    const total = data.data.reduce((sum, item) => sum + item.product_count, 0);
    
    data.data.forEach((sentiment) => {
      const percentage = ((sentiment.product_count / total) * 100).toFixed(2);
      console.log(`${sentiment.sentiment}:`);
      console.log(`  Products: ${sentiment.product_count} (${percentage}%)`);
      console.log(`  Reviews: ${sentiment.review_count}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching sentiment distribution:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 8: Get Price Segments
   ============================================ */
export const exampleGetPriceSegments = async () => {
  try {
    const data: PriceSegmentsResponse = await getPriceSegments();
    
    console.log('Price Segment Analysis');
    console.log(`Chart Type: ${data.chart_type}`);
    
    data.data.forEach((segment) => {
      console.log(`\n${segment.price_segment}`);
      console.log(`  Products: ${segment.product_count}`);
      console.log(`  Avg Rating: ${segment.avg_rating.toFixed(2)}`);
      console.log(`  Total Reviews: ${segment.total_reviews}`);
      console.log(`  High Rated: ${segment.high_rated}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching price segments:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 9: Get Platform Comparison
   ============================================ */
export const exampleGetPlatformComparison = async () => {
  try {
    const data: PlatformComparisonResponse = await getPlatformComparison();
    
    console.log('Platform Comparison: Tiki vs Lazada');
    console.log(`Chart Type: ${data.chart_type}`);
    
    data.data.forEach((platform) => {
      console.log(`\n${platform.platform.toUpperCase()}`);
      console.log(`  Products: ${platform.product_count}`);
      console.log(`  Avg Rating: ${platform.avg_rating.toFixed(2)}`);
      console.log(`  Avg Price: ${platform.avg_price.toLocaleString('vi-VN')} VND`);
      console.log(`  Total Reviews: ${platform.total_reviews}`);
      console.log(`  High Rated: ${platform.high_rated_count}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching platform comparison:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 10: Get Platform Price Comparison
   ============================================ */
export const exampleGetPlatformPriceComparison = async (category?: string) => {
  try {
    const data: PlatformPriceComparisonResponse = await getPlatformPriceComparison(
      category ? { category } : undefined
    );
    
    console.log(`Platform Price Comparison ${category ? `for ${category}` : '(All Categories)'}`);
    console.log(`Chart Type: ${data.chart_type}`);
    
    data.data.forEach((item) => {
      console.log(`\n${item.platform} - ${item.category}`);
      console.log(`  Avg Price: ${item?.avg_price?.toLocaleString('vi-VN')} VND`);
      console.log(`  Min Price: ${item.min_price.toLocaleString('vi-VN')} VND`);
      console.log(`  Max Price: ${item.max_price.toLocaleString('vi-VN')} VND`);
      console.log(`  Products: ${item.product_count}`);
    });
    
    return data;
  } catch (error) {
    console.error('Error fetching platform price comparison:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 11: Get All Analytics at Once
   ============================================ */
export const exampleGetAllAnalytics = async () => {
  try {
    console.log('Fetching all analytics data...');
    const data = await getAllAnalytics();
    
    console.log('All Analytics Data Loaded:');
    console.log(`- Dashboard Summary: ✓`);
    console.log(`- Top Rated Products: ${data.topRated.data.length} items`);
    console.log(`- Rating Distribution: ${data.ratingDistribution.data.length} buckets`);
    console.log(`- Review Trends: ${data.reviewTrends.data.length} data points`);
    console.log(`- Price vs Rating: ${data.priceVsRating.data.length} products`);
    console.log(`- Category Performance: ${data.categoryPerformance.data.length} categories`);
    console.log(`- Sentiment Distribution: ${data.sentimentDistribution.data.length} sentiments`);
    console.log(`- Price Segments: ${data.priceSegments.data.length} segments`);
    console.log(`- Platform Comparison: ${data.platformComparison.data.length} platforms`);
    console.log(`- Platform Price Comparison: ${data.platformPriceComparison.data.length} items`);
    
    return data;
  } catch (error) {
    console.error('Error fetching all analytics:', error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 12: Get Category-Specific Analytics
   ============================================ */
export const exampleGetCategoryAnalytics = async (category: string) => {
  try {
    console.log(`Fetching analytics for category: ${category}...`);
    const data = await getCategoryAnalytics(category);
    
    console.log(`Category Analytics for ${category}:`);
    console.log(`- Rating Distribution: ${data.ratingDistribution.data.length} buckets`);
    console.log(`- Price vs Rating: ${data.priceVsRating.data.length} products`);
    console.log(`- Platform Price Comparison: ${data.platformPriceComparison.data.length} items`);
    
    return data;
  } catch (error) {
    console.error(`Error fetching analytics for ${category}:`, error);
    throw error;
  }
};

/* ============================================
   EXAMPLE 13: Usage in React Component
   ============================================ */
export const reactComponentExample = `
import React, { useEffect, useState } from 'react';
import { getDashboardSummary, getTopRatedProducts } from '../services/analyticsApi';
import type { DashboardSummaryResponse, TopRatedProductsResponse } from '../services/analyticsApi';

const AnalyticsDashboard = () => {
  const [summary, setSummary] = useState<DashboardSummaryResponse | null>(null);
  const [topProducts, setTopProducts] = useState<TopRatedProductsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const [summaryData, topProductsData] = await Promise.all([
          getDashboardSummary(),
          getTopRatedProducts({ limit: 10 })
        ]);
        
        setSummary(summaryData);
        setTopProducts(topProductsData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;
  if (!summary || !topProducts) return null;

  return (
    <div>
      <h1>Analytics Dashboard</h1>
      
      {/* Summary Cards */}
      <div className="summary-cards">
        <div>Total Products: {summary.summary.total_products}</div>
        <div>Avg Rating: {summary.summary.overall_avg_rating.toFixed(2)}</div>
        <div>Total Reviews: {summary.summary.total_reviews}</div>
      </div>

      {/* Top Products */}
      <div className="top-products">
        <h2>{topProducts.title}</h2>
        {topProducts.data.map((product, index) => (
          <div key={index}>
            <h3>{product.product_name}</h3>
            <p>Rating: {product.rating_avg} ⭐</p>
            <p>Reviews: {product.review_count}</p>
            <p>Price: {product.price.toLocaleString('vi-VN')} VND</p>
          </div>
        ))}
      </div>
    </div>
  );
};

export default AnalyticsDashboard;
`;

/* ============================================
   EXAMPLE 14: Error Handling
   ============================================ */
export const exampleWithErrorHandling = async () => {
  try {
    const data = await getTopRatedProducts({ limit: 100 });
    return data;
  } catch (error: any) {
    if (error.response) {
      // Server responded with error
      console.error('Server Error:', {
        status: error.response.status,
        message: error.response.data.message || 'Unknown error',
        data: error.response.data,
      });
    } else if (error.request) {
      // Request made but no response
      console.error('Network Error:', 'No response from server');
    } else {
      // Other errors
      console.error('Error:', error.message);
    }
    throw error;
  }
};

/* ============================================
   EXAMPLE 15: Running All Examples
   ============================================ */
export const runAllExamples = async () => {
  console.log('\n========================================');
  console.log('Running All Analytics API Examples');
  console.log('========================================\n');

  try {
    await exampleGetDashboardSummary();
    console.log('\n---\n');
    
    await exampleGetTopRatedProducts();
    console.log('\n---\n');
    
    await exampleGetRatingDistribution();
    console.log('\n---\n');
    
    await exampleGetReviewTrends(7);
    console.log('\n---\n');
    
    await exampleGetCategoryPerformance();
    console.log('\n---\n');
    
    await exampleGetSentimentDistribution();
    console.log('\n---\n');
    
    await exampleGetPriceSegments();
    console.log('\n---\n');
    
    await exampleGetPlatformComparison();
    console.log('\n---\n');
    
    // Category-specific examples
    await exampleGetCategoryAnalytics('laptops');
    console.log('\n---\n');
    
    console.log('All examples completed successfully! ✓');
  } catch (error) {
    console.error('Error running examples:', error);
  }
};
