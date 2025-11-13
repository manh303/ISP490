import axios from 'axios';
import Cookies from 'js-cookie';

/** API root */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'https://ecommerce-dss-backend.onrender.com';

/** Axios instance trỏ tới /api */
const api = axios.create({
  baseURL: `${API_BASE_URL}/api`,
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

/* ------------------------- Interceptors ------------------------- */

/** Gắn Bearer token cho mọi request */
api.interceptors.request.use(
  (config) => {
    const token = Cookies.get('access_token');
    if (token) {
      config.headers = config.headers ?? {};
      (config.headers as any).Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

/* ------------------------- Type Definitions ------------------------- */

export interface ChartDataPoint {
  [key: string]: any;
}

export interface BaseChartResponse {
  chart_type: string;
  title: string;
  x_axis?: string;
  y_axis?: string | string[];
  y_axes?: string[];
  data: ChartDataPoint[];
}

// Top Rated Products
export interface TopRatedProduct {
  product_name: string;
  rating_avg: number;
  review_count: number;
  price: number;
  category: string;
}

export interface TopRatedProductsResponse extends BaseChartResponse {
  chart_type: 'bar';
  x_axis: 'product_name';
  y_axis: 'rating_avg';
  data: TopRatedProduct[];
}

export interface GetTopRatedProductsParams {
  limit?: number; // 1-100, default 20
}

// Rating Distribution
export interface RatingDistributionData {
  rating_bucket: number;
  product_count: number;
  avg_price: number;
  total_reviews: number;
}

export interface RatingDistributionResponse extends BaseChartResponse {
  chart_type: 'histogram';
  x_axis: 'rating_bucket';
  y_axis: 'product_count';
  data: RatingDistributionData[];
}

export interface GetRatingDistributionParams {
  category?: string;
}

// Review Trends
export interface ReviewTrendData {
  date: string;
  products_reviewed: number;
  avg_rating: number;
  total_reviews: number;
}

export interface ReviewTrendsResponse extends BaseChartResponse {
  chart_type: 'line';
  x_axis: 'date';
  y_axis: ['avg_rating', 'total_reviews'];
  data: ReviewTrendData[];
}

export interface GetReviewTrendsParams {
  days?: number; // 7-365, default 30
}

// Price vs Rating
export interface PriceVsRatingData {
  product_name: string;
  price: number;
  rating_avg: number;
  review_count: number;
  category: string;
}

export interface PriceVsRatingResponse extends BaseChartResponse {
  chart_type: 'scatter';
  x_axis: 'price';
  y_axis: 'rating_avg';
  size: 'review_count';
  data: PriceVsRatingData[];
}

export interface GetPriceVsRatingParams {
  category?: string;
}

// Category Performance
export interface CategoryPerformanceData {
  category: string;
  product_count: number;
  avg_rating: number;
  avg_price: number;
  total_reviews: number;
  high_rated_count: number;
}

export interface CategoryPerformanceResponse extends BaseChartResponse {
  chart_type: 'grouped_bar';
  x_axis: 'category';
  y_axes: ['avg_rating', 'product_count', 'avg_price'];
  data: CategoryPerformanceData[];
}

// Sentiment Distribution
export interface SentimentDistributionData {
  sentiment: string;
  product_count: number;
  review_count: number;
}

export interface SentimentDistributionResponse extends BaseChartResponse {
  chart_type: 'pie';
  label: 'sentiment';
  value: 'product_count';
  data: SentimentDistributionData[];
}

// Price Segments
export interface PriceSegmentData {
  price_segment: string;
  product_count: number;
  avg_rating: number;
  total_reviews: number;
  high_rated: number;
}

export interface PriceSegmentsResponse extends BaseChartResponse {
  chart_type: 'stacked_bar';
  x_axis: 'price_segment';
  y_axes: ['product_count', 'high_rated'];
  data: PriceSegmentData[];
}

// Platform Comparison
export interface PlatformComparisonData {
  platform: string;
  product_count: number;
  avg_rating: number;
  avg_price: number;
  total_reviews: number;
  high_rated_count: number;
}

export interface PlatformComparisonResponse extends BaseChartResponse {
  chart_type: 'grouped_bar';
  x_axis: 'platform';
  y_axes: ['product_count', 'avg_rating', 'total_reviews'];
  data: PlatformComparisonData[];
}

// Platform Price Comparison
export interface PlatformPriceComparisonData {
  platform: string;
  category: string;
  avg_price: number;
  min_price: number;
  max_price: number;
  product_count: number;
}

export interface PlatformPriceComparisonResponse extends BaseChartResponse {
  chart_type: 'grouped_bar';
  x_axis: 'category';
  y_axis: 'avg_price';
  group_by: 'platform';
  data: PlatformPriceComparisonData[];
}

export interface GetPlatformPriceComparisonParams {
  category?: string;
}

// Dashboard Summary
export interface DashboardSummary {
  total_products: number;
  overall_avg_rating: number;
  total_reviews: number;
  avg_price: number;
  total_categories: number;
  high_rated_products: number;
  popular_products: number;
  total_platforms: number;
}

export interface DashboardSummaryResponse {
  summary: DashboardSummary;
  timestamp: string;
}

/* ------------------------- API Functions ------------------------- */

/**
 * Get Top Rated Products
 * Top products by rating - Bar Chart
 * @param params - Query parameters (limit: 1-100, default 20)
 */
export const getTopRatedProducts = async (
  params?: GetTopRatedProductsParams
): Promise<TopRatedProductsResponse> => {
  const response = await api.get('/v1/analytics/products/top-rated', { params });
  return response.data;
};

/**
 * Get Rating Distribution
 * Rating distribution histogram
 * @param params - Query parameters (category: optional)
 */
export const getRatingDistribution = async (
  params?: GetRatingDistributionParams
): Promise<RatingDistributionResponse> => {
  const response = await api.get('/v1/analytics/products/rating-distribution', { params });
  return response.data;
};

/**
 * Get Review Trends
 * Review trends over time - Line Chart
 * @param params - Query parameters (days: 7-365, default 30)
 */
export const getReviewTrends = async (
  params?: GetReviewTrendsParams
): Promise<ReviewTrendsResponse> => {
  const response = await api.get('/v1/analytics/reviews/trends', { params });
  return response.data;
};

/**
 * Get Price vs Rating
 * Price vs Rating correlation - Scatter Plot
 * @param params - Query parameters (category: optional)
 */
export const getPriceVsRating = async (
  params?: GetPriceVsRatingParams
): Promise<PriceVsRatingResponse> => {
  const response = await api.get('/v1/analytics/products/price-vs-rating', { params });
  return response.data;
};

/**
 * Get Category Performance
 * Category performance comparison - Grouped Bar Chart
 */
export const getCategoryPerformance = async (): Promise<CategoryPerformanceResponse> => {
  const response = await api.get('/v1/analytics/products/category-performance');
  return response.data;
};

/**
 * Get Sentiment Distribution
 * Review sentiment distribution - Pie Chart
 */
export const getSentimentDistribution = async (): Promise<SentimentDistributionResponse> => {
  const response = await api.get('/v1/analytics/reviews/sentiment-distribution');
  return response.data;
};

/**
 * Get Price Segments
 * Price segment analysis - Stacked Bar Chart
 */
export const getPriceSegments = async (): Promise<PriceSegmentsResponse> => {
  const response = await api.get('/v1/analytics/products/price-segments');
  return response.data;
};

/**
 * Get Platform Comparison
 * Platform comparison - Tiki vs Lazada - Grouped Bar Chart
 */
export const getPlatformComparison = async (): Promise<PlatformComparisonResponse> => {
  const response = await api.get('/v1/analytics/platforms/comparison');
  return response.data;
};

/**
 * Get Platform Price Comparison
 * Platform price comparison by category - Box Plot data
 * @param params - Query parameters (category: optional)
 */
export const getPlatformPriceComparison = async (
  params?: GetPlatformPriceComparisonParams
): Promise<PlatformPriceComparisonResponse> => {
  const response = await api.get('/v1/analytics/platforms/price-comparison', { params });
  return response.data;
};

/**
 * Get Dashboard Summary
 * Dashboard summary metrics
 */
export const getDashboardSummary = async (): Promise<DashboardSummaryResponse> => {
  const response = await api.get('/v1/analytics/dashboard/summary');
  return response.data;
};

/* ------------------------- Helper Functions ------------------------- */

/**
 * Get all analytics data at once
 * Useful for initial dashboard load
 */
export const getAllAnalytics = async () => {
  try {
    const [
      summary,
      topRated,
      ratingDist,
      reviewTrends,
      priceVsRating,
      categoryPerf,
      sentiment,
      priceSegments,
      platformComp,
      platformPrice,
    ] = await Promise.all([
      getDashboardSummary(),
      getTopRatedProducts({ limit: 20 }),
      getRatingDistribution(),
      getReviewTrends({ days: 30 }),
      getPriceVsRating(),
      getCategoryPerformance(),
      getSentimentDistribution(),
      getPriceSegments(),
      getPlatformComparison(),
      getPlatformPriceComparison(),
    ]);

    return {
      summary,
      topRated,
      ratingDistribution: ratingDist,
      reviewTrends,
      priceVsRating,
      categoryPerformance: categoryPerf,
      sentimentDistribution: sentiment,
      priceSegments,
      platformComparison: platformComp,
      platformPriceComparison: platformPrice,
    };
  } catch (error) {
    console.error('Error fetching all analytics:', error);
    throw error;
  }
};

/**
 * Get analytics for a specific category
 */
export const getCategoryAnalytics = async (category: string) => {
  try {
    const [ratingDist, priceVsRating, platformPrice] = await Promise.all([
      getRatingDistribution({ category }),
      getPriceVsRating({ category }),
      getPlatformPriceComparison({ category }),
    ]);

    return {
      ratingDistribution: ratingDist,
      priceVsRating,
      platformPriceComparison: platformPrice,
    };
  } catch (error) {
    console.error(`Error fetching analytics for category ${category}:`, error);
    throw error;
  }
};

export default api;

