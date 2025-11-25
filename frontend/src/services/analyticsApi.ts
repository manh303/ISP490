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

// Filters
export interface Platform {
  platform_code: string;
  platform_name: string;
}

export interface Category {
  category_key: string;
  category_name: string;
  level: number | null;
  parent_key: string | null;
  platform_code: string | null;
}

export interface Product {
  product_key: string;
  product_name: string;
  platform_code: string;
  category_key: string | null;
}

export interface GetCategoriesParams {
  platform_code?: string;
  parent_category_key?: string;
}

export interface GetProductsParams {
  q: string;
  platform_code?: string;
  category_key?: string;
  limit?: number;
}

// Overview
export interface OverviewKPIs {
  from_date: string;
  to_date: string;
  platform_code?: string;
  category_key?: string;
  total_revenue: number;
  total_products: number;
  total_reviews: number;
  avg_price: number;
  avg_rating: number;
}

export interface GetOverviewKPIsParams {
  from_date: string;
  to_date: string;
  platform_code?: string;
  category_key?: string;
}

export interface OverviewTrendPoint {
  date: string;
  revenue: number;
  total_orders: number;
  avg_price: number;
  avg_rating: number;
  total_reviews: number;
}

export interface OverviewTrends {
  from_date: string;
  to_date: string;
  platform_code?: string;
  category_key?: string;
  points: OverviewTrendPoint[];
}

export interface GetOverviewTrendsParams {
  from_date: string;
  to_date: string;
  platform_code?: string;
  category_key?: string;
}

// Platforms
export interface PlatformComparisonItem {
  platform_code: string;
  platform_name: string;
  total_revenue: number;
  total_products: number;
  avg_price: number;
  avg_rating: number;
  total_reviews: number;
}

export interface GetPlatformComparisonParams {
  from_date: string;
  to_date: string;
  category_key?: string;
}

export interface CategoryShareItem {
  category_key: string;
  category_name: string;
  platform_code: string;
  revenue: number;
  revenue_share: number;
}

export interface GetCategoryShareParams {
  from_date: string;
  to_date: string;
  platform_code: string;
}

// Products
export interface TopProduct {
  product_key: string;
  product_name: string;
  platform_code: string;
  category_key: string;
  total_revenue: number;
  total_reviews: number;
  avg_rating: number;
  avg_price: number;
}

export interface GetTopProductsParams {
  from_date: string;
  to_date: string;
  metric?: 'revenue' | 'review_count' | 'avg_rating' | 'price_growth';
  platform_code?: string;
  category_key?: string;
  limit?: number;
}

export interface ProductTimeseriesPoint {
  date: string;
  avg_price: number;
  min_price: number;
  max_price: number;
  total_reviews: number;
  avg_rating: number;
  revenue: number;
}

export interface ProductTimeseries {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
  points: ProductTimeseriesPoint[];
}

export interface GetProductTimeseriesParams {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
}

export interface ProductReviewSummary {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
  total_reviews: number;
  avg_rating: number;
  rating_breakdown: {
    by_rating: { [key: string]: number };
  };
  top_helpful_reviews: any[]; // Adjust based on actual structure
}

export interface GetProductReviewSummaryParams {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
  top_n?: number;
}

// Pricing
export interface PriceDistribution {
  platform_code: string;
  category_key?: string;
  from_date: string;
  to_date: string;
  min_price: number;
  p25_price: number;
  median_price: number;
  p75_price: number;
  max_price: number;
}

export interface GetPriceDistributionParams {
  from_date: string;
  to_date: string;
  platform_code: string;
  category_key?: string;
}

export interface PriceVsRevenueItem {
  product_key: string;
  product_name: string;
  platform_code: string;
  category_key: string;
  avg_price: number;
  total_revenue: number;
  avg_rating: number;
  total_reviews: number;
}

export interface GetPriceVsRevenueParams {
  from_date: string;
  to_date: string;
  platform_code: string;
  category_key?: string;
  limit?: number;
}

export interface PriceVsRatingData {
  product_key: string;
  product_name: string;
  platform_code: string;
  category_key: string | null;
  avg_price: number;
  total_revenue: number;
  avg_rating: number | null;
  total_reviews: number;
}

export interface CategoryPerformanceData {
  category: string;
  product_count: number;
  avg_rating: number;
  high_rated_count: number;
  total_reviews: number;
}

// Reports
export interface OverviewReport {
  from_date: string;
  to_date: string;
  platform_code?: string;
  category_key?: string;
  kpis: OverviewKPIs;
  trends: OverviewTrends;
  platform_comparison: PlatformComparisonItem[];
  category_share: CategoryShareItem[];
}

export interface GetOverviewReportParams {
  from_date: string;
  to_date: string;
  platform_code?: string;
  category_key?: string;
}

export interface ProductReport {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
  timeseries: ProductTimeseries;
  review_summary: ProductReviewSummary;
}

export interface GetProductReportParams {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
}

/* ------------------------- API Functions ------------------------- */

/**
 * Get Platforms
 * List Platforms
 */
export const getPlatforms = async (): Promise<Platform[]> => {
  const response = await api.get('/v1/analytics/filters/platforms');
  return response.data;
};

/**
 * Get Categories
 * List Categories
 * @param params - Query parameters
 */
export const getCategories = async (
  params?: GetCategoriesParams
): Promise<Category[]> => {
  const response = await api.get('/v1/analytics/filters/categories', { params });
  return response.data;
};

/**
 * Search Products
 * Search Products
 * @param params - Query parameters
 */
export const getProducts = async (
  params: GetProductsParams
): Promise<Product[]> => {
  const response = await api.get('/v1/analytics/filters/products', { params });
  return response.data;
};

/**
 * Get Overview KPIs
 * Get Overview Kpis
 * @param params - Query parameters
 */
export const getOverviewKPIs = async (
  params: GetOverviewKPIsParams
): Promise<OverviewKPIs> => {
  const response = await api.get('/v1/analytics/overview/kpis', { params });
  return response.data;
};

/**
 * Get Overview Trends
 * Get Overview Trends
 * @param params - Query parameters
 */
export const getOverviewTrends = async (
  params: GetOverviewTrendsParams
): Promise<OverviewTrends> => {
  const response = await api.get('/v1/analytics/overview/trends', { params });
  return response.data;
};

/**
 * Compare Platforms
 * Compare Platforms
 * @param params - Query parameters
 */
export const getPlatformComparison = async (
  params: GetPlatformComparisonParams
): Promise<PlatformComparisonItem[]> => {
  const response = await api.get('/v1/analytics/platforms/comparison', { params });
  const result = response.data;
  if (Array.isArray(result)) return result;
  if (result.data && Array.isArray(result.data)) return result.data;
  if (result.platforms && Array.isArray(result.platforms)) return result.platforms;
  return [];
};

/**
 * Get Category Share
 * Get Category Share
 * @param params - Query parameters
 */
export const getCategoryShare = async (
  params: GetCategoryShareParams
): Promise<CategoryShareItem[]> => {
  const response = await api.get('/v1/analytics/platforms/category-share', { params });
  return response.data;
};

/**
 * Get Top Products
 * Get Top Products
 * @param params - Query parameters
 */
export const getTopProducts = async (
  params: GetTopProductsParams
): Promise<TopProduct[]> => {
  const response = await api.get('/v1/analytics/products/top', { params });
  return response.data;
};

/**
 * Get Product Timeseries
 * Get Product Timeseries
 * @param params - Query parameters
 */
export const getProductTimeseries = async (
  params: GetProductTimeseriesParams
): Promise<ProductTimeseries> => {
  const { product_key, ...queryParams } = params;
  const response = await api.get(`/v1/analytics/products/${product_key}/timeseries`, { params: queryParams });
  return response.data;
};

/**
 * Get Product Review Summary
 * Get Product Review Summary
 * @param params - Query parameters
 */
export const getProductReviewSummary = async (
  params: GetProductReviewSummaryParams
): Promise<ProductReviewSummary> => {
  const { product_key, ...queryParams } = params;
  const response = await api.get(`/v1/analytics/products/${product_key}/reviews/summary`, { params: queryParams });
  return response.data;
};

/**
 * Get Price Distribution
 * Get Price Distribution
 * @param params - Query parameters
 */
export const getPriceDistribution = async (
  params: GetPriceDistributionParams
): Promise<PriceDistribution> => {
  const response = await api.get('/v1/analytics/pricing/price-distribution', { params });
  return response.data;
};

/**
 * Get Price Vs Revenue
 * Get Price Vs Revenue
 * @param params - Query parameters
 */
export const getPriceVsRevenue = async (
  params: GetPriceVsRevenueParams
): Promise<PriceVsRevenueItem[]> => {
  const response = await api.get('/v1/analytics/pricing/price-vs-revenue', { params });
  return response.data;
};

/**
 * Get Overview Report
 * Get Overview Report
 * @param params - Query parameters
 */
export const getOverviewReport = async (
  params: GetOverviewReportParams
): Promise<OverviewReport> => {
  const response = await api.get('/v1/analytics/report/overview', { params });
  return response.data;
};

/**
 * Get Product Report
 * Get Product Report
 * @param params - Query parameters
 */
export const getProductReport = async (
  params: GetProductReportParams
): Promise<ProductReport> => {
  const response = await api.get('/v1/analytics/report/product', { params });
  return response.data;
};

/* ------------------------- Helper Functions ------------------------- */

/**
 * Get all overview data at once
 * Useful for initial dashboard load
 */
export const getAllOverviewData = async (params: GetOverviewReportParams) => {
  try {
    const report = await getOverviewReport(params);
    return report;
  } catch (error) {
    console.error('Error fetching overview data:', error);
    throw error;
  }
};

/**
 * Get analytics for a specific category
 */
export const getCategoryAnalytics = async (categoryKey: string, fromDate: string, toDate: string) => {
  try {
    const [kpis, trends] = await Promise.all([
      getOverviewKPIs({ from_date: fromDate, to_date: toDate, category_key: categoryKey }),
      getOverviewTrends({ from_date: fromDate, to_date: toDate, category_key: categoryKey }),
    ]);

    return {
      kpis,
      trends,
    };
  } catch (error) {
    console.error(`Error fetching analytics for category ${categoryKey}:`, error);
    throw error;
  }
};

export default api;

