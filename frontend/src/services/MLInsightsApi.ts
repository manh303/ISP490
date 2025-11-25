import axios from 'axios';
import Cookies from 'js-cookie';

/** API root (ví dụ: http://localhost:8000) */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'https://isp490.onrender.com';

/** Axios instance trỏ tới /api */
const api = axios.create({
  baseURL: `${API_BASE_URL}/api`,
  timeout: 10000,
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

/* ------------------------- Interfaces ------------------------- */

/** Price Optimization */
export interface PriceOptimization {
  product_sk: number;
  product_name: string;
  current_price: number;
  optimal_price: number;
  expected_margin_change: number;
  recommendation: 'Increase Price' | 'Decrease Price' | 'Maintain Price';
  price_position: string;
}

/** Demand Forecast */
export interface DemandForecast {
  product_sk: number;
  product_name: string;
  recent_demand: number;
  baseline_demand: number;
  demand_trend: 'Growing' | 'Declining' | 'Stable';
  forecast_7d: number;
  forecast_30d: number;
  quality_score: number;
  stock_recommendation: string;
}

/** Weekly Sales Forecast */
export interface WeeklySalesForecast {
  year: number;
  day_of_week: number;
  avg_reviews: number;
  avg_rating: number;
}

/** Sales Trend */
export interface SalesTrend {
  year: number;
  month: number;
  total_reviews: number;
  avg_rating: number;
  prev_month_reviews: number;
  growth_rate: number;
  trend: string;
}

/** Seasonality */
export interface Seasonality {
  season: string;
  avg_reviews: number;
  avg_rating: number;
  seasonality_index: number;
}

/** ML Summary */
export interface MLSummary {
  price_optimization: {
    increase: number;
    decrease: number;
    maintain: number;
  };
  demand_forecast: {
    growing: number;
    declining: number;
    stable: number;
  };
  total_products_analyzed: number;
}

/* ------------------------- API Functions ------------------------- */

/**
 * Get Price Optimization
 * @param limit - Maximum number of results (1-1000, default 100)
 * @param recommendation - Filter by recommendation type
 */
export const getPriceOptimization = async (
  limit: number = 100,
  recommendation?: 'Increase Price' | 'Decrease Price' | 'Maintain Price'
): Promise<PriceOptimization[]> => {
  const params: any = { limit };
  if (recommendation) {
    params.recommendation = recommendation;
  }
  
  const response = await api.get('/v1/ml/price-optimization', { params });
  return response.data;
};

/**
 * Get Demand Forecast
 * @param limit - Maximum number of results (1-1000, default 100)
 * @param trend - Filter by trend type
 */
export const getDemandForecast = async (
  limit: number = 100,
  trend?: 'Growing' | 'Declining' | 'Stable'
): Promise<DemandForecast[]> => {
  const params: any = { limit };
  if (trend) {
    params.trend = trend;
  }
  
  const response = await api.get('/v1/ml/demand-forecast', { params });
  return response.data;
};

/**
 * Get Weekly Sales Forecast
 */
export const getWeeklySalesForecast = async (): Promise<WeeklySalesForecast[]> => {
  const response = await api.get('/v1/ml/sales-forecast/weekly');
  return response.data;
};

/**
 * Get Sales Trend
 */
export const getSalesTrend = async (): Promise<SalesTrend[]> => {
  const response = await api.get('/v1/ml/sales-forecast/trend');
  return response.data;
};

/**
 * Get Seasonality
 */
export const getSeasonality = async (): Promise<Seasonality[]> => {
  const response = await api.get('/v1/ml/sales-forecast/seasonality');
  return response.data;
};

/**
 * Get ML Insights Summary
 */
export const getMLSummary = async (): Promise<MLSummary> => {
  const response = await api.get('/v1/ml/insights/summary');
  return response.data;
};

export default {
  getPriceOptimization,
  getDemandForecast,
  getWeeklySalesForecast,
  getSalesTrend,
  getSeasonality,
  getMLSummary,
};

