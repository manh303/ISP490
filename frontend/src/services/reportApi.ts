import axios from 'axios';
import Cookies from 'js-cookie';

/** API root */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

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

interface OverviewReportParams {
  from_date: string;
  to_date: string;
  platform_code?: string;
}

interface ProductsReportParams {
  from_date: string;
  to_date: string;
  platform_code?: string;
  metric?: 'revenue' | 'reviews' | 'rating' | 'price';
  limit?: number;
}

interface ReviewsReportParams {
  from_date: string;
  to_date: string;
  platform_code?: string;
  min_reviews?: number;
  limit?: number;
}

interface ReviewsDetailsReportParams {
  from_date: string;
  to_date: string;
  platform_code?: string;
  limit?: number;
}

interface ProductReviewsDetailsParams {
  product_id: string;
  from_date: string;
  to_date: string;
  platform_code?: string;
  limit?: number;
}

interface ProductsByCategoryParams {
  platform_code: string;
  from_date: string;
  to_date: string;
  category_id?: string;
  limit?: number;
}

interface ProductsByCategoryAllPlatformsParams {
  from_date: string;
  to_date: string;
  category_id?: string;
  limit?: number;
}

/* ------------------------- API Functions ------------------------- */

/** Export Overview Report */
export const exportOverviewReport = async (params: OverviewReportParams): Promise<Blob> => {
  const response = await api.get('/v1/reports/overview', {
    params,
    responseType: 'blob',
  });
  return response.data;
};

/** Export Products Report */
export const exportProductsReport = async (params: ProductsReportParams): Promise<Blob> => {
  const response = await api.get('/v1/reports/products', {
    params,
    responseType: 'blob',
  });
  return response.data;
};

/** Export Reviews/Sentiment Report */
export const exportReviewsReport = async (params: ReviewsReportParams): Promise<Blob> => {
  const response = await api.get('/v1/reports/reviews', {
    params,
    responseType: 'blob',
  });
  return response.data;
};

/** Export Reviews Details Report */
export const exportReviewsDetailsReport = async (params: ReviewsDetailsReportParams): Promise<Blob> => {
  const response = await api.get('/v1/reports/reviews-details', {
    params,
    responseType: 'blob',
  });
  return response.data;
};

/** Export Reviews Details for Specific Product */
export const exportProductReviewsDetails = async (params: ProductReviewsDetailsParams): Promise<Blob> => {
  const response = await api.get('/v1/reports/product-reviews-details', {
    params,
    responseType: 'blob',
  });
  return response.data;
};

/** Export Products by Category for Specific Platform */
export const exportProductsByCategory = async (params: ProductsByCategoryParams): Promise<Blob> => {
  const response = await api.get('/v1/reports/products-by-category', {
    params,
    responseType: 'blob',
  });
  return response.data;
};

/** Export Products by Category for All Platforms */
export const exportProductsByCategoryAllPlatforms = async (params: ProductsByCategoryAllPlatformsParams): Promise<Blob> => {
  const response = await api.get('/v1/reports/products-by-category-all-platforms', {
    params,
    responseType: 'blob',
  });
  return response.data;
};
