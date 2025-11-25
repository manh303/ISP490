import axios from 'axios';
import Cookies from 'js-cookie';

/** API root */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'https://isp490.onrender.com';

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
