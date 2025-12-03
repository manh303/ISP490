import React from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../../ui/figma/select';
import { Card, CardContent, CardHeader, CardTitle } from '../../../ui/figma/card';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

interface OverviewReport {
  trends: {
    points: any[];
  };
  platform_comparison: Array<{
    platform_code: string;
    platform_name: string;
    total_revenue: number;
    total_reviews: number;
    avg_price: number;
    avg_rating?: number;
  }>;
  category_share: Array<{
    category_name: string;
    revenue_share: number;
  }>;
}

interface AdminDashboardChartsProps {
  overviewReport: OverviewReport | null;
  selectedMetric: 'revenue' | 'reviews' | 'price' | 'rating';
  selectedPlatform: string;
  formatCurrency: (amount: number) => string;
  formatNumber: (num: number) => string;
  onMetricChange: (value: 'revenue' | 'reviews' | 'price' | 'rating') => void;
}

export default function AdminDashboardCharts({
  overviewReport,
  selectedMetric,
  selectedPlatform,
  formatCurrency,
  formatNumber,
  onMetricChange,
}: AdminDashboardChartsProps) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      {/* Overview Trend Chart */}
      <div className="lg:col-span-2">
        <Card>
          <CardHeader>
            <CardTitle>Tổng quan xu hướng</CardTitle>
            <Select value={selectedMetric} onValueChange={(value: any) => onMetricChange(value)}>
              <SelectTrigger className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="revenue">Doanh thu</SelectItem>
                <SelectItem value="reviews">Đánh giá</SelectItem>
                <SelectItem value="price">Giá</SelectItem>
                <SelectItem value="rating">Đánh giá</SelectItem>
              </SelectContent>
            </Select>
          </CardHeader>
          <CardContent>
            {overviewReport?.trends.points && overviewReport.trends.points.length > 0 ? (
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={overviewReport.trends.points}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="date" 
                      tick={{ fontSize: 12 }}
                      tickFormatter={(value) => new Date(value).toLocaleDateString('vi-VN')}
                    />
                    <YAxis 
                      tick={{ fontSize: 12 }}
                      tickFormatter={(value) => {
                        if (selectedMetric === 'revenue') return formatCurrency(value);
                        if (selectedMetric === 'price') return formatCurrency(value);
                        if (selectedMetric === 'rating') return value.toFixed(1);
                        return formatNumber(value);
                      }}
                    />
                    <Tooltip 
                      labelFormatter={(value) => `Ngày: ${new Date(value).toLocaleDateString('vi-VN')}`}
                      formatter={(value: number) => {
                        if (selectedMetric === 'revenue') return [formatCurrency(value), 'Doanh thu'];
                        if (selectedMetric === 'reviews') return [formatNumber(value), 'Đánh giá'];
                        if (selectedMetric === 'price') return [formatCurrency(value), 'Giá TB'];
                        if (selectedMetric === 'rating') return [value.toFixed(1), 'Đánh giá TB'];
                        return [value, selectedMetric];
                      }}
                    />
                    <Line 
                      type="monotone" 
                      dataKey={
                        selectedMetric === 'revenue' ? 'revenue' :
                        selectedMetric === 'reviews' ? 'total_reviews' :
                        selectedMetric === 'price' ? 'avg_price' :
                        selectedMetric === 'rating' ? 'avg_rating' : 'revenue'
                      } 
                      stroke="#3b82f6" 
                      strokeWidth={2}
                      dot={{ fill: '#3b82f6', strokeWidth: 2, r: 4 }}
                      activeDot={{ r: 6 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <div className="h-64 flex items-center justify-center text-gray-500">
                Không có dữ liệu xu hướng
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Platform Comparison & Category Share */}
      <div className="space-y-4">
        {/* Platform Comparison */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">So sánh nền tảng</CardTitle>
          </CardHeader>
          <CardContent>
            {overviewReport?.platform_comparison.map(platform => (
              <div key={platform.platform_code} className="flex justify-between items-center py-2 border-b last:border-b-0">
                <span className="font-medium">{platform.platform_name || platform.platform_code.charAt(0).toUpperCase() + platform.platform_code.slice(1)}</span>
                <span className="text-sm text-gray-600">
                  {selectedMetric === 'revenue' && formatCurrency(platform.total_revenue)}
                  {selectedMetric === 'reviews' && formatNumber(platform.total_reviews)}
                  {selectedMetric === 'price' && formatCurrency(platform.avg_price)}
                  {selectedMetric === 'rating' && `${platform.avg_rating?.toFixed(1)}⭐`}
                </span>
              </div>
            ))}
          </CardContent>
        </Card>

        {/* Category Share */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Tỷ lệ danh mục</CardTitle>
          </CardHeader>
          <CardContent>
            {selectedPlatform && selectedPlatform !== 'all-platforms' ? (
              overviewReport?.category_share.map(category => (
                <div key={category.category_name} className="flex justify-between items-center py-2 border-b last:border-b-0">
                  <span className="text-sm">{category.category_name}</span>
                  <span className="text-sm font-medium">{(category.revenue_share * 100).toFixed(1)}%</span>
                </div>
              ))
            ) : (
              <div className="text-center text-gray-500 py-4">
                Chọn một nền tảng cụ thể để xem tỷ lệ danh mục
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}