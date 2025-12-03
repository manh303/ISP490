import React from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../../ui/figma/select';
import { Card, CardContent, CardHeader, CardTitle } from '../../../ui/figma/card';

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
            <CardTitle>Overview Trends</CardTitle>
            <Select value={selectedMetric} onValueChange={(value: any) => onMetricChange(value)}>
              <SelectTrigger className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="revenue">Revenue</SelectItem>
                <SelectItem value="reviews">Reviews</SelectItem>
                <SelectItem value="price">Price</SelectItem>
                <SelectItem value="rating">Rating</SelectItem>
              </SelectContent>
            </Select>
          </CardHeader>
          <CardContent>
            {overviewReport?.trends.points && (
              <div className="h-64">
                {/* Simple chart representation - replace with actual chart library */}
                <div className="text-center text-gray-500 mt-20">
                  Chart: {selectedMetric} over time
                  <br />
                  {overviewReport.trends.points.length} data points
                </div>
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
            <CardTitle className="text-base">Platform Comparison</CardTitle>
          </CardHeader>
          <CardContent>
            {overviewReport?.platform_comparison.map(platform => (
              <div key={platform.platform_code} className="flex justify-between items-center py-2 border-b last:border-b-0">
                <span className="font-medium">{platform.platform_name}</span>
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
            <CardTitle className="text-base">Category Share</CardTitle>
          </CardHeader>
          <CardContent>
            {selectedPlatform ? (
              overviewReport?.category_share.map(category => (
                <div key={category.category_name} className="flex justify-between items-center py-2 border-b last:border-b-0">
                  <span className="text-sm">{category.category_name}</span>
                  <span className="text-sm font-medium">{(category.revenue_share * 100).toFixed(1)}%</span>
                </div>
              ))
            ) : (
              <div className="text-center text-gray-500 py-4">
                Select a platform to view category share
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}