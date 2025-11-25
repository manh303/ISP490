import React from 'react';
import { MLSummary } from '../../services/MLInsightsApi';

interface MLSummaryCardsProps {
  data: MLSummary | null;
  loading?: boolean;
}

const MLSummaryCards: React.FC<MLSummaryCardsProps> = ({ data, loading }) => {
  if (loading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        {[1, 2, 3].map((i) => (
          <div key={i} className="bg-white rounded-lg shadow-md p-6 animate-pulse">
            <div className="h-4 bg-gray-200 rounded w-3/4 mb-4"></div>
            <div className="h-8 bg-gray-200 rounded w-1/2 mb-2"></div>
            <div className="h-3 bg-gray-200 rounded w-full"></div>
          </div>
        ))}
      </div>
    );
  }

  if (!data) return null;

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
      {/* Price Optimization Card */}
      <div className="bg-white rounded-lg shadow-md p-6 border-l-4 border-blue-500">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-800">🏷️ Price Optimization</h3>
        </div>
        <div className="space-y-3">
          <div className="flex justify-between items-center">
            <span className="text-sm text-gray-600">🟢 Increase:</span>
            <span className="text-lg font-bold text-green-600">{data.price_optimization.increase.toLocaleString()}</span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-sm text-gray-600">🔴 Decrease:</span>
            <span className="text-lg font-bold text-red-600">{data.price_optimization.decrease.toLocaleString()}</span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-sm text-gray-600">🔵 Maintain:</span>
            <span className="text-lg font-bold text-blue-600">{data.price_optimization.maintain.toLocaleString()}</span>
          </div>
        </div>
      </div>

      {/* Demand Forecast Card */}
      <div className="bg-white rounded-lg shadow-md p-6 border-l-4 border-purple-500">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-800">📦 Demand Forecast</h3>
        </div>
        <div className="space-y-3">
          <div className="flex justify-between items-center">
            <span className="text-sm text-gray-600">📈 Growing:</span>
            <span className="text-lg font-bold text-green-600">{data.demand_forecast.growing.toLocaleString()}</span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-sm text-gray-600">📉 Declining:</span>
            <span className="text-lg font-bold text-red-600">{data.demand_forecast.declining.toLocaleString()}</span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-sm text-gray-600">➡️ Stable:</span>
            <span className="text-lg font-bold text-gray-600">{data.demand_forecast.stable.toLocaleString()}</span>
          </div>
        </div>
      </div>

      {/* Total Products Analyzed Card */}
      <div className="bg-white rounded-lg shadow-md p-6 border-l-4 border-orange-500">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-800">🧮 Total Analysis</h3>
        </div>
        <div className="flex flex-col items-center justify-center h-24">
          <p className="text-4xl font-bold text-orange-600 mb-2">
            {data?.total_products_analyzed?.toLocaleString()}
          </p>
          <p className="text-sm text-gray-600">Products Analyzed</p>
        </div>
      </div>
    </div>
  );
};

export default MLSummaryCards;
