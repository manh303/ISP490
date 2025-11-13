import React from 'react';
import { Seasonality as SeasonalityType } from '../../services/MLInsightsApi';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from 'recharts';

interface SeasonalityProps {
  data: SeasonalityType[];
  loading?: boolean;
}

const SEASON_COLORS: { [key: string]: string } = {
  'Spring': '#10b981',
  'Summer': '#f59e0b',
  'Fall': '#ef4444',
  'Winter': '#3b82f6',
};

const SEASON_EMOJIS: { [key: string]: string } = {
  'Spring': '🌸',
  'Summer': '☀️',
  'Fall': '🍂',
  'Winter': '❄️',
};

const Seasonality: React.FC<SeasonalityProps> = ({ data, loading }) => {
  const chartData = data.map((item) => ({
    season: item.season,
    avg_reviews: item.avg_reviews,
    avg_rating: item.avg_rating,
    seasonality_index: item.seasonality_index,
    color: SEASON_COLORS[item.season] || '#6b7280',
    emoji: SEASON_EMOJIS[item.season] || '🌤',
  }));

  const peakSeason = chartData.reduce((max, item) =>
    item.seasonality_index > max.seasonality_index ? item : max
  , chartData[0]);

  const lowestSeason = chartData.reduce((min, item) =>
    item.seasonality_index < min.seasonality_index ? item : min
  , chartData[0]);

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-200 rounded w-1/3 mb-6"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow-md p-6 mb-8">
      <h2 className="text-2xl font-bold text-gray-800 mb-6">🌤 Seasonality Analysis</h2>

      <div className="mb-6">
        <p className="text-gray-600">
          Analyze sales performance across different seasons to optimize inventory and marketing strategies.
        </p>
      </div>

      {/* Key Insights */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        <div className="bg-gradient-to-r from-green-50 to-green-100 p-4 rounded-lg border-l-4 border-green-500">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-green-700 font-medium">Peak Season</p>
              <p className="text-2xl font-bold text-green-800">
                {peakSeason?.emoji} {peakSeason?.season}
              </p>
              <p className="text-sm text-green-600 mt-1">
                Index: {peakSeason?.seasonality_index.toFixed(2)}
              </p>
            </div>
            <div className="text-4xl">📈</div>
          </div>
        </div>

        <div className="bg-gradient-to-r from-blue-50 to-blue-100 p-4 rounded-lg border-l-4 border-blue-500">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-blue-700 font-medium">Lowest Season</p>
              <p className="text-2xl font-bold text-blue-800">
                {lowestSeason?.emoji} {lowestSeason?.season}
              </p>
              <p className="text-sm text-blue-600 mt-1">
                Index: {lowestSeason?.seasonality_index.toFixed(2)}
              </p>
            </div>
            <div className="text-4xl">📉</div>
          </div>
        </div>
      </div>

      {/* Bar Chart - Average Reviews by Season */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-700 mb-4">Average Reviews by Season</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="season" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="avg_reviews" name="Avg Reviews" radius={[8, 8, 0, 0]}>
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Bar Chart - Seasonality Index */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-700 mb-4">Seasonality Index by Season</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="season" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="seasonality_index" name="Seasonality Index" fill="#8b5cf6" radius={[8, 8, 0, 0]}>
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Season Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {chartData.map((item, index) => (
          <div
            key={index}
            className="p-4 rounded-lg border-2"
            style={{ borderColor: item.color, backgroundColor: `${item.color}10` }}
          >
            <div className="text-3xl mb-2 text-center">{item.emoji}</div>
            <div className="text-center">
              <p className="text-lg font-bold" style={{ color: item.color }}>
                {item.season}
              </p>
              <div className="mt-2 space-y-1">
                <p className="text-sm text-gray-600">
                  Reviews: <span className="font-semibold">{item.avg_reviews.toLocaleString()}</span>
                </p>
                <p className="text-sm text-gray-600">
                  Rating: <span className="font-semibold">⭐ {item.avg_rating.toFixed(2)}</span>
                </p>
                <p className="text-sm text-gray-600">
                  Index: <span className="font-semibold">{item.seasonality_index.toFixed(2)}</span>
                </p>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Recommendations */}
      <div className="bg-gradient-to-r from-purple-50 to-pink-50 p-6 rounded-lg">
        <h3 className="text-lg font-semibold text-gray-800 mb-3">💡 Strategic Recommendations</h3>
        <ul className="space-y-2 text-gray-700">
          <li className="flex items-start">
            <span className="mr-2">🎯</span>
            <span>
              <strong>Peak Season ({peakSeason?.season}):</strong> Increase marketing budget and stock levels to maximize sales during this high-demand period.
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">📦</span>
            <span>
              <strong>Low Season ({lowestSeason?.season}):</strong> Consider promotional campaigns and discounts to boost sales during slower periods.
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">📊</span>
            <span>
              <strong>Inventory Planning:</strong> Adjust stock levels based on seasonality index to avoid overstocking or stockouts.
            </span>
          </li>
        </ul>
      </div>

      {/* Data Table */}
      <div className="mt-6 overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Season
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Avg Reviews
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Avg Rating
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Seasonality Index
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Performance
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {chartData.map((item, index) => (
              <tr key={index} className="hover:bg-gray-50">
                <td className="px-4 py-4 text-sm font-medium text-gray-900">
                  <span className="flex items-center">
                    <span className="text-2xl mr-2">{item.emoji}</span>
                    {item.season}
                  </span>
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  {item.avg_reviews.toLocaleString()}
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  <span className="flex items-center">
                    <span className="text-yellow-500 mr-1">⭐</span>
                    {item.avg_rating.toFixed(2)}
                  </span>
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  <span className="font-semibold">{item.seasonality_index.toFixed(2)}</span>
                </td>
                <td className="px-4 py-4 text-sm">
                  {item.seasonality_index >= 1 ? (
                    <span className="px-2 py-1 bg-green-100 text-green-800 rounded-full text-xs font-medium">
                      📈 Above Average
                    </span>
                  ) : (
                    <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded-full text-xs font-medium">
                      📊 Below Average
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default Seasonality;
