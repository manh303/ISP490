import React from 'react';
import { WeeklySalesForecast as WeeklySalesForecastType } from '../../services/MLInsightsApi';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

interface WeeklySalesForecastProps {
  data: WeeklySalesForecastType[];
  loading?: boolean;
}

const DAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

const WeeklySalesForecast: React.FC<WeeklySalesForecastProps> = ({ data, loading }) => {
  const chartData = data.map((item) => ({
    day: DAY_NAMES[item.day_of_week] || `Day ${item.day_of_week}`,
    avg_reviews: item.avg_reviews,
    avg_rating: item.avg_rating,
    year: item.year,
  }));

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
      <h2 className="text-2xl font-bold text-gray-800 mb-6">📆 Weekly Sales Forecast</h2>

      <div className="mb-6">
        <p className="text-gray-600">
          This chart shows average reviews and ratings by day of the week to identify peak activity days.
        </p>
      </div>

      <ResponsiveContainer width="100%" height={400}>
        <LineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="day" />
          <YAxis yAxisId="left" />
          <YAxis yAxisId="right" orientation="right" />
          <Tooltip />
          <Legend />
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="avg_reviews"
            stroke="#3b82f6"
            strokeWidth={2}
            name="Avg Reviews"
            dot={{ r: 5 }}
          />
          <Line
            yAxisId="right"
            type="monotone"
            dataKey="avg_rating"
            stroke="#10b981"
            strokeWidth={2}
            name="Avg Rating"
            dot={{ r: 5 }}
          />
        </LineChart>
      </ResponsiveContainer>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
        <div className="bg-blue-50 p-4 rounded-lg">
          <div className="text-sm text-blue-600 font-medium mb-1">Highest Reviews</div>
          <div className="text-2xl font-bold text-blue-700">
            {chartData.reduce((max, item) => item.avg_reviews > max.avg_reviews ? item : max, chartData[0])?.day}
          </div>
          <div className="text-sm text-blue-600 mt-1">
            {chartData.reduce((max, item) => item.avg_reviews > max.avg_reviews ? item : max, chartData[0])?.avg_reviews.toFixed(2)} reviews
          </div>
        </div>

        <div className="bg-green-50 p-4 rounded-lg">
          <div className="text-sm text-green-600 font-medium mb-1">Highest Rating</div>
          <div className="text-2xl font-bold text-green-700">
            {chartData.reduce((max, item) => item.avg_rating > max.avg_rating ? item : max, chartData[0])?.day}
          </div>
          <div className="text-sm text-green-600 mt-1">
            ⭐ {chartData.reduce((max, item) => item.avg_rating > max.avg_rating ? item : max, chartData[0])?.avg_rating.toFixed(2)} rating
          </div>
        </div>

        <div className="bg-purple-50 p-4 rounded-lg">
          <div className="text-sm text-purple-600 font-medium mb-1">Average Reviews/Day</div>
          <div className="text-2xl font-bold text-purple-700">
            {(chartData.reduce((sum, item) => sum + item.avg_reviews, 0) / chartData.length).toFixed(2)}
          </div>
          <div className="text-sm text-purple-600 mt-1">
            Across all days
          </div>
        </div>
      </div>

      {/* Data Table */}
      <div className="mt-6 overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Day of Week
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Avg Reviews
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Avg Rating
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Year
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {chartData.map((item, index) => (
              <tr key={index} className="hover:bg-gray-50">
                <td className="px-4 py-4 text-sm font-medium text-gray-900">
                  {item.day}
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  {item.avg_reviews.toFixed(2)}
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  <span className="flex items-center">
                    <span className="text-yellow-500 mr-1">⭐</span>
                    {item.avg_rating.toFixed(2)}
                  </span>
                </td>
                <td className="px-4 py-4 text-sm text-gray-600">
                  {item.year}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default WeeklySalesForecast;
