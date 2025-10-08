import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  LineChart, Line, PieChart, Pie, Cell, ResponsiveContainer,
  AreaChart, Area
} from 'recharts';
import {
  Database, TrendingUp, Users, Package, Brain, AlertTriangle,
  Activity, Server, CheckCircle, Clock, RefreshCw
} from 'lucide-react';

const ComprehensiveDashboard = () => {
  const [dashboardData, setDashboardData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);

  // Mock data - sẽ được thay thế bằng API calls
  const mockData = {
    kpi: {
      customers: 10000,
      products: 20000,
      orders: 50000,
      ml_features: 10000,
      recommendations: 20000
    },
    databases: {
      postgresql: {
        tables: 8,
        records: 80000,
        status: 'healthy'
      },
      mongodb: {
        collections: 10,
        documents: 60177,
        status: 'healthy'
      }
    },
    mlMetrics: {
      models: 1,
      features: 30000,
      predictions: 20000,
      accuracy: 89.5
    },
    recentActivity: [
      { time: '10:30', event: 'Data pipeline completed successfully', type: 'success' },
      { time: '10:15', event: 'ML model training started', type: 'info' },
      { time: '10:00', event: 'New customer features processed', type: 'success' },
      { time: '09:45', event: 'Performance alert resolved', type: 'warning' },
      { time: '09:30', event: 'Vietnam data collection completed', type: 'success' }
    ],
    performanceData: [
      { time: '06:00', cpu: 45, memory: 62, processing: 1200 },
      { time: '08:00', cpu: 52, memory: 68, processing: 1800 },
      { time: '10:00', cpu: 78, memory: 74, processing: 2400 },
      { time: '12:00', cpu: 65, memory: 71, processing: 2100 },
      { time: '14:00', cpu: 58, memory: 65, processing: 1900 },
      { time: '16:00', cpu: 62, memory: 69, processing: 2000 }
    ],
    categoryData: [
      { name: 'Electronics', value: 35, color: '#8884d8' },
      { name: 'Fashion', value: 25, color: '#82ca9d' },
      { name: 'Home & Garden', value: 20, color: '#ffc658' },
      { name: 'Sports', value: 12, color: '#ff7300' },
      { name: 'Books', value: 8, color: '#00ff00' }
    ],
    revenueData: [
      { month: 'Jan', revenue: 45000, orders: 1200, customers: 800 },
      { month: 'Feb', revenue: 52000, orders: 1350, customers: 950 },
      { month: 'Mar', revenue: 48000, orders: 1280, customers: 890 },
      { month: 'Apr', revenue: 61000, orders: 1520, customers: 1100 },
      { month: 'May', revenue: 55000, orders: 1400, customers: 1000 },
      { month: 'Jun', revenue: 67000, orders: 1650, customers: 1200 }
    ]
  };

  useEffect(() => {
    // Simulate loading
    setTimeout(() => {
      setDashboardData(mockData);
      setLoading(false);
      setLastUpdated(new Date());
    }, 1500);

    // Auto-refresh every 30 seconds
    const interval = setInterval(() => {
      setLastUpdated(new Date());
    }, 30000);

    return () => clearInterval(interval);
  }, []);

  const MetricCard = ({ title, value, icon: Icon, trend, color = "blue" }) => (
    <div className="bg-white rounded-lg shadow-md p-6 border-l-4" style={{ borderLeftColor: color }}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-600">{title}</p>
          <p className="text-2xl font-bold text-gray-900">
            {typeof value === 'number' ? value.toLocaleString() : value}
          </p>
          {trend && (
            <p className={`text-sm ${trend > 0 ? 'text-green-600' : 'text-red-600'}`}>
              {trend > 0 ? '↗' : '↘'} {Math.abs(trend)}%
            </p>
          )}
        </div>
        <Icon className="h-8 w-8 text-gray-400" />
      </div>
    </div>
  );

  const StatusIndicator = ({ status }) => (
    <div className="flex items-center">
      <div className={`w-3 h-3 rounded-full mr-2 ${
        status === 'healthy' ? 'bg-green-500' :
        status === 'warning' ? 'bg-yellow-500' : 'bg-red-500'
      }`}></div>
      <span className={`text-sm font-medium ${
        status === 'healthy' ? 'text-green-700' :
        status === 'warning' ? 'text-yellow-700' : 'text-red-700'
      }`}>
        {status.charAt(0).toUpperCase() + status.slice(1)}
      </span>
    </div>
  );

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="w-12 h-12 animate-spin text-blue-500 mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-gray-700">Loading Dashboard...</h2>
          <p className="text-gray-500">Gathering data from all systems</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">E-commerce DSS Dashboard</h1>
            <p className="text-gray-600">Real-time analytics and machine learning insights</p>
          </div>
          <div className="text-right">
            <p className="text-sm text-gray-500">Last updated</p>
            <p className="text-sm font-medium text-gray-700">
              {lastUpdated?.toLocaleTimeString()}
            </p>
          </div>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6 mb-8">
        <MetricCard
          title="Total Customers"
          value={dashboardData.kpi.customers}
          icon={Users}
          trend={12.5}
          color="#3B82F6"
        />
        <MetricCard
          title="Products"
          value={dashboardData.kpi.products}
          icon={Package}
          trend={8.2}
          color="#10B981"
        />
        <MetricCard
          title="Orders"
          value={dashboardData.kpi.orders}
          icon={TrendingUp}
          trend={15.3}
          color="#F59E0B"
        />
        <MetricCard
          title="ML Features"
          value={dashboardData.kpi.ml_features}
          icon={Brain}
          trend={22.1}
          color="#8B5CF6"
        />
        <MetricCard
          title="Recommendations"
          value={dashboardData.kpi.recommendations}
          icon={Activity}
          trend={18.7}
          color="#EF4444"
        />
      </div>

      {/* System Status & Activity */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
        {/* Database Status */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
            <Database className="w-5 h-5 mr-2" />
            Database Status
          </h3>
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="font-medium text-gray-700">PostgreSQL</p>
                <p className="text-sm text-gray-500">
                  {dashboardData.databases.postgresql.tables} tables, {dashboardData.databases.postgresql.records.toLocaleString()} records
                </p>
              </div>
              <StatusIndicator status={dashboardData.databases.postgresql.status} />
            </div>
            <div className="flex items-center justify-between">
              <div>
                <p className="font-medium text-gray-700">MongoDB</p>
                <p className="text-sm text-gray-500">
                  {dashboardData.databases.mongodb.collections} collections, {dashboardData.databases.mongodb.documents.toLocaleString()} documents
                </p>
              </div>
              <StatusIndicator status={dashboardData.databases.mongodb.status} />
            </div>
          </div>
        </div>

        {/* ML Performance */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
            <Brain className="w-5 h-5 mr-2" />
            ML Performance
          </h3>
          <div className="space-y-3">
            <div className="flex justify-between">
              <span className="text-gray-600">Active Models</span>
              <span className="font-semibold">{dashboardData.mlMetrics.models}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Features</span>
              <span className="font-semibold">{dashboardData.mlMetrics.features.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Predictions</span>
              <span className="font-semibold">{dashboardData.mlMetrics.predictions.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Accuracy</span>
              <span className="font-semibold text-green-600">{dashboardData.mlMetrics.accuracy}%</span>
            </div>
          </div>
        </div>

        {/* Recent Activity */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
            <Activity className="w-5 h-5 mr-2" />
            Recent Activity
          </h3>
          <div className="space-y-3">
            {dashboardData.recentActivity.map((activity, index) => (
              <div key={index} className="flex items-start space-x-3">
                <div className={`w-2 h-2 rounded-full mt-2 ${
                  activity.type === 'success' ? 'bg-green-500' :
                  activity.type === 'warning' ? 'bg-yellow-500' : 'bg-blue-500'
                }`}></div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-gray-900">{activity.event}</p>
                  <p className="text-xs text-gray-500">{activity.time}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Performance Chart */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">System Performance</h3>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={dashboardData.performanceData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="time" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Area type="monotone" dataKey="processing" stackId="1" stroke="#8884d8" fill="#8884d8" name="Processing (records/min)" />
              <Area type="monotone" dataKey="memory" stackId="2" stroke="#82ca9d" fill="#82ca9d" name="Memory %" />
              <Area type="monotone" dataKey="cpu" stackId="2" stroke="#ffc658" fill="#ffc658" name="CPU %" />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Category Distribution */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Product Categories</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={dashboardData.categoryData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="value"
              >
                {dashboardData.categoryData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Revenue & Orders Chart */}
      <div className="bg-white rounded-lg shadow-md p-6 mb-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Revenue & Orders Trend</h3>
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={dashboardData.revenueData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" />
            <YAxis yAxisId="left" />
            <YAxis yAxisId="right" orientation="right" />
            <Tooltip />
            <Legend />
            <Bar yAxisId="left" dataKey="revenue" fill="#8884d8" name="Revenue ($)" />
            <Line yAxisId="right" type="monotone" dataKey="orders" stroke="#82ca9d" strokeWidth={3} name="Orders" />
            <Line yAxisId="right" type="monotone" dataKey="customers" stroke="#ffc658" strokeWidth={3} name="New Customers" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Footer */}
      <div className="text-center text-gray-500 text-sm">
        <p>E-commerce Decision Support System - Real-time Analytics Dashboard</p>
        <p>Powered by Vietnam E-commerce Data • Machine Learning • Big Data Processing</p>
      </div>
    </div>
  );
};

export default ComprehensiveDashboard;