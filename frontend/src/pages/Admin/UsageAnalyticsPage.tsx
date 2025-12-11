import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { BarChart3, Users, Zap, Activity, TrendingUp, Clock, Database, Brain } from 'lucide-react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell } from 'recharts';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

// Mock data for demonstration
const apiCallsData = [
    { date: 'Mon', calls: 1250, errors: 23 },
    { date: 'Tue', calls: 1450, errors: 18 },
    { date: 'Wed', calls: 1380, errors: 31 },
    { date: 'Thu', calls: 1520, errors: 12 },
    { date: 'Fri', calls: 1680, errors: 25 },
    { date: 'Sat', calls: 890, errors: 8 },
    { date: 'Sun', calls: 720, errors: 5 },
];

const dssRunsData = [
    { date: 'Mon', price: 45, reco: 32, sentiment: 28 },
    { date: 'Tue', price: 52, reco: 38, sentiment: 35 },
    { date: 'Wed', price: 48, reco: 41, sentiment: 30 },
    { date: 'Thu', price: 61, reco: 45, sentiment: 42 },
    { date: 'Fri', price: 58, reco: 52, sentiment: 38 },
    { date: 'Sat', price: 25, reco: 18, sentiment: 15 },
    { date: 'Sun', price: 20, reco: 12, sentiment: 10 },
];

const userActivityData = [
    { name: 'Admin', value: 45 },
    { name: 'Analyst', value: 120 },
    { name: 'Data Engineer', value: 35 },
    { name: 'Customer', value: 80 },
];

const topEndpoints = [
    { endpoint: '/api/v1/analytics/overview/kpis', calls: 2450, avgLatency: 120 },
    { endpoint: '/api/v1/dss/price/run', calls: 1890, avgLatency: 850 },
    { endpoint: '/api/v1/dss/reco/run', calls: 1560, avgLatency: 720 },
    { endpoint: '/api/v1/products', calls: 1420, avgLatency: 95 },
    { endpoint: '/api/v1/analytics/products', calls: 1280, avgLatency: 180 },
];

const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444'];

export default function UsageAnalyticsPage() {
    const [timeRange, setTimeRange] = useState('7d');
    const [loading, setLoading] = useState(false);

    // KPI stats
    const totalApiCalls = apiCallsData.reduce((sum, d) => sum + d.calls, 0);
    const totalErrors = apiCallsData.reduce((sum, d) => sum + d.errors, 0);
    const errorRate = ((totalErrors / totalApiCalls) * 100).toFixed(2);
    const totalDssRuns = dssRunsData.reduce((sum, d) => sum + d.price + d.reco + d.sentiment, 0);
    const activeUsers = userActivityData.reduce((sum, d) => sum + d.value, 0);

    return (
        <div>
            <PageMeta title="Usage Analytics" description="Monitor system usage and performance metrics" />
            <PageBreadCrumb pageTitle="Usage Analytics" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center justify-between mb-8">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-purple-100 rounded-lg dark:bg-purple-950">
                            <BarChart3 className="w-6 h-6 text-purple-600" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                                Usage Analytics
                            </h1>
                            <p className="text-sm text-gray-500">
                                API calls, DSS runs, dashboard access by time/user
                            </p>
                        </div>
                    </div>

                    <select
                        value={timeRange}
                        onChange={e => setTimeRange(e.target.value)}
                        className="px-4 py-2 border border-gray-300 rounded-lg dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                    >
                        <option value="24h">Last 24 Hours</option>
                        <option value="7d">Last 7 Days</option>
                        <option value="30d">Last 30 Days</option>
                        <option value="90d">Last 90 Days</option>
                    </select>
                </div>

                {/* KPI Cards */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
                    <Card>
                        <CardContent className="p-5">
                            <div className="flex items-center gap-3">
                                <div className="p-2 bg-blue-100 rounded-lg dark:bg-blue-950">
                                    <Zap className="w-5 h-5 text-blue-600" />
                                </div>
                                <div>
                                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                        {totalApiCalls.toLocaleString()}
                                    </div>
                                    <div className="text-sm text-gray-500">Total API Calls</div>
                                </div>
                            </div>
                            <div className="mt-3 flex items-center text-sm">
                                <TrendingUp className="w-4 h-4 text-green-500 mr-1" />
                                <span className="text-green-600">+12.5%</span>
                                <span className="text-gray-500 ml-1">vs last week</span>
                            </div>
                        </CardContent>
                    </Card>

                    <Card>
                        <CardContent className="p-5">
                            <div className="flex items-center gap-3">
                                <div className="p-2 bg-red-100 rounded-lg dark:bg-red-950">
                                    <Activity className="w-5 h-5 text-red-600" />
                                </div>
                                <div>
                                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                        {errorRate}%
                                    </div>
                                    <div className="text-sm text-gray-500">Error Rate</div>
                                </div>
                            </div>
                            <div className="mt-3 flex items-center text-sm">
                                <span className="text-gray-500">{totalErrors} errors this week</span>
                            </div>
                        </CardContent>
                    </Card>

                    <Card>
                        <CardContent className="p-5">
                            <div className="flex items-center gap-3">
                                <div className="p-2 bg-green-100 rounded-lg dark:bg-green-950">
                                    <Brain className="w-5 h-5 text-green-600" />
                                </div>
                                <div>
                                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                        {totalDssRuns}
                                    </div>
                                    <div className="text-sm text-gray-500">DSS Runs</div>
                                </div>
                            </div>
                            <div className="mt-3 flex items-center text-sm">
                                <TrendingUp className="w-4 h-4 text-green-500 mr-1" />
                                <span className="text-green-600">+8.3%</span>
                                <span className="text-gray-500 ml-1">vs last week</span>
                            </div>
                        </CardContent>
                    </Card>

                    <Card>
                        <CardContent className="p-5">
                            <div className="flex items-center gap-3">
                                <div className="p-2 bg-purple-100 rounded-lg dark:bg-purple-950">
                                    <Users className="w-5 h-5 text-purple-600" />
                                </div>
                                <div>
                                    <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                        {activeUsers}
                                    </div>
                                    <div className="text-sm text-gray-500">Active Users</div>
                                </div>
                            </div>
                            <div className="mt-3 flex items-center text-sm">
                                <span className="text-gray-500">Across all roles</span>
                            </div>
                        </CardContent>
                    </Card>
                </div>

                {/* Charts Row */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
                    {/* API Calls Chart */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Zap className="w-5 h-5 text-blue-500" />
                                API Calls & Errors
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={apiCallsData}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis dataKey="date" />
                                        <YAxis />
                                        <Tooltip />
                                        <Bar dataKey="calls" name="API Calls" fill="#3B82F6" radius={[4, 4, 0, 0]} />
                                        <Bar dataKey="errors" name="Errors" fill="#EF4444" radius={[4, 4, 0, 0]} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </CardContent>
                    </Card>

                    {/* DSS Runs Chart */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Brain className="w-5 h-5 text-green-500" />
                                DSS Runs by Scenario
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <LineChart data={dssRunsData}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis dataKey="date" />
                                        <YAxis />
                                        <Tooltip />
                                        <Line type="monotone" dataKey="price" name="Price Prediction" stroke="#3B82F6" strokeWidth={2} />
                                        <Line type="monotone" dataKey="reco" name="Recommendation" stroke="#10B981" strokeWidth={2} />
                                        <Line type="monotone" dataKey="sentiment" name="Sentiment" stroke="#F59E0B" strokeWidth={2} />
                                    </LineChart>
                                </ResponsiveContainer>
                            </div>
                        </CardContent>
                    </Card>
                </div>

                {/* Bottom Row */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* User Activity by Role */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Users className="w-5 h-5 text-purple-500" />
                                Activity by Role
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="h-48">
                                <ResponsiveContainer width="100%" height="100%">
                                    <PieChart>
                                        <Pie
                                            data={userActivityData as any[]}
                                            cx="50%"
                                            cy="50%"
                                            innerRadius={40}
                                            outerRadius={70}
                                            paddingAngle={2}
                                            dataKey="value"
                                            label={(props: any) => `${props.name}: ${props.value}`}
                                        >
                                            {(userActivityData as any[]).map((_, index) => (
                                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                            ))}
                                        </Pie>
                                        <Tooltip />
                                    </PieChart>
                                </ResponsiveContainer>
                            </div>
                        </CardContent>
                    </Card>

                    {/* Top Endpoints */}
                    <Card className="lg:col-span-2">
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Database className="w-5 h-5 text-orange-500" />
                                Top API Endpoints
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-3">
                                {topEndpoints.map((ep, idx) => (
                                    <div key={idx} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-800 rounded-lg">
                                        <div className="flex-1">
                                            <code className="text-sm text-gray-900 dark:text-white">{ep.endpoint}</code>
                                        </div>
                                        <div className="flex items-center gap-6 text-sm">
                                            <div className="text-right">
                                                <div className="font-medium text-gray-900 dark:text-white">{ep.calls.toLocaleString()}</div>
                                                <div className="text-xs text-gray-500">calls</div>
                                            </div>
                                            <div className="text-right w-20">
                                                <div className={`font-medium ${ep.avgLatency > 500 ? 'text-orange-600' : 'text-green-600'}`}>
                                                    {ep.avgLatency}ms
                                                </div>
                                                <div className="text-xs text-gray-500">avg latency</div>
                                            </div>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
