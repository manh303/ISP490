import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { Bookmark, Eye, Trash2, Share, Clock, BarChart3 } from 'lucide-react';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

interface SavedView {
    id: string;
    name: string;
    type: 'chart' | 'dashboard' | 'report';
    createdAt: string;
    lastViewed: string;
    shared: boolean;
    description: string;
}

const defaultViews: SavedView[] = [
    {
        id: '1',
        name: 'Weekly Revenue Trend',
        type: 'chart',
        createdAt: '2024-12-01',
        lastViewed: '2024-12-10',
        shared: true,
        description: 'Line chart showing revenue trend over the past 4 weeks',
    },
    {
        id: '2',
        name: 'Category Performance Dashboard',
        type: 'dashboard',
        createdAt: '2024-11-28',
        lastViewed: '2024-12-09',
        shared: false,
        description: 'Dashboard comparing performance across all product categories',
    },
    {
        id: '3',
        name: 'Top Products by Rating',
        type: 'chart',
        createdAt: '2024-11-25',
        lastViewed: '2024-12-08',
        shared: true,
        description: 'Bar chart of top 20 products sorted by average rating',
    },
    {
        id: '4',
        name: 'Platform Comparison Report',
        type: 'report',
        createdAt: '2024-11-20',
        lastViewed: '2024-12-05',
        shared: false,
        description: 'Comprehensive comparison of Lazada, Shopee, and Tiki metrics',
    },
    {
        id: '5',
        name: 'Review Sentiment Analysis',
        type: 'chart',
        createdAt: '2024-11-15',
        lastViewed: '2024-12-03',
        shared: true,
        description: 'Pie chart showing sentiment distribution across all reviews',
    },
];

export default function SavedViewsPage() {
    const [views, setViews] = useState<SavedView[]>(defaultViews);
    const [filter, setFilter] = useState('all');

    const filteredViews = views.filter(v => {
        if (filter === 'all') return true;
        return v.type === filter;
    });

    const handleDelete = (id: string) => {
        if (confirm('Are you sure you want to delete this saved view?')) {
            setViews(prev => prev.filter(v => v.id !== id));
        }
    };

    const toggleShare = (id: string) => {
        setViews(prev => prev.map(v =>
            v.id === id ? { ...v, shared: !v.shared } : v
        ));
    };

    const getTypeIcon = (type: string) => {
        switch (type) {
            case 'chart':
                return <BarChart3 className="w-4 h-4 text-blue-500" />;
            case 'dashboard':
                return <BarChart3 className="w-4 h-4 text-purple-500" />;
            case 'report':
                return <BarChart3 className="w-4 h-4 text-green-500" />;
            default:
                return <BarChart3 className="w-4 h-4 text-gray-500" />;
        }
    };

    const getTypeBadge = (type: string) => {
        const colors: Record<string, string> = {
            chart: 'bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-400',
            dashboard: 'bg-purple-100 text-purple-700 dark:bg-purple-950 dark:text-purple-400',
            report: 'bg-green-100 text-green-700 dark:bg-green-950 dark:text-green-400',
        };
        return (
            <span className={`px-2 py-1 text-xs font-medium rounded ${colors[type]}`}>
                {type}
            </span>
        );
    };

    return (
        <div>
            <PageMeta title="Saved Views" description="Manage your saved charts and dashboards" />
            <PageBreadCrumb pageTitle="Saved Views" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center justify-between mb-8">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-amber-100 rounded-lg dark:bg-amber-950">
                            <Bookmark className="w-6 h-6 text-amber-600" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                                Saved Views
                            </h1>
                            <p className="text-sm text-gray-500">
                                Access your saved charts, dashboards, and reports
                            </p>
                        </div>
                    </div>

                    <select
                        value={filter}
                        onChange={e => setFilter(e.target.value)}
                        className="px-4 py-2 border border-gray-300 rounded-lg dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                    >
                        <option value="all">All Types</option>
                        <option value="chart">Charts</option>
                        <option value="dashboard">Dashboards</option>
                        <option value="report">Reports</option>
                    </select>
                </div>

                {/* Stats */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-gray-900 dark:text-white">{views.length}</div>
                            <div className="text-sm text-gray-500">Total Saved</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-blue-600">{views.filter(v => v.type === 'chart').length}</div>
                            <div className="text-sm text-gray-500">Charts</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-purple-600">{views.filter(v => v.type === 'dashboard').length}</div>
                            <div className="text-sm text-gray-500">Dashboards</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-green-600">{views.filter(v => v.shared).length}</div>
                            <div className="text-sm text-gray-500">Shared</div>
                        </CardContent>
                    </Card>
                </div>

                {/* Views List */}
                <Card>
                    <CardContent className="p-0">
                        <div className="divide-y divide-gray-200 dark:divide-gray-700">
                            {filteredViews.map(view => (
                                <div key={view.id} className="p-4 hover:bg-gray-50 dark:hover:bg-gray-800">
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-4">
                                            <div className="p-2 bg-gray-100 rounded-lg dark:bg-gray-800">
                                                {getTypeIcon(view.type)}
                                            </div>
                                            <div>
                                                <div className="flex items-center gap-2">
                                                    <span className="font-medium text-gray-900 dark:text-white">
                                                        {view.name}
                                                    </span>
                                                    {getTypeBadge(view.type)}
                                                    {view.shared && (
                                                        <span className="px-2 py-0.5 text-xs bg-gray-100 rounded dark:bg-gray-800 text-gray-600">
                                                            Shared
                                                        </span>
                                                    )}
                                                </div>
                                                <p className="text-sm text-gray-500 mt-1">{view.description}</p>
                                                <div className="flex items-center gap-4 mt-2 text-xs text-gray-400">
                                                    <span className="flex items-center gap-1">
                                                        <Clock className="w-3 h-3" />
                                                        Created: {view.createdAt}
                                                    </span>
                                                    <span className="flex items-center gap-1">
                                                        <Eye className="w-3 h-3" />
                                                        Last viewed: {view.lastViewed}
                                                    </span>
                                                </div>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <button
                                                onClick={() => alert(`Opening: ${view.name}`)}
                                                className="p-2 text-blue-600 hover:bg-blue-50 rounded-lg dark:hover:bg-blue-950"
                                            >
                                                <Eye className="w-5 h-5" />
                                            </button>
                                            <button
                                                onClick={() => toggleShare(view.id)}
                                                className={`p-2 rounded-lg ${view.shared ? 'text-green-600 hover:bg-green-50 dark:hover:bg-green-950' : 'text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800'}`}
                                            >
                                                <Share className="w-5 h-5" />
                                            </button>
                                            <button
                                                onClick={() => handleDelete(view.id)}
                                                className="p-2 text-red-600 hover:bg-red-50 rounded-lg dark:hover:bg-red-950"
                                            >
                                                <Trash2 className="w-5 h-5" />
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}
