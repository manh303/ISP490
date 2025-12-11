import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { Users, Filter, PieChart, Eye } from 'lucide-react';
import { PieChart as RechartsPie, Pie, Cell, ResponsiveContainer, Tooltip, BarChart, Bar, XAxis, YAxis, CartesianGrid } from 'recharts';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

interface Segment {
    id: string;
    name: string;
    count: number;
    avgRating: number;
    avgPrice: number;
    color: string;
}

const defaultSegments: Segment[] = [
    { id: '1', name: 'Premium Products', count: 245, avgRating: 4.5, avgPrice: 2500000, color: '#3B82F6' },
    { id: '2', name: 'Budget Friendly', count: 512, avgRating: 3.8, avgPrice: 350000, color: '#10B981' },
    { id: '3', name: 'Mid-Range', count: 388, avgRating: 4.1, avgPrice: 850000, color: '#F59E0B' },
    { id: '4', name: 'High Volume', count: 156, avgRating: 4.3, avgPrice: 1200000, color: '#8B5CF6' },
    { id: '5', name: 'New Arrivals', count: 89, avgRating: 3.9, avgPrice: 600000, color: '#EF4444' },
];

const segmentCriteria = [
    { value: 'price', label: 'By Price Range' },
    { value: 'rating', label: 'By Rating' },
    { value: 'reviews', label: 'By Review Count' },
    { value: 'category', label: 'By Category' },
    { value: 'platform', label: 'By Platform' },
];

export default function SegmentationPage() {
    const [segments, setSegments] = useState<Segment[]>(defaultSegments);
    const [selectedCriteria, setSelectedCriteria] = useState('price');
    const [selectedSegment, setSelectedSegment] = useState<Segment | null>(null);
    const [loading, setLoading] = useState(false);

    const handleRunSegmentation = async () => {
        setLoading(true);
        await new Promise(resolve => setTimeout(resolve, 1500));
        // In real app, this would call an API to perform segmentation
        setLoading(false);
    };

    const totalProducts = segments.reduce((sum, s) => sum + s.count, 0);
    const pieData = segments.map(s => ({ name: s.name, value: s.count, color: s.color }));

    const barData = segments.map(s => ({
        name: s.name.substring(0, 10),
        'Avg Rating': s.avgRating,
        'Products': s.count / 10, // Scale down for display
    }));

    return (
        <div>
            <PageMeta title="Cohort/Segmentation" description="Segment products and customers for targeted analysis" />
            <PageBreadCrumb pageTitle="Cohort/Segmentation" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center justify-between mb-8">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-violet-100 rounded-lg dark:bg-violet-950">
                            <Users className="w-6 h-6 text-violet-600" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                                Cohort/Segmentation Tool
                            </h1>
                            <p className="text-sm text-gray-500">
                                Segment products and customers for targeted analysis
                            </p>
                        </div>
                    </div>

                    <div className="flex items-center gap-3">
                        <select
                            value={selectedCriteria}
                            onChange={e => setSelectedCriteria(e.target.value)}
                            className="px-4 py-2 border border-gray-300 rounded-lg dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                        >
                            {segmentCriteria.map(c => (
                                <option key={c.value} value={c.value}>{c.label}</option>
                            ))}
                        </select>
                        <button
                            onClick={handleRunSegmentation}
                            disabled={loading}
                            className="flex items-center gap-2 px-4 py-2 text-white bg-violet-600 rounded-lg hover:bg-violet-700 disabled:opacity-50"
                        >
                            <Filter className="w-4 h-4" />
                            {loading ? 'Analyzing...' : 'Run Segmentation'}
                        </button>
                    </div>
                </div>

                {/* Stats */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-gray-900 dark:text-white">{segments.length}</div>
                            <div className="text-sm text-gray-500">Total Segments</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-gray-900 dark:text-white">{totalProducts.toLocaleString()}</div>
                            <div className="text-sm text-gray-500">Total Products</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                {(segments.reduce((sum, s) => sum + s.avgRating, 0) / segments.length).toFixed(1)}
                            </div>
                            <div className="text-sm text-gray-500">Avg Rating</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-gray-900 dark:text-white">
                                {Math.round(segments.reduce((sum, s) => sum + s.avgPrice, 0) / segments.length / 1000)}K
                            </div>
                            <div className="text-sm text-gray-500">Avg Price (VND)</div>
                        </CardContent>
                    </Card>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* Pie Chart */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <PieChart className="w-5 h-5 text-violet-500" />
                                Segment Distribution
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="h-64">
                                <ResponsiveContainer width="100%" height="100%">
                                    <RechartsPie>
                                        <Pie
                                            data={pieData as any[]}
                                            cx="50%"
                                            cy="50%"
                                            innerRadius={50}
                                            outerRadius={80}
                                            dataKey="value"
                                            label={(props: any) => props.name}
                                        >
                                            {(pieData as any[]).map((entry, index) => (
                                                <Cell key={`cell-${index}`} fill={entry.color} />
                                            ))}
                                        </Pie>
                                        <Tooltip />
                                    </RechartsPie>
                                </ResponsiveContainer>
                            </div>
                        </CardContent>
                    </Card>

                    {/* Segment List */}
                    <Card className="lg:col-span-2">
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Users className="w-5 h-5 text-blue-500" />
                                Segments
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-3">
                                {segments.map(segment => (
                                    <div
                                        key={segment.id}
                                        onClick={() => setSelectedSegment(segment)}
                                        className={`p-4 border rounded-lg cursor-pointer transition-colors ${selectedSegment?.id === segment.id
                                                ? 'border-violet-500 bg-violet-50 dark:bg-violet-950/30'
                                                : 'border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800'
                                            }`}
                                    >
                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-3">
                                                <div
                                                    className="w-3 h-3 rounded-full"
                                                    style={{ backgroundColor: segment.color }}
                                                />
                                                <span className="font-medium text-gray-900 dark:text-white">
                                                    {segment.name}
                                                </span>
                                            </div>
                                            <div className="flex items-center gap-4 text-sm">
                                                <span className="text-gray-600 dark:text-gray-400">
                                                    {segment.count} products
                                                </span>
                                                <span className="text-yellow-600">
                                                    ★ {segment.avgRating}
                                                </span>
                                                <span className="text-green-600">
                                                    {(segment.avgPrice / 1000).toFixed(0)}K VND
                                                </span>
                                                <Eye className="w-4 h-4 text-gray-400" />
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
