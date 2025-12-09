import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import { Database, AlertTriangle, CheckCircle, Clock, ArrowRight, FileWarning } from 'lucide-react';
import { Link } from 'react-router-dom';
import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
    Cell,
} from 'recharts';

interface DatasetBySchema {
    schema_name: string;
    count: number;
}

interface AtRiskDataset {
    dataset_id: number;
    table_name: string;
    schema_name: string;
    source_name?: string;
    last_loaded_at: string | null;
    missingFields: string[];
}

interface DataCatalogHealthProps {
    totalDatasets: number;
    datasetsWithOwner: number;
    datasetsWithoutDescription: number;
    datasetsNotUpdated: number;
    notUpdatedDays: number;
    datasetsBySchema: DatasetBySchema[];
    atRiskDatasets: AtRiskDataset[];
    selectedSchemaFilter?: string;
    onSchemaFilterChange?: (schema: string) => void;
    isLoading?: boolean;
}

const SCHEMA_COLORS: Record<string, string> = {
    staging: '#94A3B8',
    ods: '#60A5FA',
    dwh: '#34D399',
    ml: '#A78BFA',
    default: '#6B7280',
};

export default function DataCatalogHealth({
    totalDatasets,
    datasetsWithOwner,
    datasetsWithoutDescription,
    datasetsNotUpdated,
    notUpdatedDays = 7,
    datasetsBySchema,
    atRiskDatasets,
    selectedSchemaFilter = 'all',
    onSchemaFilterChange,
    isLoading = false,
}: DataCatalogHealthProps) {
    if (isLoading) {
        return (
            <div className="space-y-4">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Database className="w-5 h-5 text-green-600" />
                    Data Catalog & Dataset Health
                </h2>
                <div className="animate-pulse space-y-4">
                    <div className="grid grid-cols-4 gap-4">
                        {[1, 2, 3, 4].map((i) => (
                            <div key={i} className="h-20 bg-gray-200 dark:bg-gray-700 rounded"></div>
                        ))}
                    </div>
                    <div className="grid grid-cols-2 gap-6">
                        <div className="h-64 bg-gray-200 dark:bg-gray-700 rounded"></div>
                        <div className="h-64 bg-gray-200 dark:bg-gray-700 rounded"></div>
                    </div>
                </div>
            </div>
        );
    }

    const kpiItems = [
        {
            label: 'Total Datasets',
            value: totalDatasets,
            icon: Database,
            color: 'text-blue-600',
        },
        {
            label: 'With Owner',
            value: datasetsWithOwner,
            icon: CheckCircle,
            color: 'text-green-600',
        },
        {
            label: 'No Description',
            value: datasetsWithoutDescription,
            icon: FileWarning,
            color: 'text-orange-600',
            warning: datasetsWithoutDescription > 0,
        },
        {
            label: `Not Updated (>${notUpdatedDays}d)`,
            value: datasetsNotUpdated,
            icon: Clock,
            color: 'text-red-600',
            warning: datasetsNotUpdated > 0,
        },
    ];

    const formatTimeAgo = (dateString: string | null) => {
        if (!dateString) return 'Never';
        const date = new Date(dateString);
        const now = new Date();
        const diffMs = now.getTime() - date.getDate();
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

        if (diffDays === 0) return 'Today';
        if (diffDays === 1) return 'Yesterday';
        if (diffDays < 30) return `${diffDays} days ago`;
        return date.toLocaleDateString('vi-VN');
    };

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Database className="w-5 h-5 text-green-600" />
                    Data Catalog & Dataset Health
                </h2>
                <Link to="/data-catalog">
                    <Button variant="outline" size="sm">
                        View All Datasets
                        <ArrowRight className="w-4 h-4 ml-2" />
                    </Button>
                </Link>
            </div>

            {/* Mini KPIs */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {kpiItems.map((item, index) => {
                    const Icon = item.icon;
                    return (
                        <div
                            key={index}
                            className={`p-4 rounded-lg border ${item.warning ? 'border-orange-200 bg-orange-50 dark:border-orange-800 dark:bg-orange-950' : 'border-gray-200 bg-white dark:border-gray-700 dark:bg-gray-800'}`}
                        >
                            <div className="flex items-center gap-2">
                                <Icon className={`w-4 h-4 ${item.color}`} />
                                <span className="text-sm text-gray-600 dark:text-gray-400">{item.label}</span>
                            </div>
                            <div className={`text-2xl font-bold mt-1 ${item.warning ? 'text-orange-600' : 'text-gray-900 dark:text-white'}`}>
                                {item.value}
                            </div>
                        </div>
                    );
                })}
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Datasets by Schema Chart */}
                <Card>
                    <CardHeader>
                        <CardTitle className="text-base">Datasets by Schema</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-64">
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={datasetsBySchema} layout="vertical">
                                    <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                                    <XAxis type="number" />
                                    <YAxis type="category" dataKey="schema_name" width={80} />
                                    <Tooltip
                                        formatter={(value: number) => [`${value} datasets`, 'Count']}
                                    />
                                    <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                                        {datasetsBySchema.map((entry, index) => (
                                            <Cell
                                                key={`cell-${index}`}
                                                fill={SCHEMA_COLORS[entry.schema_name.toLowerCase()] || SCHEMA_COLORS.default}
                                            />
                                        ))}
                                    </Bar>
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </CardContent>
                </Card>

                {/* At-Risk Datasets Table */}
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between">
                        <CardTitle className="text-base flex items-center gap-2">
                            <AlertTriangle className="w-4 h-4 text-orange-500" />
                            At-Risk Datasets
                        </CardTitle>
                        {onSchemaFilterChange && (
                            <select
                                value={selectedSchemaFilter}
                                onChange={(e) => onSchemaFilterChange(e.target.value)}
                                className="text-sm border rounded px-2 py-1 dark:bg-gray-800 dark:border-gray-700"
                            >
                                <option value="all">All Schemas</option>
                                {datasetsBySchema.map((s) => (
                                    <option key={s.schema_name} value={s.schema_name}>{s.schema_name}</option>
                                ))}
                            </select>
                        )}
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-2 max-h-64 overflow-y-auto">
                            {atRiskDatasets.length === 0 ? (
                                <div className="text-center text-gray-500 py-8 flex flex-col items-center">
                                    <CheckCircle className="w-8 h-8 text-green-500 mb-2" />
                                    <span>All datasets are healthy!</span>
                                </div>
                            ) : (
                                atRiskDatasets.slice(0, 10).map((dataset) => (
                                    <Link
                                        to={`/data-catalog/datasets/${dataset.dataset_id}`}
                                        key={dataset.dataset_id}
                                        className="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors border border-orange-200 dark:border-orange-800 group"
                                    >
                                        <div>
                                            <div className="font-medium text-gray-900 dark:text-white text-sm">
                                                {dataset.table_name}
                                            </div>
                                            <div className="text-xs text-gray-500">
                                                {dataset.schema_name} • {dataset.source_name || 'Unknown source'}
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <div className="flex flex-wrap gap-1">
                                                {dataset.missingFields.map((field) => (
                                                    <Badge key={field} variant="outline" className="text-xs text-orange-600 border-orange-300">
                                                        {field}
                                                    </Badge>
                                                ))}
                                            </div>
                                            <div className="text-xs text-gray-500">
                                                {formatTimeAgo(dataset.last_loaded_at)}
                                            </div>
                                            <ArrowRight className="w-4 h-4 text-gray-400 opacity-0 group-hover:opacity-100 transition-opacity" />
                                        </div>
                                    </Link>
                                ))
                            )}
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}
