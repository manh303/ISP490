import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import { Activity, XCircle, ArrowRight, Cog, Cpu } from 'lucide-react';
import { Link } from 'react-router-dom';
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
    Legend,
} from 'recharts';

interface PipelineRun {
    run_id: number;
    job_name: string;
    job_type: 'ETL' | 'ML';
    status: 'SUCCESS' | 'FAILED' | 'RUNNING';
    started_at: string;
    duration_minutes: number;
    triggered_by: string;
}

interface PipelineRunsOverTime {
    date: string;
    success: number;
    failed: number;
}

interface PipelinesSystemHealthProps {
    etlRunsLast24h: number;
    etlFailuresLast24h: number;
    mlTrainsLast7d: number;
    mlFailuresLast7d: number;
    pipelineRunsOverTime: PipelineRunsOverTime[];
    recentPipelineRuns: PipelineRun[];
    isLoading?: boolean;
}

const getStatusBadge = (status: string) => {
    switch (status) {
        case 'SUCCESS':
            return <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300">Success</Badge>;
        case 'FAILED':
            return <Badge variant="destructive">Failed</Badge>;
        case 'RUNNING':
            return <Badge className="bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300">Running</Badge>;
        default:
            return <Badge variant="outline">{status}</Badge>;
    }
};

const getTypeBadge = (type: string) => {
    if (type === 'ETL') {
        return <Badge variant="outline" className="border-blue-300 text-blue-700 dark:border-blue-600 dark:text-blue-400">ETL</Badge>;
    }
    return <Badge variant="outline" className="border-purple-300 text-purple-700 dark:border-purple-600 dark:text-purple-400">ML</Badge>;
};

export default function PipelinesSystemHealth({
    etlRunsLast24h,
    etlFailuresLast24h,
    mlTrainsLast7d,
    mlFailuresLast7d,
    pipelineRunsOverTime,
    recentPipelineRuns,
    isLoading = false,
}: PipelinesSystemHealthProps) {
    if (isLoading) {
        return (
            <div className="space-y-4">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Activity className="w-5 h-5 text-blue-600" />
                    Pipelines & System Health
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
            label: 'ETL Runs (24h)',
            value: etlRunsLast24h,
            icon: Cog,
            color: 'text-blue-600',
        },
        {
            label: 'ETL Failures (24h)',
            value: etlFailuresLast24h,
            icon: XCircle,
            color: 'text-red-600',
            warning: etlFailuresLast24h > 0,
        },
        {
            label: 'ML Trains (7d)',
            value: mlTrainsLast7d,
            icon: Cpu,
            color: 'text-purple-600',
        },
        {
            label: 'ML Failures (7d)',
            value: mlFailuresLast7d,
            icon: XCircle,
            color: 'text-red-600',
            warning: mlFailuresLast7d > 0,
        },
    ];

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Activity className="w-5 h-5 text-blue-600" />
                    Pipelines & System Health
                </h2>
                <Link to="/data-engineer/pipelines">
                    <Button variant="outline" size="sm">
                        View All Pipelines
                        <ArrowRight className="w-4 h-4 ml-2" />
                    </Button>
                </Link>
            </div>

            {/* KPIs */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {kpiItems.map((item, index) => {
                    const Icon = item.icon;
                    return (
                        <div
                            key={index}
                            className={`p-4 rounded-lg border ${item.warning ? 'border-red-200 bg-red-50 dark:border-red-800 dark:bg-red-950' : 'border-gray-200 bg-white dark:border-gray-700 dark:bg-gray-800'}`}
                        >
                            <div className="flex items-center gap-2">
                                <Icon className={`w-4 h-4 ${item.color}`} />
                                <span className="text-sm text-gray-600 dark:text-gray-400">{item.label}</span>
                            </div>
                            <div className={`text-2xl font-bold mt-1 ${item.warning ? 'text-red-600' : 'text-gray-900 dark:text-white'}`}>
                                {item.value}
                            </div>
                        </div>
                    );
                })}
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Pipeline Runs Over Time Chart */}
                <Card>
                    <CardHeader>
                        <CardTitle className="text-base">Pipeline Runs Over Time</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-64">
                            <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={pipelineRunsOverTime}>
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="date" tick={{ fontSize: 12 }} />
                                    <YAxis />
                                    <Tooltip />
                                    <Legend />
                                    <Line
                                        type="monotone"
                                        dataKey="success"
                                        stroke="#10B981"
                                        strokeWidth={2}
                                        dot={{ r: 3 }}
                                        name="Success"
                                    />
                                    <Line
                                        type="monotone"
                                        dataKey="failed"
                                        stroke="#EF4444"
                                        strokeWidth={2}
                                        dot={{ r: 3 }}
                                        name="Failed"
                                    />
                                </LineChart>
                            </ResponsiveContainer>
                        </div>
                    </CardContent>
                </Card>

                {/* Recent Pipeline Runs Table */}
                <Card>
                    <CardHeader>
                        <CardTitle className="text-base">Recent Pipeline Runs</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-2 max-h-64 overflow-y-auto">
                            {recentPipelineRuns.length === 0 ? (
                                <div className="text-center text-gray-500 py-8">No pipeline runs found</div>
                            ) : (
                                recentPipelineRuns.slice(0, 10).map((run) => (
                                    <div
                                        key={run.run_id}
                                        className={`flex items-center justify-between p-3 rounded-lg border transition-colors ${run.status === 'FAILED'
                                            ? 'bg-red-50 border-red-200 dark:bg-red-950 dark:border-red-800'
                                            : 'bg-white border-gray-200 dark:bg-gray-800 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-750'
                                            }`}
                                    >
                                        <div className="flex items-center gap-3">
                                            {run.status === 'FAILED' && <XCircle className="w-4 h-4 text-red-500" />}
                                            <div>
                                                <div className="font-medium text-gray-900 dark:text-white text-sm">
                                                    {run.job_name}
                                                </div>
                                                <div className="text-xs text-gray-500">
                                                    {new Date(run.started_at).toLocaleString('vi-VN')} • {run.duration_minutes} min
                                                </div>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            {getTypeBadge(run.job_type)}
                                            {getStatusBadge(run.status)}
                                            <span className="text-xs text-gray-500">{run.triggered_by}</span>
                                        </div>
                                    </div>
                                ))
                            )}
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}
