import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import { Brain, TrendingUp, Users, CheckCircle, ArrowRight, BarChart3 } from 'lucide-react';
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

interface DSSRunsByScenario {
    scenario_name: string;
    scenario_key: string;
    runs: number;
}

interface DSSDecision {
    decision_id: number;
    title: string;
    scenario_key: string;
    status: string;
    created_by: number;
    created_by_email?: string;
    created_at: string;
    num_actions: number;
}

interface DSSAnalyticsUsageProps {
    dssRunsLast7d: number;
    decisionsCreatedLast30d: number;
    decisionsImplemented: number;
    uniqueAnalystUsers: number;
    runsByScenario: DSSRunsByScenario[];
    recentDecisions: DSSDecision[];
    selectedStatusFilter?: string;
    onStatusFilterChange?: (status: string) => void;
    onViewDecision?: (decisionId: number) => void;
    isLoading?: boolean;
}

const SCENARIO_COLORS = [
    '#3B82F6', // blue - price
    '#10B981', // green - recommendation  
    '#F59E0B', // amber - sentiment
    '#8B5CF6', // purple - other
    '#EC4899', // pink
];

const getStatusBadge = (status: string) => {
    switch (status) {
        case 'DRAFT':
            return <Badge variant="secondary">Draft</Badge>;
        case 'APPROVED':
            return <Badge className="bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300">Approved</Badge>;
        case 'IMPLEMENTED':
            return <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300">Implemented</Badge>;
        case 'REJECTED':
            return <Badge variant="destructive">Rejected</Badge>;
        case 'NEED_APPROVAL':
            return <Badge className="bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300 animate-pulse">Needs Approval</Badge>;
        default:
            return <Badge variant="outline">{status}</Badge>;
    }
};

export default function DSSAnalyticsUsage({
    dssRunsLast7d,
    decisionsCreatedLast30d,
    decisionsImplemented,
    uniqueAnalystUsers,
    runsByScenario,
    recentDecisions,
    selectedStatusFilter = 'all',
    onStatusFilterChange,
    onViewDecision,
    isLoading = false,
}: DSSAnalyticsUsageProps) {
    if (isLoading) {
        return (
            <div className="space-y-4">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Brain className="w-5 h-5 text-orange-600" />
                    DSS & Analytics Usage
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
            label: 'DSS Runs (7d)',
            value: dssRunsLast7d,
            icon: BarChart3,
            color: 'text-blue-600',
        },
        {
            label: 'Decisions Created (30d)',
            value: decisionsCreatedLast30d,
            icon: TrendingUp,
            color: 'text-green-600',
        },
        {
            label: 'Implemented',
            value: decisionsImplemented,
            icon: CheckCircle,
            color: 'text-purple-600',
        },
        {
            label: 'Unique Analysts',
            value: uniqueAnalystUsers,
            icon: Users,
            color: 'text-orange-600',
        },
    ];

    const needsApprovalCount = recentDecisions.filter(d => d.status === 'NEED_APPROVAL').length;

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Brain className="w-5 h-5 text-orange-600" />
                    DSS & Analytics Usage
                    {needsApprovalCount > 0 && (
                        <Badge className="bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300 ml-2">
                            {needsApprovalCount} pending approval
                        </Badge>
                    )}
                </h2>
                <Link to="/dss/scenarios">
                    <Button variant="outline" size="sm">
                        View DSS Scenarios
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
                            className="p-4 rounded-lg border border-gray-200 bg-white dark:border-gray-700 dark:bg-gray-800"
                        >
                            <div className="flex items-center gap-2">
                                <Icon className={`w-4 h-4 ${item.color}`} />
                                <span className="text-sm text-gray-600 dark:text-gray-400">{item.label}</span>
                            </div>
                            <div className="text-2xl font-bold mt-1 text-gray-900 dark:text-white">
                                {item.value}
                            </div>
                        </div>
                    );
                })}
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* DSS Runs by Scenario Chart */}
                <Card>
                    <CardHeader>
                        <CardTitle className="text-base">DSS Runs by Scenario</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-64">
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={runsByScenario}>
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="scenario_name" tick={{ fontSize: 11 }} angle={-15} textAnchor="end" height={60} />
                                    <YAxis />
                                    <Tooltip
                                        formatter={(value: number) => [`${value} runs`, 'Total Runs']}
                                    />
                                    <Bar dataKey="runs" radius={[4, 4, 0, 0]}>
                                        {runsByScenario.map((entry, index) => (
                                            <Cell key={`cell-${index}`} fill={SCENARIO_COLORS[index % SCENARIO_COLORS.length]} />
                                        ))}
                                    </Bar>
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </CardContent>
                </Card>

                {/* Recent DSS Decisions Table */}
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between">
                        <CardTitle className="text-base">Recent DSS Decisions</CardTitle>
                        {onStatusFilterChange && (
                            <select
                                value={selectedStatusFilter}
                                onChange={(e) => onStatusFilterChange(e.target.value)}
                                className="text-sm border rounded px-2 py-1 dark:bg-gray-800 dark:border-gray-700"
                            >
                                <option value="all">All Status</option>
                                <option value="NEED_APPROVAL">Needs Approval</option>
                                <option value="DRAFT">Draft</option>
                                <option value="APPROVED">Approved</option>
                                <option value="IMPLEMENTED">Implemented</option>
                                <option value="REJECTED">Rejected</option>
                            </select>
                        )}
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-2 max-h-64 overflow-y-auto">
                            {recentDecisions.length === 0 ? (
                                <div className="text-center text-gray-500 py-8">No decisions found</div>
                            ) : (
                                recentDecisions.slice(0, 10).map((decision) => (
                                    <div
                                        key={decision.decision_id}
                                        onClick={() => onViewDecision?.(decision.decision_id)}
                                        className={`flex items-center justify-between p-3 rounded-lg border transition-colors cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-750 ${decision.status === 'NEED_APPROVAL'
                                            ? 'bg-orange-50 border-orange-200 dark:bg-orange-950 dark:border-orange-800'
                                            : 'bg-white border-gray-200 dark:bg-gray-800 dark:border-gray-700'
                                            }`}
                                    >
                                        <div>
                                            <div className="font-medium text-gray-900 dark:text-white text-sm line-clamp-1">
                                                {decision.title}
                                            </div>
                                            <div className="text-xs text-gray-500">
                                                {decision.scenario_key} • {decision.created_by_email || `User ${decision.created_by}`}
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            {getStatusBadge(decision.status)}
                                            <div className="text-xs text-gray-500">
                                                {new Date(decision.created_at).toLocaleDateString('vi-VN')}
                                            </div>
                                            <ArrowRight className="w-4 h-4 text-gray-400" />
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
