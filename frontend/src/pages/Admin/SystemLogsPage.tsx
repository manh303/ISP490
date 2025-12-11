import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { Terminal, AlertCircle, RefreshCw, Download, Filter, Clock, Server, Database, Cpu } from 'lucide-react';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

interface SystemLog {
    id: number;
    timestamp: string;
    level: 'ERROR' | 'WARNING' | 'INFO' | 'DEBUG';
    source: string;
    message: string;
    details?: string;
}

// Mock data for demonstration
const mockLogs: SystemLog[] = [
    {
        id: 1,
        timestamp: new Date(Date.now() - 5 * 60000).toISOString(),
        level: 'ERROR',
        source: 'Database',
        message: 'Connection pool exhausted',
        details: 'Max connections: 20, Active: 20, Waiting: 5',
    },
    {
        id: 2,
        timestamp: new Date(Date.now() - 15 * 60000).toISOString(),
        level: 'WARNING',
        source: 'AI/Gemini',
        message: 'Rate limit approaching',
        details: 'Used: 850/1000 requests per minute',
    },
    {
        id: 3,
        timestamp: new Date(Date.now() - 30 * 60000).toISOString(),
        level: 'INFO',
        source: 'ETL Pipeline',
        message: 'Daily ETL completed successfully',
        details: 'Duration: 45 minutes, Records processed: 125,000',
    },
    {
        id: 4,
        timestamp: new Date(Date.now() - 45 * 60000).toISOString(),
        level: 'ERROR',
        source: 'API',
        message: 'Endpoint /api/v1/dss/price/run failed',
        details: 'Status: 500, Error: Internal Server Error',
    },
    {
        id: 5,
        timestamp: new Date(Date.now() - 60 * 60000).toISOString(),
        level: 'WARNING',
        source: 'ML Service',
        message: 'Model prediction latency high',
        details: 'Average latency: 2.5s (threshold: 1s)',
    },
    {
        id: 6,
        timestamp: new Date(Date.now() - 90 * 60000).toISOString(),
        level: 'INFO',
        source: 'System',
        message: 'Daily backup completed',
        details: 'Size: 2.3 GB, Duration: 12 minutes',
    },
    {
        id: 7,
        timestamp: new Date(Date.now() - 120 * 60000).toISOString(),
        level: 'DEBUG',
        source: 'API',
        message: 'Cache miss for category analytics',
        details: 'Key: category_kpi_electronics_30d',
    },
    {
        id: 8,
        timestamp: new Date(Date.now() - 180 * 60000).toISOString(),
        level: 'ERROR',
        source: 'Database',
        message: 'Query timeout exceeded',
        details: 'Query: SELECT * FROM dwh.fact_product_daily, Duration: 35s',
    },
];

export default function SystemLogsPage() {
    const [logs, setLogs] = useState<SystemLog[]>(mockLogs);
    const [loading, setLoading] = useState(false);
    const [levelFilter, setLevelFilter] = useState<string>('all');
    const [sourceFilter, setSourceFilter] = useState<string>('all');
    const [autoRefresh, setAutoRefresh] = useState(false);

    const uniqueSources = [...new Set(mockLogs.map(l => l.source))];

    const filteredLogs = logs.filter(log => {
        if (levelFilter !== 'all' && log.level !== levelFilter) return false;
        if (sourceFilter !== 'all' && log.source !== sourceFilter) return false;
        return true;
    });

    const handleRefresh = async () => {
        setLoading(true);
        await new Promise(resolve => setTimeout(resolve, 1000));
        setLogs([...mockLogs]);
        setLoading(false);
    };

    useEffect(() => {
        if (autoRefresh) {
            const interval = setInterval(handleRefresh, 30000);
            return () => clearInterval(interval);
        }
    }, [autoRefresh]);

    const getLevelBadge = (level: string) => {
        const colors: Record<string, string> = {
            ERROR: 'bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-400',
            WARNING: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-950 dark:text-yellow-400',
            INFO: 'bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-400',
            DEBUG: 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300',
        };
        return (
            <span className={`px-2 py-1 text-xs font-medium rounded ${colors[level] || colors.INFO}`}>
                {level}
            </span>
        );
    };

    const getSourceIcon = (source: string) => {
        switch (source) {
            case 'Database':
                return <Database className="w-4 h-4 text-blue-500" />;
            case 'API':
                return <Server className="w-4 h-4 text-green-500" />;
            case 'AI/Gemini':
            case 'ML Service':
                return <Cpu className="w-4 h-4 text-purple-500" />;
            default:
                return <Terminal className="w-4 h-4 text-gray-500" />;
        }
    };

    const errorCount = logs.filter(l => l.level === 'ERROR').length;
    const warningCount = logs.filter(l => l.level === 'WARNING').length;

    return (
        <div>
            <PageMeta title="System Logs" description="Backend/API errors, DB query failures, AI rate limits" />
            <PageBreadCrumb pageTitle="System Logs" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center justify-between mb-6">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-gray-100 rounded-lg dark:bg-gray-800">
                            <Terminal className="w-6 h-6 text-gray-600 dark:text-gray-400" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                                System Logs
                            </h1>
                            <p className="text-sm text-gray-500">
                                Backend/API errors, DB query failures, AI rate limits
                            </p>
                        </div>
                    </div>

                    <div className="flex items-center gap-3">
                        <label className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400">
                            <input
                                type="checkbox"
                                checked={autoRefresh}
                                onChange={e => setAutoRefresh(e.target.checked)}
                                className="rounded"
                            />
                            Auto-refresh (30s)
                        </label>
                        <button
                            onClick={handleRefresh}
                            disabled={loading}
                            className="flex items-center gap-2 px-3 py-2 text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-300 disabled:opacity-50"
                        >
                            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
                            Refresh
                        </button>
                        <button className="flex items-center gap-2 px-3 py-2 text-white bg-blue-600 rounded-lg hover:bg-blue-700">
                            <Download className="w-4 h-4" />
                            Export
                        </button>
                    </div>
                </div>

                {/* Stats */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                    <Card>
                        <CardContent className="p-4 flex items-center gap-3">
                            <div className="p-2 bg-red-100 rounded-lg dark:bg-red-950">
                                <AlertCircle className="w-5 h-5 text-red-600" />
                            </div>
                            <div>
                                <div className="text-xl font-bold text-red-600">{errorCount}</div>
                                <div className="text-sm text-gray-500">Errors</div>
                            </div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4 flex items-center gap-3">
                            <div className="p-2 bg-yellow-100 rounded-lg dark:bg-yellow-950">
                                <AlertCircle className="w-5 h-5 text-yellow-600" />
                            </div>
                            <div>
                                <div className="text-xl font-bold text-yellow-600">{warningCount}</div>
                                <div className="text-sm text-gray-500">Warnings</div>
                            </div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4 flex items-center gap-3">
                            <div className="p-2 bg-blue-100 rounded-lg dark:bg-blue-950">
                                <Terminal className="w-5 h-5 text-blue-600" />
                            </div>
                            <div>
                                <div className="text-xl font-bold text-gray-900 dark:text-white">{logs.length}</div>
                                <div className="text-sm text-gray-500">Total Logs</div>
                            </div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4 flex items-center gap-3">
                            <div className="p-2 bg-green-100 rounded-lg dark:bg-green-950">
                                <Clock className="w-5 h-5 text-green-600" />
                            </div>
                            <div>
                                <div className="text-xl font-bold text-gray-900 dark:text-white">3h</div>
                                <div className="text-sm text-gray-500">Time Range</div>
                            </div>
                        </CardContent>
                    </Card>
                </div>

                {/* Filters */}
                <div className="flex gap-4 mb-6">
                    <div className="flex items-center gap-2">
                        <Filter className="w-4 h-4 text-gray-500" />
                        <select
                            value={levelFilter}
                            onChange={e => setLevelFilter(e.target.value)}
                            className="px-3 py-2 border border-gray-300 rounded-lg dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                        >
                            <option value="all">All Levels</option>
                            <option value="ERROR">ERROR</option>
                            <option value="WARNING">WARNING</option>
                            <option value="INFO">INFO</option>
                            <option value="DEBUG">DEBUG</option>
                        </select>
                    </div>
                    <select
                        value={sourceFilter}
                        onChange={e => setSourceFilter(e.target.value)}
                        className="px-3 py-2 border border-gray-300 rounded-lg dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                    >
                        <option value="all">All Sources</option>
                        {uniqueSources.map(src => (
                            <option key={src} value={src}>{src}</option>
                        ))}
                    </select>
                </div>

                {/* Logs Table */}
                <Card>
                    <CardContent className="p-0">
                        <div className="overflow-x-auto">
                            <table className="w-full">
                                <thead className="bg-gray-50 dark:bg-gray-800">
                                    <tr>
                                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Time</th>
                                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Level</th>
                                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Source</th>
                                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Message</th>
                                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Details</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                                    {filteredLogs.map(log => (
                                        <tr
                                            key={log.id}
                                            className={`hover:bg-gray-50 dark:hover:bg-gray-800 ${log.level === 'ERROR' ? 'bg-red-50 dark:bg-red-950/20' : ''
                                                }`}
                                        >
                                            <td className="px-4 py-3 text-sm text-gray-500 whitespace-nowrap">
                                                {new Date(log.timestamp).toLocaleString('en-US')}
                                            </td>
                                            <td className="px-4 py-3">
                                                {getLevelBadge(log.level)}
                                            </td>
                                            <td className="px-4 py-3">
                                                <div className="flex items-center gap-2">
                                                    {getSourceIcon(log.source)}
                                                    <span className="text-sm text-gray-900 dark:text-white">{log.source}</span>
                                                </div>
                                            </td>
                                            <td className="px-4 py-3 text-sm text-gray-900 dark:text-white">
                                                {log.message}
                                            </td>
                                            <td className="px-4 py-3 text-sm text-gray-500 max-w-xs truncate">
                                                {log.details}
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}
