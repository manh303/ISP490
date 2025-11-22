import React, { useEffect, useState } from "react";
import { getActivityStats } from "../../services/adminApi";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line } from 'recharts';
import { Activity, TrendingUp, Calendar, Users } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';

interface ActivityStats {
    total_logs: number;
    logs_by_action: Record<string, number>;
    logs_by_day: Array<{ date: string; count: number }>;
}

interface ActivityStatsChartProps {
    days?: number;
}

export default function ActivityStatsChart({ days = 7 }: ActivityStatsChartProps) {
    const [stats, setStats] = useState<ActivityStats | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchStats = async () => {
            setLoading(true);
            setError(null);
            try {
                const data = await getActivityStats({ days });
                setStats(data);
            } catch (err: any) {
                setError(err.message || 'Failed to fetch activity stats');
            } finally {
                setLoading(false);
            }
        };

        fetchStats();
    }, [days]);

    if (loading) {
        return <div className="text-center py-8">Loading activity statistics...</div>;
    }

    if (error) {
        return <div className="text-center py-8 text-red-500">Error: {error}</div>;
    }

    if (!stats) {
        return <div className="text-center py-8">No data available</div>;
    }

    // Prepare data for action chart
    const actionChartData = Object.entries(stats.logs_by_action).map(([action, count]) => ({
        action,
        count
    }));

    // Prepare data for day chart
    const dayChartData = stats.logs_by_day.map(item => ({
        date: new Date(item.date).toLocaleDateString(),
        count: item.count
    }));

    return (
        <div className="activity-stats-chart space-y-6">
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Total Logs</CardTitle>
                        <Activity className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats.total_logs.toLocaleString()}</div>
                        <p className="text-xs text-muted-foreground">
                            Last {days} days
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Unique Actions</CardTitle>
                        <TrendingUp className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{Object.keys(stats.logs_by_action).length}</div>
                        <p className="text-xs text-muted-foreground">
                            Different action types
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Avg Daily Logs</CardTitle>
                        <Calendar className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">
                            {Math.round(stats.total_logs / days)}
                        </div>
                        <p className="text-xs text-muted-foreground">
                            Per day average
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Most Active Day</CardTitle>
                        <Users className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">
                            {stats.logs_by_day.length > 0 ? Math.max(...stats.logs_by_day.map(d => d.count)) : 0}
                        </div>
                        <p className="text-xs text-muted-foreground">
                            Max logs in a day
                        </p>
                    </CardContent>
                </Card>
            </div>

            {/* Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Logs by Action */}
                <Card>
                    <CardHeader>
                        <CardTitle>Activity by Action Type</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <ResponsiveContainer width="100%" height={300}>
                            <BarChart data={actionChartData}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="action" />
                                <YAxis />
                                <Tooltip />
                                <Bar dataKey="count" fill="#3b82f6" />
                            </BarChart>
                        </ResponsiveContainer>
                    </CardContent>
                </Card>

                {/* Logs by Day */}
                <Card>
                    <CardHeader>
                        <CardTitle>Daily Activity Trend</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <ResponsiveContainer width="100%" height={300}>
                            <LineChart data={dayChartData}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="date" />
                                <YAxis />
                                <Tooltip />
                                <Line type="monotone" dataKey="count" stroke="#10b981" strokeWidth={2} />
                            </LineChart>
                        </ResponsiveContainer>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}