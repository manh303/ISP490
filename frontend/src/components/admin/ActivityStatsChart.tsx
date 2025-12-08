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
    // Temporarily disable API call since it's not working
    // const [stats, setStats] = useState<ActivityStats | null>(null);
    // const [loading, setLoading] = useState(false);
    // const [error, setError] = useState<string | null>(null);

    // useEffect(() => {
    //     const fetchStats = async () => {
    //         setLoading(true);
    //         setError(null);
    //         try {
    //             const data = await getActivityStats({ days });
    //             setStats(data);
    //         } catch (err: any) {
    //             setError(err.message || 'Failed to fetch activity stats');
    //         } finally {
    //             setLoading(false);
    //         }
    //     };

    //     fetchStats();
    // }, [days]);

    return (
        <div className="activity-stats-chart space-y-6">
            {/* Coming Soon Message */}
            <div className="text-center py-12">
                <div className="max-w-md mx-auto">
                    <div className="mb-4">
                        <Activity className="h-16 w-16 text-gray-400 mx-auto" />
                    </div>
                    <h3 className="text-xl font-semibold text-gray-900 mb-2">
                        Activity Statistics Feature
                    </h3>
                    <p className="text-gray-600 mb-4">
                        This feature is under development and will be updated in the near future.
                    </p>
                    <p className="text-sm text-gray-500">
                        Please come back later to view detailed statistics about system activity.
                    </p>
                </div>
            </div>

            {/* Placeholder Cards - commented out for now */}
            {/*
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Total Logs</CardTitle>
                        <Activity className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">0</div>
                        <p className="text-xs text-muted-foreground">
                            In the last {days} days
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Unique Actions</CardTitle>
                        <TrendingUp className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">0</div>
                        <p className="text-xs text-muted-foreground">
                            Different types of actions
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Average logs/day</CardTitle>
                        <Calendar className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">0</div>
                        <p className="text-xs text-muted-foreground">
                            Average per day
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Most Active Day</CardTitle>
                        <Users className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">0</div>
                        <p className="text-xs text-muted-foreground">
                            Maximum logs in a day
                        </p>
                    </CardContent>
                </Card>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <Card>
                    <CardHeader>
                        <CardTitle>Activity by Action Type</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-[300px] flex items-center justify-center text-gray-500">
                            Data will be displayed when the feature is active
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader>
                        <CardTitle>Daily Activity Trend</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-[300px] flex items-center justify-center text-gray-500">
                            Data will be displayed when the feature is active
                        </div>
                    </CardContent>
                </Card>
            </div>
            */}
        </div>
    );
}