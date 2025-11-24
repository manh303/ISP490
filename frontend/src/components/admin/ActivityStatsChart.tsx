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
                        Tính năng thống kê hoạt động
                    </h3>
                    <p className="text-gray-600 mb-4">
                        Tính năng này đang được phát triển và sẽ được cập nhật trong tương lai gần.
                    </p>
                    <p className="text-sm text-gray-500">
                        Vui lòng quay lại sau để xem thống kê chi tiết về hoạt động của hệ thống.
                    </p>
                </div>
            </div>

            {/* Placeholder Cards - commented out for now */}
            {/*
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Tổng số nhật ký</CardTitle>
                        <Activity className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">0</div>
                        <p className="text-xs text-muted-foreground">
                            Trong {days} ngày qua
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Hành động duy nhất</CardTitle>
                        <TrendingUp className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">0</div>
                        <p className="text-xs text-muted-foreground">
                            Các loại hành động khác nhau
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Trung bình nhật ký/ngày</CardTitle>
                        <Calendar className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">0</div>
                        <p className="text-xs text-muted-foreground">
                            Trung bình mỗi ngày
                        </p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Ngày hoạt động nhiều nhất</CardTitle>
                        <Users className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">0</div>
                        <p className="text-xs text-muted-foreground">
                            Số nhật ký tối đa trong một ngày
                        </p>
                    </CardContent>
                </Card>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <Card>
                    <CardHeader>
                        <CardTitle>Hoạt động theo loại hành động</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-[300px] flex items-center justify-center text-gray-500">
                            Dữ liệu sẽ được hiển thị khi tính năng hoạt động
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader>
                        <CardTitle>Xu hướng hoạt động hàng ngày</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-[300px] flex items-center justify-center text-gray-500">
                            Dữ liệu sẽ được hiển thị khi tính năng hoạt động
                        </div>
                    </CardContent>
                </Card>
            </div>
            */}
        </div>
    );
}