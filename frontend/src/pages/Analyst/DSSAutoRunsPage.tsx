import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { Clock, Play, Pause, Trash2, Plus, Calendar, Settings } from 'lucide-react';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

interface ScheduledTask {
    id: string;
    name: string;
    type: 'report' | 'dss';
    schedule: string;
    nextRun: string;
    lastRun: string;
    status: 'active' | 'paused';
    recipients?: string[];
}

const defaultTasks: ScheduledTask[] = [
    {
        id: '1',
        name: 'Weekly Performance Report',
        type: 'report',
        schedule: 'Every Monday at 8:00 AM',
        nextRun: '2024-12-16 08:00',
        lastRun: '2024-12-09 08:00',
        status: 'active',
        recipients: ['analyst@company.com', 'manager@company.com'],
    },
    {
        id: '2',
        name: 'Monthly Category Analysis',
        type: 'report',
        schedule: 'First day of month at 9:00 AM',
        nextRun: '2025-01-01 09:00',
        lastRun: '2024-12-01 09:05',
        status: 'active',
        recipients: ['analyst@company.com'],
    },
    {
        id: '3',
        name: 'Price Optimization DSS',
        type: 'dss',
        schedule: 'Every day at 6:00 AM',
        nextRun: '2024-12-11 06:00',
        lastRun: '2024-12-10 06:02',
        status: 'active',
    },
    {
        id: '4',
        name: 'Review Sentiment Analysis',
        type: 'dss',
        schedule: 'Every Sunday at 10:00 PM',
        nextRun: '2024-12-15 22:00',
        lastRun: '2024-12-08 22:00',
        status: 'paused',
    },
    {
        id: '5',
        name: 'Daily Sales Summary',
        type: 'report',
        schedule: 'Every day at 11:00 PM',
        nextRun: '2024-12-10 23:00',
        lastRun: '2024-12-09 23:00',
        status: 'active',
        recipients: ['sales@company.com'],
    },
];

export default function DSSAutoRunsPage() {
    const [tasks, setTasks] = useState<ScheduledTask[]>(defaultTasks);
    const [filter, setFilter] = useState('all');

    const filteredTasks = tasks.filter(t => {
        if (filter === 'all') return true;
        return t.type === filter;
    });

    const toggleStatus = (id: string) => {
        setTasks(prev => prev.map(t =>
            t.id === id ? { ...t, status: t.status === 'active' ? 'paused' : 'active' } : t
        ));
    };

    const handleDelete = (id: string) => {
        if (confirm('Are you sure you want to delete this scheduled task?')) {
            setTasks(prev => prev.filter(t => t.id !== id));
        }
    };

    const activeCount = tasks.filter(t => t.status === 'active').length;
    const dssCount = tasks.filter(t => t.type === 'dss').length;
    const reportCount = tasks.filter(t => t.type === 'report').length;

    return (
        <div>
            <PageMeta title="DSS Auto-runs" description="Manage scheduled DSS runs and reports" />
            <PageBreadCrumb pageTitle="DSS Auto-runs" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center justify-between mb-8">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-teal-100 rounded-lg dark:bg-teal-950">
                            <Clock className="w-6 h-6 text-teal-600" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                                DSS Auto-runs & Scheduled Tasks
                            </h1>
                            <p className="text-sm text-gray-500">
                                Configure scheduled DSS runs and automated reports
                            </p>
                        </div>
                    </div>

                    <div className="flex items-center gap-3">
                        <select
                            value={filter}
                            onChange={e => setFilter(e.target.value)}
                            className="px-4 py-2 border border-gray-300 rounded-lg dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                        >
                            <option value="all">All Tasks</option>
                            <option value="dss">DSS Only</option>
                            <option value="report">Reports Only</option>
                        </select>
                        <button className="flex items-center gap-2 px-4 py-2 text-white bg-teal-600 rounded-lg hover:bg-teal-700">
                            <Plus className="w-4 h-4" />
                            Add Schedule
                        </button>
                    </div>
                </div>

                {/* Stats */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-gray-900 dark:text-white">{tasks.length}</div>
                            <div className="text-sm text-gray-500">Total Scheduled</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-green-600">{activeCount}</div>
                            <div className="text-sm text-gray-500">Active</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-blue-600">{dssCount}</div>
                            <div className="text-sm text-gray-500">DSS Tasks</div>
                        </CardContent>
                    </Card>
                    <Card>
                        <CardContent className="p-4">
                            <div className="text-2xl font-bold text-purple-600">{reportCount}</div>
                            <div className="text-sm text-gray-500">Report Tasks</div>
                        </CardContent>
                    </Card>
                </div>

                {/* Tasks List */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Calendar className="w-5 h-5 text-teal-500" />
                            Scheduled Tasks
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="p-0">
                        <div className="divide-y divide-gray-200 dark:divide-gray-700">
                            {filteredTasks.map(task => (
                                <div key={task.id} className="p-4 hover:bg-gray-50 dark:hover:bg-gray-800">
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-4">
                                            <div className={`p-2 rounded-lg ${task.status === 'active' ? 'bg-green-100 dark:bg-green-950' : 'bg-gray-100 dark:bg-gray-800'}`}>
                                                <Clock className={`w-5 h-5 ${task.status === 'active' ? 'text-green-600' : 'text-gray-400'}`} />
                                            </div>
                                            <div>
                                                <div className="flex items-center gap-2">
                                                    <span className="font-medium text-gray-900 dark:text-white">
                                                        {task.name}
                                                    </span>
                                                    <span className={`px-2 py-0.5 text-xs font-medium rounded ${task.type === 'dss'
                                                            ? 'bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-400'
                                                            : 'bg-purple-100 text-purple-700 dark:bg-purple-950 dark:text-purple-400'
                                                        }`}>
                                                        {task.type.toUpperCase()}
                                                    </span>
                                                    <span className={`px-2 py-0.5 text-xs font-medium rounded ${task.status === 'active'
                                                            ? 'bg-green-100 text-green-700 dark:bg-green-950 dark:text-green-400'
                                                            : 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400'
                                                        }`}>
                                                        {task.status}
                                                    </span>
                                                </div>
                                                <p className="text-sm text-gray-500 mt-1">{task.schedule}</p>
                                                <div className="flex items-center gap-4 mt-2 text-xs text-gray-400">
                                                    <span>Next run: {task.nextRun}</span>
                                                    <span>Last run: {task.lastRun}</span>
                                                    {task.recipients && (
                                                        <span>Recipients: {task.recipients.length}</span>
                                                    )}
                                                </div>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <button
                                                onClick={() => toggleStatus(task.id)}
                                                className={`p-2 rounded-lg ${task.status === 'active'
                                                        ? 'text-orange-600 hover:bg-orange-50 dark:hover:bg-orange-950'
                                                        : 'text-green-600 hover:bg-green-50 dark:hover:bg-green-950'
                                                    }`}
                                                title={task.status === 'active' ? 'Pause' : 'Resume'}
                                            >
                                                {task.status === 'active' ? (
                                                    <Pause className="w-5 h-5" />
                                                ) : (
                                                    <Play className="w-5 h-5" />
                                                )}
                                            </button>
                                            <button
                                                className="p-2 text-gray-600 hover:bg-gray-100 rounded-lg dark:hover:bg-gray-800"
                                                title="Settings"
                                            >
                                                <Settings className="w-5 h-5" />
                                            </button>
                                            <button
                                                onClick={() => handleDelete(task.id)}
                                                className="p-2 text-red-600 hover:bg-red-50 rounded-lg dark:hover:bg-red-950"
                                                title="Delete"
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
