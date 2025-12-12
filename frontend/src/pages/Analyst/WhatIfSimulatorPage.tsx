import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { FlaskConical, TrendingUp, DollarSign, Play, RotateCcw } from 'lucide-react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line } from 'recharts';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

interface SimulationResult {
    metric: string;
    baseline: number;
    simulated: number;
    change: number;
    changePercent: number;
}

export default function WhatIfSimulatorPage() {
    const [priceChange, setPriceChange] = useState(0);
    const [ratingChange, setRatingChange] = useState(0);
    const [reviewCount, setReviewCount] = useState(100);
    const [running, setRunning] = useState(false);
    const [results, setResults] = useState<SimulationResult[] | null>(null);

    const runSimulation = async () => {
        setRunning(true);
        await new Promise(resolve => setTimeout(resolve, 1500));

        // Calculate simulated results based on inputs
        const priceMultiplier = 1 + (priceChange / 100);
        const ratingMultiplier = 1 + (ratingChange / 10);
        const reviewMultiplier = reviewCount / 100;

        const simulatedResults: SimulationResult[] = [
            {
                metric: 'Revenue',
                baseline: 100000,
                simulated: Math.round(100000 * priceMultiplier * (1 - priceChange / 200)),
                change: 0,
                changePercent: 0,
            },
            {
                metric: 'Sales Volume',
                baseline: 5000,
                simulated: Math.round(5000 * (1 - priceChange / 150) * ratingMultiplier),
                change: 0,
                changePercent: 0,
            },
            {
                metric: 'Conversion Rate',
                baseline: 3.5,
                simulated: Math.round((3.5 * ratingMultiplier * reviewMultiplier) * 100) / 100,
                change: 0,
                changePercent: 0,
            },
            {
                metric: 'Customer Satisfaction',
                baseline: 4.2,
                simulated: Math.round((4.2 + ratingChange / 2) * 10) / 10,
                change: 0,
                changePercent: 0,
            },
        ];

        // Calculate changes
        simulatedResults.forEach(r => {
            r.change = r.simulated - r.baseline;
            r.changePercent = Math.round(((r.simulated - r.baseline) / r.baseline) * 100 * 10) / 10;
        });

        setResults(simulatedResults);
        setRunning(false);
    };

    const resetSimulation = () => {
        setPriceChange(0);
        setRatingChange(0);
        setReviewCount(100);
        setResults(null);
    };

    const chartData = results?.map(r => ({
        name: r.metric,
        Baseline: r.baseline,
        Simulated: r.simulated,
    }));

    return (
        <div>
            <PageMeta title="What-If Simulator" description="Simulate scenario changes and predict outcomes" />
            <PageBreadCrumb pageTitle="What-If Simulator" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center gap-3 mb-8">
                    <div className="p-2 bg-orange-100 rounded-lg dark:bg-orange-950">
                        <FlaskConical className="w-6 h-6 text-orange-600" />
                    </div>
                    <div>
                        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                            What-If Simulator
                        </h1>
                        <p className="text-sm text-gray-500">
                            Test hypothetical scenarios and predict business outcomes
                        </p>
                    </div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* Input Parameters */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <DollarSign className="w-5 h-5 text-green-500" />
                                Simulation Parameters
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-6">
                            <div>
                                <label className="block mb-2 text-sm font-medium text-gray-900 dark:text-white">
                                    Price Change: {priceChange > 0 ? '+' : ''}{priceChange}%
                                </label>
                                <input
                                    type="range"
                                    min="-50"
                                    max="50"
                                    value={priceChange}
                                    onChange={e => setPriceChange(Number(e.target.value))}
                                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer dark:bg-gray-700"
                                />
                                <div className="flex justify-between text-xs text-gray-500 mt-1">
                                    <span>-50%</span>
                                    <span>0%</span>
                                    <span>+50%</span>
                                </div>
                            </div>

                            <div>
                                <label className="block mb-2 text-sm font-medium text-gray-900 dark:text-white">
                                    Rating Change: {ratingChange > 0 ? '+' : ''}{ratingChange / 10} stars
                                </label>
                                <input
                                    type="range"
                                    min="-10"
                                    max="10"
                                    value={ratingChange}
                                    onChange={e => setRatingChange(Number(e.target.value))}
                                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer dark:bg-gray-700"
                                />
                                <div className="flex justify-between text-xs text-gray-500 mt-1">
                                    <span>-1.0</span>
                                    <span>0</span>
                                    <span>+1.0</span>
                                </div>
                            </div>

                            <div>
                                <label className="block mb-2 text-sm font-medium text-gray-900 dark:text-white">
                                    Review Count Multiplier: {reviewCount}%
                                </label>
                                <input
                                    type="range"
                                    min="50"
                                    max="200"
                                    value={reviewCount}
                                    onChange={e => setReviewCount(Number(e.target.value))}
                                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer dark:bg-gray-700"
                                />
                                <div className="flex justify-between text-xs text-gray-500 mt-1">
                                    <span>50%</span>
                                    <span>100%</span>
                                    <span>200%</span>
                                </div>
                            </div>

                            <div className="flex gap-3 pt-4">
                                <button
                                    onClick={resetSimulation}
                                    className="flex-1 flex items-center justify-center gap-2 px-4 py-2 text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-300"
                                >
                                    <RotateCcw className="w-4 h-4" />
                                    Reset
                                </button>
                                <button
                                    onClick={runSimulation}
                                    disabled={running}
                                    className="flex-1 flex items-center justify-center gap-2 px-4 py-2 text-white bg-orange-600 rounded-lg hover:bg-orange-700 disabled:opacity-50"
                                >
                                    <Play className="w-4 h-4" />
                                    {running ? 'Running...' : 'Run Simulation'}
                                </button>
                            </div>
                        </CardContent>
                    </Card>

                    {/* Results */}
                    <Card className="lg:col-span-2">
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <TrendingUp className="w-5 h-5 text-blue-500" />
                                Simulation Results
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            {results ? (
                                <div className="space-y-6">
                                    {/* Results Table */}
                                    <div className="overflow-x-auto">
                                        <table className="w-full">
                                            <thead className="bg-gray-50 dark:bg-gray-800">
                                                <tr>
                                                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Metric</th>
                                                    <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase">Baseline</th>
                                                    <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase">Simulated</th>
                                                    <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase">Change</th>
                                                </tr>
                                            </thead>
                                            <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                                                {results.map(r => (
                                                    <tr key={r.metric}>
                                                        <td className="px-4 py-3 font-medium text-gray-900 dark:text-white">{r.metric}</td>
                                                        <td className="px-4 py-3 text-right text-gray-600 dark:text-gray-400">
                                                            {r.baseline.toLocaleString()}
                                                        </td>
                                                        <td className="px-4 py-3 text-right font-medium text-gray-900 dark:text-white">
                                                            {r.simulated.toLocaleString()}
                                                        </td>
                                                        <td className={`px-4 py-3 text-right font-medium ${r.changePercent >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                                            {r.changePercent >= 0 ? '+' : ''}{r.changePercent}%
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>

                                    {/* Chart */}
                                    <div className="h-48">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <BarChart data={chartData}>
                                                <CartesianGrid strokeDasharray="3 3" />
                                                <XAxis dataKey="name" fontSize={12} />
                                                <YAxis />
                                                <Tooltip />
                                                <Bar dataKey="Baseline" fill="#94A3B8" />
                                                <Bar dataKey="Simulated" fill="#3B82F6" />
                                            </BarChart>
                                        </ResponsiveContainer>
                                    </div>
                                </div>
                            ) : (
                                <div className="flex items-center justify-center h-64 text-gray-400">
                                    <div className="text-center">
                                        <FlaskConical className="w-12 h-12 mx-auto mb-3 opacity-50" />
                                        <p>Adjust parameters and run simulation to see results</p>
                                    </div>
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
