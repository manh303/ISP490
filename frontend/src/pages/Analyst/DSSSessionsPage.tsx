import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { listDSSSessions, DSSSessionListResponse, DSSSessionItem, runPricePredictionDSS, runProductRecommendationDSS, runReviewSentimentDSS } from '../../services/DSSApi';
import { Clock, TrendingUp, Users, MessageSquare, CheckCircle, XCircle, ArrowRight, RefreshCw, Eye, Loader2 } from 'lucide-react';

const DSSSessionsPage: React.FC = () => {
    const navigate = useNavigate();
    const [sessions, setSessions] = useState<DSSSessionItem[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [pageSize] = useState(10);
    const [loadingSessionId, setLoadingSessionId] = useState<number | null>(null);

    // Filters
    const [scenarioKey, setScenarioKey] = useState<string>('');

    const fetchSessions = async () => {
        try {
            setLoading(true);
            const params: any = { page, page_size: pageSize };
            if (scenarioKey) params.scenario_key = scenarioKey;

            const response: DSSSessionListResponse = await listDSSSessions(params);
            setSessions(response.items);
            setTotal(response.total);
        } catch (err) {
            setError('Failed to load analysis history');
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchSessions();
    }, [page, scenarioKey]);

    const getScenarioIcon = (key: string) => {
        switch (key) {
            case 'price_prediction':
                return <TrendingUp className="w-5 h-5 text-blue-500" />;
            case 'product_recommendation':
                return <Users className="w-5 h-5 text-green-500" />;
            case 'review_sentiment':
                return <MessageSquare className="w-5 h-5 text-amber-500" />;
            default:
                return <Clock className="w-5 h-5 text-gray-500" />;
        }
    };

    const getAIStatusColor = (status: string) => {
        switch (status) {
            case 'completed':
                return 'bg-green-100 text-green-800';
            case 'pending':
            case 'generating':
                return 'bg-yellow-100 text-yellow-800';
            case 'failed':
                return 'bg-red-100 text-red-800';
            case 'skipped':
                return 'bg-gray-100 text-gray-800';
            default:
                return 'bg-gray-100 text-gray-800';
        }
    };

    const formatDate = (dateStr: string) => {
        const date = new Date(dateStr);
        return date.toLocaleString('vi-VN', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    };

    // Re-run DSS analysis with saved filters and navigate to results page
    const handleViewResults = async (session: DSSSessionItem) => {
        setLoadingSessionId(session.session_id);
        try {
            const filters = session.filters;
            let result;

            // Re-run the DSS analysis based on scenario type
            if (session.scenario_key === 'price_prediction') {
                result = await runPricePredictionDSS({
                    from_date: filters.from_date,
                    to_date: filters.to_date,
                    platforms: filters.platforms,
                    categories: filters.categories,
                    scope_mode: filters.scope_mode || 'by_category',
                    product_keys: filters.product_keys,
                    min_price_change_pct: 0.0,
                    page: 1,
                    page_size: 50
                });
            } else if (session.scenario_key === 'product_recommendation') {
                result = await runProductRecommendationDSS({
                    from_date: filters.from_date,
                    to_date: filters.to_date,
                    platforms: filters.platforms,
                    categories: filters.categories,
                    scope_mode: filters.scope_mode || 'by_category',
                    source_product_key: filters.source_product_key,
                    top_k: 50
                });
            } else if (session.scenario_key === 'review_sentiment') {
                result = await runReviewSentimentDSS({
                    from_date: filters.from_date,
                    to_date: filters.to_date,
                    platforms: filters.platforms,
                    categories: filters.categories
                });
            }

            if (result) {
                navigate(`/analyst/dss/${session.scenario_key}/results`, {
                    state: {
                        inputData: filters,
                        dssResults: result
                    }
                });
            }
        } catch (err) {
            console.error('Failed to re-run analysis:', err);
            alert('Failed to load results. Please try again.');
        } finally {
            setLoadingSessionId(null);
        }
    };

    const handleCreateDecision = (session: DSSSessionItem) => {
        navigate('/analyst/dss-decisions/create', {
            state: {
                scenario_key: session.scenario_key,
                kpi_summary: session.kpi_summary,
                filters: session.filters,
                title: `${session.scenario_name} - ${formatDate(session.generated_at)}`,
                description: `Analysis from session #${session.session_id}`
            }
        });
    };

    if (loading) {
        return (
            <div className="p-6">
                <div className="flex justify-center items-center h-64">
                    <RefreshCw className="w-8 h-8 animate-spin text-blue-500" />
                    <span className="ml-2 text-lg">Loading analysis history...</span>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="p-6">
                <div className="text-center text-red-600">{error}</div>
            </div>
        );
    }

    return (
        <div className="p-6">
            <div className="flex justify-between items-center mb-6">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900 dark:text-white">DSS Analysis History</h1>
                    <p className="text-gray-500 mt-1">View your past DSS analysis runs</p>
                </div>
                <button
                    onClick={fetchSessions}
                    className="flex items-center gap-2 bg-gray-100 hover:bg-gray-200 text-gray-700 px-4 py-2 rounded-lg transition-colors"
                >
                    <RefreshCw className="w-4 h-4" />
                    Refresh
                </button>
            </div>

            {/* Filters */}
            <div className="bg-white dark:bg-gray-800 p-4 rounded-lg shadow mb-6">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                            Scenario
                        </label>
                        <select
                            value={scenarioKey}
                            onChange={(e) => setScenarioKey(e.target.value)}
                            className="w-full border border-gray-300 dark:border-gray-600 rounded px-3 py-2 dark:bg-gray-700 dark:text-white"
                        >
                            <option value="">All Scenarios</option>
                            <option value="price_prediction">Price Prediction</option>
                            <option value="product_recommendation">Product Recommendation</option>
                            <option value="review_sentiment">Review Sentiment</option>
                        </select>
                    </div>
                </div>
            </div>

            {/* Sessions List */}
            {sessions.length === 0 ? (
                <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-8 text-center">
                    <Clock className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                    <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">No Analysis History</h3>
                    <p className="text-gray-500">Run a DSS analysis to see your history here</p>
                </div>
            ) : (
                <div className="space-y-4">
                    {sessions.map((session) => (
                        <div
                            key={session.session_id}
                            className="bg-white dark:bg-gray-800 rounded-lg shadow p-4 hover:shadow-md transition-shadow"
                        >
                            <div className="flex items-start justify-between">
                                <div className="flex items-start gap-4">
                                    <div className="p-2 bg-gray-100 dark:bg-gray-700 rounded-lg">
                                        {getScenarioIcon(session.scenario_key)}
                                    </div>
                                    <div>
                                        <h3 className="font-semibold text-gray-900 dark:text-white">
                                            {session.scenario_name}
                                        </h3>
                                        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                            Session #{session.session_id} • {formatDate(session.generated_at)}
                                        </p>
                                        {session.user_email && (
                                            <p className="text-sm text-gray-400 dark:text-gray-500">
                                                By: {session.user_email}
                                            </p>
                                        )}

                                        {/* Filters summary */}
                                        <div className="flex flex-wrap gap-2 mt-2">
                                            {session.filters.platforms?.length > 0 && (
                                                <span className="text-xs bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 px-2 py-1 rounded">
                                                    Platforms: {session.filters.platforms.join(', ')}
                                                </span>
                                            )}
                                            {session.filters.from_date && (
                                                <span className="text-xs bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300 px-2 py-1 rounded">
                                                    {session.filters.from_date} → {session.filters.to_date}
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                </div>

                                <div className="flex flex-col items-end gap-2">
                                    {/* AI Status */}
                                    <span className={`text-xs px-2 py-1 rounded-full ${getAIStatusColor(session.ai_generation_status)}`}>
                                        AI: {session.ai_generation_status}
                                    </span>

                                    {/* Decision Status */}
                                    {session.has_decision ? (
                                        <span className="flex items-center gap-1 text-xs text-green-600">
                                            <CheckCircle className="w-3 h-3" />
                                            Decision saved
                                        </span>
                                    ) : (
                                        <span className="flex items-center gap-1 text-xs text-gray-400">
                                            <XCircle className="w-3 h-3" />
                                            No decision
                                        </span>
                                    )}

                                    {/* Actions */}
                                    <div className="flex gap-2 mt-2">
                                        <button
                                            onClick={() => handleViewResults(session)}
                                            disabled={loadingSessionId === session.session_id}
                                            className="flex items-center gap-1 text-sm bg-indigo-600 hover:bg-indigo-700 text-white px-3 py-1 rounded transition-colors disabled:opacity-50"
                                        >
                                            {loadingSessionId === session.session_id ? (
                                                <><Loader2 className="w-3 h-3 animate-spin" /> Loading...</>
                                            ) : (
                                                <><Eye className="w-3 h-3" /> View Results</>
                                            )}
                                        </button>
                                        {!session.has_decision && (
                                            <button
                                                onClick={() => handleCreateDecision(session)}
                                                className="text-sm bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded transition-colors"
                                            >
                                                Create Decision
                                            </button>
                                        )}
                                        {session.has_decision && session.decision_id && (
                                            <button
                                                onClick={() => navigate(`/analyst/dss-decisions/${session.decision_id}`)}
                                                className="flex items-center gap-1 text-sm bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded transition-colors"
                                            >
                                                View Decision <ArrowRight className="w-3 h-3" />
                                            </button>
                                        )}
                                    </div>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Pagination */}
            {total > pageSize && (
                <div className="flex justify-center items-center gap-4 mt-6">
                    <button
                        disabled={page <= 1}
                        onClick={() => setPage(page - 1)}
                        className="px-4 py-2 border border-gray-300 rounded disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        Previous
                    </button>
                    <span className="text-gray-600 dark:text-gray-300">
                        Page {page} of {Math.ceil(total / pageSize)}
                    </span>
                    <button
                        disabled={page >= Math.ceil(total / pageSize)}
                        onClick={() => setPage(page + 1)}
                        className="px-4 py-2 border border-gray-300 rounded disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        Next
                    </button>
                </div>
            )}

            {/* Total count */}
            <div className="text-center text-sm text-gray-500 mt-4">
                Total: {total} analysis sessions
            </div>
        </div>
    );
};

export default DSSSessionsPage;
