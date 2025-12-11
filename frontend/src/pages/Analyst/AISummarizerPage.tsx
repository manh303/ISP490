import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/figma/card';
import { Brain, Sparkles, FileText, Copy, RefreshCw } from 'lucide-react';
import PageMeta from '../../components/common/PageMeta';
import PageBreadCrumb from '../../components/common/PageBreadCrumb';

const sampleReviews = [
    "Great product! Works exactly as described. Fast shipping and excellent quality.",
    "Not satisfied with the purchase. The item arrived damaged and customer service was slow to respond.",
    "Amazing value for money. Would definitely recommend to friends and family.",
    "Product broke after one week of use. Very disappointed with the durability.",
    "Exceeded my expectations! The features are even better than advertised.",
];

export default function AISummarizerPage() {
    const [inputText, setInputText] = useState('');
    const [summary, setSummary] = useState('');
    const [loading, setLoading] = useState(false);
    const [summaryType, setSummaryType] = useState('reviews');

    const handleSummarize = async () => {
        if (!inputText.trim()) return;

        setLoading(true);
        // Simulate AI processing
        await new Promise(resolve => setTimeout(resolve, 2000));

        // Mock AI summary response
        const mockSummary = summaryType === 'reviews'
            ? `**Review Summary (${inputText.split('\n').filter(l => l.trim()).length} reviews analyzed)**\n\n` +
            `**Overall Sentiment:** Mixed (60% Positive, 40% Negative)\n\n` +
            `**Key Positives:**\n- Product quality praised by majority\n- Fast shipping mentioned frequently\n- Good value for money\n\n` +
            `**Key Concerns:**\n- Some durability issues reported\n- Customer service response time could improve\n\n` +
            `**Recommendation:** Consider addressing durability concerns and improving customer support response times.`
            : `**Text Summary**\n\n${inputText.substring(0, 200)}...\n\n**Key Points:**\n- Point 1 extracted from text\n- Point 2 extracted from text\n- Point 3 extracted from text`;

        setSummary(mockSummary);
        setLoading(false);
    };

    const loadSampleReviews = () => {
        setInputText(sampleReviews.join('\n\n'));
    };

    const copyToClipboard = () => {
        navigator.clipboard.writeText(summary);
        alert('Summary copied to clipboard!');
    };

    return (
        <div>
            <PageMeta title="AI Summarizer" description="Summarize reviews and logs using AI" />
            <PageBreadCrumb pageTitle="AI Summarizer" />

            <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
                {/* Header */}
                <div className="flex items-center gap-3 mb-8">
                    <div className="p-2 bg-purple-100 rounded-lg dark:bg-purple-950">
                        <Brain className="w-6 h-6 text-purple-600" />
                    </div>
                    <div>
                        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                            AI Summarizer
                        </h1>
                        <p className="text-sm text-gray-500">
                            Summarize reviews, logs, and text content using AI
                        </p>
                    </div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Input Section */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center justify-between">
                                <span className="flex items-center gap-2">
                                    <FileText className="w-5 h-5 text-blue-500" />
                                    Input Text
                                </span>
                                <select
                                    value={summaryType}
                                    onChange={e => setSummaryType(e.target.value)}
                                    className="px-3 py-1 text-sm border border-gray-300 rounded dark:border-gray-700 dark:bg-gray-800"
                                >
                                    <option value="reviews">Product Reviews</option>
                                    <option value="logs">System Logs</option>
                                    <option value="general">General Text</option>
                                </select>
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <textarea
                                value={inputText}
                                onChange={e => setInputText(e.target.value)}
                                placeholder="Paste reviews, logs, or any text you want to summarize..."
                                className="w-full h-64 p-4 border border-gray-300 rounded-lg resize-none dark:border-gray-700 dark:bg-gray-800 dark:text-white"
                            />
                            <div className="flex gap-3 mt-4">
                                <button
                                    onClick={loadSampleReviews}
                                    className="flex items-center gap-2 px-4 py-2 text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-300"
                                >
                                    Load Sample Data
                                </button>
                                <button
                                    onClick={handleSummarize}
                                    disabled={loading || !inputText.trim()}
                                    className="flex items-center gap-2 px-4 py-2 text-white bg-purple-600 rounded-lg hover:bg-purple-700 disabled:opacity-50"
                                >
                                    {loading ? (
                                        <>
                                            <RefreshCw className="w-4 h-4 animate-spin" />
                                            Processing...
                                        </>
                                    ) : (
                                        <>
                                            <Sparkles className="w-4 h-4" />
                                            Summarize with AI
                                        </>
                                    )}
                                </button>
                            </div>
                        </CardContent>
                    </Card>

                    {/* Output Section */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center justify-between">
                                <span className="flex items-center gap-2">
                                    <Sparkles className="w-5 h-5 text-purple-500" />
                                    AI Summary
                                </span>
                                {summary && (
                                    <button
                                        onClick={copyToClipboard}
                                        className="flex items-center gap-1 px-3 py-1 text-sm text-gray-600 border border-gray-300 rounded hover:bg-gray-100 dark:border-gray-700 dark:text-gray-300"
                                    >
                                        <Copy className="w-4 h-4" />
                                        Copy
                                    </button>
                                )}
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="w-full h-64 p-4 overflow-y-auto border border-gray-300 rounded-lg bg-gray-50 dark:border-gray-700 dark:bg-gray-800">
                                {loading ? (
                                    <div className="flex items-center justify-center h-full">
                                        <div className="text-center">
                                            <RefreshCw className="w-8 h-8 mx-auto mb-2 text-purple-500 animate-spin" />
                                            <p className="text-gray-500">AI is processing your text...</p>
                                        </div>
                                    </div>
                                ) : summary ? (
                                    <pre className="whitespace-pre-wrap text-sm text-gray-900 dark:text-white font-sans">
                                        {summary}
                                    </pre>
                                ) : (
                                    <div className="flex items-center justify-center h-full text-gray-400">
                                        Summary will appear here after processing
                                    </div>
                                )}
                            </div>
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
