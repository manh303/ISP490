import React, { useState } from 'react';
import { getSentimentSummary, onlineSentiment, SentimentSummary, OnlineSentimentRequest, OnlineSentimentResponse } from '../../services/machineLearningApi';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';
import { Table } from '../../components/ui/table';

const SentimentAnalysisPage: React.FC = () => {
  const [summary, setSummary] = useState<SentimentSummary | null>(null);
  const [onlineResult, setOnlineResult] = useState<OnlineSentimentResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [onlineLoading, setOnlineLoading] = useState(false);

  // Form states
  const [summaryForm, setSummaryForm] = useState({
    product_key: '',
    platform_code: '',
    from_date: '',
    to_date: '',
    model_name: '',
    model_version: ''
  });

  const [onlineForm, setOnlineForm] = useState<OnlineSentimentRequest>({
    platform_code: '',
    product_key: '',
    review_text: '',
    model_name: 'sentiment_tfidf_logreg',
    model_version: 'v1.0'
  });

  const handleSummarySubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setLoading(true);
      const data = await getSentimentSummary(summaryForm);
      setSummary(data);
    } catch (error) {
      console.error('Error fetching sentiment summary:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleOnlineSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setOnlineLoading(true);
      const data = await onlineSentiment(onlineForm);
      setOnlineResult(data);
    } catch (error) {
      console.error('Error getting online sentiment:', error);
    } finally {
      setOnlineLoading(false);
    }
  };

  const handleSummaryChange = (field: string, value: string) => {
    setSummaryForm(prev => ({ ...prev, [field]: value }));
  };

  const handleOnlineChange = (field: string, value: any) => {
    setOnlineForm(prev => ({ ...prev, [field]: value }));
  };

  const platformOptions = [
    { value: 'tiki', label: 'Tiki' },
    { value: 'lazada', label: 'Lazada' }
  ];

  const getSentimentColor = (sentiment: string) => {
    switch (sentiment.toLowerCase()) {
      case 'positive': return 'text-green-600';
      case 'negative': return 'text-red-600';
      case 'neutral': return 'text-yellow-600';
      default: return 'text-gray-600';
    }
  };

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Sentiment Analysis</h1>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Summary Section */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Sentiment Summary</h2>

          <Form onSubmit={handleSummarySubmit}>
            <div className="grid grid-cols-2 gap-4 mb-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Product Key *</label>
                <Input
                  type="text"
                  value={summaryForm.product_key}
                  onChange={(e) => handleSummaryChange('product_key', e.target.value)}
                  placeholder="e.g., tiki_123456"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Platform Code *</label>
                <Select
                  options={platformOptions}
                  defaultValue={summaryForm.platform_code}
                  onChange={(value) => handleSummaryChange('platform_code', value)}
                  placeholder="Select platform"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">From Date *</label>
                <Input
                  type="date"
                  value={summaryForm.from_date}
                  onChange={(e) => handleSummaryChange('from_date', e.target.value)}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">To Date *</label>
                <Input
                  type="date"
                  value={summaryForm.to_date}
                  onChange={(e) => handleSummaryChange('to_date', e.target.value)}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model Name</label>
                <Input
                  type="text"
                  value={summaryForm.model_name}
                  onChange={(e) => handleSummaryChange('model_name', e.target.value)}
                  placeholder="Optional"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model Version</label>
                <Input
                  type="text"
                  value={summaryForm.model_version}
                  onChange={(e) => handleSummaryChange('model_version', e.target.value)}
                  placeholder="Optional"
                />
              </div>
            </div>

            <Button disabled={loading}>
              {loading ? 'Loading...' : 'Get Summary'}
            </Button>
          </Form>

          {/* Summary Results */}
          {summary && (
            <div className="mt-6">
              <h3 className="text-lg font-medium mb-3">Sentiment Summary for {summary.product_key}</h3>
              <div className="overflow-x-auto">
                <Table>
                  <thead>
                    <tr>
                      <th className="px-4 py-2 text-left">Date</th>
                      <th className="px-4 py-2 text-left">Total Reviews</th>
                      <th className="px-4 py-2 text-left">Positive</th>
                      <th className="px-4 py-2 text-left">Negative</th>
                      <th className="px-4 py-2 text-left">Neutral</th>
                      <th className="px-4 py-2 text-left">Positive Ratio</th>
                    </tr>
                  </thead>
                  <tbody>
                    {summary.points.map((point, index) => (
                      <tr key={index} className="border-t">
                        <td className="px-4 py-2">{new Date(point.date).toLocaleDateString()}</td>
                        <td className="px-4 py-2">{point.total_reviews}</td>
                        <td className="px-4 py-2 text-green-600">{point.positive}</td>
                        <td className="px-4 py-2 text-red-600">{point.negative}</td>
                        <td className="px-4 py-2 text-yellow-600">{point.neutral}</td>
                        <td className="px-4 py-2 font-medium">{(point.positive_ratio * 100).toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </Table>
              </div>
            </div>
          )}
        </div>

        {/* Online Analysis Section */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Online Sentiment Analysis</h2>

          <Form onSubmit={handleOnlineSubmit}>
            <div className="space-y-4 mb-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Platform Code *</label>
                <Select
                  options={platformOptions}
                  defaultValue={onlineForm.platform_code}
                  onChange={(value) => handleOnlineChange('platform_code', value)}
                  placeholder="Select platform"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Product Key *</label>
                <Input
                  type="text"
                  value={onlineForm.product_key}
                  onChange={(e) => handleOnlineChange('product_key', e.target.value)}
                  placeholder="e.g., tiki_123456"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Review Text *</label>
                <textarea
                  className="w-full p-2 border rounded"
                  rows={4}
                  value={onlineForm.review_text}
                  onChange={(e) => handleOnlineChange('review_text', e.target.value)}
                  required
                  placeholder="Enter review text to analyze sentiment..."
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model Name</label>
                <Input
                  type="text"
                  value={onlineForm.model_name}
                  onChange={(e) => handleOnlineChange('model_name', e.target.value)}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model Version</label>
                <Input
                  type="text"
                  value={onlineForm.model_version}
                  onChange={(e) => handleOnlineChange('model_version', e.target.value)}
                />
              </div>
            </div>

            <Button disabled={onlineLoading}>
              {onlineLoading ? 'Analyzing...' : 'Analyze Sentiment'}
            </Button>
          </Form>

          {/* Online Analysis Result */}
          {onlineResult && (
            <div className="mt-6 p-4 bg-blue-50 rounded-lg">
              <h3 className="text-lg font-medium mb-3">Analysis Result</h3>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <span className="font-medium">Sentiment:</span>
                  <div className={`text-2xl font-bold ${getSentimentColor(onlineResult.label)}`}>
                    {onlineResult.label.toUpperCase()}
                  </div>
                </div>
                <div>
                  <span className="font-medium">Confidence Score:</span>
                  <div className="text-2xl font-bold text-blue-600">
                    {(onlineResult.score * 100).toFixed(1)}%
                  </div>
                </div>
                <div>
                  <span className="font-medium">Model:</span>
                  <div>{onlineResult.model_name} ({onlineResult.model_version})</div>
                </div>
                <div>
                  <span className="font-medium">Latency:</span>
                  <div>{onlineResult.latency_ms}ms</div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default SentimentAnalysisPage;