import React, { useState, useEffect } from 'react';
import { getPricePredictionHistory, onlinePricePrediction, PricePredictionHistory, OnlinePricePredictionRequest, OnlinePricePredictionResponse } from '../../services/machineLearningApi';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';
import { Table } from '../../components/ui/table';

const PricePredictionPage: React.FC = () => {
  const [history, setHistory] = useState<PricePredictionHistory | null>(null);
  const [onlineResult, setOnlineResult] = useState<OnlinePricePredictionResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [onlineLoading, setOnlineLoading] = useState(false);

  // Form states
  const [historyForm, setHistoryForm] = useState({
    product_key: '',
    platform_code: '',
    from_date: '',
    to_date: '',
    model_name: '',
    model_version: ''
  });

  const [onlineForm, setOnlineForm] = useState<OnlinePricePredictionRequest>({
    platform_code: '',
    product_key: '',
    current_price: 0,
    avg_rating: 0,
    review_count: 0,
    model_name: 'price_forecast_rf',
    model_version: 'v1.0'
  });

  const handleHistorySubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setLoading(true);
      const data = await getPricePredictionHistory(historyForm);
      setHistory(data);
    } catch (error) {
      console.error('Error fetching price prediction history:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleOnlineSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setOnlineLoading(true);
      const data = await onlinePricePrediction(onlineForm);
      setOnlineResult(data);
    } catch (error) {
      console.error('Error getting online price prediction:', error);
    } finally {
      setOnlineLoading(false);
    }
  };

  const handleHistoryChange = (field: string, value: string) => {
    setHistoryForm(prev => ({ ...prev, [field]: value }));
  };

  const handleOnlineChange = (field: string, value: any) => {
    setOnlineForm(prev => ({ ...prev, [field]: value }));
  };

  const platformOptions = [
    { value: 'tiki', label: 'Tiki' },
    { value: 'lazada', label: 'Lazada' }
  ];

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Price Prediction</h1>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* History Section */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Prediction History</h2>

          <Form onSubmit={handleHistorySubmit}>
            <div className="grid grid-cols-2 gap-4 mb-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Product Key *</label>
                <Input
                  type="text"
                  value={historyForm.product_key}
                  onChange={(e) => handleHistoryChange('product_key', e.target.value)}
                  placeholder="e.g., tiki_123456"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Platform Code *</label>
                <Select
                  options={platformOptions}
                  defaultValue={historyForm.platform_code}
                  onChange={(value) => handleHistoryChange('platform_code', value)}
                  placeholder="Select platform"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">From Date *</label>
                <Input
                  type="date"
                  value={historyForm.from_date}
                  onChange={(e) => handleHistoryChange('from_date', e.target.value)}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">To Date *</label>
                <Input
                  type="date"
                  value={historyForm.to_date}
                  onChange={(e) => handleHistoryChange('to_date', e.target.value)}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model Name</label>
                <Input
                  type="text"
                  value={historyForm.model_name}
                  onChange={(e) => handleHistoryChange('model_name', e.target.value)}
                  placeholder="Optional"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model Version</label>
                <Input
                  type="text"
                  value={historyForm.model_version}
                  onChange={(e) => handleHistoryChange('model_version', e.target.value)}
                  placeholder="Optional"
                />
              </div>
            </div>

            <Button disabled={loading}>
              {loading ? 'Loading...' : 'Get History'}
            </Button>
          </Form>

          {/* History Results */}
          {history && (
            <div className="mt-6">
              <h3 className="text-lg font-medium mb-3">Prediction History for {history.product_key}</h3>
              <div className="overflow-x-auto">
                <Table>
                  <thead>
                    <tr>
                      <th className="px-4 py-2 text-left">Date</th>
                      <th className="px-4 py-2 text-left">Predicted Price</th>
                      <th className="px-4 py-2 text-left">CI Lower</th>
                      <th className="px-4 py-2 text-left">CI Upper</th>
                      <th className="px-4 py-2 text-left">Run ID</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.points.map((point, index) => (
                      <tr key={index} className="border-t">
                        <td className="px-4 py-2">{new Date(point.date).toLocaleDateString()}</td>
                        <td className="px-4 py-2">{point.predicted_price.toLocaleString()} VND</td>
                        <td className="px-4 py-2">{point.ci_lower.toLocaleString()} VND</td>
                        <td className="px-4 py-2">{point.ci_upper.toLocaleString()} VND</td>
                        <td className="px-4 py-2">{point.run_id}</td>
                      </tr>
                    ))}
                  </tbody>
                </Table>
              </div>
            </div>
          )}
        </div>

        {/* Online Prediction Section */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Online Price Prediction</h2>

          <Form onSubmit={handleOnlineSubmit}>
            <div className="grid grid-cols-2 gap-4 mb-4">
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
                <label className="block text-sm font-medium text-gray-700 mb-2">Current Price *</label>
                <Input
                  type="number"
                  value={onlineForm.current_price}
                  onChange={(e) => handleOnlineChange('current_price', parseFloat(e.target.value))}
                  min="0"
                  step={0.01}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Average Rating *</label>
                <Input
                  type="number"
                  value={onlineForm.avg_rating}
                  onChange={(e) => handleOnlineChange('avg_rating', parseFloat(e.target.value))}
                  min="0"
                  max="5"
                  step={0.1}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Review Count *</label>
                <Input
                  type="number"
                  value={onlineForm.review_count}
                  onChange={(e) => handleOnlineChange('review_count', parseInt(e.target.value))}
                  min="0"
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
              {onlineLoading ? 'Predicting...' : 'Predict Price'}
            </Button>
          </Form>

          {/* Online Prediction Result */}
          {onlineResult && (
            <div className="mt-6 p-4 bg-blue-50 rounded-lg">
              <h3 className="text-lg font-medium mb-3">Prediction Result</h3>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <span className="font-medium">Predicted Price:</span>
                  <div className="text-2xl font-bold text-blue-600">
                    {onlineResult.predicted_price.toLocaleString()} VND
                  </div>
                </div>
                <div>
                  <span className="font-medium">Confidence Interval:</span>
                  <div className="text-sm">
                    {onlineResult.ci_lower.toLocaleString()} - {onlineResult.ci_upper.toLocaleString()} VND
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

export default PricePredictionPage;