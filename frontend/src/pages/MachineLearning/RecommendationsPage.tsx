import React, { useState } from 'react';
import { getRecommendations, Recommendations } from '../../services/machineLearningApi';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';

const RecommendationsPage: React.FC = () => {
  const [recommendations, setRecommendations] = useState<Recommendations | null>(null);
  const [loading, setLoading] = useState(false);

  const [formData, setFormData] = useState({
    source_product_key: '',
    platform_code: '',
    model_name: '',
    model_version: '',
    limit: 10
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setLoading(true);
      const data = await getRecommendations(formData);
      setRecommendations(data);
    } catch (error) {
      console.error('Error fetching recommendations:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const platformOptions = [
    { value: 'tiki', label: 'Tiki' },
    { value: 'lazada', label: 'Lazada' }
  ];

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Product Recommendations</h1>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Form Section */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Get Recommendations</h2>

          <Form onSubmit={handleSubmit}>
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Source Product Key *</label>
                <Input
                  type="text"
                  value={formData.source_product_key}
                  onChange={(e) => handleChange('source_product_key', e.target.value)}
                  placeholder="e.g., tiki_123456"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Platform Code *</label>
                <Select
                  options={platformOptions}
                  defaultValue={formData.platform_code}
                  onChange={(value) => handleChange('platform_code', value)}
                  placeholder="Select platform"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model Name</label>
                <Input
                  type="text"
                  value={formData.model_name}
                  onChange={(e) => handleChange('model_name', e.target.value)}
                  placeholder="Optional"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model Version</label>
                <Input
                  type="text"
                  value={formData.model_version}
                  onChange={(e) => handleChange('model_version', e.target.value)}
                  placeholder="Optional"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Limit</label>
                <Input
                  type="number"
                  value={formData.limit}
                  onChange={(e) => handleChange('limit', parseInt(e.target.value))}
                  min="1"
                  max="50"
                />
              </div>

              <Button disabled={loading} className="w-full">
                {loading ? 'Loading...' : 'Get Recommendations'}
              </Button>
            </div>
          </Form>
        </div>

        {/* Results Section */}
        <div className="lg:col-span-2">
          {recommendations ? (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">
                Recommendations for {recommendations.source_product_key}
              </h2>
              <p className="text-sm text-gray-600 mb-4">
                Model: {recommendations.model_name} ({recommendations.model_version}) | Date: {new Date(recommendations.date).toLocaleDateString()}
              </p>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {recommendations.recommendations.map((rec) => (
                  <div key={rec.rank} className="border rounded-lg p-4 hover:shadow-md transition-shadow">
                    <div className="flex justify-between items-start mb-2">
                      <span className="text-sm font-medium text-blue-600">Rank #{rec.rank}</span>
                      <span className="text-sm text-gray-500">Score: {rec.similarity_score.toFixed(3)}</span>
                    </div>

                    <h3 className="font-medium text-gray-900 mb-2 line-clamp-2">
                      {rec.product_name}
                    </h3>

                    <div className="flex justify-between items-center text-sm text-gray-600">
                      <span>Min Price: {rec.min_price.toLocaleString()} VND</span>
                      <span>Rating: {rec.avg_rating.toFixed(1)} ⭐</span>
                    </div>

                    <div className="mt-2 text-xs text-gray-500">
                      Key: {rec.recommended_product_key}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="bg-white rounded-lg shadow p-6 flex items-center justify-center h-64">
              <div className="text-center text-gray-500">
                <div className="text-4xl mb-4">🎯</div>
                <p>Enter product details to get recommendations</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default RecommendationsPage;