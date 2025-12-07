import React, { useState, useEffect } from 'react';
import { getRecommendations, Recommendations } from '../../services/machineLearningApi';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
//import { CategorySelect } from '../../components/analytics/CategorySelect';
import { ProductSearch } from '../../components/analytics/ProductSearch';
import { listModels, MLModel } from '../../services/machineLearningApi';
const RecommendationsPage: React.FC = () => {
  const [recommendations, setRecommendations] = useState<Recommendations | null>(null);
  const [loading, setLoading] = useState(false);

  // Select states
  const [platformCode, setPlatformCode] = useState<string>('tiki');
  const [categoryKey, setCategoryKey] = useState<string>('');
  const [productId, setProductId] = useState<string>('');
  const [productName, setProductName] = useState<string>('');
  const [models, setModels] = useState<MLModel[]>([]);

  const [formData, setFormData] = useState({
    source_product_key: '',
    platform_code: 'tiki',
    model_name: '',
    model_version: '',
    limit: 10
  });

  useEffect(() => {
    const fetchModels = async () => {
      try {
        const data = await listModels();
        setModels(data);
      } catch (error) {
        console.error('Error fetching models:', error);
      }
    };
    fetchModels();
  }, []);

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

  const modelOptions = models.map(model => ({
    value: `${model.model_name}|${model.model_version}`,
    label: `${model.model_name} (${model.model_version})`
  }));

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
                <label className="block text-sm font-medium text-gray-700 mb-2">Source Product *</label>
                <ProductSearch
                  value={productName}
                  onProductSelect={(productKey, productName) => {
                    setProductId(productKey);
                    setProductName(productName);
                    handleChange('source_product_key', productKey);
                  }}
                  platformCode={platformCode}
                  placeholder="Search products..."
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Platform Code *</label>
                <PlatformSelect
                  value={platformCode}
                  onValueChange={(value) => {
                    setPlatformCode(value || 'tiki');
                    handleChange('platform_code', value || 'tiki');
                  }}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Model</label>
                <Select
                  options={modelOptions}
                  defaultValue={formData.model_name && formData.model_version ? `${formData.model_name}|${formData.model_version}` : ''}
                  onChange={(value) => {
                    const [name, version] = value.split('|');
                    handleChange('model_name', name);
                    handleChange('model_version', version);
                  }}
                  placeholder="Select model"
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