import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { createModel, CreateModelRequest } from '../../services/machineLearningApi';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';

const CreateModelPage: React.FC = () => {
  const navigate = useNavigate();
  const [formData, setFormData] = useState<CreateModelRequest>({
    model_name: '',
    model_type: '',
    model_version: '',
    training_data_until: '',
    metrics: {},
    status: 'active'
  });
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setLoading(true);
      await createModel(formData);
      navigate('/ml/models');
    } catch (error) {
      console.error('Error creating model:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const modelTypeOptions = [
    { value: 'sentiment', label: 'Sentiment' },
    { value: 'recommendation', label: 'Recommendation' },
    { value: 'price', label: 'Price' }
  ];

  const statusOptions = [
    { value: 'active', label: 'Active' },
    { value: 'deprecated', label: 'Deprecated' },
    { value: 'training', label: 'Training' }
  ];

  return (
    <div className="p-6">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-bold">Create New Model</h1>
        <Button variant="outline" onClick={() => navigate('/ml/models')}>Back to List</Button>
      </div>

      <div className="bg-white rounded-lg shadow p-6">
        <Form onSubmit={handleSubmit}>
          <div className="grid grid-cols-2 gap-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Model Name *</label>
              <Input
                type="text"
                value={formData.model_name}
                onChange={(e) => handleChange('model_name', e.target.value)}
                placeholder="e.g., sentiment_tfidf_logreg"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Model Type *</label>
              <Select
                options={modelTypeOptions}
                defaultValue={formData.model_type}
                onChange={(value) => handleChange('model_type', value)}
                placeholder="Select model type"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Model Version *</label>
              <Input
                type="text"
                value={formData.model_version}
                onChange={(e) => handleChange('model_version', e.target.value)}
                placeholder="e.g., v1.0"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Status</label>
              <Select
                options={statusOptions}
                defaultValue={formData.status}
                onChange={(value) => handleChange('status', value)}
              />
            </div>

            <div className="col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-2">Training Data Until *</label>
              <Input
                type="date"
                value={formData.training_data_until}
                onChange={(e) => handleChange('training_data_until', e.target.value)}
              />
            </div>

            <div className="col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-2">Metrics (JSON)</label>
              <textarea
                className="w-full p-2 border rounded"
                rows={4}
                value={JSON.stringify(formData.metrics, null, 2)}
                onChange={(e) => {
                  try {
                    const parsed = JSON.parse(e.target.value);
                    handleChange('metrics', parsed);
                  } catch (error) {
                    // Invalid JSON, keep as string for now
                  }
                }}
                placeholder='{"accuracy": 0.95, "precision": 0.92}'
              />
            </div>
          </div>

          <div className="flex justify-end gap-4 mt-6">
            <Button
              variant="outline"
              onClick={() => navigate('/ml/models')}
            >
              Cancel
            </Button>
            <Button disabled={loading}>
              {loading ? 'Creating...' : 'Create Model'}
            </Button>
          </div>
        </Form>
      </div>
    </div>
  );
};

export default CreateModelPage;