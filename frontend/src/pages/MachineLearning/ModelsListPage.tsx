import React, { useState, useEffect } from 'react';
import { listModels, MLModel } from '../../services/machineLearningApi';
import Button from '../../components/ui/button/Button';
import { Table } from '../../components/ui/table';
import Select from '../../components/form/Select';
import { useNavigate } from 'react-router-dom';

const ModelsListPage: React.FC = () => {
  const [models, setModels] = useState<MLModel[]>([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({
    type: '',
    status: ''
  });
  const navigate = useNavigate();

  useEffect(() => {
    fetchModels();
  }, [filters]);

  const fetchModels = async () => {
    try {
      setLoading(true);
      const params = {
        ...(filters.type && { type: filters.type }),
        ...(filters.status && { status: filters.status })
      };
      const data = await listModels(params);
      setModels(data);
    } catch (error) {
      console.error('Error fetching models:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleFilterChange = (field: string, value: string) => {
    setFilters(prev => ({ ...prev, [field]: value }));
  };

  const handleViewModel = (model_sk: number) => {
    navigate(`/ml/models/${model_sk}`);
  };

  const handleEditModel = (model_sk: number) => {
    navigate(`/ml/models/${model_sk}/edit`);
  };

  const handleCreateModel = () => {
    navigate('/ml/models/create');
  };

  const modelTypeOptions = [
    { value: '', label: 'All Types' },
    { value: 'sentiment', label: 'Sentiment' },
    { value: 'recommendation', label: 'Recommendation' },
    { value: 'price', label: 'Price' }
  ];

  const statusOptions = [
    { value: '', label: 'All Status' },
    { value: 'active', label: 'Active' },
    { value: 'deprecated', label: 'Deprecated' },
    { value: 'training', label: 'Training' }
  ];

  return (
    <div className="p-6">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-bold">Models Management</h1>
        <Button onClick={handleCreateModel}>Create New Model</Button>
      </div>

      {/* Filters */}
      <div className="flex gap-4 mb-6">
        <div className="w-48">
          <Select
            options={modelTypeOptions}
            defaultValue={filters.type}
            onChange={(value) => handleFilterChange('type', value)}
            placeholder="Filter by Type"
          />
        </div>
        <div className="w-48">
          <Select
            options={statusOptions}
            defaultValue={filters.status}
            onChange={(value) => handleFilterChange('status', value)}
            placeholder="Filter by Status"
          />
        </div>
      </div>

      {/* Models Table */}
      <div className="bg-white rounded-lg shadow">
        <Table>
          <thead>
            <tr>
              <th className="px-4 py-3 text-left">Model Name</th>
              <th className="px-4 py-3 text-left">Type</th>
              <th className="px-4 py-3 text-left">Version</th>
              <th className="px-4 py-3 text-left">Status</th>
              <th className="px-4 py-3 text-left">Training Until</th>
              <th className="px-4 py-3 text-left">Created At</th>
              <th className="px-4 py-3 text-left">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center">Loading...</td>
              </tr>
            ) : models.length === 0 ? (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center">No models found</td>
              </tr>
            ) : (
              models.map((model) => (
                <tr key={model.model_sk} className="border-t">
                  <td className="px-4 py-3">{model.model_name}</td>
                  <td className="px-4 py-3 capitalize">{model.model_type}</td>
                  <td className="px-4 py-3">{model.model_version}</td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-1 rounded text-sm ${
                      model.status === 'active' ? 'bg-green-100 text-green-800' :
                      model.status === 'deprecated' ? 'bg-red-100 text-red-800' :
                      'bg-yellow-100 text-yellow-800'
                    }`}>
                      {model.status}
                    </span>
                  </td>
                  <td className="px-4 py-3">{new Date(model.training_data_until).toLocaleDateString()}</td>
                  <td className="px-4 py-3">{new Date(model.created_at).toLocaleDateString()}</td>
                  <td className="px-4 py-3">
                    <div className="flex gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleViewModel(model.model_sk)}
                      >
                        View
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleEditModel(model.model_sk)}
                      >
                        Edit
                      </Button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </Table>
      </div>
    </div>
  );
};

export default ModelsListPage;