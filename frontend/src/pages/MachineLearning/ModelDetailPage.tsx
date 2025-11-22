import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { getModel, updateModel, MLModel, UpdateModelRequest } from '../../services/machineLearningApi';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';

const ModelDetailPage: React.FC = () => {
  const { model_sk } = useParams<{ model_sk: string }>();
  const navigate = useNavigate();
  const [model, setModel] = useState<MLModel | null>(null);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(false);
  const [formData, setFormData] = useState<UpdateModelRequest>({});

  useEffect(() => {
    if (model_sk) {
      fetchModel(parseInt(model_sk));
    }
  }, [model_sk]);

  const fetchModel = async (id: number) => {
    try {
      setLoading(true);
      const data = await getModel(id);
      setModel(data);
      setFormData({
        training_data_until: data.training_data_until,
        metrics: data.metrics,
        status: data.status
      });
    } catch (error) {
      console.error('Error fetching model:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    if (!model || !model_sk) return;

    try {
      const updatedModel = await updateModel(parseInt(model_sk), formData);
      setModel(updatedModel);
      setEditing(false);
    } catch (error) {
      console.error('Error updating model:', error);
    }
  };

  const handleCancel = () => {
    if (model) {
      setFormData({
        training_data_until: model.training_data_until,
        metrics: model.metrics,
        status: model.status
      });
    }
    setEditing(false);
  };

  const handleFormChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const statusOptions = [
    { value: 'active', label: 'Active' },
    { value: 'deprecated', label: 'Deprecated' },
    { value: 'training', label: 'Training' }
  ];

  if (loading) {
    return <div className="p-6">Loading...</div>;
  }

  if (!model) {
    return <div className="p-6">Model not found</div>;
  }

  return (
    <div className="p-6">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-bold">Model Details</h1>
        <div className="flex gap-2">
          {!editing ? (
            <Button onClick={() => setEditing(true)}>Edit Model</Button>
          ) : (
            <>
              <Button onClick={handleSave}>Save Changes</Button>
              <Button variant="outline" onClick={handleCancel}>Cancel</Button>
            </>
          )}
          <Button variant="outline" onClick={() => navigate('/ml/models')}>Back to List</Button>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow p-6">
        <div className="grid grid-cols-2 gap-6">
          {/* Read-only fields */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Model Name</label>
            <div className="p-2 bg-gray-50 rounded">{model.model_name}</div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Model Type</label>
            <div className="p-2 bg-gray-50 rounded capitalize">{model.model_type}</div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Model Version</label>
            <div className="p-2 bg-gray-50 rounded">{model.model_version}</div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Model SK</label>
            <div className="p-2 bg-gray-50 rounded">{model.model_sk}</div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Created At</label>
            <div className="p-2 bg-gray-50 rounded">{new Date(model.created_at).toLocaleString()}</div>
          </div>

          {/* Editable fields */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Status</label>
            {editing ? (
              <Select
                options={statusOptions}
                defaultValue={formData.status || ''}
                onChange={(value) => handleFormChange('status', value)}
              />
            ) : (
              <div className="p-2 bg-gray-50 rounded">
                <span className={`px-2 py-1 rounded text-sm ${
                  model.status === 'active' ? 'bg-green-100 text-green-800' :
                  model.status === 'deprecated' ? 'bg-red-100 text-red-800' :
                  'bg-yellow-100 text-yellow-800'
                }`}>
                  {model.status}
                </span>
              </div>
            )}
          </div>

          <div className="col-span-2">
            <label className="block text-sm font-medium text-gray-700 mb-2">Training Data Until</label>
            {editing ? (
              <Input
                type="date"
                value={formData.training_data_until || ''}
                onChange={(e) => handleFormChange('training_data_until', e.target.value)}
              />
            ) : (
              <div className="p-2 bg-gray-50 rounded">{new Date(model.training_data_until).toLocaleDateString()}</div>
            )}
          </div>

          <div className="col-span-2">
            <label className="block text-sm font-medium text-gray-700 mb-2">Metrics</label>
            {editing ? (
              <textarea
                className="w-full p-2 border rounded"
                rows={4}
                value={JSON.stringify(formData.metrics, null, 2)}
                onChange={(e) => {
                  try {
                    const parsed = JSON.parse(e.target.value);
                    handleFormChange('metrics', parsed);
                  } catch (error) {
                    // Invalid JSON, keep as string for now
                  }
                }}
              />
            ) : (
              <pre className="p-2 bg-gray-50 rounded text-sm overflow-auto">
                {JSON.stringify(model.metrics, null, 2)}
              </pre>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ModelDetailPage;