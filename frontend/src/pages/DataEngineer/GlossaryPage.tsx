import React, { useState, useEffect } from 'react';
import { getAllBusinessTerms, createBusinessTerm, getBusinessTermDetail, searchBusinessGlossary, BusinessTerm, BusinessTermDetail } from '../../services/businessMetadataApi';

const GlossaryPage: React.FC = () => {
  const [terms, setTerms] = useState<BusinessTerm[]>([]);
  const [selectedTerm, setSelectedTerm] = useState<BusinessTermDetail | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newTerm, setNewTerm] = useState({ term_name: '', definition: '', steward: '', status: 'draft' });
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadTerms();
  }, []);

  const loadTerms = async () => {
    setLoading(true);
    try {
      const data = await getAllBusinessTerms();
      setTerms(data);
    } catch (error) {
      console.error('Error loading terms:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadTermDetails = async (term_id: number) => {
    setLoading(true);
    try {
      const data = await getBusinessTermDetail(term_id);
      setSelectedTerm(data);
    } catch (error) {
      console.error('Error loading term details:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = async () => {
    if (searchQuery.trim()) {
      setLoading(true);
      try {
        const data = await searchBusinessGlossary(searchQuery);
        setTerms(data);
      } catch (error) {
        console.error('Error searching glossary:', error);
      } finally {
        setLoading(false);
      }
    } else {
      loadTerms();
    }
  };

  const handleCreateTerm = async () => {
    try {
      await createBusinessTerm(newTerm);
      setNewTerm({ term_name: '', definition: '', steward: '', status: 'draft' });
      setShowCreateForm(false);
      loadTerms();
    } catch (error) {
      console.error('Error creating term:', error);
    }
  };

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Business Glossary</h1>

      {/* Search and Create */}
      <div className="mb-6 flex flex-col sm:flex-row gap-2 sm:gap-4 w-full">
        <div className="flex flex-col sm:flex-row gap-2 flex-1 w-full">
          <input
            type="text"
            placeholder="Search terms..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="flex-1 p-2 border rounded min-w-[120px]"
          />
          <button onClick={handleSearch} className="px-4 py-2 bg-blue-500 text-white rounded min-w-[100px]">
            Search
          </button>
        </div>
        <button
          onClick={() => setShowCreateForm(!showCreateForm)}
          className="px-4 py-2 bg-green-500 text-white rounded min-w-[120px]"
        >
          Create New
        </button>
      </div>

      {/* Create Form */}
      {showCreateForm && (
        <div className="mb-6 p-4 bg-gray-50 rounded-lg">
          <h3 className="font-medium mb-4">Create New Term</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 sm:gap-4">
            <input
              type="text"
              placeholder="Term Name"
              value={newTerm.term_name}
              onChange={(e) => setNewTerm({ ...newTerm, term_name: e.target.value })}
              className="p-2 border rounded min-w-[120px]"
            />
            <input
              type="text"
              placeholder="Steward"
              value={newTerm.steward}
              onChange={(e) => setNewTerm({ ...newTerm, steward: e.target.value })}
              className="p-2 border rounded min-w-[120px]"
            />
            <select
              value={newTerm.status}
              onChange={(e) => setNewTerm({ ...newTerm, status: e.target.value })}
              className="p-2 border rounded min-w-[120px]"
            >
              <option value="draft">Draft</option>
              <option value="approved">Approved</option>
            </select>
          </div>
          <textarea
            placeholder="Definition"
            value={newTerm.definition}
            onChange={(e) => setNewTerm({ ...newTerm, definition: e.target.value })}
            className="w-full p-2 border rounded mt-2 sm:mt-4"
            rows={3}
          />
          <div className="mt-4 flex flex-col sm:flex-row gap-2 sm:gap-4 w-full">
            <button onClick={handleCreateTerm} className="px-4 py-2 bg-green-500 text-white rounded min-w-[100px]">
              Create
            </button>
            <button onClick={() => setShowCreateForm(false)} className="px-4 py-2 bg-gray-500 text-white rounded min-w-[100px]">
              Cancel
            </button>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Terms List */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Term List</h2>
          {loading && <div>Loading...</div>}
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {terms.map((term) => (
              <div
                key={term.term_id}
                className="p-3 border rounded cursor-pointer hover:bg-gray-50"
                onClick={() => loadTermDetails(term.term_id)}
              >
                <div className="font-medium">{term.term_name}</div>
                <div className="text-sm text-gray-600">Status: {term.status}</div>
                <div className="text-sm text-gray-600">Steward: {term.steward}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Term Details */}
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-lg font-semibold mb-4">Term Details</h2>
          {selectedTerm ? (
            <div>
              <h3 className="font-medium text-lg">{selectedTerm.term_name}</h3>
              <p className="text-gray-600 mb-2">Status: {selectedTerm.status}</p>
              <p className="text-gray-600 mb-4">Steward: {selectedTerm.steward}</p>
              <div className="mb-4">
                <strong>Definition:</strong>
                <p className="mt-1">{selectedTerm.definition}</p>
              </div>
              {selectedTerm.related_datasets && selectedTerm.related_datasets.length > 0 && (
                <div>
                  <strong>Related Datasets:</strong>
                  <div className="mt-2 space-y-1">
                    {selectedTerm.related_datasets.map((dataset: any, index: number) => (
                      <div key={index} className="text-sm p-2 bg-gray-50 rounded">
                        {dataset.schema_name}.{dataset.table_name} ({dataset.layer})
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ) : (
            <p className="text-gray-500">Select a term to view details</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default GlossaryPage;