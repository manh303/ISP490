import React, { useState } from 'react';
import MLOverview from './MLOverview';
import PriceIntelligence from './PriceIntelligence';
import DemandSalesForecasting from './DemandSalesForecasting';
import ProductMLInsights from './ProductMLInsights';

export default function MLIPage() {
  const [activeTab, setActiveTab] = useState<'overview' | 'price' | 'demand' | 'product'>('overview');

  const tabs = [
    { id: 'overview', label: '📊 ML Overview', component: MLOverview },
    { id: 'price', label: '🏷️ Price Intelligence', component: PriceIntelligence },
    { id: 'demand', label: '📈 Demand & Sales Forecasting', component: DemandSalesForecasting },
    { id: 'product', label: '📦 Product ML Insights', component: ProductMLInsights },
  ];

  const ActiveComponent = tabs.find(tab => tab.id === activeTab)?.component || MLOverview;

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Tabs */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-6">
          <nav className="flex -mb-px overflow-x-auto">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as any)}
                className={`flex-shrink-0 px-6 py-4 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </nav>
        </div>
      </div>

      {/* Tab Content */}
      <ActiveComponent />
    </div>
  );
}
