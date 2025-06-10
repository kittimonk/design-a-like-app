
import React, { useState } from 'react';
import Sidebar from '@/components/Sidebar';
import DataMappingHub from '@/components/DataMappingHub';
import TestDataGenerator from '@/components/TestDataGenerator';
import MetadataManagement from '@/components/MetadataManagement';
import SearchMetadata from '@/components/SearchMetadata';

const Index = () => {
  const [activeSection, setActiveSection] = useState('data-mapping');

  const renderContent = () => {
    switch (activeSection) {
      case 'data-mapping':
        return <DataMappingHub />;
      case 'test-generator':
        return <TestDataGenerator />;
      case 'metadata':
        return <MetadataManagement />;
      case 'search':
        return <SearchMetadata />;
      case 'lineage':
      case 'ai-assistant':
      case 'manage-tables':
      case 'manage-columns':
      case 'settings':
        return (
          <div className="p-6 flex items-center justify-center h-96">
            <div className="text-center">
              <h2 className="text-xl font-semibold text-slate-700 mb-2">
                {activeSection.split('-').map(word => 
                  word.charAt(0).toUpperCase() + word.slice(1)
                ).join(' ')}
              </h2>
              <p className="text-slate-500">This section is coming soon.</p>
            </div>
          </div>
        );
      default:
        return <DataMappingHub />;
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 flex">
      <Sidebar activeSection={activeSection} onSectionChange={setActiveSection} />
      
      <div className="flex-1 ml-64">
        <main className="min-h-screen">
          {renderContent()}
        </main>
      </div>
    </div>
  );
};

export default Index;
