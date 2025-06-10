
import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { TestTube, Database, Table } from 'lucide-react';

const TestDataGenerator = () => {
  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Test Data Generator</h1>
        <p className="text-slate-600 mt-2">
          Search for business descriptions and metadata across knowledge bases and columns.
        </p>
      </div>

      <div className="text-center py-16">
        <TestTube size={64} className="mx-auto text-slate-400 mb-4" />
        <h3 className="text-lg font-medium text-slate-600 mb-2">No mapping data available for lineage view</h3>
      </div>
    </div>
  );
};

export default TestDataGenerator;
