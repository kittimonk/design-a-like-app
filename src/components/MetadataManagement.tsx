
import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Plus } from 'lucide-react';

const MetadataManagement = () => {
  const sections = [
    { title: 'Manage Columns', description: 'Column Name' },
    { title: 'Manage Tables', description: 'Table Name' },
    { title: 'Manage Malcodes', description: 'Malcode' }
  ];

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Metadata Management</h1>
        <p className="text-slate-600 mt-2">Search Metadata</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {sections.map((section, index) => (
          <Card key={index} className="h-64 flex flex-col">
            <CardHeader className="text-center flex-1 flex flex-col justify-center">
              <CardTitle className="text-lg mb-4">{section.title}</CardTitle>
              <p className="text-sm text-slate-600 mb-4">{section.description}</p>
              <p className="text-xs text-slate-500">Business Description</p>
            </CardHeader>
            <CardContent className="pt-0">
              <Button 
                className="w-full bg-slate-600 hover:bg-slate-700 text-white"
                size="sm"
              >
                <Plus size={16} className="mr-2" />
                Add {section.title.split(' ')[1]}
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
};

export default MetadataManagement;
