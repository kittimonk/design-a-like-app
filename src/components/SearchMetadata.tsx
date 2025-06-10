
import React from 'react';
import { Search } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';

const SearchMetadata = () => {
  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Search Business Metadata</h1>
        <p className="text-slate-600 mt-2">
          Search for business descriptions and metadata across knowledge bases and columns.
        </p>
      </div>

      <div className="max-w-2xl">
        <div className="flex space-x-3">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-slate-400" size={20} />
            <Input 
              placeholder="Search Metadata"
              className="pl-10"
            />
          </div>
          <Button className="bg-slate-900 hover:bg-slate-800">
            Search
          </Button>
        </div>
      </div>

      <div className="text-center py-16">
        <Search size={64} className="mx-auto text-slate-400 mb-4" />
        <h3 className="text-lg font-medium text-slate-600">Start searching for metadata</h3>
        <p className="text-slate-500 mt-2">Enter keywords to find relevant business descriptions and column metadata.</p>
      </div>
    </div>
  );
};

export default SearchMetadata;
