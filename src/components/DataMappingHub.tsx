
import React, { useState } from 'react';
import { Plus, Upload, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { useToast } from '@/hooks/use-toast';

const DataMappingHub = () => {
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const { toast } = useToast();

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      setSelectedFile(e.dataTransfer.files[0]);
    }
  };

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setSelectedFile(e.target.files[0]);
    }
  };

  const handleUpload = () => {
    if (!selectedFile) {
      toast({
        title: "No file selected",
        description: "Please select a file to upload.",
        variant: "destructive"
      });
      return;
    }

    // Simulate upload error as shown in screenshots
    toast({
      title: "Error processing file",
      description: "No valid mapping data found in the file.",
      variant: "destructive"
    });

    setShowUploadModal(false);
    setSelectedFile(null);
  };

  const removeFile = () => {
    setSelectedFile(null);
  };

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Data Mapping Hub</h1>
          <div className="flex space-x-6 mt-2 text-sm">
            <span className="text-green-600">Approved: 0</span>
            <span className="text-yellow-600">Pending: 0</span>
            <span className="text-red-600">Rejected: 0</span>
          </div>
        </div>
        <div className="flex space-x-3">
          <Button 
            variant="outline"
            className="flex items-center space-x-2"
          >
            <span>AI Assistant</span>
          </Button>
          <Button 
            className="flex items-center space-x-2 bg-green-600 hover:bg-green-700"
            onClick={() => setShowUploadModal(true)}
          >
            <Plus size={16} />
            <span>Add Mapping</span>
          </Button>
          <Button variant="outline">Test Data</Button>
        </div>
      </div>

      {/* Main Content */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* No Mapping Data Available */}
        <div className="lg:col-span-2">
          <Card className="h-96 flex items-center justify-center">
            <CardContent className="text-center">
              <div className="text-slate-400 mb-4">
                <Upload size={64} className="mx-auto mb-4" />
              </div>
              <h3 className="text-lg font-medium text-slate-600 mb-2">No Mapping Data Available</h3>
              <p className="text-slate-500 text-sm mb-4">
                Upload a CSV or Excel file containing your source-to-target mappings to get started or add a mapping manually.
              </p>
              <Button 
                onClick={() => setShowUploadModal(true)}
                className="bg-blue-600 hover:bg-blue-700"
              >
                Upload File
              </Button>
            </CardContent>
          </Card>
        </div>

        {/* Data Lineage Visualization */}
        <div>
          <Card className="h-96">
            <CardHeader>
              <CardTitle className="text-lg">Data Lineage Visualization</CardTitle>
            </CardHeader>
            <CardContent className="flex items-center justify-center h-full">
              <div className="text-center text-slate-500">
                <p className="text-sm">No mapping data available for lineage view</p>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Upload Modal */}
      <Dialog open={showUploadModal} onOpenChange={setShowUploadModal}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center justify-between">
              Upload Mapping File
              <Button 
                variant="ghost" 
                size="sm" 
                onClick={() => setShowUploadModal(false)}
              >
                <X size={16} />
              </Button>
            </DialogTitle>
          </DialogHeader>
          
          <div className="space-y-4">
            <p className="text-sm text-slate-600">
              Upload a CSV or Excel file containing your source-to-target mappings.
            </p>
            
            <div
              className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
                dragActive 
                  ? 'border-blue-400 bg-blue-50' 
                  : 'border-slate-300 hover:border-slate-400'
              }`}
              onDragEnter={handleDrag}
              onDragLeave={handleDrag}
              onDragOver={handleDrag}
              onDrop={handleDrop}
            >
              <Upload className="mx-auto mb-4 text-slate-400" size={48} />
              <p className="text-sm text-slate-600 mb-2">
                Drag and drop your file here, or{' '}
                <label className="text-blue-600 cursor-pointer hover:underline">
                  browse
                  <input
                    type="file"
                    className="hidden"
                    accept=".csv,.xlsx,.xls"
                    onChange={handleFileSelect}
                  />
                </label>
              </p>
              <p className="text-xs text-slate-500">
                Supports CSV and Excel files with mapping data
              </p>
            </div>

            {selectedFile && (
              <div className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                <span className="text-sm text-slate-700">{selectedFile.name}</span>
                <Button variant="ghost" size="sm" onClick={removeFile}>
                  Remove
                </Button>
              </div>
            )}

            <div className="flex justify-end space-x-3">
              <Button 
                variant="outline" 
                onClick={() => setShowUploadModal(false)}
              >
                Cancel
              </Button>
              <Button 
                onClick={handleUpload}
                className="bg-slate-900 hover:bg-slate-800"
              >
                Upload
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default DataMappingHub;
