import React, { useState } from 'react';
import { Plus, Upload, X, CheckCircle, XCircle, Clock } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useToast } from '@/hooks/use-toast';

interface MappingResult {
  approved: number;
  pending: number;
  rejected: number;
  details?: any[];
}

const DataMappingHub = () => {
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [userDetails, setUserDetails] = useState('abc123xy');
  const [isProcessing, setIsProcessing] = useState(false);
  const [mappingResults, setMappingResults] = useState<MappingResult>({
    approved: 0,
    pending: 0,
    rejected: 0
  });
  const { toast } = useToast();

  // Get the current origin and use port 3000 for backend (to match your main.py)
  const getBackendUrl = () => {
    const currentHost = window.location.hostname;
    return `http://${currentHost}:3000`;
  };

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

  const handleUpload = async () => {
    if (!selectedFile) {
      toast({
        title: "No file selected",
        description: "Please select a file to upload.",
        variant: "destructive"
      });
      return;
    }

    if (!userDetails.trim()) {
      toast({
        title: "User details required",
        description: "Please enter user details.",
        variant: "destructive"
      });
      return;
    }

    setIsProcessing(true);
    
    try {
      const formData = new FormData();
      formData.append('file', selectedFile);
      formData.append('user', userDetails); // Backend expects 'user' field, not 'user_details'

      console.log('Uploading to:', `${getBackendUrl()}/compare-and-recommend`);
      console.log('File details:', {
        name: selectedFile.name,
        type: selectedFile.type,
        size: selectedFile.size
      });
      console.log('User details:', userDetails);
      
      const response = await fetch(`${getBackendUrl()}/compare-and-recommend`, {
        method: 'POST',
        body: formData,
        mode: 'cors',
      });

      console.log('Response status:', response.status);

      if (!response.ok) {
        const errorText = await response.text();
        console.error('Error response:', errorText);
        throw new Error(`Failed to process file: ${response.status} ${response.statusText} - ${errorText}`);
      }

      const result = await response.json();
      console.log('Backend response:', result);
      
      // Parse the response based on your backend structure
      const approvedCount = result.approved_rows?.length || 0;
      const rejectedCount = result.rejected_rows?.length || 0;

      setMappingResults({
        approved: approvedCount,
        pending: 0,
        rejected: rejectedCount,
        details: result
      });

      toast({
        title: "File processed successfully",
        description: result.message || `Approved: ${approvedCount}, Rejected: ${rejectedCount}`,
      });

      setShowUploadModal(false);
      setSelectedFile(null);
    } catch (error) {
      console.error('Upload error:', error);
      toast({
        title: "Error processing file",
        description: error instanceof Error ? error.message : "Failed to process the mapping file. Please try again.",
        variant: "destructive"
      });
    } finally {
      setIsProcessing(false);
    }
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
            <span className="text-green-600 flex items-center">
              <CheckCircle size={16} className="mr-1" />
              Approved: {mappingResults.approved}
            </span>
            <span className="text-yellow-600 flex items-center">
              <Clock size={16} className="mr-1" />
              Pending: {mappingResults.pending}
            </span>
            <span className="text-red-600 flex items-center">
              <XCircle size={16} className="mr-1" />
              Rejected: {mappingResults.rejected}
            </span>
          </div>
          <div className="mt-2 text-xs text-slate-500">
            Backend URL: {getBackendUrl()}
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
        {/* Mapping Results */}
        <div className="lg:col-span-2">
          <Card className="h-96">
            <CardHeader>
              <CardTitle>Mapping Results</CardTitle>
            </CardHeader>
            <CardContent>
              {mappingResults.approved > 0 || mappingResults.rejected > 0 ? (
                <div className="space-y-4">
                  <div className="grid grid-cols-3 gap-4 text-center">
                    <div className="p-4 bg-green-50 rounded-lg">
                      <CheckCircle className="mx-auto text-green-600 mb-2" size={24} />
                      <div className="text-2xl font-bold text-green-600">{mappingResults.approved}</div>
                      <div className="text-sm text-green-700">Approved</div>
                    </div>
                    <div className="p-4 bg-yellow-50 rounded-lg">
                      <Clock className="mx-auto text-yellow-600 mb-2" size={24} />
                      <div className="text-2xl font-bold text-yellow-600">{mappingResults.pending}</div>
                      <div className="text-sm text-yellow-700">Pending</div>
                    </div>
                    <div className="p-4 bg-red-50 rounded-lg">
                      <XCircle className="mx-auto text-red-600 mb-2" size={24} />
                      <div className="text-2xl font-bold text-red-600">{mappingResults.rejected}</div>
                      <div className="text-sm text-red-700">Rejected</div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center">
                    <Upload size={64} className="mx-auto text-slate-400 mb-4" />
                    <h3 className="text-lg font-medium text-slate-600 mb-2">No Mapping Data Available</h3>
                    <p className="text-slate-500 text-sm mb-4">
                      Upload a CSV or Excel file containing your source-to-target mappings to get started.
                    </p>
                    <Button 
                      onClick={() => setShowUploadModal(true)}
                      className="bg-blue-600 hover:bg-blue-700"
                    >
                      Upload File
                    </Button>
                  </div>
                </div>
              )}
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
                <p className="text-sm">
                  {mappingResults.approved > 0 
                    ? "Lineage view available for approved mappings" 
                    : "No mapping data available for lineage view"
                  }
                </p>
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
                disabled={isProcessing}
              >
                <X size={16} />
              </Button>
            </DialogTitle>
          </DialogHeader>
          
          <div className="space-y-4">
            <p className="text-sm text-slate-600">
              Upload a CSV or Excel file containing your source-to-target mappings. The file will be processed using Azure OpenAI to validate and approve/reject mappings.
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
                    disabled={isProcessing}
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
                <Button 
                  variant="ghost" 
                  size="sm" 
                  onClick={removeFile}
                  disabled={isProcessing}
                >
                  Remove
                </Button>
              </div>
            )}

            {/* User Details Input - Updated for simple string input */}
            <div className="space-y-2">
              <Label htmlFor="user-details">User ID</Label>
              <Input
                id="user-details"
                value={userDetails}
                onChange={(e) => setUserDetails(e.target.value)}
                placeholder="Enter your user ID (e.g., abc123xy)"
                disabled={isProcessing}
              />
              <p className="text-xs text-slate-500">
                Enter your user ID as a simple string
              </p>
            </div>

            <div className="flex justify-end space-x-3">
              <Button 
                variant="outline" 
                onClick={() => setShowUploadModal(false)}
                disabled={isProcessing}
              >
                Cancel
              </Button>
              <Button 
                onClick={handleUpload}
                className="bg-slate-900 hover:bg-slate-800"
                disabled={isProcessing || !selectedFile}
              >
                {isProcessing ? "Processing..." : "Upload"}
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default DataMappingHub;
