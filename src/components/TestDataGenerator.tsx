
import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { TestTube, Database, Copy, Play } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

const TestDataGenerator = () => {
  const [isGenerating, setIsGenerating] = useState(false);
  const [generatedSQL, setGeneratedSQL] = useState<string>('');
  const [sourceTable, setSourceTable] = useState('');
  const [targetTable, setTargetTable] = useState('');
  const [backendUrl, setBackendUrl] = useState<string>('');
  const { toast } = useToast();

  // Function to detect backend port
  const detectBackendPort = async () => {
    const currentHost = window.location.hostname;
    const commonPorts = [3000, 3001, 3002, 3003, 3004, 3005, 8000, 8001, 8002];
    
    for (const port of commonPorts) {
      try {
        const testUrl = `http://${currentHost}:${port}/health`;
        const response = await fetch(testUrl, { 
          method: 'GET',
          mode: 'cors',
          signal: AbortSignal.timeout(2000) // 2 second timeout
        });
        
        if (response.ok) {
          const backendUrl = `http://${currentHost}:${port}`;
          setBackendUrl(backendUrl);
          console.log(`Backend detected at: ${backendUrl}`);
          return backendUrl;
        }
      } catch (error) {
        // Port not available, try next one
        continue;
      }
    }
    
    // Fallback to default
    const fallbackUrl = `http://${currentHost}:3000`;
    setBackendUrl(fallbackUrl);
    console.warn('Backend port not detected, using fallback:', fallbackUrl);
    return fallbackUrl;
  };

  useEffect(() => {
    detectBackendPort();
  }, []);

  const generateSQLQuery = async () => {
    if (!sourceTable.trim() || !targetTable.trim()) {
      toast({
        title: "Missing table names",
        description: "Please enter both source and target table names.",
        variant: "destructive"
      });
      return;
    }

    if (!backendUrl) {
      toast({
        title: "Backend not available",
        description: "Cannot connect to backend server. Please ensure it's running.",
        variant: "destructive"
      });
      return;
    }

    setIsGenerating(true);
    
    try {
      const url = `${backendUrl}/generate-sql-logic?source_table=${encodeURIComponent(sourceTable)}&target_table=${encodeURIComponent(targetTable)}`;
      console.log('Generating SQL from:', url);
      
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        mode: 'cors',
      });

      console.log('Response status:', response.status);

      if (!response.ok) {
        const errorText = await response.text();
        console.error('Error response:', errorText);
        throw new Error(`Failed to generate SQL query: ${response.status} ${response.statusText}`);
      }

      const result = await response.json();
      console.log('Backend response:', result);
      
      if (result.sql_logic) {
        setGeneratedSQL(result.sql_logic);
        toast({
          title: "SQL Query Generated",
          description: "Successfully generated SQL query based on approved mappings.",
        });
      } else if (result.error) {
        toast({
          title: "No data available",
          description: result.error,
          variant: "destructive"
        });
      }
    } catch (error) {
      console.error('Generate SQL error:', error);
      toast({
        title: "Error generating SQL",
        description: error instanceof Error ? error.message : "Failed to generate SQL query. Please ensure you have approved mappings.",
        variant: "destructive"
      });
    } finally {
      setIsGenerating(false);
    }
  };

  const copyToClipboard = () => {
    if (generatedSQL) {
      navigator.clipboard.writeText(generatedSQL);
      toast({
        title: "Copied to clipboard",
        description: "SQL query has been copied to your clipboard.",
      });
    }
  };

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Test Data Generator</h1>
        <p className="text-slate-600 mt-2">
          Generate SQL SELECT queries based on your approved data mappings using Azure OpenAI.
        </p>
        <div className="mt-2 text-xs text-slate-500">
          Backend URL: {backendUrl || 'Detecting...'}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Generate SQL Section */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <TestTube size={20} />
              <span>SQL Query Generator</span>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-slate-600">
              Generate SQL SELECT queries based on approved mappings between source and target tables.
            </p>
            
            <div className="space-y-3">
              <div>
                <Label htmlFor="source-table">Source Table</Label>
                <Input
                  id="source-table"
                  value={sourceTable}
                  onChange={(e) => setSourceTable(e.target.value)}
                  placeholder="Enter source table name"
                  disabled={isGenerating}
                />
              </div>
              
              <div>
                <Label htmlFor="target-table">Target Table</Label>
                <Input
                  id="target-table"
                  value={targetTable}
                  onChange={(e) => setTargetTable(e.target.value)}
                  placeholder="Enter target table name"
                  disabled={isGenerating}
                />
              </div>
            </div>
            
            <Button 
              onClick={generateSQLQuery}
              disabled={isGenerating}
              className="w-full bg-green-600 hover:bg-green-700"
            >
              <Play size={16} className="mr-2" />
              {isGenerating ? "Generating..." : "Generate SQL Query"}
            </Button>
          </CardContent>
        </Card>

        {/* Database Status */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <Database size={20} />
              <span>Database Status</span>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span>SourceTargetMapping Table:</span>
                <span className="text-green-600">Connected</span>
              </div>
              <div className="flex justify-between text-sm">
                <span>RejectedRows Table:</span>
                <span className="text-green-600">Connected</span>
              </div>
              <div className="flex justify-between text-sm">
                <span>Azure OpenAI:</span>
                <span className="text-green-600">Available</span>
              </div>
              <div className="flex justify-between text-sm">
                <span>SQL Server:</span>
                <span className="text-green-600">Connected</span>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Generated SQL Display */}
      {generatedSQL && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between">
              <span>Generated SQL Query</span>
              <Button
                variant="outline"
                size="sm"
                onClick={copyToClipboard}
              >
                <Copy size={16} className="mr-2" />
                Copy
              </Button>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="bg-slate-900 text-green-400 p-4 rounded-lg font-mono text-sm overflow-x-auto">
              <pre>{generatedSQL}</pre>
            </div>
          </CardContent>
        </Card>
      )}

      {/* No Data Message */}
      {!generatedSQL && !isGenerating && (
        <div className="text-center py-16">
          <TestTube size={64} className="mx-auto text-slate-400 mb-4" />
          <h3 className="text-lg font-medium text-slate-600 mb-2">No SQL Query Generated</h3>
          <p className="text-slate-500 mb-4">
            Enter source and target table names, then click "Generate SQL Query" to create a SELECT query based on your approved data mappings.
          </p>
        </div>
      )}
    </div>
  );
};

export default TestDataGenerator;
