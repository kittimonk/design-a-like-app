
import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { TestTube, Database, Copy, Play } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

const TestDataGenerator = () => {
  const [isGenerating, setIsGenerating] = useState(false);
  const [generatedSQL, setGeneratedSQL] = useState<string>('');
  const { toast } = useToast();

  // Get the current origin and use port 3000 for backend
  const getBackendUrl = () => {
    const currentHost = window.location.hostname;
    return `http://${currentHost}:3000`;
  };

  const generateSQLQuery = async () => {
    setIsGenerating(true);
    
    try {
      console.log('Generating SQL from:', `${getBackendUrl()}/generate-sql-logic`);
      
      const response = await fetch(`${getBackendUrl()}/generate-sql-logic`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        mode: 'cors',
        body: JSON.stringify({
          user_details: {
            user_id: 'user123',
            session_id: 'session123'
          }
        }),
      });

      console.log('Response status:', response.status);

      if (!response.ok) {
        const errorText = await response.text();
        console.error('Error response:', errorText);
        throw new Error(`Failed to generate SQL query: ${response.status} ${response.statusText}`);
      }

      const result = await response.json();
      console.log('Backend response:', result);
      
      if (result.sql_query) {
        setGeneratedSQL(result.sql_query);
        toast({
          title: "SQL Query Generated",
          description: "Successfully generated SQL query based on approved mappings.",
        });
      } else {
        toast({
          title: "No data available",
          description: result.message || "No approved mapping data found to generate SQL query.",
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
          Backend URL: {getBackendUrl()}
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
              Generate SQL SELECT queries based on approved mappings in your database tables.
            </p>
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
            Click "Generate SQL Query" to create a SELECT query based on your approved data mappings.
          </p>
        </div>
      )}
    </div>
  );
};

export default TestDataGenerator;
