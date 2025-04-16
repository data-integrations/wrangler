'use client';

import { useState, useEffect } from 'react';
import ClickHouseForm from '@/components/ClickHouseForm';
import FlatFileForm from '@/components/FlatFileForm';
import TableSelection from '@/components/TableSelection';
import DataPreview from '@/components/DataPreview';
import IngestionResults from '@/components/IngestionResults';
import StatusIndicator from '@/components/StatusIndicator';
import api, { ClickHouseConfig, FlatFileConfig, IngestionRequest, TableInfo, IngestionResponse } from '@/lib/api';

// Step enum
enum Step {
  SourceSelection = 1,
  SourceConfiguration,
  TargetConfiguration,
  SchemaSelection,
  Preview,
  Results
}

export default function Home() {
  // State
  const [currentStep, setCurrentStep] = useState<Step>(Step.SourceSelection);
  const [source, setSource] = useState<'clickhouse' | 'flatfile' | ''>('');
  const [target, setTarget] = useState<'clickhouse' | 'flatfile' | ''>('');
  const [clickHouseConfig, setClickHouseConfig] = useState<ClickHouseConfig>({
    host: '',
    port: 8123,
    database: '',
    user: '',
    jwtToken: '',
    secure: false
  });
  const [flatFileConfig, setFlatFileConfig] = useState<FlatFileConfig>({
    delimiter: ',',
    hasHeader: true,
    filePath: '',
    fileName: ''
  });
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [joinCondition, setJoinCondition] = useState<string>('');
  const [useJoin, setUseJoin] = useState<boolean>(false);
  const [previewData, setPreviewData] = useState<string[][]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>('');
  const [ingestionResults, setIngestionResults] = useState<IngestionResponse | null>(null);
  const [status, setStatus] = useState<'idle' | 'connecting' | 'fetching' | 'ingesting' | 'completed' | 'error'>('idle');
  const [statusMessage, setStatusMessage] = useState<string>('');

  // Effect to track status changes
  useEffect(() => {
    if (isLoading) {
      if (currentStep === Step.SourceConfiguration || currentStep === Step.TargetConfiguration) {
        setStatus('connecting');
        setStatusMessage('Connecting to the data source...');
      } else if (currentStep === Step.SchemaSelection) {
        setStatus('fetching');
        setStatusMessage('Fetching schema information...');
      } else if (currentStep === Step.Preview) {
        setStatus('fetching');
        setStatusMessage('Generating data preview...');
      } else if (currentStep === Step.Results) {
        setStatus('ingesting');
        setStatusMessage('Ingesting data between sources...');
      }
    } else if (error) {
      setStatus('error');
      setStatusMessage(error);
    } else if (currentStep === Step.Results && ingestionResults) {
      setStatus('completed');
      setStatusMessage(ingestionResults.message || 'Operation completed successfully');
    } else {
      setStatus('idle');
      setStatusMessage('');
    }
  }, [isLoading, error, currentStep, ingestionResults]);

  // Handlers
  const handleSourceSelection = (selectedSource: 'clickhouse' | 'flatfile') => {
    setSource(selectedSource);
    setTarget(selectedSource === 'clickhouse' ? 'flatfile' : 'clickhouse');
    setCurrentStep(Step.SourceConfiguration);
  };

  const handleClickHouseConfigSubmit = async (config: ClickHouseConfig) => {
    try {
      setIsLoading(true);
      setError('');
      
      // Test connection
      const testResult = await api.testClickHouseConnection(config);
      if (!testResult.success) {
        throw new Error(testResult.message);
      }

      // Save config
      setClickHouseConfig(config);

      // If ClickHouse is the source, fetch tables
      if (source === 'clickhouse') {
        const tables = await api.getClickHouseTables(config);
        setTables(tables);
        setCurrentStep(Step.SchemaSelection);
      } else {
        // ClickHouse is the target
        setCurrentStep(Step.TargetConfiguration);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to connect to ClickHouse');
    } finally {
      setIsLoading(false);
    }
  };

  const handleFlatFileConfigSubmit = async (config: FlatFileConfig, file?: File) => {
    try {
      setIsLoading(true);
      setError('');

      // If file upload is required (when flat file is the source)
      if (source === 'flatfile' && file) {
        const uploadResult = await api.uploadFile(file, config.delimiter, config.hasHeader);
        if (!uploadResult.success) {
          throw new Error(uploadResult.message);
        }

        // Set config with file path
        setFlatFileConfig({
          ...config,
          filePath: uploadResult.filePath
        });

        // Set schema information
        setTables([uploadResult.schema]);
        setCurrentStep(Step.SchemaSelection);
      } else {
        // Just store the config (flat file is the target)
        setFlatFileConfig(config);
        setCurrentStep(source === 'clickhouse' ? Step.SchemaSelection : Step.TargetConfiguration);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to process flat file');
    } finally {
      setIsLoading(false);
    }
  };

  const handleTableSelection = (selectedTables: TableInfo[], useJoinOption: boolean, joinConditionText: string) => {
    setTables(selectedTables);
    setUseJoin(useJoinOption);
    setJoinCondition(joinConditionText);
    setCurrentStep(Step.Preview);

    // Generate a preview
    generatePreview(selectedTables, useJoinOption, joinConditionText);
  };

  const generatePreview = async (selectedTables: TableInfo[], useJoinOption: boolean, joinConditionText: string) => {
    try {
      setIsLoading(true);
      setError('');

      const request: IngestionRequest = {
        source: source as 'clickhouse' | 'flatfile',
        target: target as 'clickhouse' | 'flatfile',
        clickHouseConfig: source === 'clickhouse' ? clickHouseConfig : target === 'clickhouse' ? clickHouseConfig : undefined,
        flatFileConfig: source === 'flatfile' ? flatFileConfig : target === 'flatfile' ? flatFileConfig : undefined,
        tables: selectedTables,
        joinCondition: joinConditionText,
        useJoin: useJoinOption
      };

      const previewResult = await api.previewData(request);
      if (!previewResult.success) {
        throw new Error(previewResult.message);
      }

      setPreviewData(previewResult.data);
    } catch (err: any) {
      setError(err.message || 'Failed to generate preview');
    } finally {
      setIsLoading(false);
    }
  };

  const handleStartIngestion = async () => {
    try {
      setIsLoading(true);
      setError('');

      const request: IngestionRequest = {
        source: source as 'clickhouse' | 'flatfile',
        target: target as 'clickhouse' | 'flatfile',
        clickHouseConfig: source === 'clickhouse' ? clickHouseConfig : target === 'clickhouse' ? clickHouseConfig : undefined,
        flatFileConfig: source === 'flatfile' ? flatFileConfig : target === 'flatfile' ? flatFileConfig : undefined,
        tables,
        joinCondition,
        useJoin
      };

      const response = await api.ingestData(request);
      setIngestionResults(response);
      setCurrentStep(Step.Results);
    } catch (err: any) {
      setError(err.message || 'Ingestion failed');
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = () => {
    setCurrentStep(Step.SourceSelection);
    setSource('');
    setTarget('');
    setTables([]);
    setJoinCondition('');
    setUseJoin(false);
    setPreviewData([]);
    setIngestionResults(null);
    setError('');
  };

  // Helper functions for the UI
  const getStepName = (step: Step) => {
    switch (step) {
      case Step.SourceSelection: return 'Source Selection';
      case Step.SourceConfiguration: return 'Source Config';
      case Step.TargetConfiguration: return 'Target Config';
      case Step.SchemaSelection: return 'Column Selection';
      case Step.Preview: return 'Preview';
      case Step.Results: return 'Results';
      default: return '';
    }
  };

  // Function to check if a step is accessible for navigation
  const canNavigateToStep = (step: Step): boolean => {
    // Always allow going to initial step
    if (step === Step.SourceSelection) return true;
    
    // Don't allow going back to completed steps after results
    if (currentStep === Step.Results) return false;
    
    // Only allow going to steps that have been completed or are the current one
    if (step <= currentStep) return true;
    
    return false;
  };

  // Function to handle step navigation when clicking on the progress indicator
  const handleStepClick = (step: Step) => {
    if (!canNavigateToStep(step)) return;
    
    // If loading, don't allow navigation
    if (isLoading) return;
    
    setCurrentStep(step);
  };

  // Render different steps
  const renderStep = () => {
    switch (currentStep) {
      case Step.SourceSelection:
        return (
          <div className="card">
            <h2 className="text-xl font-semibold mb-6">Select Data Source and Target</h2>
            <div className="flex flex-col space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <button
                  className={`btn ${source === 'clickhouse' ? 'btn-primary' : 'btn-secondary'} p-6 text-center transition-all transform hover:scale-105`}
                  onClick={() => handleSourceSelection('clickhouse')}
                >
                  <div className="flex items-center justify-center mb-3">
                    <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4" />
                    </svg>
                  </div>
                  <h3 className="text-lg font-medium">ClickHouse to Flat File</h3>
                  <p className="mt-2 text-sm opacity-90">Export data from ClickHouse database to a flat file (CSV)</p>
                </button>
                <button
                  className={`btn ${source === 'flatfile' ? 'btn-primary' : 'btn-secondary'} p-6 text-center transition-all transform hover:scale-105`}
                  onClick={() => handleSourceSelection('flatfile')}
                >
                  <div className="flex items-center justify-center mb-3">
                    <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                    </svg>
                  </div>
                  <h3 className="text-lg font-medium">Flat File to ClickHouse</h3>
                  <p className="mt-2 text-sm opacity-90">Import data from a flat file (CSV) to ClickHouse database</p>
                </button>
              </div>
            </div>
          </div>
        );

      case Step.SourceConfiguration:
        return source === 'clickhouse' ? (
          <ClickHouseForm 
            onSubmit={handleClickHouseConfigSubmit} 
            initialValues={clickHouseConfig} 
            isLoading={isLoading}
            title="ClickHouse Source Configuration"
          />
        ) : (
          <FlatFileForm 
            onSubmit={handleFlatFileConfigSubmit} 
            initialValues={flatFileConfig}
            requireFile={true}
            isLoading={isLoading}
            title="Flat File Source Configuration" 
          />
        );

      case Step.TargetConfiguration:
        return target === 'clickhouse' ? (
          <ClickHouseForm 
            onSubmit={handleClickHouseConfigSubmit} 
            initialValues={clickHouseConfig} 
            isLoading={isLoading}
            title="ClickHouse Target Configuration"
          />
        ) : (
          <FlatFileForm 
            onSubmit={handleFlatFileConfigSubmit} 
            initialValues={flatFileConfig}
            requireFile={false}
            isLoading={isLoading}
            title="Flat File Target Configuration" 
          />
        );

      case Step.SchemaSelection:
        return (
          <TableSelection 
            tables={tables} 
            onSubmit={handleTableSelection}
            isMultiTableEnabled={source === 'clickhouse'} 
            isLoading={isLoading}
          />
        );

      case Step.Preview:
        return (
          <DataPreview 
            data={previewData} 
            onStartIngestion={handleStartIngestion}
            onBack={() => setCurrentStep(Step.SchemaSelection)}
            isLoading={isLoading}
          />
        );

      case Step.Results:
        return (
          <IngestionResults 
            results={ingestionResults} 
            onReset={handleReset}
          />
        );

      default:
        return null;
    }
  };

  return (
    <div>
      {/* Progress indicator */}
      <div className="mb-8">
        <ol className="flex items-center justify-center w-full">
          {Object.values(Step).filter(step => typeof step === 'number').map((step) => {
            const isNavigable = canNavigateToStep(step as Step);
            return (
              <li 
                key={step} 
                className={`flex items-center ${currentStep >= step ? 'text-primary' : 'text-gray-400'}`}
                onClick={() => handleStepClick(step as Step)}
              >
                <div className={`flex flex-col items-center ${isNavigable ? 'cursor-pointer hover:opacity-80' : ''}`}>
                  <span className={`flex items-center justify-center w-10 h-10 rounded-full ${
                    currentStep > step ? 'bg-primary text-white' : 
                    currentStep === step ? 'bg-primary text-white ring-4 ring-blue-100' : 
                    'bg-gray-200'
                  }`}>
                    {step}
                  </span>
                  <span className="text-xs mt-1">{getStepName(step as Step)}</span>
                </div>
                {step < Object.keys(Step).length / 2 && (
                  <div className={`w-16 h-1 mx-2 ${currentStep > step ? 'bg-primary' : 'bg-gray-200'}`}></div>
                )}
              </li>
            );
          })}
        </ol>
      </div>

      {/* Status indicator */}
      {(status !== 'idle' || error) && (
        <StatusIndicator status={status} message={statusMessage} />
      )}

      {/* Current step content */}
      {renderStep()}
    </div>
  );
} 