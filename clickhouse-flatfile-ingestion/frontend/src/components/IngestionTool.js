import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Stepper,
  Step,
  StepLabel,
  Button,
  Alert,
  CircularProgress
} from '@mui/material';
import { toast } from 'react-toastify';
import SourceSelection from './SourceSelection';
import ConnectionForm from './ConnectionForm';
import SchemaSelection from './SchemaSelection';
import ColumnSelection from './ColumnSelection';
import DataPreview from './DataPreview';
import ProgressDisplay from './ProgressDisplay';
import ingestionService from '../services/ingestionService';
import dataTypeMapper from '../utils/dataTypeMapper';

const steps = [
  'Select Source',
  'Configure Connection',
  'Select Schema',
  'Select Columns',
  'Preview Data',
  'Execute Ingestion'
];

function IngestionTool() {
  const [activeStep, setActiveStep] = useState(0);
  const [sourceType, setSourceType] = useState(null);
  const [connectionConfig, setConnectionConfig] = useState(null);
  const [selectedTable, setSelectedTable] = useState(null);
  const [availableColumns, setAvailableColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [previewData, setPreviewData] = useState(null);
  const [progress, setProgress] = useState(0);
  const [isProcessing, setIsProcessing] = useState(false);
  const [recordCount, setRecordCount] = useState(null);
  const [error, setError] = useState(null);
  const [jobId, setJobId] = useState(null);

  useEffect(() => {
    if (jobId) {
      const progressInterval = setInterval(async () => {
        try {
          const progressData = await ingestionService.getProgress(jobId);
          setProgress(progressData.progress);
          if (progressData.status === 'COMPLETED') {
            clearInterval(progressInterval);
            setIsProcessing(false);
            toast.success('Ingestion completed successfully!');
          } else if (progressData.status === 'FAILED') {
            clearInterval(progressInterval);
            setIsProcessing(false);
            setError(progressData.error);
            toast.error('Ingestion failed: ' + progressData.error);
          }
        } catch (error) {
          clearInterval(progressInterval);
          setIsProcessing(false);
          setError(error.message);
          toast.error('Failed to fetch progress: ' + error.message);
        }
      }, 1000);

      return () => clearInterval(progressInterval);
    }
  }, [jobId]);

  const handleNext = () => {
    setActiveStep((prevStep) => prevStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevStep) => prevStep - 1);
  };

  const handleSourceSelect = (source) => {
    setSourceType(source);
    handleNext();
  };

  const handleConnectionSubmit = async (config) => {
    try {
      setConnectionConfig(config);
      // Get record count
      const count = await ingestionService.getRecordCount(sourceType, config);
      setRecordCount(count);
      handleNext();
    } catch (error) {
      setError(error.message);
      toast.error('Failed to get record count: ' + error.message);
    }
  };

  const handleTableSelect = async (table) => {
    try {
      setSelectedTable(table);
      // Get schema information
      const schema = await ingestionService.getSchema(sourceType, {
        ...connectionConfig,
        table
      });
      setAvailableColumns(schema.columns);
      handleNext();
    } catch (error) {
      setError(error.message);
      toast.error('Failed to get schema: ' + error.message);
    }
  };

  const handleColumnSelect = (columns) => {
    setSelectedColumns(columns);
    handleNext();
  };

  const handlePreview = async (data) => {
    setPreviewData(data);
    handleNext();
  };

  const handleExecute = async () => {
    setIsProcessing(true);
    setError(null);
    try {
      const config = {
        sourceType,
        connectionConfig,
        selectedTable,
        selectedColumns
      };

      let response;
      if (sourceType === 'clickhouse') {
        response = await ingestionService.exportToFile(config);
      } else {
        response = await ingestionService.importFromFile(config);
      }

      setJobId(response.jobId);
      toast.success('Ingestion started successfully!');
    } catch (error) {
      setIsProcessing(false);
      setError(error.message);
      toast.error('Failed to start ingestion: ' + error.message);
    }
  };

  const getStepContent = (step) => {
    switch (step) {
      case 0:
        return <SourceSelection onSelect={handleSourceSelect} />;
      case 1:
        return (
          <ConnectionForm
            sourceType={sourceType}
            onSubmit={handleConnectionSubmit}
          />
        );
      case 2:
        return (
          <SchemaSelection
            sourceType={sourceType}
            connectionConfig={connectionConfig}
            onSelect={handleTableSelect}
          />
        );
      case 3:
        return (
          <ColumnSelection
            sourceType={sourceType}
            connectionConfig={connectionConfig}
            selectedTable={selectedTable}
            availableColumns={availableColumns}
            onSelect={handleColumnSelect}
          />
        );
      case 4:
        return (
          <DataPreview
            sourceType={sourceType}
            connectionConfig={connectionConfig}
            selectedTable={selectedTable}
            selectedColumns={selectedColumns}
            onPreview={handlePreview}
          />
        );
      case 5:
        return (
          <ProgressDisplay
            progress={progress}
            isProcessing={isProcessing}
            onExecute={handleExecute}
            recordCount={recordCount}
          />
        );
      default:
        return 'Unknown step';
    }
  };

  return (
    <Paper sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Data Ingestion Tool
      </Typography>
      <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
        {steps.map((label) => (
          <Step key={label}>
            <StepLabel>{label}</StepLabel>
          </Step>
        ))}
      </Stepper>
      <Box sx={{ mt: 2 }}>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}
        {getStepContent(activeStep)}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 3 }}>
          <Button
            disabled={activeStep === 0 || isProcessing}
            onClick={handleBack}
          >
            Back
          </Button>
          {activeStep === steps.length - 1 ? (
            <Button
              variant="contained"
              onClick={handleExecute}
              disabled={isProcessing}
            >
              {isProcessing ? 'Processing...' : 'Execute'}
            </Button>
          ) : (
            <Button
              variant="contained"
              onClick={handleNext}
              disabled={!sourceType && activeStep === 0}
            >
              Next
            </Button>
          )}
        </Box>
      </Box>
    </Paper>
  );
}

export default IngestionTool; 