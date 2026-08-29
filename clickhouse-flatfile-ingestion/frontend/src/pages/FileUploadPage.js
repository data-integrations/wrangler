import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Typography,
  Card,
  CardContent,
  Alert,
  Stepper,
  Step,
  StepLabel,
  Button
} from '@mui/material';
import FileUpload from '../components/FileUpload';

const FileUploadPage = () => {
  const navigate = useNavigate();
  const [activeStep, setActiveStep] = useState(0);
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [error, setError] = useState('');

  const steps = [
    'Upload Files',
    'Configure Mapping',
    'Start Ingestion'
  ];

  const handleFileUploaded = (file) => {
    setUploadedFiles(prev => [...prev, file]);
    setError('');
  };

  const handleNext = () => {
    if (uploadedFiles.length === 0) {
      setError('Please upload at least one file before proceeding');
      return;
    }

    if (activeStep === 0) {
      // Navigate to table mapping page with uploaded files info
      navigate('/mapping', { 
        state: { 
          files: uploadedFiles.map(f => ({
            name: f.name,
            size: f.size,
            type: f.type
          }))
        }
      });
    }
  };

  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        Upload Files
      </Typography>
      <Typography variant="subtitle1" color="text.secondary" paragraph>
        Upload your CSV, TSV, or TXT files for ingestion into ClickHouse
      </Typography>

      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
            {steps.map((label) => (
              <Step key={label}>
                <StepLabel>{label}</StepLabel>
              </Step>
            ))}
          </Stepper>

          {error && (
            <Alert severity="error" sx={{ mb: 3 }}>
              {error}
            </Alert>
          )}

          <FileUpload onFileUploaded={handleFileUploaded} />
        </CardContent>
      </Card>

      <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
        <Button
          variant="contained"
          onClick={handleNext}
          disabled={uploadedFiles.length === 0}
        >
          Configure Mapping
        </Button>
      </Box>
    </Box>
  );
};

export default FileUploadPage; 