import React, { useState, useEffect } from 'react';
import { 
  Typography, 
  Box, 
  Button, 
  Card, 
  CardContent, 
  Grid,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert,
  Snackbar,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  CircularProgress,
  LinearProgress,
  Divider,
  Chip
} from '@mui/material';
import { useNavigate } from 'react-router-dom';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import RefreshIcon from '@mui/icons-material/Refresh';
import axios from 'axios';

const DataIngestion = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState(false);
  const [openSnackbar, setOpenSnackbar] = useState(false);
  const [snackbarMessage, setSnackbarMessage] = useState('');
  
  // Form state
  const [formData, setFormData] = useState({
    batchSize: 1000,
    maxRetries: 3,
    timeout: 30000,
    validateData: true
  });
  
  // Ingestion state
  const [ingestionStatus, setIngestionStatus] = useState({
    status: 'idle', // idle, running, completed, failed, paused
    progress: 0,
    recordsProcessed: 0,
    totalRecords: 0,
    startTime: null,
    endTime: null,
    errorMessage: null
  });
  
  // Available mappings
  const [availableMappings, setAvailableMappings] = useState([]);
  const [selectedMapping, setSelectedMapping] = useState('');
  
  // Load available mappings when component mounts
  useEffect(() => {
    // In a real app, this would be an API call
    // const fetchMappings = async () => {
    //   try {
    //     const response = await axios.get('/api/mappings');
    //     setAvailableMappings(response.data);
    //   } catch (err) {
    //     setError('Failed to load available mappings');
    //   }
    // };
    // fetchMappings();
    
    // Sample data
    setAvailableMappings([
      { id: '1', name: 'Users Mapping', tableName: 'users' },
      { id: '2', name: 'Orders Mapping', tableName: 'orders' },
      { id: '3', name: 'Products Mapping', tableName: 'products' }
    ]);
  }, []);
  
  // Poll for ingestion status if ingestion is running
  useEffect(() => {
    let intervalId;
    
    if (ingestionStatus.status === 'running') {
      intervalId = setInterval(() => {
        // In a real app, this would be an API call
        // const fetchStatus = async () => {
        //   try {
        //     const response = await axios.get(`/api/ingestion/status/${ingestionStatus.jobId}`);
        //     setIngestionStatus(response.data);
        //     
        //     if (response.data.status === 'completed' || response.data.status === 'failed') {
        //       clearInterval(intervalId);
        //     }
        //   } catch (err) {
        //     setError('Failed to fetch ingestion status');
        //     clearInterval(intervalId);
        //   }
        // };
        // fetchStatus();
        
        // Sample data - simulate progress
        setIngestionStatus(prevStatus => {
          const newProgress = Math.min(prevStatus.progress + 5, 100);
          const newRecordsProcessed = Math.floor((newProgress / 100) * prevStatus.totalRecords);
          
          if (newProgress === 100) {
            return {
              ...prevStatus,
              status: 'completed',
              progress: newProgress,
              recordsProcessed: newRecordsProcessed,
              endTime: new Date().toISOString()
            };
          }
          
          return {
            ...prevStatus,
            progress: newProgress,
            recordsProcessed: newRecordsProcessed
          };
        });
      }, 2000);
    }
    
    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [ingestionStatus.status]);
  
  const handleFormChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData({
      ...formData,
      [name]: type === 'checkbox' ? checked : value
    });
  };
  
  const handleMappingChange = (event) => {
    setSelectedMapping(event.target.value);
  };
  
  const startIngestion = async () => {
    if (!selectedMapping) {
      setError('Please select a mapping');
      return;
    }
    
    setLoading(true);
    setError('');
    
    try {
      // In a real application, this would be an API call to your backend
      // const response = await axios.post('/api/ingestion/start', {
      //   mappingId: selectedMapping,
      //   batchSize: formData.batchSize,
      //   maxRetries: formData.maxRetries,
      //   timeout: formData.timeout,
      //   validateData: formData.validateData
      // });
      
      // Simulating API call for demonstration
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Sample response
      const jobId = 'job-' + Math.random().toString(36).substring(2, 9);
      
      setIngestionStatus({
        status: 'running',
        progress: 0,
        recordsProcessed: 0,
        totalRecords: 10000,
        startTime: new Date().toISOString(),
        endTime: null,
        errorMessage: null,
        jobId
      });
      
      setSuccess(true);
      setSnackbarMessage('Ingestion started successfully');
      setOpenSnackbar(true);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to start ingestion');
    } finally {
      setLoading(false);
    }
  };
  
  const stopIngestion = async () => {
    setLoading(true);
    
    try {
      // In a real application, this would be an API call to your backend
      // await axios.post(`/api/ingestion/stop/${ingestionStatus.jobId}`);
      
      // Simulating API call for demonstration
      await new Promise(resolve => setTimeout(resolve, 500));
      
      setIngestionStatus(prevStatus => ({
        ...prevStatus,
        status: 'paused',
        endTime: new Date().toISOString()
      }));
      
      setSnackbarMessage('Ingestion paused');
      setOpenSnackbar(true);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to stop ingestion');
    } finally {
      setLoading(false);
    }
  };
  
  const handleCloseSnackbar = () => {
    setOpenSnackbar(false);
  };
  
  const getStatusColor = (status) => {
    switch (status) {
      case 'running':
        return 'primary';
      case 'completed':
        return 'success';
      case 'failed':
        return 'error';
      case 'paused':
        return 'warning';
      default:
        return 'default';
    }
  };
  
  const formatTime = (timeString) => {
    if (!timeString) return '';
    const date = new Date(timeString);
    return date.toLocaleString();
  };
  
  const calculateDuration = (startTime, endTime) => {
    if (!startTime) return '';
    const start = new Date(startTime);
    const end = endTime ? new Date(endTime) : new Date();
    const diffMs = end - start;
    const diffSec = Math.floor(diffMs / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffHour = Math.floor(diffMin / 60);
    
    if (diffHour > 0) {
      return `${diffHour}h ${diffMin % 60}m ${diffSec % 60}s`;
    } else if (diffMin > 0) {
      return `${diffMin}m ${diffSec % 60}s`;
    } else {
      return `${diffSec}s`;
    }
  };
  
  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        Data Ingestion
      </Typography>
      <Typography variant="subtitle1" color="text.secondary" paragraph>
        Start and monitor data ingestion into ClickHouse
      </Typography>
      
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Ingestion Configuration
              </Typography>
              
              <FormControl fullWidth sx={{ mb: 2 }}>
                <InputLabel id="mapping-select-label">Select Mapping</InputLabel>
                <Select
                  labelId="mapping-select-label"
                  value={selectedMapping}
                  onChange={handleMappingChange}
                  label="Select Mapping"
                  disabled={ingestionStatus.status === 'running'}
                >
                  {availableMappings.map((mapping) => (
                    <MenuItem key={mapping.id} value={mapping.id}>
                      {mapping.name} ({mapping.tableName})
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              
              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Batch Size"
                    name="batchSize"
                    type="number"
                    value={formData.batchSize}
                    onChange={handleFormChange}
                    disabled={ingestionStatus.status === 'running'}
                    InputProps={{ inputProps: { min: 1 } }}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Max Retries"
                    name="maxRetries"
                    type="number"
                    value={formData.maxRetries}
                    onChange={handleFormChange}
                    disabled={ingestionStatus.status === 'running'}
                    InputProps={{ inputProps: { min: 0 } }}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Timeout (ms)"
                    name="timeout"
                    type="number"
                    value={formData.timeout}
                    onChange={handleFormChange}
                    disabled={ingestionStatus.status === 'running'}
                    InputProps={{ inputProps: { min: 1000 } }}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <FormControl fullWidth>
                    <InputLabel id="validate-data-label">Validate Data</InputLabel>
                    <Select
                      labelId="validate-data-label"
                      name="validateData"
                      value={formData.validateData}
                      onChange={handleFormChange}
                      label="Validate Data"
                      disabled={ingestionStatus.status === 'running'}
                    >
                      <MenuItem value={true}>Yes</MenuItem>
                      <MenuItem value={false}>No</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
              
              <Box sx={{ mt: 3, display: 'flex', justifyContent: 'space-between' }}>
                <Button
                  variant="contained"
                  color="primary"
                  startIcon={<PlayArrowIcon />}
                  onClick={startIngestion}
                  disabled={loading || ingestionStatus.status === 'running' || !selectedMapping}
                >
                  Start Ingestion
                </Button>
                
                {ingestionStatus.status === 'running' && (
                  <Button
                    variant="outlined"
                    color="error"
                    startIcon={<StopIcon />}
                    onClick={stopIngestion}
                    disabled={loading}
                  >
                    Stop Ingestion
                  </Button>
                )}
              </Box>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Ingestion Status
              </Typography>
              
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <Typography variant="body1" sx={{ mr: 2 }}>
                  Status:
                </Typography>
                <Chip 
                  label={ingestionStatus.status.charAt(0).toUpperCase() + ingestionStatus.status.slice(1)} 
                  color={getStatusColor(ingestionStatus.status)}
                  size="small"
                />
              </Box>
              
              {ingestionStatus.status !== 'idle' && (
                <>
                  <Box sx={{ mb: 2 }}>
                    <Typography variant="body2" color="text.secondary" gutterBottom>
                      Progress: {ingestionStatus.progress}%
                    </Typography>
                    <LinearProgress 
                      variant="determinate" 
                      value={ingestionStatus.progress} 
                      color={ingestionStatus.status === 'completed' ? 'success' : 'primary'}
                    />
                  </Box>
                  
                  <Grid container spacing={2} sx={{ mb: 2 }}>
                    <Grid item xs={12} sm={6}>
                      <Typography variant="body2" color="text.secondary">
                        Records Processed:
                      </Typography>
                      <Typography variant="body1">
                        {ingestionStatus.recordsProcessed.toLocaleString()} / {ingestionStatus.totalRecords.toLocaleString()}
                      </Typography>
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <Typography variant="body2" color="text.secondary">
                        Duration:
                      </Typography>
                      <Typography variant="body1">
                        {calculateDuration(ingestionStatus.startTime, ingestionStatus.endTime)}
                      </Typography>
                    </Grid>
                  </Grid>
                  
                  <Divider sx={{ my: 2 }} />
                  
                  <Grid container spacing={2}>
                    <Grid item xs={12} sm={6}>
                      <Typography variant="body2" color="text.secondary">
                        Start Time:
                      </Typography>
                      <Typography variant="body1">
                        {formatTime(ingestionStatus.startTime)}
                      </Typography>
                    </Grid>
                    {ingestionStatus.endTime && (
                      <Grid item xs={12} sm={6}>
                        <Typography variant="body2" color="text.secondary">
                          End Time:
                        </Typography>
                        <Typography variant="body1">
                          {formatTime(ingestionStatus.endTime)}
                        </Typography>
                      </Grid>
                    )}
                  </Grid>
                  
                  {ingestionStatus.errorMessage && (
                    <Alert severity="error" sx={{ mt: 2 }}>
                      {ingestionStatus.errorMessage}
                    </Alert>
                  )}
                </>
              )}
              
              {ingestionStatus.status === 'idle' && (
                <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 200 }}>
                  <Typography variant="body1" color="text.secondary">
                    No ingestion in progress. Select a mapping and click "Start Ingestion" to begin.
                  </Typography>
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="h6">
                  Recent Ingestion Jobs
                </Typography>
                <Button
                  startIcon={<RefreshIcon />}
                  onClick={() => navigate('/ingestion-status')}
                >
                  View All
                </Button>
              </Box>
              
              <TableContainer component={Paper} variant="outlined">
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Job ID</TableCell>
                      <TableCell>Mapping</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Records</TableCell>
                      <TableCell>Start Time</TableCell>
                      <TableCell>Duration</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {/* Sample data - in a real app, this would come from an API call */}
                    <TableRow>
                      <TableCell>job-abc123</TableCell>
                      <TableCell>Users Mapping</TableCell>
                      <TableCell>
                        <Chip label="Completed" color="success" size="small" />
                      </TableCell>
                      <TableCell>10,000 / 10,000</TableCell>
                      <TableCell>2023-04-15 10:30:45</TableCell>
                      <TableCell>2m 15s</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>job-def456</TableCell>
                      <TableCell>Orders Mapping</TableCell>
                      <TableCell>
                        <Chip label="Failed" color="error" size="small" />
                      </TableCell>
                      <TableCell>5,000 / 20,000</TableCell>
                      <TableCell>2023-04-14 15:20:10</TableCell>
                      <TableCell>1m 30s</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>job-ghi789</TableCell>
                      <TableCell>Products Mapping</TableCell>
                      <TableCell>
                        <Chip label="Running" color="primary" size="small" />
                      </TableCell>
                      <TableCell>7,500 / 15,000</TableCell>
                      <TableCell>2023-04-13 09:15:30</TableCell>
                      <TableCell>3m 45s</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </TableContainer>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
      
      {error && (
        <Alert severity="error" sx={{ mt: 3 }}>
          {error}
        </Alert>
      )}
      
      <Snackbar
        open={openSnackbar}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert onClose={handleCloseSnackbar} severity="success" sx={{ width: '100%' }}>
          {snackbarMessage}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default DataIngestion; 