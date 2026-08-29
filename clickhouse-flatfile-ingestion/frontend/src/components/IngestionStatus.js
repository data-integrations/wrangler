import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  CircularProgress,
  Alert,
  IconButton,
  Tooltip
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import StopIcon from '@mui/icons-material/Stop';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import ingestionService from '../services/ingestionService';

const statusColors = {
  RUNNING: 'primary',
  COMPLETED: 'success',
  FAILED: 'error',
  STOPPED: 'warning',
  PENDING: 'default'
};

const IngestionStatus = ({ mappingId, onStatusChange }) => {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [recentJobs, setRecentJobs] = useState([]);
  const [pollingInterval, setPollingInterval] = useState(null);

  useEffect(() => {
    loadStatus();
    
    // Set up polling for status updates
    const interval = setInterval(() => {
      loadStatus();
    }, 5000); // Poll every 5 seconds
    
    setPollingInterval(interval);
    
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [mappingId]);

  const loadStatus = async () => {
    if (!mappingId) return;
    
    try {
      setLoading(true);
      const response = await ingestionService.getStatus(mappingId);
      setStatus(response.status);
      setRecentJobs(response.recentJobs || []);
      setError('');
      
      if (onStatusChange) {
        onStatusChange(response.status);
      }
    } catch (error) {
      setError('Failed to load ingestion status: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleStartIngestion = async () => {
    try {
      setLoading(true);
      await ingestionService.startIngestion(mappingId);
      await loadStatus();
    } catch (error) {
      setError('Failed to start ingestion: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleStopIngestion = async () => {
    try {
      setLoading(true);
      await ingestionService.stopIngestion(mappingId);
      await loadStatus();
    } catch (error) {
      setError('Failed to stop ingestion: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    loadStatus();
  };

  const formatDate = (dateString) => {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleString();
  };

  return (
    <Card>
      <CardContent>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h6">
            Ingestion Status
          </Typography>
          <Box>
            <Tooltip title="Refresh">
              <IconButton onClick={handleRefresh} disabled={loading}>
                <RefreshIcon />
              </IconButton>
            </Tooltip>
            {status?.state === 'RUNNING' && (
              <Tooltip title="Stop Ingestion">
                <IconButton 
                  color="error" 
                  onClick={handleStopIngestion}
                  disabled={loading}
                >
                  <StopIcon />
                </IconButton>
              </Tooltip>
            )}
            {status?.state !== 'RUNNING' && (
              <Tooltip title="Start Ingestion">
                <IconButton 
                  color="primary" 
                  onClick={handleStartIngestion}
                  disabled={loading}
                >
                  <PlayArrowIcon />
                </IconButton>
              </Tooltip>
            )}
          </Box>
        </Box>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {loading ? (
          <Box display="flex" justifyContent="center" p={3}>
            <CircularProgress />
          </Box>
        ) : (
          <>
            {status && (
              <Box mb={3}>
                <Typography variant="subtitle1" gutterBottom>
                  Current Status:
                </Typography>
                <Box display="flex" alignItems="center">
                  <Chip 
                    label={status.state} 
                    color={statusColors[status.state] || 'default'} 
                    sx={{ mr: 2 }}
                  />
                  <Typography variant="body2">
                    Last Updated: {formatDate(status.lastUpdated)}
                  </Typography>
                </Box>
                {status.progress && (
                  <Box mt={1}>
                    <Typography variant="body2" gutterBottom>
                      Progress: {status.progress.percentage}%
                    </Typography>
                    <Typography variant="body2">
                      Processed: {status.progress.processedRecords} records
                    </Typography>
                    {status.progress.errors > 0 && (
                      <Typography variant="body2" color="error">
                        Errors: {status.progress.errors}
                      </Typography>
                    )}
                  </Box>
                )}
              </Box>
            )}

            <Typography variant="subtitle1" gutterBottom>
              Recent Jobs
            </Typography>
            <TableContainer component={Paper}>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Job ID</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Started</TableCell>
                    <TableCell>Completed</TableCell>
                    <TableCell>Records</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {recentJobs.length > 0 ? (
                    recentJobs.map((job) => (
                      <TableRow key={job.id}>
                        <TableCell>{job.id}</TableCell>
                        <TableCell>
                          <Chip 
                            label={job.status} 
                            color={statusColors[job.status] || 'default'} 
                            size="small"
                          />
                        </TableCell>
                        <TableCell>{formatDate(job.startTime)}</TableCell>
                        <TableCell>{formatDate(job.endTime)}</TableCell>
                        <TableCell>{job.processedRecords || 0}</TableCell>
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell colSpan={5} align="center">
                        No recent jobs found
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </>
        )}
      </CardContent>
    </Card>
  );
};

export default IngestionStatus; 