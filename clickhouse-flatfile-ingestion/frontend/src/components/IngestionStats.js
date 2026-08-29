import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  CircularProgress,
  Alert,
  IconButton,
  Tooltip,
  Divider,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Button,
  Chip
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import DownloadIcon from '@mui/icons-material/Download';
import ingestionService from '../services/ingestionService';

const IngestionStats = ({ mappingId }) => {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [timeRange, setTimeRange] = useState('24h'); // Default to last 24 hours

  useEffect(() => {
    if (mappingId) {
      loadStats();
    }
  }, [mappingId, timeRange]);

  const loadStats = async () => {
    if (!mappingId) return;
    
    try {
      setLoading(true);
      const response = await ingestionService.getStats(mappingId, { timeRange });
      setStats(response);
      setError('');
    } catch (error) {
      setError('Failed to load statistics: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    loadStats();
  };

  const handleTimeRangeChange = (range) => {
    setTimeRange(range);
  };

  const handleDownloadReport = () => {
    if (!stats) return;
    
    // Create a CSV string from the stats data
    const csvContent = generateCSV(stats);
    
    // Create a blob and download link
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', `ingestion-stats-${mappingId}-${new Date().toISOString()}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const generateCSV = (data) => {
    // Simple CSV generation for the stats data
    const headers = ['Metric', 'Value'];
    const rows = [
      ['Total Records Processed', data.totalRecords],
      ['Success Rate', `${data.successRate}%`],
      ['Average Processing Time', `${data.avgProcessingTime}ms`],
      ['Failed Records', data.failedRecords],
      ['Last Successful Ingestion', data.lastSuccessfulIngestion],
      ['Last Failed Ingestion', data.lastFailedIngestion || 'N/A']
    ];
    
    return [
      headers.join(','),
      ...rows.map(row => row.join(','))
    ].join('\n');
  };

  const formatDate = (dateString) => {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleString();
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'success':
        return 'success';
      case 'failed':
        return 'error';
      case 'in_progress':
        return 'info';
      default:
        return 'default';
    }
  };

  if (loading) {
    return (
      <Card>
        <CardContent>
          <Box display="flex" justifyContent="center" p={3}>
            <CircularProgress />
          </Box>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardContent>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h6">
            Ingestion Statistics
          </Typography>
          <Box>
            <Tooltip title="Refresh">
              <IconButton onClick={handleRefresh} disabled={loading}>
                <RefreshIcon />
              </IconButton>
            </Tooltip>
            <Tooltip title="Download Report">
              <IconButton onClick={handleDownloadReport} disabled={!stats}>
                <DownloadIcon />
              </IconButton>
            </Tooltip>
          </Box>
        </Box>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {stats ? (
          <>
            <Box mb={2}>
              <Button 
                variant={timeRange === '24h' ? 'contained' : 'outlined'} 
                size="small" 
                onClick={() => handleTimeRangeChange('24h')}
                sx={{ mr: 1 }}
              >
                24 Hours
              </Button>
              <Button 
                variant={timeRange === '7d' ? 'contained' : 'outlined'} 
                size="small" 
                onClick={() => handleTimeRangeChange('7d')}
                sx={{ mr: 1 }}
              >
                7 Days
              </Button>
              <Button 
                variant={timeRange === '30d' ? 'contained' : 'outlined'} 
                size="small" 
                onClick={() => handleTimeRangeChange('30d')}
              >
                30 Days
              </Button>
            </Box>

            <Grid container spacing={3} mb={3}>
              <Grid item xs={12} sm={6} md={4}>
                <Card variant="outlined">
                  <CardContent>
                    <Typography color="textSecondary" gutterBottom>
                      Total Records Processed
                    </Typography>
                    <Typography variant="h4">
                      {stats.totalRecords.toLocaleString()}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} sm={6} md={4}>
                <Card variant="outlined">
                  <CardContent>
                    <Typography color="textSecondary" gutterBottom>
                      Success Rate
                    </Typography>
                    <Typography variant="h4" color={stats.successRate >= 95 ? 'success.main' : 'warning.main'}>
                      {stats.successRate}%
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} sm={6} md={4}>
                <Card variant="outlined">
                  <CardContent>
                    <Typography color="textSecondary" gutterBottom>
                      Average Processing Time
                    </Typography>
                    <Typography variant="h4">
                      {stats.avgProcessingTime}ms
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            </Grid>

            <Divider sx={{ my: 2 }} />

            <Typography variant="subtitle1" gutterBottom>
              Recent Ingestion Jobs
            </Typography>

            <TableContainer component={Paper} sx={{ mb: 3 }}>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Job ID</TableCell>
                    <TableCell>Start Time</TableCell>
                    <TableCell>End Time</TableCell>
                    <TableCell>Records Processed</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {stats.recentJobs && stats.recentJobs.length > 0 ? (
                    stats.recentJobs.map((job) => (
                      <TableRow key={job.id}>
                        <TableCell>{job.id}</TableCell>
                        <TableCell>{formatDate(job.startTime)}</TableCell>
                        <TableCell>{formatDate(job.endTime)}</TableCell>
                        <TableCell>{job.recordsProcessed.toLocaleString()}</TableCell>
                        <TableCell>
                          <Chip 
                            label={job.status} 
                            color={getStatusColor(job.status)} 
                            size="small" 
                          />
                        </TableCell>
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

            <Divider sx={{ my: 2 }} />

            <Typography variant="subtitle1" gutterBottom>
              Performance Metrics
            </Typography>

            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <Card variant="outlined">
                  <CardContent>
                    <Typography color="textSecondary" gutterBottom>
                      Throughput (Records/Second)
                    </Typography>
                    <Typography variant="h5">
                      {stats.throughput.toLocaleString()}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6}>
                <Card variant="outlined">
                  <CardContent>
                    <Typography color="textSecondary" gutterBottom>
                      Failed Records
                    </Typography>
                    <Typography variant="h5" color="error.main">
                      {stats.failedRecords.toLocaleString()}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            </Grid>

            <Divider sx={{ my: 2 }} />

            <Typography variant="subtitle1" gutterBottom>
              Last Ingestion Details
            </Typography>

            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <Card variant="outlined">
                  <CardContent>
                    <Typography color="textSecondary" gutterBottom>
                      Last Successful Ingestion
                    </Typography>
                    <Typography variant="body1">
                      {formatDate(stats.lastSuccessfulIngestion)}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
              <Grid item xs={12} md={6}>
                <Card variant="outlined">
                  <CardContent>
                    <Typography color="textSecondary" gutterBottom>
                      Last Failed Ingestion
                    </Typography>
                    <Typography variant="body1" color="error.main">
                      {formatDate(stats.lastFailedIngestion) || 'N/A'}
                    </Typography>
                  </CardContent>
                </Card>
              </Grid>
            </Grid>
          </>
        ) : (
          <Alert severity="info">
            No statistics available
          </Alert>
        )}
      </CardContent>
    </Card>
  );
};

export default IngestionStats; 