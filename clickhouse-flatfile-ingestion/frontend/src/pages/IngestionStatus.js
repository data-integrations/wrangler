import React, { useState, useEffect } from 'react';
import { 
  Typography, 
  Box, 
  Card, 
  CardContent, 
  Grid,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Chip,
  TextField,
  InputAdornment,
  IconButton,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Alert,
  CircularProgress,
  Divider
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import RefreshIcon from '@mui/icons-material/Refresh';
import VisibilityIcon from '@mui/icons-material/Visibility';
import DownloadIcon from '@mui/icons-material/Download';
import axios from 'axios';

const IngestionStatus = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [jobs, setJobs] = useState([]);
  const [filteredJobs, setFilteredJobs] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [selectedJob, setSelectedJob] = useState(null);
  const [openDialog, setOpenDialog] = useState(false);
  
  // Load jobs when component mounts
  useEffect(() => {
    fetchJobs();
  }, []);
  
  // Filter jobs when search term changes
  useEffect(() => {
    if (searchTerm) {
      const filtered = jobs.filter(job => 
        job.jobId.toLowerCase().includes(searchTerm.toLowerCase()) ||
        job.mappingName.toLowerCase().includes(searchTerm.toLowerCase()) ||
        job.status.toLowerCase().includes(searchTerm.toLowerCase())
      );
      setFilteredJobs(filtered);
    } else {
      setFilteredJobs(jobs);
    }
    setPage(0);
  }, [searchTerm, jobs]);
  
  const fetchJobs = async () => {
    setLoading(true);
    setError('');
    
    try {
      // In a real app, this would be an API call
      // const response = await axios.get('/api/ingestion/jobs');
      // setJobs(response.data);
      
      // Sample data
      await new Promise(resolve => setTimeout(resolve, 1000));
      setJobs([
        {
          jobId: 'job-abc123',
          mappingName: 'Users Mapping',
          tableName: 'users',
          status: 'completed',
          recordsProcessed: 10000,
          totalRecords: 10000,
          startTime: '2023-04-15T10:30:45Z',
          endTime: '2023-04-15T10:33:00Z',
          errorMessage: null,
          batchSize: 1000,
          maxRetries: 3,
          validateData: true
        },
        {
          jobId: 'job-def456',
          mappingName: 'Orders Mapping',
          tableName: 'orders',
          status: 'failed',
          recordsProcessed: 5000,
          totalRecords: 20000,
          startTime: '2023-04-14T15:20:10Z',
          endTime: '2023-04-14T15:21:40Z',
          errorMessage: 'Connection timeout after 3 retries',
          batchSize: 500,
          maxRetries: 3,
          validateData: true
        },
        {
          jobId: 'job-ghi789',
          mappingName: 'Products Mapping',
          tableName: 'products',
          status: 'running',
          recordsProcessed: 7500,
          totalRecords: 15000,
          startTime: '2023-04-13T09:15:30Z',
          endTime: null,
          errorMessage: null,
          batchSize: 1000,
          maxRetries: 3,
          validateData: true
        }
      ]);
    } catch (err) {
      setError('Failed to load ingestion jobs');
    } finally {
      setLoading(false);
    }
  };
  
  const handleSearchChange = (event) => {
    setSearchTerm(event.target.value);
  };
  
  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };
  
  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };
  
  const handleViewDetails = (job) => {
    setSelectedJob(job);
    setOpenDialog(true);
  };
  
  const handleCloseDialog = () => {
    setOpenDialog(false);
    setSelectedJob(null);
  };
  
  const handleDownloadLogs = async (jobId) => {
    try {
      // In a real app, this would be an API call
      // const response = await axios.get(`/api/ingestion/jobs/${jobId}/logs`, {
      //   responseType: 'blob'
      // });
      // const url = window.URL.createObjectURL(new Blob([response.data]));
      // const link = document.createElement('a');
      // link.href = url;
      // link.setAttribute('download', `ingestion-${jobId}-logs.txt`);
      // document.body.appendChild(link);
      // link.click();
      // link.remove();
      
      // Simulating API call for demonstration
      await new Promise(resolve => setTimeout(resolve, 500));
      alert('Logs downloaded successfully');
    } catch (err) {
      setError('Failed to download logs');
    }
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
        Ingestion Status
      </Typography>
      <Typography variant="subtitle1" color="text.secondary" paragraph>
        View and monitor data ingestion jobs
      </Typography>
      
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                placeholder="Search by Job ID, Mapping, or Status"
                value={searchTerm}
                onChange={handleSearchChange}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon />
                    </InputAdornment>
                  ),
                }}
              />
            </Grid>
            <Grid item xs={12} sm={6} sx={{ display: 'flex', justifyContent: 'flex-end' }}>
              <Button
                variant="outlined"
                startIcon={<RefreshIcon />}
                onClick={fetchJobs}
                disabled={loading}
              >
                Refresh
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>
      
      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}
      
      <Card>
        <CardContent>
          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
              <CircularProgress />
            </Box>
          ) : (
            <>
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
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {filteredJobs
                      .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                      .map((job) => (
                        <TableRow key={job.jobId}>
                          <TableCell>{job.jobId}</TableCell>
                          <TableCell>{job.mappingName}</TableCell>
                          <TableCell>
                            <Chip 
                              label={job.status.charAt(0).toUpperCase() + job.status.slice(1)} 
                              color={getStatusColor(job.status)}
                              size="small"
                            />
                          </TableCell>
                          <TableCell>
                            {job.recordsProcessed.toLocaleString()} / {job.totalRecords.toLocaleString()}
                          </TableCell>
                          <TableCell>{formatTime(job.startTime)}</TableCell>
                          <TableCell>{calculateDuration(job.startTime, job.endTime)}</TableCell>
                          <TableCell>
                            <IconButton
                              size="small"
                              onClick={() => handleViewDetails(job)}
                              title="View Details"
                            >
                              <VisibilityIcon />
                            </IconButton>
                            <IconButton
                              size="small"
                              onClick={() => handleDownloadLogs(job.jobId)}
                              title="Download Logs"
                            >
                              <DownloadIcon />
                            </IconButton>
                          </TableCell>
                        </TableRow>
                      ))}
                  </TableBody>
                </Table>
              </TableContainer>
              
              <TablePagination
                component="div"
                count={filteredJobs.length}
                page={page}
                onPageChange={handleChangePage}
                rowsPerPage={rowsPerPage}
                onRowsPerPageChange={handleChangeRowsPerPage}
                rowsPerPageOptions={[5, 10, 25, 50]}
              />
            </>
          )}
        </CardContent>
      </Card>
      
      <Dialog
        open={openDialog}
        onClose={handleCloseDialog}
        maxWidth="md"
        fullWidth
      >
        {selectedJob && (
          <>
            <DialogTitle>
              Job Details: {selectedJob.jobId}
            </DialogTitle>
            <DialogContent>
              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Mapping Name
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {selectedJob.mappingName}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Table Name
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {selectedJob.tableName}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Status
                  </Typography>
                  <Chip 
                    label={selectedJob.status.charAt(0).toUpperCase() + selectedJob.status.slice(1)} 
                    color={getStatusColor(selectedJob.status)}
                    size="small"
                    sx={{ mt: 0.5 }}
                  />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Records Processed
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {selectedJob.recordsProcessed.toLocaleString()} / {selectedJob.totalRecords.toLocaleString()}
                  </Typography>
                </Grid>
                <Grid item xs={12}>
                  <Divider sx={{ my: 2 }} />
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Start Time
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {formatTime(selectedJob.startTime)}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    End Time
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {selectedJob.endTime ? formatTime(selectedJob.endTime) : 'In Progress'}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Duration
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {calculateDuration(selectedJob.startTime, selectedJob.endTime)}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Batch Size
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {selectedJob.batchSize.toLocaleString()}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Max Retries
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {selectedJob.maxRetries}
                  </Typography>
                </Grid>
                <Grid item xs={12} sm={6}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Data Validation
                  </Typography>
                  <Typography variant="body1" gutterBottom>
                    {selectedJob.validateData ? 'Enabled' : 'Disabled'}
                  </Typography>
                </Grid>
                {selectedJob.errorMessage && (
                  <Grid item xs={12}>
                    <Divider sx={{ my: 2 }} />
                    <Typography variant="subtitle2" color="text.secondary">
                      Error Message
                    </Typography>
                    <Alert severity="error" sx={{ mt: 0.5 }}>
                      {selectedJob.errorMessage}
                    </Alert>
                  </Grid>
                )}
              </Grid>
            </DialogContent>
            <DialogActions>
              <Button onClick={handleCloseDialog}>Close</Button>
              <Button
                startIcon={<DownloadIcon />}
                onClick={() => handleDownloadLogs(selectedJob.jobId)}
              >
                Download Logs
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>
    </Box>
  );
};

export default IngestionStatus; 