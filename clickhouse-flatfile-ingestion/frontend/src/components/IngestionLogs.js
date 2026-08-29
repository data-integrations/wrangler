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
  CircularProgress,
  Alert,
  IconButton,
  Tooltip,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  Button,
  Pagination
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import FilterListIcon from '@mui/icons-material/FilterList';
import ingestionService from '../services/ingestionService';

const IngestionLogs = ({ mappingId }) => {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [logLevel, setLogLevel] = useState('all');
  const [searchTerm, setSearchTerm] = useState('');
  const [showFilters, setShowFilters] = useState(false);

  useEffect(() => {
    if (mappingId) {
      loadLogs();
    }
  }, [mappingId, page, pageSize, logLevel]);

  const loadLogs = async () => {
    if (!mappingId) return;
    
    try {
      setLoading(true);
      const options = {
        page,
        pageSize,
        level: logLevel !== 'all' ? logLevel : undefined,
        search: searchTerm || undefined
      };
      
      const response = await ingestionService.getLogs(mappingId, options);
      setLogs(response.logs || []);
      setTotalPages(Math.ceil(response.total / pageSize));
      setError('');
    } catch (error) {
      setError('Failed to load logs: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = () => {
    loadLogs();
  };

  const handlePageChange = (event, value) => {
    setPage(value);
  };

  const handlePageSizeChange = (event) => {
    setPageSize(event.target.value);
    setPage(1); // Reset to first page when changing page size
  };

  const handleLogLevelChange = (event) => {
    setLogLevel(event.target.value);
    setPage(1); // Reset to first page when changing filter
  };

  const handleSearchChange = (event) => {
    setSearchTerm(event.target.value);
  };

  const handleSearch = () => {
    setPage(1); // Reset to first page when searching
    loadLogs();
  };

  const toggleFilters = () => {
    setShowFilters(!showFilters);
  };

  const getLogLevelColor = (level) => {
    switch (level.toLowerCase()) {
      case 'error':
        return 'error';
      case 'warn':
      case 'warning':
        return 'warning';
      case 'info':
        return 'info';
      case 'debug':
        return 'default';
      default:
        return 'default';
    }
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
            Ingestion Logs
          </Typography>
          <Box>
            <Tooltip title="Refresh">
              <IconButton onClick={handleRefresh} disabled={loading}>
                <RefreshIcon />
              </IconButton>
            </Tooltip>
            <Tooltip title="Filters">
              <IconButton onClick={toggleFilters}>
                <FilterListIcon />
              </IconButton>
            </Tooltip>
          </Box>
        </Box>

        {showFilters && (
          <Box mb={3} p={2} bgcolor="background.paper" borderRadius={1}>
            <Box display="flex" flexDirection={{ xs: 'column', sm: 'row' }} gap={2} mb={2}>
              <FormControl sx={{ minWidth: 120 }}>
                <InputLabel id="log-level-label">Log Level</InputLabel>
                <Select
                  labelId="log-level-label"
                  value={logLevel}
                  label="Log Level"
                  onChange={handleLogLevelChange}
                  size="small"
                >
                  <MenuItem value="all">All</MenuItem>
                  <MenuItem value="error">Error</MenuItem>
                  <MenuItem value="warn">Warning</MenuItem>
                  <MenuItem value="info">Info</MenuItem>
                  <MenuItem value="debug">Debug</MenuItem>
                </Select>
              </FormControl>
              
              <Box display="flex" flexGrow={1} gap={1}>
                <TextField
                  label="Search"
                  variant="outlined"
                  size="small"
                  value={searchTerm}
                  onChange={handleSearchChange}
                  fullWidth
                />
                <Button 
                  variant="contained" 
                  onClick={handleSearch}
                  sx={{ minWidth: 100 }}
                >
                  Search
                </Button>
              </Box>
            </Box>
          </Box>
        )}

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
            {logs.length > 0 ? (
              <>
                <TableContainer component={Paper} sx={{ mb: 2 }}>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Timestamp</TableCell>
                        <TableCell>Level</TableCell>
                        <TableCell>Message</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {logs.map((log, index) => (
                        <TableRow key={index}>
                          <TableCell>{formatDate(log.timestamp)}</TableCell>
                          <TableCell>
                            <Typography 
                              color={getLogLevelColor(log.level)}
                              fontWeight="bold"
                            >
                              {log.level}
                            </Typography>
                          </TableCell>
                          <TableCell>{log.message}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
                
                <Box display="flex" justifyContent="space-between" alignItems="center">
                  <FormControl size="small" sx={{ minWidth: 120 }}>
                    <InputLabel id="page-size-label">Page Size</InputLabel>
                    <Select
                      labelId="page-size-label"
                      value={pageSize}
                      label="Page Size"
                      onChange={handlePageSizeChange}
                    >
                      <MenuItem value={10}>10</MenuItem>
                      <MenuItem value={25}>25</MenuItem>
                      <MenuItem value={50}>50</MenuItem>
                      <MenuItem value={100}>100</MenuItem>
                    </Select>
                  </FormControl>
                  
                  <Pagination 
                    count={totalPages} 
                    page={page} 
                    onChange={handlePageChange} 
                    color="primary" 
                  />
                </Box>
              </>
            ) : (
              <Alert severity="info">
                No logs found
              </Alert>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
};

 