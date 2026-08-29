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
  IconButton,
  Tooltip,
  Divider
} from '@mui/material';
import { useNavigate } from 'react-router-dom';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import SaveIcon from '@mui/icons-material/Save';
import axios from 'axios';

const TableMapping = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState(false);
  const [openSnackbar, setOpenSnackbar] = useState(false);
  
  // Sample data - in a real app, this would come from API calls
  const [availableTables, setAvailableTables] = useState([
    'users', 'orders', 'products', 'customers'
  ]);
  const [selectedTable, setSelectedTable] = useState('');
  const [tableColumns, setTableColumns] = useState([]);
  const [fileColumns, setFileColumns] = useState([]);
  const [mappings, setMappings] = useState([]);
  
  // Form state
  const [formData, setFormData] = useState({
    tableName: '',
    fileColumns: '',
    delimiter: ',',
    hasHeader: true
  });

  // Load available tables when component mounts
  useEffect(() => {
    // In a real app, this would be an API call
    // const fetchTables = async () => {
    //   try {
    //     const response = await axios.get('/api/tables');
    //     setAvailableTables(response.data);
    //   } catch (err) {
    //     setError('Failed to load available tables');
    //   }
    // };
    // fetchTables();
  }, []);

  // Load table columns when a table is selected
  useEffect(() => {
    if (selectedTable) {
      // In a real app, this would be an API call
      // const fetchTableColumns = async () => {
      //   try {
      //     const response = await axios.get(`/api/tables/${selectedTable}/columns`);
      //     setTableColumns(response.data);
      //   } catch (err) {
      //     setError('Failed to load table columns');
      //   }
      // };
      // fetchTableColumns();
      
      // Sample data
      setTableColumns([
        { name: 'id', type: 'UInt32' },
        { name: 'name', type: 'String' },
        { name: 'email', type: 'String' },
        { name: 'created_at', type: 'DateTime' }
      ]);
    }
  }, [selectedTable]);

  const handleTableChange = (event) => {
    setSelectedTable(event.target.value);
  };

  const handleFormChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData({
      ...formData,
      [name]: type === 'checkbox' ? checked : value
    });
  };

  const handleFileUpload = (event) => {
    const file = event.target.files[0];
    if (file) {
      // In a real app, you might want to parse the file to get columns
      // For now, we'll use sample data
      setFileColumns(['id', 'full_name', 'email_address', 'registration_date']);
      
      // Create initial mappings
      const initialMappings = fileColumns.map((fileCol, index) => {
        const tableCol = tableColumns[index] ? tableColumns[index].name : '';
        return {
          fileColumn: fileCol,
          tableColumn: tableCol,
          transformation: ''
        };
      });
      setMappings(initialMappings);
    }
  };

  const handleMappingChange = (index, field, value) => {
    const updatedMappings = [...mappings];
    updatedMappings[index] = {
      ...updatedMappings[index],
      [field]: value
    };
    setMappings(updatedMappings);
  };

  const addMapping = () => {
    setMappings([
      ...mappings,
      { fileColumn: '', tableColumn: '', transformation: '' }
    ]);
  };

  const removeMapping = (index) => {
    const updatedMappings = [...mappings];
    updatedMappings.splice(index, 1);
    setMappings(updatedMappings);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    
    try {
      // In a real application, this would be an API call to your backend
      // const response = await axios.post('/api/table-mappings', {
      //   tableName: selectedTable,
      //   mappings: mappings
      // });
      
      // Simulating API call for demonstration
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      setSuccess(true);
      setOpenSnackbar(true);
      
      // Navigate to data ingestion after successful mapping
      setTimeout(() => {
        navigate('/data-ingestion');
      }, 1500);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to save table mapping');
    } finally {
      setLoading(false);
    }
  };

  const handleCloseSnackbar = () => {
    setOpenSnackbar(false);
  };

  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        Table Mapping
      </Typography>
      <Typography variant="subtitle1" color="text.secondary" paragraph>
        Map your flat file columns to ClickHouse table columns
      </Typography>
      
      <Card sx={{ mt: 3 }}>
        <CardContent>
          <form onSubmit={handleSubmit}>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel id="table-select-label">Select ClickHouse Table</InputLabel>
                  <Select
                    labelId="table-select-label"
                    value={selectedTable}
                    onChange={handleTableChange}
                    label="Select ClickHouse Table"
                  >
                    {availableTables.map((table) => (
                      <MenuItem key={table} value={table}>{table}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              
              <Grid item xs={12} md={6}>
                <Button
                  variant="contained"
                  component="label"
                  fullWidth
                  sx={{ height: '56px' }}
                >
                  Upload Flat File
                  <input
                    type="file"
                    hidden
                    onChange={handleFileUpload}
                    accept=".csv,.txt"
                  />
                </Button>
              </Grid>
              
              {selectedTable && fileColumns.length > 0 && (
                <>
                  <Grid item xs={12}>
                    <Divider sx={{ my: 2 }} />
                    <Typography variant="h6" gutterBottom>
                      Column Mappings
                    </Typography>
                    <Typography variant="body2" color="text.secondary" paragraph>
                      Map your flat file columns to the selected table columns
                    </Typography>
                  </Grid>
                  
                  <Grid item xs={12}>
                    <TableContainer component={Paper} variant="outlined">
                      <Table>
                        <TableHead>
                          <TableRow>
                            <TableCell>File Column</TableCell>
                            <TableCell>Table Column</TableCell>
                            <TableCell>Transformation (Optional)</TableCell>
                            <TableCell width="50px"></TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {mappings.map((mapping, index) => (
                            <TableRow key={index}>
                              <TableCell>
                                <FormControl fullWidth>
                                  <Select
                                    value={mapping.fileColumn}
                                    onChange={(e) => handleMappingChange(index, 'fileColumn', e.target.value)}
                                    displayEmpty
                                  >
                                    <MenuItem value="" disabled>Select column</MenuItem>
                                    {fileColumns.map((col) => (
                                      <MenuItem key={col} value={col}>{col}</MenuItem>
                                    ))}
                                  </Select>
                                </FormControl>
                              </TableCell>
                              <TableCell>
                                <FormControl fullWidth>
                                  <Select
                                    value={mapping.tableColumn}
                                    onChange={(e) => handleMappingChange(index, 'tableColumn', e.target.value)}
                                    displayEmpty
                                  >
                                    <MenuItem value="" disabled>Select column</MenuItem>
                                    {tableColumns.map((col) => (
                                      <MenuItem key={col.name} value={col.name}>{col.name} ({col.type})</MenuItem>
                                    ))}
                                  </Select>
                                </FormControl>
                              </TableCell>
                              <TableCell>
                                <TextField
                                  fullWidth
                                  placeholder="e.g., toUpperCase()"
                                  value={mapping.transformation}
                                  onChange={(e) => handleMappingChange(index, 'transformation', e.target.value)}
                                />
                              </TableCell>
                              <TableCell>
                                <Tooltip title="Remove mapping">
                                  <IconButton 
                                    color="error" 
                                    onClick={() => removeMapping(index)}
                                    disabled={mappings.length <= 1}
                                  >
                                    <DeleteIcon />
                                  </IconButton>
                                </Tooltip>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                  </Grid>
                  
                  <Grid item xs={12}>
                    <Button
                      startIcon={<AddIcon />}
                      onClick={addMapping}
                      sx={{ mt: 1 }}
                    >
                      Add Mapping
                    </Button>
                  </Grid>
                  
                  <Grid item xs={12}>
                    <Button
                      type="submit"
                      variant="contained"
                      color="primary"
                      size="large"
                      disabled={loading || !selectedTable || mappings.length === 0}
                      startIcon={<SaveIcon />}
                      sx={{ mt: 2 }}
                    >
                      {loading ? 'Saving...' : 'Save Mapping'}
                    </Button>
                  </Grid>
                </>
              )}
            </Grid>
          </form>
          
          {error && (
            <Alert severity="error" sx={{ mt: 3 }}>
              {error}
            </Alert>
          )}
        </CardContent>
      </Card>
      
      <Snackbar
        open={openSnackbar}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert onClose={handleCloseSnackbar} severity="success" sx={{ width: '100%' }}>
          Table mapping saved successfully!
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default TableMapping; 