import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Grid,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  IconButton,
  Alert,
  CircularProgress
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import clickhouseService from '../services/clickhouseService';

const SchemaMapping = ({ 
  clickhouseConfig, 
  selectedTable, 
  fileSchema, 
  onMappingComplete 
}) => {
  const [clickhouseSchema, setClickhouseSchema] = useState([]);
  const [mappings, setMappings] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  useEffect(() => {
    if (clickhouseConfig && selectedTable) {
      loadClickHouseSchema();
    }
  }, [clickhouseConfig, selectedTable]);

  const loadClickHouseSchema = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await clickhouseService.getTableSchema(
        clickhouseConfig,
        selectedTable
      );
      setClickhouseSchema(response.data);
    } catch (err) {
      setError('Failed to load ClickHouse schema');
    } finally {
      setLoading(false);
    }
  };

  const handleAddMapping = () => {
    setMappings([
      ...mappings,
      { sourceColumn: '', targetColumn: '', dataType: '' }
    ]);
  };

  const handleRemoveMapping = (index) => {
    const newMappings = mappings.filter((_, i) => i !== index);
    setMappings(newMappings);
  };

  const handleMappingChange = (index, field, value) => {
    const newMappings = mappings.map((mapping, i) => {
      if (i === index) {
        return { ...mapping, [field]: value };
      }
      return mapping;
    });
    setMappings(newMappings);
  };

  const handleSaveMapping = () => {
    // Validate mappings
    const isValid = mappings.every(mapping => 
      mapping.sourceColumn && mapping.targetColumn
    );

    if (!isValid) {
      setError('Please fill in all mapping fields');
      return;
    }

    setSuccess('Schema mapping saved successfully');
    if (onMappingComplete) {
      onMappingComplete(mappings);
    }
  };

  return (
    <Paper sx={{ p: 3, mb: 3 }}>
      <Typography variant="h6" gutterBottom>
        Schema Mapping
      </Typography>

      {loading ? (
        <Box display="flex" justifyContent="center" p={3}>
          <CircularProgress />
        </Box>
      ) : (
        <>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Source Column</TableCell>
                  <TableCell>Target Column</TableCell>
                  <TableCell>Data Type</TableCell>
                  <TableCell>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {mappings.map((mapping, index) => (
                  <TableRow key={index}>
                    <TableCell>
                      <FormControl fullWidth>
                        <Select
                          value={mapping.sourceColumn}
                          onChange={(e) => handleMappingChange(index, 'sourceColumn', e.target.value)}
                        >
                          {fileSchema.map((column) => (
                            <MenuItem key={column.name} value={column.name}>
                              {column.name}
                            </MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </TableCell>
                    <TableCell>
                      <FormControl fullWidth>
                        <Select
                          value={mapping.targetColumn}
                          onChange={(e) => handleMappingChange(index, 'targetColumn', e.target.value)}
                        >
                          {clickhouseSchema.map((column) => (
                            <MenuItem key={column.name} value={column.name}>
                              {column.name}
                            </MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    </TableCell>
                    <TableCell>
                      <FormControl fullWidth>
                        <Select
                          value={mapping.dataType}
                          onChange={(e) => handleMappingChange(index, 'dataType', e.target.value)}
                        >
                          <MenuItem value="String">String</MenuItem>
                          <MenuItem value="Int32">Int32</MenuItem>
                          <MenuItem value="Int64">Int64</MenuItem>
                          <MenuItem value="Float32">Float32</MenuItem>
                          <MenuItem value="Float64">Float64</MenuItem>
                          <MenuItem value="DateTime">DateTime</MenuItem>
                          <MenuItem value="Date">Date</MenuItem>
                          <MenuItem value="Boolean">Boolean</MenuItem>
                        </Select>
                      </FormControl>
                    </TableCell>
                    <TableCell>
                      <IconButton 
                        onClick={() => handleRemoveMapping(index)}
                        color="error"
                      >
                        <DeleteIcon />
                      </IconButton>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>

          <Box sx={{ mt: 2, display: 'flex', gap: 2 }}>
            <Button
              variant="outlined"
              startIcon={<AddIcon />}
              onClick={handleAddMapping}
            >
              Add Mapping
            </Button>
            <Button
              variant="contained"
              onClick={handleSaveMapping}
            >
              Save Mapping
            </Button>
          </Box>

          {error && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {error}
            </Alert>
          )}

          {success && (
            <Alert severity="success" sx={{ mt: 2 }}>
              {success}
            </Alert>
          )}
        </>
      )}
    </Paper>
  );
};

export default SchemaMapping; 