import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  TextField,
  Button,
  Grid,
  Typography,
  Alert,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  IconButton,
  Tooltip
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import clickhouseService from '../services/clickhouseService';

const TableMapping = ({ config }) => {
  const [mappings, setMappings] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [editingMapping, setEditingMapping] = useState(null);
  const [formData, setFormData] = useState({
    name: '',
    sourceFile: '',
    targetTable: '',
    columnMappings: []
  });

  useEffect(() => {
    if (config) {
      loadMappings();
    }
  }, [config]);

  const loadMappings = async () => {
    setLoading(true);
    try {
      const response = await clickhouseService.getMappings(config);
      setMappings(response);
    } catch (error) {
      setError('Failed to load mappings: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    setSuccess('');

    try {
      if (editingMapping) {
        await clickhouseService.saveMapping(config, {
          ...formData,
          id: editingMapping.id
        });
        setSuccess('Mapping updated successfully!');
      } else {
        await clickhouseService.saveMapping(config, formData);
        setSuccess('Mapping created successfully!');
      }
      loadMappings();
      resetForm();
    } catch (error) {
      setError('Failed to save mapping: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (mappingId) => {
    if (!window.confirm('Are you sure you want to delete this mapping?')) {
      return;
    }

    setLoading(true);
    try {
      await clickhouseService.deleteMapping(config, mappingId);
      setSuccess('Mapping deleted successfully!');
      loadMappings();
    } catch (error) {
      setError('Failed to delete mapping: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleEdit = (mapping) => {
    setEditingMapping(mapping);
    setFormData({
      name: mapping.name,
      sourceFile: mapping.sourceFile,
      targetTable: mapping.targetTable,
      columnMappings: mapping.columnMappings
    });
  };

  const resetForm = () => {
    setEditingMapping(null);
    setFormData({
      name: '',
      sourceFile: '',
      targetTable: '',
      columnMappings: []
    });
  };

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          Table Mappings
        </Typography>

        <form onSubmit={handleSubmit}>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Mapping Name"
                name="name"
                value={formData.name}
                onChange={handleChange}
                required
              />
            </Grid>
            <Grid item xs={12} sm={6}>
              <TextField
                fullWidth
                label="Source File"
                name="sourceFile"
                value={formData.sourceFile}
                onChange={handleChange}
                required
              />
            </Grid>
            <Grid item xs={12}>
              <TextField
                fullWidth
                label="Target Table"
                name="targetTable"
                value={formData.targetTable}
                onChange={handleChange}
                required
              />
            </Grid>
          </Grid>

          <Box mt={2}>
            {error && (
              <Alert severity="error" sx={{ mb: 2 }}>
                {error}
              </Alert>
            )}
            {success && (
              <Alert severity="success" sx={{ mb: 2 }}>
                {success}
              </Alert>
            )}
            <Button
              type="submit"
              variant="contained"
              color="primary"
              disabled={loading}
            >
              {loading ? <CircularProgress size={24} /> : (editingMapping ? 'Update Mapping' : 'Create Mapping')}
            </Button>
            {editingMapping && (
              <Button
                variant="outlined"
                onClick={resetForm}
                sx={{ ml: 2 }}
              >
                Cancel
              </Button>
            )}
          </Box>
        </form>

        <Box mt={4}>
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Name</TableCell>
                  <TableCell>Source File</TableCell>
                  <TableCell>Target Table</TableCell>
                  <TableCell>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {mappings.map((mapping) => (
                  <TableRow key={mapping.id}>
                    <TableCell>{mapping.name}</TableCell>
                    <TableCell>{mapping.sourceFile}</TableCell>
                    <TableCell>{mapping.targetTable}</TableCell>
                    <TableCell>
                      <Tooltip title="Edit">
                        <IconButton onClick={() => handleEdit(mapping)}>
                          <EditIcon />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete">
                        <IconButton onClick={() => handleDelete(mapping.id)}>
                          <DeleteIcon />
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      </CardContent>
    </Card>
  );
};

export default TableMapping; 