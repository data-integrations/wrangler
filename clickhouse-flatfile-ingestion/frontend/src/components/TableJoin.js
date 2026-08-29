import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  TextField,
  Button,
  IconButton,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  CircularProgress,
  Alert
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import ingestionService from '../services/ingestionService';
import { toast } from 'react-toastify';

function TableJoin({ sourceType, connectionConfig, onJoinComplete }) {
  const [tables, setTables] = useState([]);
  const [selectedTables, setSelectedTables] = useState([]);
  const [joinConditions, setJoinConditions] = useState([]);
  const [previewData, setPreviewData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchTables();
  }, [sourceType, connectionConfig]);

  const fetchTables = async () => {
    try {
      setLoading(true);
      const response = await ingestionService.getSchema(sourceType, connectionConfig);
      setTables(response.tables || []);
    } catch (error) {
      setError(error.message);
      toast.error('Failed to fetch tables: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleAddTable = () => {
    if (selectedTables.length < tables.length) {
      setSelectedTables([...selectedTables, { table: '', columns: [] }]);
    }
  };

  const handleRemoveTable = (index) => {
    const newSelectedTables = selectedTables.filter((_, i) => i !== index);
    setSelectedTables(newSelectedTables);
    setJoinConditions(joinConditions.filter((_, i) => i !== index));
  };

  const handleTableChange = (index, table) => {
    const newSelectedTables = [...selectedTables];
    newSelectedTables[index] = { ...newSelectedTables[index], table };
    setSelectedTables(newSelectedTables);
  };

  const handleAddJoinCondition = () => {
    if (selectedTables.length >= 2) {
      setJoinConditions([...joinConditions, { leftTable: '', leftColumn: '', rightTable: '', rightColumn: '', type: 'INNER' }]);
    }
  };

  const handleJoinConditionChange = (index, field, value) => {
    const newConditions = [...joinConditions];
    newConditions[index] = { ...newConditions[index], [field]: value };
    setJoinConditions(newConditions);
  };

  const handlePreview = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await ingestionService.previewJoin({
        sourceType,
        connectionConfig,
        tables: selectedTables,
        joinConditions
      });
      setPreviewData(response.data);
      toast.success('Preview generated successfully');
    } catch (error) {
      setError(error.message);
      toast.error('Failed to generate preview: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleComplete = () => {
    onJoinComplete({
      tables: selectedTables,
      joinConditions
    });
  };

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Multi-Table Join Configuration
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="subtitle1" gutterBottom>
          Selected Tables
        </Typography>
        {selectedTables.map((table, index) => (
          <Grid container spacing={2} key={index} sx={{ mb: 2 }}>
            <Grid item xs={11}>
              <FormControl fullWidth>
                <InputLabel>Select Table</InputLabel>
                <Select
                  value={table.table}
                  onChange={(e) => handleTableChange(index, e.target.value)}
                  label="Select Table"
                >
                  {tables.map((t) => (
                    <MenuItem key={t.name} value={t.name}>
                      {t.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={1}>
              <IconButton onClick={() => handleRemoveTable(index)} color="error">
                <DeleteIcon />
              </IconButton>
            </Grid>
          </Grid>
        ))}
        <Button
          startIcon={<AddIcon />}
          onClick={handleAddTable}
          disabled={selectedTables.length >= tables.length}
        >
          Add Table
        </Button>
      </Paper>

      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="subtitle1" gutterBottom>
          Join Conditions
        </Typography>
        {joinConditions.map((condition, index) => (
          <Grid container spacing={2} key={index} sx={{ mb: 2 }}>
            <Grid item xs={2}>
              <FormControl fullWidth>
                <InputLabel>Left Table</InputLabel>
                <Select
                  value={condition.leftTable}
                  onChange={(e) => handleJoinConditionChange(index, 'leftTable', e.target.value)}
                  label="Left Table"
                >
                  {selectedTables.map((t) => (
                    <MenuItem key={t.table} value={t.table}>
                      {t.table}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={3}>
              <TextField
                fullWidth
                label="Left Column"
                value={condition.leftColumn}
                onChange={(e) => handleJoinConditionChange(index, 'leftColumn', e.target.value)}
              />
            </Grid>
            <Grid item xs={2}>
              <FormControl fullWidth>
                <InputLabel>Join Type</InputLabel>
                <Select
                  value={condition.type}
                  onChange={(e) => handleJoinConditionChange(index, 'type', e.target.value)}
                  label="Join Type"
                >
                  <MenuItem value="INNER">Inner Join</MenuItem>
                  <MenuItem value="LEFT">Left Join</MenuItem>
                  <MenuItem value="RIGHT">Right Join</MenuItem>
                  <MenuItem value="FULL">Full Join</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={3}>
              <TextField
                fullWidth
                label="Right Column"
                value={condition.rightColumn}
                onChange={(e) => handleJoinConditionChange(index, 'rightColumn', e.target.value)}
              />
            </Grid>
            <Grid item xs={2}>
              <FormControl fullWidth>
                <InputLabel>Right Table</InputLabel>
                <Select
                  value={condition.rightTable}
                  onChange={(e) => handleJoinConditionChange(index, 'rightTable', e.target.value)}
                  label="Right Table"
                >
                  {selectedTables.map((t) => (
                    <MenuItem key={t.table} value={t.table}>
                      {t.table}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
          </Grid>
        ))}
        <Button
          startIcon={<AddIcon />}
          onClick={handleAddJoinCondition}
          disabled={selectedTables.length < 2}
        >
          Add Join Condition
        </Button>
      </Paper>

      {previewData && (
        <Paper sx={{ p: 3, mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Preview Data
          </Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  {Object.keys(previewData[0] || {}).map((column) => (
                    <TableCell key={column}>{column}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {previewData.slice(0, 5).map((row, index) => (
                  <TableRow key={index}>
                    {Object.values(row).map((value, i) => (
                      <TableCell key={i}>{value}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Paper>
      )}

      <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 3 }}>
        <Button
          variant="outlined"
          onClick={handlePreview}
          disabled={loading || selectedTables.length < 2}
        >
          {loading ? <CircularProgress size={24} /> : 'Preview'}
        </Button>
        <Button
          variant="contained"
          onClick={handleComplete}
          disabled={loading || selectedTables.length < 2 || joinConditions.length === 0}
        >
          Complete
        </Button>
      </Box>
    </Box>
  );
}

export default TableJoin; 