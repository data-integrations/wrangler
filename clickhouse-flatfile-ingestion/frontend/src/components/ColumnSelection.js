import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  List,
  ListItem,
  Checkbox,
  ListItemText,
  ListItemIcon,
  Button,
  CircularProgress,
  Alert,
  FormControlLabel,
} from '@mui/material';
import axios from 'axios';
import { toast } from 'react-toastify';

function ColumnSelection({ sourceType, connectionConfig, selectedTable, onSelect }) {
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchColumns = async () => {
      try {
        setLoading(true);
        setError(null);

        if (sourceType === 'clickhouse') {
          const response = await axios.get(`/api/ingestion/tables/${selectedTable}/columns`, {
            data: connectionConfig,
          });
          setColumns(response.data);
        } else if (sourceType === 'flatfile') {
          const formData = new FormData();
          formData.append('file', connectionConfig.file);
          formData.append('delimiter', ',');
          
          const response = await axios.post('/api/ingestion/file/columns', formData);
          setColumns(response.data);
        }
      } catch (err) {
        setError(err.response?.data || 'Failed to fetch columns');
        toast.error('Failed to fetch columns');
      } finally {
        setLoading(false);
      }
    };

    fetchColumns();
  }, [sourceType, connectionConfig, selectedTable]);

  const handleToggle = (column) => {
    setSelectedColumns((prev) => {
      const isSelected = prev.includes(column);
      if (isSelected) {
        return prev.filter((c) => c !== column);
      } else {
        return [...prev, column];
      }
    });
  };

  const handleSelectAll = (event) => {
    if (event.target.checked) {
      setSelectedColumns(columns);
    } else {
      setSelectedColumns([]);
    }
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ mt: 2 }}>
        {error}
      </Alert>
    );
  }

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Select Columns
      </Typography>
      
      <FormControlLabel
        control={
          <Checkbox
            checked={selectedColumns.length === columns.length}
            indeterminate={selectedColumns.length > 0 && selectedColumns.length < columns.length}
            onChange={handleSelectAll}
          />
        }
        label="Select All"
      />

      <List>
        {columns.map((column) => (
          <ListItem key={column} disablePadding>
            <ListItemIcon>
              <Checkbox
                edge="start"
                checked={selectedColumns.includes(column)}
                onChange={() => handleToggle(column)}
              />
            </ListItemIcon>
            <ListItemText primary={column} />
          </ListItem>
        ))}
      </List>

      <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
        <Button
          variant="contained"
          onClick={() => onSelect(selectedColumns)}
          disabled={selectedColumns.length === 0}
        >
          Confirm Selection
        </Button>
      </Box>
    </Box>
  );
}

export default ColumnSelection; 