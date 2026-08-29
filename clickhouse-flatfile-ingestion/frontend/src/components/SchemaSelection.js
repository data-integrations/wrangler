import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  CircularProgress,
  Alert,
} from '@mui/material';
import axios from 'axios';
import { toast } from 'react-toastify';

function SchemaSelection({ sourceType, connectionConfig, onSelect }) {
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchTables = async () => {
      try {
        setLoading(true);
        setError(null);

        if (sourceType === 'clickhouse') {
          const response = await axios.post('/api/ingestion/connect', connectionConfig);
          setTables(response.data);
        } else if (sourceType === 'flatfile') {
          // For flat files, we'll use the file name as the table name
          setTables([{ name: connectionConfig.file.name }]);
        }
      } catch (err) {
        setError(err.response?.data || 'Failed to fetch tables');
        toast.error('Failed to fetch tables');
      } finally {
        setLoading(false);
      }
    };

    fetchTables();
  }, [sourceType, connectionConfig]);

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
        Select {sourceType === 'clickhouse' ? 'Table' : 'File'}
      </Typography>
      <List>
        {tables.map((table) => (
          <ListItem key={table.name} disablePadding>
            <ListItemButton onClick={() => onSelect(table.name)}>
              <ListItemText
                primary={table.name}
                secondary={`Click to select ${table.name}`}
              />
            </ListItemButton>
          </ListItem>
        ))}
      </List>
    </Box>
  );
}

export default SchemaSelection; 