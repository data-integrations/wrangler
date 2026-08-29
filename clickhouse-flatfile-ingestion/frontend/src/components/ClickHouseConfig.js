import React, { useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  TextField,
  Button,
  Grid,
  Typography,
  Alert,
  CircularProgress
} from '@mui/material';
import clickhouseService from '../services/clickhouseService';

const ClickHouseConfig = ({ onConfigValid }) => {
  const [config, setConfig] = useState({
    host: '',
    port: 8123,
    database: '',
    username: '',
    password: ''
  });

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const handleChange = (e) => {
    const { name, value } = e.target;
    setConfig(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleTestConnection = async () => {
    setLoading(true);
    setError('');
    setSuccess('');

    try {
      await clickhouseService.testConnection(config);
      setSuccess('Connection successful!');
      onConfigValid(config);
    } catch (error) {
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          ClickHouse Configuration
        </Typography>
        
        <Grid container spacing={2}>
          <Grid item xs={12} sm={6}>
            <TextField
              fullWidth
              label="Host"
              name="host"
              value={config.host}
              onChange={handleChange}
              required
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              fullWidth
              label="Port"
              name="port"
              type="number"
              value={config.port}
              onChange={handleChange}
              required
            />
          </Grid>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Database"
              name="database"
              value={config.database}
              onChange={handleChange}
              required
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              fullWidth
              label="Username"
              name="username"
              value={config.username}
              onChange={handleChange}
              required
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              fullWidth
              label="Password"
              name="password"
              type="password"
              value={config.password}
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
            variant="contained"
            color="primary"
            onClick={handleTestConnection}
            disabled={loading}
          >
            {loading ? <CircularProgress size={24} /> : 'Test Connection'}
          </Button>
        </Box>
      </CardContent>
    </Card>
  );
};

export default ClickHouseConfig; 