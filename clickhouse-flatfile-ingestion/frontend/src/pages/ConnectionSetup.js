import React, { useState } from 'react';
import { 
  Typography, 
  Box, 
  TextField, 
  Button, 
  Card, 
  CardContent, 
  Grid,
  Alert,
  Snackbar
} from '@mui/material';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';

const ConnectionSetup = () => {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    host: '',
    port: '8123',
    database: '',
    username: '',
    password: '',
    secure: false
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState(false);
  const [openSnackbar, setOpenSnackbar] = useState(false);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData({
      ...formData,
      [name]: type === 'checkbox' ? checked : value
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    
    try {
      // In a real application, this would be an API call to your backend
      // const response = await axios.post('/api/connections', formData);
      
      // Simulating API call for demonstration
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      setSuccess(true);
      setOpenSnackbar(true);
      
      // Navigate to table mapping after successful connection
      setTimeout(() => {
        navigate('/table-mapping');
      }, 1500);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to establish connection');
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
        ClickHouse Connection Setup
      </Typography>
      <Typography variant="subtitle1" color="text.secondary" paragraph>
        Configure your connection to the ClickHouse database
      </Typography>
      
      <Card sx={{ mt: 3 }}>
        <CardContent>
          <form onSubmit={handleSubmit}>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <TextField
                  required
                  fullWidth
                  label="Host"
                  name="host"
                  value={formData.host}
                  onChange={handleChange}
                  placeholder="e.g., localhost or 192.168.1.100"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  required
                  fullWidth
                  label="Port"
                  name="port"
                  value={formData.port}
                  onChange={handleChange}
                  placeholder="e.g., 8123"
                />
              </Grid>
              <Grid item xs={12}>
                <TextField
                  required
                  fullWidth
                  label="Database"
                  name="database"
                  value={formData.database}
                  onChange={handleChange}
                  placeholder="e.g., default"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  required
                  fullWidth
                  label="Username"
                  name="username"
                  value={formData.username}
                  onChange={handleChange}
                  placeholder="e.g., default"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  required
                  fullWidth
                  label="Password"
                  name="password"
                  type="password"
                  value={formData.password}
                  onChange={handleChange}
                />
              </Grid>
              <Grid item xs={12}>
                <Button
                  type="submit"
                  variant="contained"
                  color="primary"
                  size="large"
                  disabled={loading}
                  sx={{ mt: 2 }}
                >
                  {loading ? 'Connecting...' : 'Test & Save Connection'}
                </Button>
              </Grid>
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
          Connection established successfully!
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default ConnectionSetup; 