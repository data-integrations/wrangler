import React, { useState } from 'react';
import {
  Box,
  Typography,
  TextField,
  Button,
  FormControlLabel,
  Switch,
  Grid,
} from '@mui/material';
import { useDropzone } from 'react-dropzone';
import { toast } from 'react-toastify';

function ConnectionForm({ sourceType, onSubmit }) {
  const [formData, setFormData] = useState({
    host: '',
    port: '',
    database: '',
    user: '',
    jwtToken: '',
    ssl: true,
    sslMode: 'STRICT',
  });

  const [file, setFile] = useState(null);

  const { getRootProps, getInputProps } = useDropzone({
    accept: {
      'text/csv': ['.csv'],
      'text/plain': ['.txt'],
    },
    maxFiles: 1,
    onDrop: (acceptedFiles) => {
      setFile(acceptedFiles[0]);
      toast.success('File uploaded successfully');
    },
  });

  const handleChange = (e) => {
    const { name, value, checked } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: name === 'ssl' ? checked : value,
    }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (sourceType === 'clickhouse') {
      onSubmit(formData);
    } else if (sourceType === 'flatfile') {
      if (!file) {
        toast.error('Please upload a file');
        return;
      }
      onSubmit({ file });
    }
  };

  return (
    <Box component="form" onSubmit={handleSubmit}>
      <Typography variant="h6" gutterBottom>
        {sourceType === 'clickhouse' ? 'ClickHouse Connection' : 'File Upload'}
      </Typography>

      {sourceType === 'clickhouse' ? (
        <Grid container spacing={3}>
          <Grid item xs={12} sm={6}>
            <TextField
              required
              fullWidth
              label="Host"
              name="host"
              value={formData.host}
              onChange={handleChange}
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              required
              fullWidth
              label="Port"
              name="port"
              type="number"
              value={formData.port}
              onChange={handleChange}
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
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              required
              fullWidth
              label="User"
              name="user"
              value={formData.user}
              onChange={handleChange}
            />
          </Grid>
          <Grid item xs={12} sm={6}>
            <TextField
              required
              fullWidth
              label="JWT Token"
              name="jwtToken"
              type="password"
              value={formData.jwtToken}
              onChange={handleChange}
            />
          </Grid>
          <Grid item xs={12}>
            <FormControlLabel
              control={
                <Switch
                  checked={formData.ssl}
                  onChange={handleChange}
                  name="ssl"
                />
              }
              label="Use SSL"
            />
          </Grid>
        </Grid>
      ) : (
        <Box
          {...getRootProps()}
          sx={{
            border: '2px dashed #ccc',
            borderRadius: 2,
            p: 3,
            textAlign: 'center',
            cursor: 'pointer',
            '&:hover': {
              borderColor: 'primary.main',
            },
          }}
        >
          <input {...getInputProps()} />
          <Typography>
            {file ? file.name : 'Drag and drop a file here, or click to select'}
          </Typography>
        </Box>
      )}

      <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
        <Button
          type="submit"
          variant="contained"
          disabled={sourceType === 'flatfile' && !file}
        >
          Connect
        </Button>
      </Box>
    </Box>
  );
}

export default ConnectionForm; 