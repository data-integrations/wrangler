import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  Grid,
  FormControlLabel,
  Switch,
  Alert,
  CircularProgress,
  Divider,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  InputAdornment,
  Tooltip,
  IconButton
} from '@mui/material';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';
import SaveIcon from '@mui/icons-material/Save';
import RefreshIcon from '@mui/icons-material/Refresh';
import ingestionService from '../services/ingestionService';

const IngestionConfig = ({ mappingId }) => {
  const [config, setConfig] = useState({
    batchSize: 1000,
    maxRetries: 3,
    timeout: 30000,
    validateData: true,
    skipInvalidRecords: true,
    compressionEnabled: false,
    compressionLevel: 'medium',
    deduplicationEnabled: false,
    deduplicationWindow: 24,
    errorThreshold: 100,
    errorAction: 'stop',
    loggingLevel: 'info'
  });
  
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  useEffect(() => {
    if (mappingId) {
      loadConfig();
    }
  }, [mappingId]);

  const loadConfig = async () => {
    if (!mappingId) return;
    
    try {
      setLoading(true);
      const response = await ingestionService.getConfig(mappingId);
      setConfig(response);
      setError('');
    } catch (error) {
      setError('Failed to load configuration: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (event) => {
    const { name, value, checked } = event.target;
    setConfig({
      ...config,
      [name]: event.target.type === 'checkbox' ? checked : value
    });
  };

  const handleNumberChange = (event) => {
    const { name, value } = event.target;
    setConfig({
      ...config,
      [name]: value === '' ? '' : Number(value)
    });
  };

  const handleSave = async () => {
    if (!mappingId) return;
    
    try {
      setSaving(true);
      await ingestionService.updateConfig(mappingId, config);
      setSuccess('Configuration saved successfully');
      setError('');
      
      // Clear success message after 3 seconds
      setTimeout(() => {
        setSuccess('');
      }, 3000);
    } catch (error) {
      setError('Failed to save configuration: ' + error.message);
    } finally {
      setSaving(false);
    }
  };

  const handleReset = () => {
    loadConfig();
  };

  const getTooltipText = (field) => {
    const tooltips = {
      batchSize: 'Number of records to process in a single batch',
      maxRetries: 'Maximum number of retry attempts for failed operations',
      timeout: 'Timeout in milliseconds for operations',
      validateData: 'Validate data before ingestion',
      skipInvalidRecords: 'Skip records that fail validation',
      compressionEnabled: 'Enable compression for data transfer',
      compressionLevel: 'Level of compression to apply',
      deduplicationEnabled: 'Enable deduplication of records',
      deduplicationWindow: 'Time window in hours for deduplication',
      errorThreshold: 'Maximum number of errors before taking action',
      errorAction: 'Action to take when error threshold is reached',
      loggingLevel: 'Level of detail in logs'
    };
    
    return tooltips[field] || '';
  };

  if (loading) {
    return (
      <Card>
        <CardContent>
          <Box display="flex" justifyContent="center" p={3}>
            <CircularProgress />
          </Box>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardContent>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h6">
            Ingestion Configuration
          </Typography>
          <Box>
            <Tooltip title="Refresh">
              <IconButton onClick={handleReset} disabled={saving}>
                <RefreshIcon />
              </IconButton>
            </Tooltip>
            <Button
              variant="contained"
              color="primary"
              startIcon={<SaveIcon />}
              onClick={handleSave}
              disabled={saving}
            >
              {saving ? <CircularProgress size={24} /> : 'Save'}
            </Button>
          </Box>
        </Box>

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

        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Typography variant="subtitle1" gutterBottom>
              Performance Settings
            </Typography>
            
            <TextField
              fullWidth
              label="Batch Size"
              name="batchSize"
              type="number"
              value={config.batchSize}
              onChange={handleNumberChange}
              margin="normal"
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <Tooltip title={getTooltipText('batchSize')}>
                      <IconButton size="small">
                        <HelpOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </InputAdornment>
                )
              }}
            />
            
            <TextField
              fullWidth
              label="Max Retries"
              name="maxRetries"
              type="number"
              value={config.maxRetries}
              onChange={handleNumberChange}
              margin="normal"
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <Tooltip title={getTooltipText('maxRetries')}>
                      <IconButton size="small">
                        <HelpOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </InputAdornment>
                )
              }}
            />
            
            <TextField
              fullWidth
              label="Timeout (ms)"
              name="timeout"
              type="number"
              value={config.timeout}
              onChange={handleNumberChange}
              margin="normal"
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <Tooltip title={getTooltipText('timeout')}>
                      <IconButton size="small">
                        <HelpOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </InputAdornment>
                )
              }}
            />
            
            <FormControl fullWidth margin="normal">
              <InputLabel id="logging-level-label">Logging Level</InputLabel>
              <Select
                labelId="logging-level-label"
                name="loggingLevel"
                value={config.loggingLevel}
                label="Logging Level"
                onChange={handleChange}
                endAdornment={
                  <InputAdornment position="end">
                    <Tooltip title={getTooltipText('loggingLevel')}>
                      <IconButton size="small">
                        <HelpOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </InputAdornment>
                }
              >
                <MenuItem value="debug">Debug</MenuItem>
                <MenuItem value="info">Info</MenuItem>
                <MenuItem value="warn">Warning</MenuItem>
                <MenuItem value="error">Error</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <Typography variant="subtitle1" gutterBottom>
              Data Processing
            </Typography>
            
            <FormControlLabel
              control={
                <Switch
                  checked={config.validateData}
                  onChange={handleChange}
                  name="validateData"
                />
              }
              label={
                <Box display="flex" alignItems="center">
                  <Typography>Validate Data</Typography>
                  <Tooltip title={getTooltipText('validateData')}>
                    <IconButton size="small" sx={{ ml: 1 }}>
                      <HelpOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Box>
              }
            />
            
            <FormControlLabel
              control={
                <Switch
                  checked={config.skipInvalidRecords}
                  onChange={handleChange}
                  name="skipInvalidRecords"
                />
              }
              label={
                <Box display="flex" alignItems="center">
                  <Typography>Skip Invalid Records</Typography>
                  <Tooltip title={getTooltipText('skipInvalidRecords')}>
                    <IconButton size="small" sx={{ ml: 1 }}>
                      <HelpOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Box>
              }
            />
            
            <Divider sx={{ my: 2 }} />
            
            <Typography variant="subtitle1" gutterBottom>
              Compression
            </Typography>
            
            <FormControlLabel
              control={
                <Switch
                  checked={config.compressionEnabled}
                  onChange={handleChange}
                  name="compressionEnabled"
                />
              }
              label={
                <Box display="flex" alignItems="center">
                  <Typography>Enable Compression</Typography>
                  <Tooltip title={getTooltipText('compressionEnabled')}>
                    <IconButton size="small" sx={{ ml: 1 }}>
                      <HelpOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Box>
              }
            />
            
            {config.compressionEnabled && (
              <FormControl fullWidth margin="normal">
                <InputLabel id="compression-level-label">Compression Level</InputLabel>
                <Select
                  labelId="compression-level-label"
                  name="compressionLevel"
                  value={config.compressionLevel}
                  label="Compression Level"
                  onChange={handleChange}
                  endAdornment={
                    <InputAdornment position="end">
                      <Tooltip title={getTooltipText('compressionLevel')}>
                        <IconButton size="small">
                          <HelpOutlineIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </InputAdornment>
                  }
                >
                  <MenuItem value="low">Low</MenuItem>
                  <MenuItem value="medium">Medium</MenuItem>
                  <MenuItem value="high">High</MenuItem>
                </Select>
              </FormControl>
            )}
            
            <Divider sx={{ my: 2 }} />
            
            <Typography variant="subtitle1" gutterBottom>
              Deduplication
            </Typography>
            
            <FormControlLabel
              control={
                <Switch
                  checked={config.deduplicationEnabled}
                  onChange={handleChange}
                  name="deduplicationEnabled"
                />
              }
              label={
                <Box display="flex" alignItems="center">
                  <Typography>Enable Deduplication</Typography>
                  <Tooltip title={getTooltipText('deduplicationEnabled')}>
                    <IconButton size="small" sx={{ ml: 1 }}>
                      <HelpOutlineIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </Box>
              }
            />
            
            {config.deduplicationEnabled && (
              <TextField
                fullWidth
                label="Deduplication Window (hours)"
                name="deduplicationWindow"
                type="number"
                value={config.deduplicationWindow}
                onChange={handleNumberChange}
                margin="normal"
                InputProps={{
                  endAdornment: (
                    <InputAdornment position="end">
                      <Tooltip title={getTooltipText('deduplicationWindow')}>
                        <IconButton size="small">
                          <HelpOutlineIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </InputAdornment>
                  )
                }}
              />
            )}
            
            <Divider sx={{ my: 2 }} />
            
            <Typography variant="subtitle1" gutterBottom>
              Error Handling
            </Typography>
            
            <TextField
              fullWidth
              label="Error Threshold"
              name="errorThreshold"
              type="number"
              value={config.errorThreshold}
              onChange={handleNumberChange}
              margin="normal"
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <Tooltip title={getTooltipText('errorThreshold')}>
                      <IconButton size="small">
                        <HelpOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </InputAdornment>
                )
              }}
            />
            
            <FormControl fullWidth margin="normal">
              <InputLabel id="error-action-label">Error Action</InputLabel>
              <Select
                labelId="error-action-label"
                name="errorAction"
                value={config.errorAction}
                label="Error Action"
                onChange={handleChange}
                endAdornment={
                  <InputAdornment position="end">
                    <Tooltip title={getTooltipText('errorAction')}>
                      <IconButton size="small">
                        <HelpOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </InputAdornment>
                }
              >
                <MenuItem value="stop">Stop Ingestion</MenuItem>
                <MenuItem value="continue">Continue</MenuItem>
                <MenuItem value="alert">Alert Only</MenuItem>
              </Select>
            </FormControl>
          </Grid>
        </Grid>
      </CardContent>
    </Card>
  );
};

export default IngestionConfig; 