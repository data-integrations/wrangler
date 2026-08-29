import React from 'react';
import {
  Box,
  Typography,
  LinearProgress,
  Paper,
  Button,
  Grid,
  Card,
  CardContent
} from '@mui/material';
import { formatNumber } from '../utils/formatters';

function ProgressDisplay({ progress, isProcessing, onExecute, recordCount }) {
  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Data Ingestion Progress
      </Typography>

      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="subtitle1" gutterBottom>
                Record Count
              </Typography>
              <Typography variant="h4">
                {recordCount ? formatNumber(recordCount) : 'N/A'}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="subtitle1" gutterBottom>
                Progress
              </Typography>
              <Typography variant="h4">
                {progress}%
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Paper sx={{ p: 3, mt: 3 }}>
        <Box sx={{ width: '100%', mb: 2 }}>
          <Typography variant="body1" gutterBottom>
            {isProcessing ? 'Processing...' : 'Ready to start ingestion'}
          </Typography>
          <LinearProgress
            variant="determinate"
            value={progress}
            sx={{ height: 10, borderRadius: 5 }}
          />
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            {progress}% Complete
          </Typography>
        </Box>

        <Box sx={{ mt: 2 }}>
          <Typography variant="body1" gutterBottom>
            {isProcessing ? (
              'Please wait while the data is being processed...'
            ) : (
              'Click the button below to start the ingestion process.'
            )}
          </Typography>
        </Box>

        <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
          <Button
            variant="contained"
            onClick={onExecute}
            disabled={isProcessing}
          >
            {isProcessing ? 'Processing...' : 'Start Ingestion'}
          </Button>
        </Box>
      </Paper>
    </Box>
  );
}

export default ProgressDisplay; 