import React from 'react';
import { 
  Typography, 
  Grid, 
  Card, 
  CardContent, 
  CardActions, 
  Button,
  Box
} from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import StorageIcon from '@mui/icons-material/Storage';
import TableChartIcon from '@mui/icons-material/TableChart';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import AssessmentIcon from '@mui/icons-material/Assessment';

const Dashboard = () => {
  return (
    <Box>
      <Typography variant="h4" component="h1" gutterBottom>
        ClickHouse Flat File Ingestion
      </Typography>
      <Typography variant="subtitle1" color="text.secondary" paragraph>
        A tool for ingesting flat files into ClickHouse databases with mapping capabilities
      </Typography>
      
      <Grid container spacing={3} sx={{ mt: 2 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Box sx={{ display: 'flex', justifyContent: 'center', mb: 2 }}>
                <StorageIcon fontSize="large" color="primary" />
              </Box>
              <Typography gutterBottom variant="h5" component="h2" align="center">
                Connection Setup
              </Typography>
              <Typography align="center">
                Configure your ClickHouse database connection settings
              </Typography>
            </CardContent>
            <CardActions sx={{ justifyContent: 'center', pb: 2 }}>
              <Button 
                component={RouterLink} 
                to="/connection" 
                variant="contained" 
                color="primary"
              >
                Setup Connection
              </Button>
            </CardActions>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Box sx={{ display: 'flex', justifyContent: 'center', mb: 2 }}>
                <TableChartIcon fontSize="large" color="primary" />
              </Box>
              <Typography gutterBottom variant="h5" component="h2" align="center">
                Table Mapping
              </Typography>
              <Typography align="center">
                Map your flat file columns to ClickHouse table columns
              </Typography>
            </CardContent>
            <CardActions sx={{ justifyContent: 'center', pb: 2 }}>
              <Button 
                component={RouterLink} 
                to="/table-mapping" 
                variant="contained" 
                color="primary"
              >
                Configure Mapping
              </Button>
            </CardActions>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Box sx={{ display: 'flex', justifyContent: 'center', mb: 2 }}>
                <CloudUploadIcon fontSize="large" color="primary" />
              </Box>
              <Typography gutterBottom variant="h5" component="h2" align="center">
                Data Ingestion
              </Typography>
              <Typography align="center">
                Upload and process your flat files for ingestion
              </Typography>
            </CardContent>
            <CardActions sx={{ justifyContent: 'center', pb: 2 }}>
              <Button 
                component={RouterLink} 
                to="/data-ingestion" 
                variant="contained" 
                color="primary"
              >
                Start Ingestion
              </Button>
            </CardActions>
          </Card>
        </Grid>
        
        <Grid item xs={12} sm={6} md={3}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Box sx={{ display: 'flex', justifyContent: 'center', mb: 2 }}>
                <AssessmentIcon fontSize="large" color="primary" />
              </Box>
              <Typography gutterBottom variant="h5" component="h2" align="center">
                Ingestion Status
              </Typography>
              <Typography align="center">
                Monitor the status of your data ingestion jobs
              </Typography>
            </CardContent>
            <CardActions sx={{ justifyContent: 'center', pb: 2 }}>
              <Button 
                component={RouterLink} 
                to="/ingestion-status" 
                variant="contained" 
                color="primary"
              >
                View Status
              </Button>
            </CardActions>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Dashboard; 