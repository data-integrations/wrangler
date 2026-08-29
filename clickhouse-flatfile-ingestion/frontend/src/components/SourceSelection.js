import React from 'react';
import {
  Box,
  Typography,
  Card,
  CardContent,
  CardActionArea,
  Grid,
} from '@mui/material';
import StorageIcon from '@mui/icons-material/Storage';
import DescriptionIcon from '@mui/icons-material/Description';

function SourceSelection({ onSelect }) {
  const sources = [
    {
      id: 'clickhouse',
      name: 'ClickHouse Database',
      description: 'Import data from ClickHouse database',
      icon: <StorageIcon sx={{ fontSize: 40 }} />,
    },
    {
      id: 'flatfile',
      name: 'Flat File',
      description: 'Import data from CSV or text file',
      icon: <DescriptionIcon sx={{ fontSize: 40 }} />,
    },
  ];

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Select Data Source
      </Typography>
      <Grid container spacing={3}>
        {sources.map((source) => (
          <Grid item xs={12} sm={6} key={source.id}>
            <Card>
              <CardActionArea onClick={() => onSelect(source.id)}>
                <CardContent>
                  <Box
                    sx={{
                      display: 'flex',
                      flexDirection: 'column',
                      alignItems: 'center',
                      textAlign: 'center',
                    }}
                  >
                    {source.icon}
                    <Typography variant="h6" sx={{ mt: 2 }}>
                      {source.name}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      {source.description}
                    </Typography>
                  </Box>
                </CardContent>
              </CardActionArea>
            </Card>
          </Grid>
        ))}
      </Grid>
    </Box>
  );
}

export default SourceSelection; 