import React, { useState, useRef } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Grid,
  Alert,
  CircularProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  IconButton,
  Tooltip,
  LinearProgress
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import VisibilityIcon from '@mui/icons-material/Visibility';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import fileService from '../services/fileService';

const FileUpload = ({ onFileSelected }) => {
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [previewData, setPreviewData] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const fileInputRef = useRef(null);

  const handleFileChange = (event) => {
    const selectedFiles = Array.from(event.target.files);
    if (selectedFiles.length > 0) {
      setFiles(selectedFiles);
    }
  };

  const handleUpload = async () => {
    if (files.length === 0) {
      setError('Please select a file to upload');
      return;
    }

    setLoading(true);
    setError('');
    setSuccess('');
    setUploadProgress(0);

    try {
      const formData = new FormData();
      formData.append('file', files[0]);

      const response = await fileService.upload(formData, (progressEvent) => {
        const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
        setUploadProgress(progress);
      });

      setSuccess('File uploaded successfully!');
      setFiles([]);
      if (onFileSelected) {
        onFileSelected(response);
      }
    } catch (error) {
      setError('Failed to upload file: ' + error.message);
    } finally {
      setLoading(false);
      setUploadProgress(0);
    }
  };

  const handlePreview = async (fileId) => {
    setPreviewLoading(true);
    setPreviewData(null);
    setError('');

    try {
      const response = await fileService.getPreview(fileId);
      setPreviewData(response);
    } catch (error) {
      setError('Failed to load preview: ' + error.message);
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleDelete = async (fileId) => {
    if (!window.confirm('Are you sure you want to delete this file?')) {
      return;
    }

    setLoading(true);
    try {
      await fileService.delete(fileId);
      setSuccess('File deleted successfully!');
      if (previewData && previewData.id === fileId) {
        setPreviewData(null);
      }
    } catch (error) {
      setError('Failed to delete file: ' + error.message);
    } finally {
      setLoading(false);
    }
  };

  const handleBrowseClick = () => {
    fileInputRef.current.click();
  };

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          File Upload
        </Typography>

        <Box mb={3}>
          <input
            type="file"
            ref={fileInputRef}
            onChange={handleFileChange}
            style={{ display: 'none' }}
            accept=".csv,.tsv,.txt,.json,.parquet"
          />
          <Button
            variant="outlined"
            startIcon={<CloudUploadIcon />}
            onClick={handleBrowseClick}
            sx={{ mr: 2 }}
          >
            Browse Files
          </Button>
          <Button
            variant="contained"
            color="primary"
            onClick={handleUpload}
            disabled={loading || files.length === 0}
          >
            {loading ? <CircularProgress size={24} /> : 'Upload'}
          </Button>
          {files.length > 0 && (
            <Typography variant="body2" sx={{ mt: 1 }}>
              Selected: {files[0].name} ({(files[0].size / 1024).toFixed(2)} KB)
            </Typography>
          )}
          {uploadProgress > 0 && uploadProgress < 100 && (
            <Box sx={{ width: '100%', mt: 2 }}>
              <LinearProgress variant="determinate" value={uploadProgress} />
              <Typography variant="body2" color="text.secondary" align="center">
                {uploadProgress}%
              </Typography>
            </Box>
          )}
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

        {previewData && (
          <Box mt={4}>
            <Typography variant="subtitle1" gutterBottom>
              File Preview: {previewData.filename}
            </Typography>
            {previewLoading ? (
              <Box display="flex" justifyContent="center" p={3}>
                <CircularProgress />
              </Box>
            ) : (
              <TableContainer component={Paper}>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      {previewData.headers.map((header, index) => (
                        <TableCell key={index}>{header}</TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {previewData.rows.map((row, rowIndex) => (
                      <TableRow key={rowIndex}>
                        {row.map((cell, cellIndex) => (
                          <TableCell key={cellIndex}>{cell}</TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Box>
        )}
      </CardContent>
    </Card>
  );
};

export default FileUpload; 