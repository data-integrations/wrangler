import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8080/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json'
  }
});

// File Upload Service
export const fileService = {
  upload: async (file, onProgress) => {
    const formData = new FormData();
    formData.append('file', file);

    return api.post('/files/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data'
      },
      onUploadProgress: (progressEvent) => {
        const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
        if (onProgress) {
          onProgress(progress);
        }
      }
    });
  },

  getPreview: async (fileId) => {
    return api.get(`/files/${fileId}/preview`);
  },

  delete: async (fileId) => {
    return api.delete(`/files/${fileId}`);
  }
};

// Table Mapping Service
export const mappingService = {
  getAvailableTables: async () => {
    return api.get('/tables');
  },

  getTableColumns: async (tableName) => {
    return api.get(`/tables/${tableName}/columns`);
  },

  saveMapping: async (mapping) => {
    return api.post('/mappings', mapping);
  },

  getMappings: async () => {
    return api.get('/mappings');
  },

  getMapping: async (mappingId) => {
    return api.get(`/mappings/${mappingId}`);
  },

  deleteMapping: async (mappingId) => {
    return api.delete(`/mappings/${mappingId}`);
  }
};

// Ingestion Service
export const ingestionService = {
  startIngestion: async (config) => {
    return api.post('/ingestion/start', config);
  },

  stopIngestion: async (jobId) => {
    return api.post(`/ingestion/${jobId}/stop`);
  },

  getStatus: async (jobId) => {
    return api.get(`/ingestion/${jobId}/status`);
  },

  getJobs: async (params) => {
    return api.get('/ingestion/jobs', { params });
  },

  getJobDetails: async (jobId) => {
    return api.get(`/ingestion/jobs/${jobId}`);
  },

  getLogs: async (jobId) => {
    return api.get(`/ingestion/jobs/${jobId}/logs`, {
      responseType: 'blob'
    });
  }
};

// Connection Service
export const connectionService = {
  testConnection: async (config) => {
    return api.post('/connection/test', config);
  },

  saveConnection: async (config) => {
    return api.post('/connection', config);
  },

  getConnection: async () => {
    return api.get('/connection');
  },

  updateConnection: async (config) => {
    return api.put('/connection', config);
  }
};

// Error Interceptor
api.interceptors.response.use(
  (response) => response,
  (error) => {
    const message = error.response?.data?.message || 'An unexpected error occurred';
    console.error('API Error:', message);
    return Promise.reject(error);
  }
);

export default api; 