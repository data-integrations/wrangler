import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8080/api';

const ingestionService = {
  /**
   * Get the current status of an ingestion job
   * @param {string} mappingId - The ID of the mapping
   * @returns {Promise<Object>} The status object containing state, progress, and recent jobs
   */
  async getStatus(mappingId) {
    try {
      const response = await axios.get(`${API_URL}/ingestion/${mappingId}/status`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get ingestion status');
    }
  },

  /**
   * Start a new ingestion job
   * @param {string} mappingId - The ID of the mapping
   * @param {Object} config - Optional configuration for the ingestion job
   * @returns {Promise<Object>} The created job details
   */
  async startIngestion(mappingId, config = {}) {
    try {
      const response = await axios.post(`${API_URL}/ingestion/${mappingId}/start`, config);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to start ingestion');
    }
  },

  /**
   * Stop a running ingestion job
   * @param {string} mappingId - The ID of the mapping
   * @returns {Promise<Object>} The updated job status
   */
  async stopIngestion(mappingId) {
    try {
      const response = await axios.post(`${API_URL}/ingestion/${mappingId}/stop`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to stop ingestion');
    }
  },

  /**
   * Get the configuration for an ingestion job
   * @param {string} mappingId - The ID of the mapping
   * @returns {Promise<Object>} The ingestion configuration
   */
  async getConfig(mappingId) {
    try {
      const response = await axios.get(`${API_URL}/ingestion/${mappingId}/config`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get ingestion configuration');
    }
  },

  /**
   * Update the configuration for an ingestion job
   * @param {string} mappingId - The ID of the mapping
   * @param {Object} config - The new configuration
   * @returns {Promise<Object>} The updated configuration
   */
  async updateConfig(mappingId, config) {
    try {
      const response = await axios.put(`${API_URL}/ingestion/${mappingId}/config`, config);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to update ingestion configuration');
    }
  },

  /**
   * Get the logs for an ingestion job
   * @param {string} mappingId - The ID of the mapping
   * @param {Object} options - Optional parameters for filtering logs
   * @returns {Promise<Array>} Array of log entries
   */
  async getLogs(mappingId, options = {}) {
    try {
      const response = await axios.get(`${API_URL}/ingestion/${mappingId}/logs`, { params: options });
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get ingestion logs');
    }
  },

  /**
   * Get statistics for an ingestion job
   * @param {string} mappingId - The ID of the mapping
   * @param {Object} options - Options for filtering statistics
   * @returns {Promise<Object>} The statistics object
   */
  async getStats(mappingId, options = {}) {
    try {
      const response = await axios.get(`${API_URL}/ingestion/${mappingId}/stats`, { params: options });
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get ingestion statistics');
    }
  },

  // ClickHouse to Flat File
  exportToFile: async (config) => {
    try {
      const response = await axios.post(`${API_URL}/ingestion/export`, config, {
        responseType: 'blob'
      });
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Export failed' };
    }
  },

  // Flat File to ClickHouse
  importFromFile: async (config) => {
    try {
      const formData = new FormData();
      formData.append('file', config.file);
      formData.append('config', JSON.stringify(config));
      
      const response = await axios.post(`${API_URL}/ingestion/import`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      });
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Import failed' };
    }
  },

  // Get schema information
  getSchema: async (sourceType, config) => {
    try {
      const response = await axios.post(`${API_URL}/ingestion/schema`, {
        sourceType,
        ...config
      });
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to fetch schema' };
    }
  },

  // Get data type mappings
  getDataTypeMappings: async () => {
    try {
      const response = await axios.get(`${API_URL}/ingestion/mappings`);
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to fetch data type mappings' };
    }
  },

  // Get record count
  getRecordCount: async (sourceType, config) => {
    try {
      const response = await axios.post(`${API_URL}/ingestion/count`, {
        sourceType,
        ...config
      });
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to fetch record count' };
    }
  },

  // Get ingestion progress
  getProgress: async (jobId) => {
    try {
      const response = await axios.get(`${API_URL}/ingestion/progress/${jobId}`);
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to fetch progress' };
    }
  },

  // Preview data with filtering and sorting
  getPreviewData: async (config) => {
    try {
      const response = await axios.post(`${API_URL}/ingestion/preview`, config);
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to fetch preview data' };
    }
  },

  // Export preview data
  exportPreviewData: async (config) => {
    try {
      const response = await axios.post(`${API_URL}/ingestion/preview/export`, config, {
        responseType: 'blob'
      });
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to export preview data' };
    }
  },

  // Preview join results
  previewJoin: async (config) => {
    try {
      const response = await axios.post(`${API_URL}/ingestion/preview/join`, config);
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to preview join results' };
    }
  },

  // Execute join operation
  executeJoin: async (config) => {
    try {
      const response = await axios.post(`${API_URL}/ingestion/join`, config);
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to execute join operation' };
    }
  },

  // Get join progress
  getJoinProgress: async (jobId) => {
    try {
      const response = await axios.get(`${API_URL}/ingestion/join/progress/${jobId}`);
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to fetch join progress' };
    }
  },

  // Get join statistics
  getJoinStats: async (jobId) => {
    try {
      const response = await axios.get(`${API_URL}/ingestion/join/stats/${jobId}`);
      return response.data;
    } catch (error) {
      throw error.response?.data || { message: 'Failed to fetch join statistics' };
    }
  }
};

export default ingestionService; 