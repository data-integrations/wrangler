import axios from 'axios';
import api from './api';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8080/api';

/**
 * Service for interacting with ClickHouse database
 */
const clickhouseService = {
  /**
   * Test connection to ClickHouse database
   * @param {Object} config - Connection configuration
   * @returns {Promise} - Connection test result
   */
  testConnection: async (config) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/test-connection`, null, {
        headers: {
          'X-ClickHouse-Host': config.host,
          'X-ClickHouse-Port': config.port,
          'X-ClickHouse-Database': config.database,
          'X-ClickHouse-Username': config.username,
          'X-ClickHouse-Password': config.password
        }
      });
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to test connection');
    }
  },

  /**
   * Get list of available databases
   * @param {Object} config - Connection configuration
   * @returns {Promise} - List of databases
   */
  getDatabases: async (config) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/clickhouse/databases`, config);
      return response.data;
    } catch (error) {
      throw new Error('Failed to fetch databases');
    }
  },

  /**
   * Get list of tables in a database
   * @param {Object} config - Connection configuration
   * @param {string} database - Database name
   * @returns {Promise} - List of tables
   */
  getTables: async (config) => {
    try {
      const response = await axios.get(`${API_BASE_URL}/tables`, {
        headers: {
          'X-ClickHouse-Host': config.host,
          'X-ClickHouse-Port': config.port,
          'X-ClickHouse-Database': config.database,
          'X-ClickHouse-Username': config.username,
          'X-ClickHouse-Password': config.password
        }
      });
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to fetch tables');
    }
  },

  /**
   * Get schema for a specific table
   * @param {Object} config - Connection configuration
   * @param {string} table - Table name
   * @returns {Promise} - Table schema
   */
  getTableSchema: async (config, table) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/clickhouse/schema`, {
        ...config,
        table
      });
      return response.data;
    } catch (error) {
      throw new Error('Failed to fetch table schema');
    }
  },

  /**
   * Preview data from a table
   * @param {Object} config - Connection configuration
   * @param {string} table - Table name
   * @param {number} limit - Number of rows to preview
   * @returns {Promise} - Preview data
   */
  previewData: async (config, table, limit = 100) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/clickhouse/preview`, {
        ...config,
        table,
        limit
      });
      return response.data;
    } catch (error) {
      throw new Error('Failed to preview data');
    }
  },

  /**
   * Execute a custom query
   * @param {Object} config - Connection configuration
   * @param {string} query - SQL query
   * @returns {Promise} - Query results
   */
  executeQuery: async (config, query) => {
    return api.post('/clickhouse/execute', { ...config, query });
  },

  /**
   * Export data from ClickHouse to a file
   * @param {Object} config - Connection configuration
   * @param {Object} exportConfig - Export configuration
   * @returns {Promise} - Export job status
   */
  exportData: async (config, exportConfig) => {
    return api.post('/clickhouse/export', { ...config, ...exportConfig });
  },

  /**
   * Get status of an export job
   * @param {string} jobId - Export job ID
   * @returns {Promise} - Job status
   */
  getExportStatus: async (jobId) => {
    return api.get(`/clickhouse/export/${jobId}/status`);
  },

  /**
   * Cancel an export job
   * @param {string} jobId - Export job ID
   * @returns {Promise} - Cancellation result
   */
  cancelExport: async (jobId) => {
    return api.post(`/clickhouse/export/${jobId}/cancel`);
  },

  // Save table mapping
  saveMapping: async (config, mapping) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/mappings`, mapping, {
        headers: {
          'X-ClickHouse-Host': config.host,
          'X-ClickHouse-Port': config.port,
          'X-ClickHouse-Database': config.database,
          'X-ClickHouse-Username': config.username,
          'X-ClickHouse-Password': config.password
        }
      });
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to save mapping');
    }
  },

  // Get saved mappings
  getMappings: async (config) => {
    try {
      const response = await axios.get(`${API_BASE_URL}/mappings`, {
        headers: {
          'X-ClickHouse-Host': config.host,
          'X-ClickHouse-Port': config.port,
          'X-ClickHouse-Database': config.database,
          'X-ClickHouse-Username': config.username,
          'X-ClickHouse-Password': config.password
        }
      });
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to fetch mappings');
    }
  },

  // Delete mapping
  deleteMapping: async (config, mappingId) => {
    try {
      const response = await axios.delete(`${API_BASE_URL}/mappings/${mappingId}`, {
        headers: {
          'X-ClickHouse-Host': config.host,
          'X-ClickHouse-Port': config.port,
          'X-ClickHouse-Database': config.database,
          'X-ClickHouse-Username': config.username,
          'X-ClickHouse-Password': config.password
        }
      });
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to delete mapping');
    }
  }
};

export default clickhouseService; 