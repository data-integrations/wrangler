import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8080/api';

/**
 * Service for handling table mapping operations between flat files and ClickHouse tables
 */
const mappingService = {
  /**
   * Get all available mappings
   * @returns {Promise<Array>} List of mapping objects
   */
  async getMappings() {
    try {
      const response = await axios.get(`${API_BASE_URL}/mappings`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get mappings');
    }
  },

  /**
   * Get a specific mapping by ID
   * @param {string} mappingId - The ID of the mapping
   * @returns {Promise<Object>} The mapping object
   */
  async getMapping(mappingId) {
    try {
      const response = await axios.get(`${API_BASE_URL}/mappings/${mappingId}`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get mapping');
    }
  },

  /**
   * Create a new mapping
   * @param {Object} mapping - The mapping configuration
   * @returns {Promise<Object>} The created mapping
   */
  async createMapping(mapping) {
    try {
      const response = await axios.post(`${API_BASE_URL}/mappings`, mapping);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to create mapping');
    }
  },

  /**
   * Update an existing mapping
   * @param {string} mappingId - The ID of the mapping to update
   * @param {Object} mapping - The updated mapping configuration
   * @returns {Promise<Object>} The updated mapping
   */
  async updateMapping(mappingId, mapping) {
    try {
      const response = await axios.put(`${API_BASE_URL}/mappings/${mappingId}`, mapping);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to update mapping');
    }
  },

  /**
   * Delete a mapping
   * @param {string} mappingId - The ID of the mapping to delete
   * @returns {Promise<Object>} The deletion response
   */
  async deleteMapping(mappingId) {
    try {
      const response = await axios.delete(`${API_BASE_URL}/mappings/${mappingId}`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to delete mapping');
    }
  },

  /**
   * Get available ClickHouse tables
   * @returns {Promise<Array>} List of available tables
   */
  async getAvailableTables() {
    try {
      const response = await axios.get(`${API_BASE_URL}/tables`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get available tables');
    }
  },

  /**
   * Get schema for a specific table
   * @param {string} tableName - The name of the table
   * @returns {Promise<Object>} The table schema
   */
  async getTableSchema(tableName) {
    try {
      const response = await axios.get(`${API_BASE_URL}/tables/${tableName}/schema`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get table schema');
    }
  },

  /**
   * Test a mapping configuration
   * @param {Object} mapping - The mapping configuration to test
   * @returns {Promise<Object>} The test results
   */
  async testMapping(mapping) {
    try {
      const response = await axios.post(`${API_BASE_URL}/mappings/test`, mapping);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to test mapping');
    }
  },

  /**
   * Get mapping statistics
   * @param {string} mappingId - The ID of the mapping
   * @returns {Promise<Object>} The mapping statistics
   */
  async getMappingStats(mappingId) {
    try {
      const response = await axios.get(`${API_BASE_URL}/mappings/${mappingId}/stats`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get mapping statistics');
    }
  }
};

export default mappingService; 