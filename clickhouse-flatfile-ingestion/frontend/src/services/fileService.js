import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8080/api';

/**
 * Service for handling file operations
 */
const fileService = {
  /**
   * Upload a file with progress tracking
   * @param {FormData} formData - FormData containing the file
   * @param {Function} onProgress - Callback for upload progress
   * @returns {Promise} - Upload response
   */
  upload: async (formData, onProgress) => {
    try {
      const response = await axios.post(`${API_BASE_URL}/files/upload`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        },
        onUploadProgress: onProgress
      });
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to upload file');
    }
  },

  /**
   * Get a preview of a file
   * @param {string} fileId - ID of the file to preview
   * @returns {Promise} - Preview data
   */
  getPreview: async (fileId) => {
    try {
      const response = await axios.get(`${API_BASE_URL}/files/${fileId}/preview`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to get file preview');
    }
  },

  /**
   * Delete a file
   * @param {string} fileId - ID of the file to delete
   * @returns {Promise} - Deletion response
   */
  delete: async (fileId) => {
    try {
      const response = await axios.delete(`${API_BASE_URL}/files/${fileId}`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to delete file');
    }
  },

  /**
   * List all uploaded files
   * @returns {Promise} - List of files
   */
  list: async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/files`);
      return response.data;
    } catch (error) {
      throw new Error(error.response?.data?.message || 'Failed to list files');
    }
  }
};

export default fileService; 