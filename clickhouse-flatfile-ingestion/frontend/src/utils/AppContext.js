import React, { createContext, useContext, useState, useEffect } from 'react';
import { useSnackbar } from 'notistack';
import clickhouseService from '../services/clickhouseService';
import mappingService from '../services/mappingService';

// Create context
const AppContext = createContext();

// Custom hook to use the app context
export const useAppContext = () => useContext(AppContext);

// Provider component
export const AppProvider = ({ children }) => {
  const { enqueueSnackbar } = useSnackbar();
  
  // State
  const [connections, setConnections] = useState([]);
  const [mappings, setMappings] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  // Load connections on mount
  useEffect(() => {
    loadConnections();
  }, []);
  
  // Load mappings on mount
  useEffect(() => {
    loadMappings();
  }, []);
  
  // Load all connections
  const loadConnections = async () => {
    try {
      setLoading(true);
      const data = await clickhouseService.getConnections();
      setConnections(data);
      setError(null);
    } catch (err) {
      setError(err.message);
      enqueueSnackbar(`Failed to load connections: ${err.message}`, { variant: 'error' });
    } finally {
      setLoading(false);
    }
  };
  
  // Load all mappings
  const loadMappings = async () => {
    try {
      setLoading(true);
      const data = await mappingService.getMappings();
      setMappings(data);
      setError(null);
    } catch (err) {
      setError(err.message);
      enqueueSnackbar(`Failed to load mappings: ${err.message}`, { variant: 'error' });
    } finally {
      setLoading(false);
    }
  };
  
  // Add a new connection
  const addConnection = async (connection) => {
    try {
      setLoading(true);
      const newConnection = await clickhouseService.createConnection(connection);
      setConnections([...connections, newConnection]);
      enqueueSnackbar('Connection added successfully', { variant: 'success' });
      return newConnection;
    } catch (err) {
      setError(err.message);
      enqueueSnackbar(`Failed to add connection: ${err.message}`, { variant: 'error' });
      throw err;
    } finally {
      setLoading(false);
    }
  };
  
  // Update an existing connection
  const updateConnection = async (id, connection) => {
    try {
      setLoading(true);
      const updatedConnection = await clickhouseService.updateConnection(id, connection);
      setConnections(connections.map(conn => conn.id === id ? updatedConnection : conn));
      enqueueSnackbar('Connection updated successfully', { variant: 'success' });
      return updatedConnection;
    } catch (err) {
      setError(err.message);
      enqueueSnackbar(`Failed to update connection: ${err.message}`, { variant: 'error' });
      throw err;
    } finally {
      setLoading(false);
    }
  };
  
  // Delete a connection
  const deleteConnection = async (id) => {
    try {
      setLoading(true);
      await clickhouseService.deleteConnection(id);
      setConnections(connections.filter(conn => conn.id !== id));
      enqueueSnackbar('Connection deleted successfully', { variant: 'success' });
    } catch (err) {
      setError(err.message);
      enqueueSnackbar(`Failed to delete connection: ${err.message}`, { variant: 'error' });
      throw err;
    } finally {
      setLoading(false);
    }
  };
  
  // Add a new mapping
  const addMapping = async (mapping) => {
    try {
      setLoading(true);
      const newMapping = await mappingService.createMapping(mapping);
      setMappings([...mappings, newMapping]);
      enqueueSnackbar('Mapping added successfully', { variant: 'success' });
      return newMapping;
    } catch (err) {
      setError(err.message);
      enqueueSnackbar(`Failed to add mapping: ${err.message}`, { variant: 'error' });
      throw err;
    } finally {
      setLoading(false);
    }
  };
  
  // Update an existing mapping
  const updateMapping = async (id, mapping) => {
    try {
      setLoading(true);
      const updatedMapping = await mappingService.updateMapping(id, mapping);
      setMappings(mappings.map(map => map.id === id ? updatedMapping : map));
      enqueueSnackbar('Mapping updated successfully', { variant: 'success' });
      return updatedMapping;
    } catch (err) {
      setError(err.message);
      enqueueSnackbar(`Failed to update mapping: ${err.message}`, { variant: 'error' });
      throw err;
    } finally {
      setLoading(false);
    }
  };
  
  // Delete a mapping
  const deleteMapping = async (id) => {
    try {
      setLoading(true);
      await mappingService.deleteMapping(id);
      setMappings(mappings.filter(map => map.id !== id));
      enqueueSnackbar('Mapping deleted successfully', { variant: 'success' });
    } catch (err) {
      setError(err.message);
      enqueueSnackbar(`Failed to delete mapping: ${err.message}`, { variant: 'error' });
      throw err;
    } finally {
      setLoading(false);
    }
  };
  
  // Context value
  const value = {
    connections,
    mappings,
    loading,
    error,
    loadConnections,
    loadMappings,
    addConnection,
    updateConnection,
    deleteConnection,
    addMapping,
    updateMapping,
    deleteMapping
  };
  
  return (
    <AppContext.Provider value={value}>
      {children}
    </AppContext.Provider>
  );
}; 