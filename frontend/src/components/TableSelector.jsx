import React, { useState, useEffect } from 'react';
import { toast } from 'react-toastify';
import { getTables } from '../services/api';

function TableSelector({ connectionId, tables, onTablesLoaded, onTableSelection }) {
  const [selectedTables, setSelectedTables] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (connectionId) {
      loadTables();
    }
  }, [connectionId]);

  const loadTables = async () => {
    setLoading(true);
    
    try {
      const response = await getTables({ connection_id: connectionId });
      
      if (response && response.tables) {
        onTablesLoaded(response.tables);
      } else {
        toast.error('Failed to load tables');
      }
    } catch (error) {
      toast.error(`Error loading tables: ${error.message || 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const handleTableChange = (table, checked) => {
    let updatedSelection;
    
    if (checked) {
      updatedSelection = [...selectedTables, table];
    } else {
      updatedSelection = selectedTables.filter(t => t !== table);
    }
    
    setSelectedTables(updatedSelection);
    onTableSelection(updatedSelection);
  };

  return (
    <div className="table-selector">
      <h3>Select Tables</h3>
      
      {loading ? (
        <div>Loading tables...</div>
      ) : (
        <div className="table-list">
          {tables.length > 0 ? (
            tables.map(table => (
              <div key={table} className="checkbox-container">
                <input
                  type="checkbox"
                  id={`table-${table}`}
                  checked={selectedTables.includes(table)}
                  onChange={(e) => handleTableChange(table, e.target.checked)}
                />
                <label htmlFor={`table-${table}`}>{table}</label>
              </div>
            ))
          ) : (
            <div>No tables available</div>
          )}
        </div>
      )}
      
      <button className="secondary" onClick={loadTables} disabled={loading}>
        {loading ? 'Loading...' : 'Refresh Tables'}
      </button>
    </div>
  );
}

export default TableSelector;