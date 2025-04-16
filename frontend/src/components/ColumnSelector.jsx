import React, { useState, useEffect } from 'react';
import { toast } from 'react-toastify';
import api from '../services/api';

function ColumnSelector({ tableName, onColumnsLoaded, onColumnSelection }) {
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [allSelected, setAllSelected] = useState(false);

  useEffect(() => {
    if (tableName) {
      loadColumns();
    }
  }, [tableName]);

  const loadColumns = async () => {
    setLoading(true);
    try {
      const columns = await api.getTableSchema(tableName);
      setColumns(columns);
      onColumnsLoaded(columns);
      
      // Default select all columns
      const initialSelection = columns.map(col => col.name);
      setSelectedColumns(initialSelection);
      onColumnSelection(initialSelection);
      setAllSelected(true);
    } catch (error) {
      toast.error(`Failed to load columns: ${error.message}`);
      setColumns([]);
      setSelectedColumns([]);
    } finally {
      setLoading(false);
    }
  };

  const handleColumnChange = (columnName, isChecked) => {
    const newSelection = isChecked
      ? [...selectedColumns, columnName]
      : selectedColumns.filter(name => name !== columnName);
      
    setSelectedColumns(newSelection);
    setAllSelected(newSelection.length === columns.length);
    onColumnSelection(newSelection);
  };

  const handleSelectAll = (isChecked) => {
    const newSelection = isChecked ? columns.map(col => col.name) : [];
    setSelectedColumns(newSelection);
    setAllSelected(isChecked);
    onColumnSelection(newSelection);
  };

  return (
    <div className="column-selector">
      <h3>Select Columns from {tableName}</h3>
      
      <div className="controls">
        <div className="select-all">
          <input
            type="checkbox"
            id="select-all"
            checked={allSelected}
            onChange={(e) => handleSelectAll(e.target.checked)}
            disabled={!columns.length || loading}
          />
          <label htmlFor="select-all">Select All</label>
        </div>
        
        <button 
          type="button" 
          onClick={loadColumns}
          disabled={loading}
        >
          {loading ? 'Refreshing...' : 'Refresh Columns'}
        </button>
      </div>

      {loading ? (
        <div className="loading">Loading columns...</div>
      ) : (
        <div className="column-grid">
          {columns.map(column => (
            <div key={column.name} className="column-item">
              <input
                type="checkbox"
                id={column.name}
                checked={selectedColumns.includes(column.name)}
                onChange={(e) => handleColumnChange(column.name, e.target.checked)}
              />
              <label htmlFor={column.name}>
                <span className="column-name">{column.name}</span>
                <span className="column-type">{column.type}</span>
              </label>
            </div>
          ))}
          
          {!columns.length && !loading && (
            <div className="empty-state">No columns available</div>
          )}
        </div>
      )}
    </div>
  );
}

export default ColumnSelector;