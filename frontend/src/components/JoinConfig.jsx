import React, { useState, useEffect } from 'react';

function JoinConfig({ tables, onJoinConfigChange }) {
  const [joinType, setJoinType] = useState('inner');
  const [joinColumns, setJoinColumns] = useState({});

  useEffect(() => {
    // Initialize join columns with empty values when tables change
    const initialJoinColumns = {};
    tables.forEach(table => {
      initialJoinColumns[table] = '';
    });
    setJoinColumns(initialJoinColumns);
  }, [tables]);

  useEffect(() => {
    // Notify parent component when join config changes
    onJoinConfigChange({
      join_type: joinType,
      join_columns: joinColumns
    });
  }, [joinType, joinColumns]);

  const handleJoinTypeChange = (e) => {
    setJoinType(e.target.value);
  };

  const handleJoinColumnChange = (table, columnName) => {
    setJoinColumns({
      ...joinColumns,
      [table]: columnName
    });
  };

  return (
    <div className="join-config">
      <h3>Join Configuration</h3>
      
      <div className="form-group">
        <label htmlFor="join-type">Join Type:</label>
        <select
          id="join-type"
          value={joinType}
          onChange={handleJoinTypeChange}
        >
          <option value="inner">INNER JOIN</option>
          <option value="left">LEFT JOIN</option>
          <option value="right">RIGHT JOIN</option>
          <option value="full">FULL JOIN</option>
        </select>
      </div>
      
      <div className="join-columns">
        <h4>Join Columns</h4>
        <p className="helper-text">Specify which columns to use for joining tables</p>
        
        {tables.map(table => (
          <div key={table} className="form-group">
            <label htmlFor={`join-column-${table}`}>{table}:</label>
            <input
              type="text"
              id={`join-column-${table}`}
              value={joinColumns[table] || ''}
              onChange={(e) => handleJoinColumnChange(table, e.target.value)}
              placeholder={`Join column for ${table}`}
            />
          </div>
        ))}
      </div>
    </div>
  );
}

export default JoinConfig;