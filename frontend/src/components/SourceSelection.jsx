import React from 'react';

function SourceSelection({ onSelect, selected }) {
  return (
    <div className="source-selection-options">
      <div className="form-group">
        <label>Select Source/Target Type:</label>
        <div className="selection-buttons">
          <button 
            className={selected === 'clickhouse' ? 'active' : 'secondary'}
            onClick={() => onSelect('clickhouse')}
          >
            ClickHouse
          </button>
          <button 
            className={selected === 'flatfile' ? 'active' : 'secondary'}
            onClick={() => onSelect('flatfile')}
            style={{ marginLeft: '10px' }}
          >
            Flat File
          </button>
        </div>
      </div>
    </div>
  );
}

export default SourceSelection;
