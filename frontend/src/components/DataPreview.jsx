import React from 'react';


const DataPreview = ({ data }) => {
  if (!data || !data.headers || !data.rows || data.rows.length === 0) {
    return (
      <div className="data-preview-container empty-preview">
        <p>No preview data available</p>
      </div>
    );
  }

  return (
    <div className="data-preview-container">
      <h3>Data Preview (First {data.rows.length} Records)</h3>
      
      <div className="preview-table-wrapper">
        <table className="preview-table">
          <thead>
            <tr>
              {data.headers.map((header, index) => (
                <th key={index}>{header}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.rows.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {data.headers.map((header, colIndex) => (
                  <td key={`${rowIndex}-${colIndex}`}>
                    {row[header] !== undefined ? String(row[header]) : ''}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      <div className="preview-info">
        <p>Showing {data.rows.length} of 100 available preview records</p>
        <p className="preview-note">
          Note: This is just a preview. The actual ingestion will process all matched records.
        </p>
      </div>
    </div>
  );
};

export default DataPreview;