import React, { useEffect, useState } from 'react';

const IngestionStatus = ({ status, progress, recordCount, errorMessage }) => {
  const [statusMessage, setStatusMessage] = useState('');
  
  useEffect(() => {
    switch (status) {
      case 'connecting':
        setStatusMessage('Connecting to source...');
        break;
      case 'fetching':
        setStatusMessage('Fetching table schema...');
        break;
      case 'previewing':
        setStatusMessage('Loading data preview...');
        break;
      case 'preview_ready':
        setStatusMessage('Preview ready');
        break;
      case 'ingesting':
        setStatusMessage('Ingestion in progress...');
        break;
      case 'completed':
        setStatusMessage('Ingestion completed successfully');
        break;
      case 'error':
        setStatusMessage('Error occurred');
        break;
      default:
        setStatusMessage('');
    }
  }, [status]);
  
  const renderProgressBar = () => {
    if (status !== 'ingesting' && status !== 'completed') return null;
    
    return (
      <div className="progress-container">
        <div 
          className="progress-bar" 
          style={{ width: `${progress}%` }}
          role="progressbar" 
          aria-valuenow={progress} 
          aria-valuemin="0" 
          aria-valuemax="100"
        />
        <div className="progress-label">{Math.round(progress)}%</div>
      </div>
    );
  };
  
  const renderRecordCount = () => {
    if (status !== 'completed' || recordCount === 0) return null;
    
    return (
      <div className="record-count">
        <h4>Ingestion Results</h4>
        <p>Total records processed: <strong>{recordCount.toLocaleString()}</strong></p>
        <p className="success-message">
          <span className="check-icon">✓</span> Data ingestion completed successfully
        </p>
      </div>
    );
  };
  
  const renderError = () => {
    if (status !== 'error' || !errorMessage) return null;
    
    return (
      <div className="error-message">
        <h4>Error</h4>
        <p>{errorMessage}</p>
        <p>Please check your configuration and try again.</p>
      </div>
    );
  };
  
  // Don't render anything if status is idle
  if (status === 'idle') return null;
  
  return (
    <div className={`ingestion-status ${status}`}>
      <div className="status-message">
        <span className="status-indicator"></span>
        <h3>{statusMessage}</h3>
      </div>
      
      {renderProgressBar()}
      {renderRecordCount()}
      {renderError()}
      
      {status === 'ingesting' && (
        <div className="ingestion-note">
          <p>Please do not close this browser window during ingestion.</p>
        </div>
      )}
    </div>
  );
};

export default IngestionStatus;