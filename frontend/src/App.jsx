import React, { useState } from 'react';
import SourceSelection from './components/SourceSelection';
import ClickHouseConfig from './components/ClickHouseConfig';
import FlatFileConfig from './components/FlatFileConfig';
import TableSelector from './components/TableSelector';
import ColumnSelector from './components/ColumnSelector';
import JoinConfig from './components/JoinConfig';
import IngestionStatus from './components/IngestionStatus';
import DataPreview from './components/DataPreview';
import './App.css';

function App() {
  // State management for the application
  const [source, setSource] = useState(''); // 'ClickHouse' or 'FlatFile'
  const [target, setTarget] = useState(''); // Auto-set opposite of source
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');
  
  // ClickHouse configuration state
  const [clickHouseConfig, setClickHouseConfig] = useState({
    host: '',
    port: '',
    database: '',
    user: '',
    jwtToken: ''
  });
  
  // Flat File configuration state
  const [flatFileConfig, setFlatFileConfig] = useState({
    fileName: '',
    delimiter: ','
  });
  
  // Data structure states
  const [availableTables, setAvailableTables] = useState([]);
  const [selectedTables, setSelectedTables] = useState([]);
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  
  // Join configuration for bonus requirement
  const [joinConditions, setJoinConditions] = useState([]);
  const [enableJoin, setEnableJoin] = useState(false);
  
  // Status tracking
  const [status, setStatus] = useState('idle'); // idle, connecting, fetching, previewing, ingesting, completed, error
  const [progress, setProgress] = useState(0);
  const [recordCount, setRecordCount] = useState(0);
  
  // Preview data state
  const [previewData, setPreviewData] = useState(null);
  
  // Handle source selection change
  const handleSourceChange = (newSource) => {
    setSource(newSource);
    setTarget(newSource === 'ClickHouse' ? 'FlatFile' : 'ClickHouse');
    resetState();
  };
  
  // Reset application state for new connection
  const handleConnectionParamChange = (configType, field, value) => {
    if (configType === 'clickHouse') {
      setClickHouseConfig(prev => ({ ...prev, [field]: value }));
    } else {
      setFlatFileConfig(prev => ({ ...prev, [field]: value }));
    }
  };
  
  // Reset application state
  const resetState = () => {
    setIsConnected(false);
    setAvailableTables([]);
    setSelectedTables([]);
    setColumns([]);
    setSelectedColumns([]);
    setJoinConditions([]);
    setEnableJoin(false);
    setStatus('idle');
    setProgress(0);
    setRecordCount(0);
    setErrorMessage('');
    setPreviewData(null);
  };
  
  // Connect to the selected source
  const handleConnect = async () => {
    setIsLoading(true);
    setStatus('connecting');
    setErrorMessage('');
    
    try {
      // This would be an actual API call in production
      setTimeout(() => {
        setIsConnected(true);
        setIsLoading(false);
        setStatus('connected');
        
        // Mock data for demo purposes
        if (source === 'ClickHouse') {
          setAvailableTables(['uk_price_paid', 'ontime', 'users', 'visits']);
        } else {
          // For flat file, we assume schema discovery happens here
          setColumns([
            { name: 'id', type: 'Int32' },
            { name: 'name', type: 'String' },
            { name: 'email', type: 'String' },
            { name: 'created_at', type: 'DateTime' }
          ]);
        }
      }, 1000);
    } catch (error) {
      setIsLoading(false);
      setStatus('error');
      setErrorMessage(`Connection failed: ${error.message}`);
    }
  };
  
  // Handle table selection (ClickHouse source)
  const handleTableSelect = (tables) => {
    setSelectedTables(tables);
    setIsLoading(true);
    setStatus('fetching');
    
    // Simulate fetching columns from the selected tables
    setTimeout(() => {
      const mockColumns = [
        { name: 'id', type: 'Int32' },
        { name: 'price', type: 'UInt64' },
        { name: 'date', type: 'Date' },
        { name: 'postcode', type: 'String' },
        { name: 'property_type', type: 'String' }
      ];
      
      setColumns(mockColumns);
      setIsLoading(false);
      setStatus('columns_loaded');
    }, 800);
  };
  
  // Handle column selection
  const handleColumnSelect = (columns) => {
    setSelectedColumns(columns);
  };
  
  // Handle join conditions change
  const handleJoinConditionChange = (conditions) => {
    setJoinConditions(conditions);
  };
  
  // Toggle join functionality
  const toggleJoin = () => {
    setEnableJoin(!enableJoin);
    if (!enableJoin) {
      setJoinConditions([{ leftTable: '', leftColumn: '', rightTable: '', rightColumn: '' }]);
    } else {
      setJoinConditions([]);
    }
  };
  
  // Handle preview request
  const handlePreview = () => {
    setIsLoading(true);
    setStatus('previewing');
    
    // Simulate fetching preview data
    setTimeout(() => {
      const previewColumns = selectedColumns.length > 0 
        ? selectedColumns 
        : columns.slice(0, Math.min(5, columns.length));
      
      const mockData = Array(5).fill().map((_, rowIndex) => {
        const row = {};
        previewColumns.forEach(col => {
          if (col.type === 'Int32' || col.type === 'UInt64') {
            row[col.name] = Math.floor(Math.random() * 1000);
          } else if (col.type === 'Date' || col.type === 'DateTime') {
            row[col.name] = new Date().toISOString().split('T')[0];
          } else {
            row[col.name] = `Sample ${col.name} ${rowIndex + 1}`;
          }
        });
        return row;
      });
      
      setPreviewData({
        headers: previewColumns.map(col => col.name),
        rows: mockData
      });
      
      setIsLoading(false);
      setStatus('preview_ready');
    }, 1000);
  };
  
  // Handle start ingestion
  const handleStartIngestion = () => {
    setStatus('ingesting');
    setProgress(0);
    
    // Simulate ingestion process with progress updates
    const interval = setInterval(() => {
      setProgress(prev => {
        const newProgress = prev + 5;
        if (newProgress >= 100) {
          clearInterval(interval);
          setStatus('completed');
          setRecordCount(12345); // Mock record count
          return 100;
        }
        return newProgress;
      });
    }, 200);
  };

  return (
    <div className="app-container">
      <h1>Bidirectional Data Ingestion Tool</h1>
      
      {/* Source/Target Selection */}
      <div className="section">
        <h2>1. Select Source and Target</h2>
        <SourceSelection 
          source={source}
          target={target}
          onSourceChange={handleSourceChange}
        />
      </div>
      
      {/* Source Configuration */}
      {source && (
        <div className="section">
          <h2>2. Configure {source} Connection</h2>
          
          {source === 'ClickHouse' ? (
            <ClickHouseConfig 
              config={clickHouseConfig}
              onChange={(field, value) => handleConnectionParamChange('clickHouse', field, value)}
            />
          ) : (
            <FlatFileConfig 
              config={flatFileConfig}
              onChange={(field, value) => handleConnectionParamChange('flatFile', field, value)}
            />
          )}
          
          <button 
            className="action-button" 
            onClick={handleConnect}
            disabled={isLoading}
          >
            {isLoading ? 'Connecting...' : 'Connect'}
          </button>
        </div>
      )}
      
      {/* Table Selection (ClickHouse source only) */}
      {isConnected && source === 'ClickHouse' && (
        <div className="section">
          <h2>3. Select Tables</h2>
          <TableSelector 
            tables={availableTables}
            selectedTables={selectedTables}
            onTableSelect={handleTableSelect}
          />
          
          {selectedTables.length > 1 && (
            <div className="join-toggle">
              <label>
                <input 
                  type="checkbox" 
                  checked={enableJoin} 
                  onChange={toggleJoin} 
                />
                Enable Join between tables
              </label>
            </div>
          )}
          
          {enableJoin && selectedTables.length > 1 && (
            <JoinConfig 
              tables={selectedTables}
              columns={columns}
              joinConditions={joinConditions}
              onChange={handleJoinConditionChange}
            />
          )}
        </div>
      )}
      
      {/* Column Selection */}
      {columns.length > 0 && (
        <div className="section">
          <h2>{source === 'ClickHouse' ? '4' : '3'}. Select Columns</h2>
          <ColumnSelector 
            columns={columns}
            selectedColumns={selectedColumns}
            onColumnSelect={handleColumnSelect}
          />
        </div>
      )}
      
      {/* Preview & Ingestion Controls */}
      {columns.length > 0 && (
        <div className="section">
          <h2>{source === 'ClickHouse' ? '5' : '4'}. Preview and Execute</h2>
          
          <div className="action-buttons">
            <button 
              className="preview-button"
              onClick={handlePreview}
              disabled={isLoading || columns.length === 0}
            >
              {isLoading && status === 'previewing' ? 'Loading Preview...' : 'Preview Data'}
            </button>
            
            <button 
              className="ingestion-button"
              onClick={handleStartIngestion}
              disabled={isLoading || columns.length === 0 || selectedColumns.length === 0}
            >
              Start Ingestion
            </button>
          </div>
          
          {/* Preview Data Display */}
          {previewData && (
            <DataPreview data={previewData} />
          )}
          
          {/* Ingestion Status */}
          {status !== 'idle' && status !== 'connected' && status !== 'columns_loaded' && (
            <IngestionStatus 
              status={status} 
              progress={progress} 
              recordCount={recordCount}
              errorMessage={errorMessage}
            />
          )}
        </div>
      )}
    </div>
  );
}

export default App;