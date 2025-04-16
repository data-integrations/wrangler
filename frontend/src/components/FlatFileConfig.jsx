import React, { useState } from 'react';
import { toast } from 'react-toastify';
import { connectFlatFile } from '../services/api';

function FlatFileConfig({ onConnected }) {
  const [config, setConfig] = useState({
    fileName: '',
    delimiter: ','
  });
  const [loading, setLoading] = useState(false);

  const handleConnect = async (e) => {
    e.preventDefault();
    
    if (!config.fileName) {
      toast.error('Please enter a file name');
      return;
    }
    
    setLoading(true);
    
    try {
      const response = await connectFlatFile({
        fileName: config.fileName,
        delimiter: config.delimiter
      });
      
      if (response.success) {
        toast.success(response.message);
        onConnected({
          fileName: config.fileName,
          delimiter: config.delimiter
        });
      } else {
        toast.error(response.message);
      }
    } catch (error) {
      toast.error(`File connection error: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (field, value) => {
    setConfig(prev => ({
      ...prev,
      [field]: value
    }));
  };

  return (
    <div className="flatfile-config">
      <form onSubmit={handleConnect}>
        <div className="form-group">
          <label>File Name:</label>
          <input
            type="text"
            value={config.fileName}
            onChange={(e) => handleChange('fileName', e.target.value)}
            placeholder="example.csv"
            required
          />
          <small>File must be in the uploads directory</small>
        </div>

        <div className="form-group">
          <label>Delimiter:</label>
          <select
            value={config.delimiter}
            onChange={(e) => handleChange('delimiter', e.target.value)}
          >
            <option value=",">Comma (,)</option>
            <option value="\t">Tab (\t)</option>
            <option value=";">Semicolon (;)</option>
            <option value="|">Pipe (|)</option>
          </select>
        </div>

        <button type="submit" disabled={loading || !config.fileName}>
          {loading ? 'Connecting...' : 'Connect to File'}
        </button>
      </form>
    </div>
  );
}

export default FlatFileConfig;