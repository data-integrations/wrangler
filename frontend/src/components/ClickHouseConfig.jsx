import React, { useState } from 'react';
import { toast } from 'react-toastify';
import { connectClickHouse } from '../services/api';

function ClickHouseConfig({ onConnected, isSource }) {
  const [config, setConfig] = useState({
    host: 'localhost',
    port: 8123,
    protocol: 'http',
    user: 'default',
    jwtToken: '',
    database: 'default'
  });
  const [connecting, setConnecting] = useState(false);

  const handleConnect = async (e) => {
    e.preventDefault();
    
    if (!config.host || !config.port || !config.database) {
      toast.error('Please fill in all required fields');
      return;
    }
    
    setConnecting(true);
    
    try {
      const response = await connectClickHouse({
        host: config.host,
        port: Number(config.port),
        database: config.database,
        user: config.user,
        jwtToken: config.jwtToken,
        protocol: config.protocol
      });
      
      if (response.success) {
        toast.success(response.message);
        onConnected(); // No need to pass connection ID as we're using singleton
      } else {
        toast.error(response.message);
      }
    } catch (error) {
      toast.error(`Connection error: ${error.message}`);
    } finally {
      setConnecting(false);
    }
  };

  const handleChange = (field, value) => {
    setConfig(prev => ({
      ...prev,
      [field]: value
    }));
  };
  
  return (
    <div className="clickhouse-config">
      <form onSubmit={handleConnect}>
        <div className="form-group">
          <label>Protocol:</label>
          <select 
            value={config.protocol}
            onChange={(e) => handleChange('protocol', e.target.value)}
          >
            <option value="http">HTTP</option>
            <option value="https">HTTPS</option>
          </select>
        </div>

        <div className="form-group">
          <label>Host:</label>
          <input
            type="text"
            value={config.host}
            onChange={(e) => handleChange('host', e.target.value)}
            placeholder="localhost"
            required
          />
        </div>

        <div className="form-group">
          <label>Port:</label>
          <input
            type="number"
            value={config.port}
            onChange={(e) => handleChange('port', e.target.value)}
            placeholder={config.protocol === 'http' ? '8123' : '8443'}
            required
          />
        </div>

        <div className="form-group">
          <label>Database:</label>
          <input
            type="text"
            value={config.database}
            onChange={(e) => handleChange('database', e.target.value)}
            required
          />
        </div>

        <div className="form-group">
          <label>User:</label>
          <input
            type="text"
            value={config.user}
            onChange={(e) => handleChange('user', e.target.value)}
          />
        </div>

        <div className="form-group">
          <label>JWT Token:</label>
          <input
            type="password"
            value={config.jwtToken}
            onChange={(e) => handleChange('jwtToken', e.target.value)}
          />
        </div>

        <button type="submit" disabled={connecting}>
          {connecting ? 'Connecting...' : 'Connect to ClickHouse'}
        </button>
      </form>
    </div>
  );
}

export default ClickHouseConfig;