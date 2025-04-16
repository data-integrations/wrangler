'use client';

import React, { useState } from 'react';
import { ClickHouseConfig } from '@/lib/api';

interface ClickHouseFormProps {
  onSubmit: (config: ClickHouseConfig) => void;
  initialValues: ClickHouseConfig;
  isLoading: boolean;
  title: string;
}

const ClickHouseForm: React.FC<ClickHouseFormProps> = ({ onSubmit, initialValues, isLoading, title }) => {
  const [config, setConfig] = useState<ClickHouseConfig>(initialValues);
  const [validation, setValidation] = useState<Record<string, string>>({});

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value, type, checked } = e.target;
    const newValue = type === 'checkbox' ? checked : value;
    const newConfig = { ...config, [name]: type === 'number' ? Number(value) : newValue };
    setConfig(newConfig);
    
    // Clear validation error when field is updated
    if (validation[name]) {
      setValidation(prev => ({ ...prev, [name]: '' }));
    }
  };

  const validateForm = (): boolean => {
    const errors: Record<string, string> = {};
    
    if (!config.host.trim()) {
      errors.host = 'Host is required';
    }
    
    if (!config.port || config.port <= 0) {
      errors.port = 'Port must be a positive number';
    }
    
    if (!config.database.trim()) {
      errors.database = 'Database is required';
    }
    
    if (!config.user.trim()) {
      errors.user = 'Username is required';
    }
    
    if (!config.jwtToken.trim()) {
      errors.jwtToken = 'JWT token is required';
    }
    
    setValidation(errors);
    return Object.keys(errors).length === 0;
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (validateForm()) {
      onSubmit(config);
    }
  };

  return (
    <div className="bg-white shadow-lg rounded-lg p-6 max-w-xl mx-auto transition-all duration-300 hover:shadow-xl">
      <h2 className="text-2xl font-semibold mb-6 text-primary">{title}</h2>
      
      <form onSubmit={handleSubmit} className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Host */}
          <div className="form-control">
            <label className="label">
              <span className={`label-text font-medium ${validation.host ? 'text-red-600' : ''}`}>Host</span>
            </label>
            <input
              type="text"
              name="host"
              value={config.host}
              onChange={handleChange}
              placeholder="localhost"
              className={`input ${validation.host ? 'border-red-500 bg-red-50' : 'border-gray-300'} w-full rounded-md p-2 focus:outline-none focus:ring-2 ${validation.host ? 'focus:ring-red-500' : 'focus:ring-primary'}`}
              disabled={isLoading}
            />
            {validation.host && <p className="text-red-600 text-sm mt-1 font-medium">{validation.host}</p>}
          </div>
          
          {/* Port */}
          <div className="form-control">
            <label className="label">
              <span className={`label-text font-medium ${validation.port ? 'text-red-600' : ''}`}>Port</span>
            </label>
            <input
              type="number"
              name="port"
              value={config.port}
              onChange={handleChange}
              placeholder="8123"
              className={`input ${validation.port ? 'border-red-500 bg-red-50' : 'border-gray-300'} w-full rounded-md p-2 focus:outline-none focus:ring-2 ${validation.port ? 'focus:ring-red-500' : 'focus:ring-primary'}`}
              disabled={isLoading}
            />
            {validation.port && <p className="text-red-600 text-sm mt-1 font-medium">{validation.port}</p>}
          </div>
        </div>

        {/* Database */}
        <div className="form-control">
          <label className="label">
            <span className={`label-text font-medium ${validation.database ? 'text-red-600' : ''}`}>Database</span>
          </label>
          <input
            type="text"
            name="database"
            value={config.database}
            onChange={handleChange}
            placeholder="default"
            className={`input ${validation.database ? 'border-red-500 bg-red-50' : 'border-gray-300'} w-full rounded-md p-2 focus:outline-none focus:ring-2 ${validation.database ? 'focus:ring-red-500' : 'focus:ring-primary'}`}
            disabled={isLoading}
          />
          {validation.database && <p className="text-red-600 text-sm mt-1 font-medium">{validation.database}</p>}
        </div>
        
        {/* User */}
        <div className="form-control">
          <label className="label">
            <span className={`label-text font-medium ${validation.user ? 'text-red-600' : ''}`}>Username</span>
          </label>
          <input
            type="text"
            name="user"
            value={config.user}
            onChange={handleChange}
            placeholder="default"
            className={`input ${validation.user ? 'border-red-500 bg-red-50' : 'border-gray-300'} w-full rounded-md p-2 focus:outline-none focus:ring-2 ${validation.user ? 'focus:ring-red-500' : 'focus:ring-primary'}`}
            disabled={isLoading}
          />
          {validation.user && <p className="text-red-600 text-sm mt-1 font-medium">{validation.user}</p>}
        </div>
        
        {/* JWT Token */}
        <div className="form-control">
          <label className="label">
            <span className={`label-text font-medium ${validation.jwtToken ? 'text-red-600' : ''}`}>JWT Token</span>
          </label>
          <input
            type="password"
            name="jwtToken"
            value={config.jwtToken}
            onChange={handleChange}
            placeholder="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
            className={`input ${validation.jwtToken ? 'border-red-500 bg-red-50' : 'border-gray-300'} w-full rounded-md p-2 focus:outline-none focus:ring-2 ${validation.jwtToken ? 'focus:ring-red-500' : 'focus:ring-primary'}`}
            disabled={isLoading}
          />
          {validation.jwtToken && <p className="text-red-600 text-sm mt-1 font-medium">{validation.jwtToken}</p>}
        </div>
        
        {/* Secure */}
        <div className="form-control">
          <label className="cursor-pointer label justify-start gap-2">
            <input
              type="checkbox"
              name="secure"
              checked={config.secure}
              onChange={handleChange}
              className="checkbox checkbox-primary"
              disabled={isLoading}
            />
            <span className="label-text">Use HTTPS</span>
          </label>
        </div>
        
        <div className="mt-8">
          <button
            type="submit"
            className="btn btn-primary w-full"
            disabled={isLoading}
          >
            {isLoading ? (
              <>
                <span className="loading loading-spinner loading-sm mr-2"></span>
                Connecting...
              </>
            ) : (
              'Connect'
            )}
          </button>
        </div>
        
        {Object.keys(validation).length > 0 && (
          <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-md">
            <div className="flex items-center">
              <svg className="h-5 w-5 text-red-500 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
              </svg>
              <span className="text-red-600 font-medium">Please fix the errors before continuing</span>
            </div>
          </div>
        )}
      </form>
    </div>
  );
};

export default ClickHouseForm; 