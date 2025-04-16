'use client';

import { useState, useRef, useCallback } from 'react';
import { FlatFileConfig } from '@/lib/api';

interface FlatFileFormProps {
  onSubmit: (config: FlatFileConfig, file?: File) => void;
  initialValues: FlatFileConfig;
  requireFile: boolean;
  isLoading: boolean;
  title: string;
}

export default function FlatFileForm({
  onSubmit,
  initialValues,
  requireFile,
  isLoading,
  title
}: FlatFileFormProps) {
  const [config, setConfig] = useState<FlatFileConfig>(initialValues);
  const [file, setFile] = useState<File | null>(null);
  const [validation, setValidation] = useState<Record<string, string>>({});
  const [isDragActive, setIsDragActive] = useState<boolean>(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value, type } = e.target as HTMLInputElement;
    
    setConfig({
      ...config,
      [name]: type === 'checkbox' ? (e.target as HTMLInputElement).checked : value
    });

    // Clear validation error when field is updated
    if (validation[name]) {
      setValidation(prev => ({ ...prev, [name]: '' }));
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      setFile(e.target.files[0]);
      if (validation['file']) {
        setValidation(prev => ({ ...prev, 'file': '' }));
      }
    }
  };

  const handleDragEnter = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragActive(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragActive(false);
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const handleDrop = useCallback((e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      const droppedFile = e.dataTransfer.files[0];
      // Check if file is CSV or TXT
      if (droppedFile.name.endsWith('.csv') || droppedFile.name.endsWith('.txt')) {
        setFile(droppedFile);
        if (validation['file']) {
          setValidation(prev => ({ ...prev, 'file': '' }));
        }
      } else {
        setValidation(prev => ({ 
          ...prev, 
          'file': 'Only CSV and TXT files are supported' 
        }));
      }
    }
  }, [validation]);

  const openFileSelector = () => {
    if (fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  const validateForm = (): boolean => {
    const errors: Record<string, string> = {};
    
    if (requireFile && !file) {
      errors.file = 'Please select a file to upload';
    }

    if (!requireFile && (!config.fileName || !config.fileName.trim())) {
      errors.fileName = 'File name is required for export';
    }
    
    setValidation(errors);
    return Object.keys(errors).length === 0;
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (validateForm()) {
      onSubmit(config, file || undefined);
    }
  };

  return (
    <div className="bg-white shadow-lg rounded-lg p-6 max-w-xl mx-auto transition-all duration-300 hover:shadow-xl">
      <h2 className="text-2xl font-semibold mb-6 text-primary">{title}</h2>
      <form onSubmit={handleSubmit} className="space-y-6">
        {requireFile && (
          <div className="mb-6">
            <label className={`block text-sm font-medium ${validation.file ? 'text-red-600' : 'text-gray-700'} mb-2`}>
              Upload File
            </label>
            
            <div 
              className={`border-2 border-dashed rounded-lg p-6 transition-colors ${
                isDragActive 
                  ? 'border-primary bg-blue-50' 
                  : file 
                    ? 'border-green-400 bg-green-50' 
                    : validation.file 
                      ? 'border-red-300 bg-red-50'
                      : 'border-gray-300 hover:border-gray-400'
              }`}
              onDragEnter={handleDragEnter}
              onDragLeave={handleDragLeave}
              onDragOver={handleDragOver}
              onDrop={handleDrop}
              onClick={openFileSelector}
            >
              <input
                type="file"
                id="file"
                name="file"
                ref={fileInputRef}
                className="hidden"
                accept=".csv,.txt"
                onChange={handleFileChange}
                required={requireFile}
              />
              
              <div className="text-center">
                {!file ? (
                  <>
                    <svg className={`mx-auto h-12 w-12 ${validation.file ? 'text-red-400' : 'text-gray-400'}`} stroke="currentColor" fill="none" viewBox="0 0 48 48" aria-hidden="true">
                      <path 
                        d="M28 8H12a4 4 0 00-4 4v20m32-12v8m0 0v8a4 4 0 01-4 4H12a4 4 0 01-4-4v-4m32-4l-3.172-3.172a4 4 0 00-5.656 0L28 28M8 32l9.172-9.172a4 4 0 015.656 0L28 28m0 0l4 4m4-24h8m-4-4v8m-12 4h.02" 
                        strokeWidth="2" 
                        strokeLinecap="round" 
                        strokeLinejoin="round" 
                      />
                    </svg>
                    <p className="mt-2 text-sm text-gray-600">
                      {isDragActive ? 'Drop your file here' : 'Drag and drop your file here, or click to browse'}
                    </p>
                    <p className="mt-1 text-xs text-gray-500">
                      Supported formats: CSV, TXT
                    </p>
                  </>
                ) : (
                  <div className="flex items-center justify-center space-x-2">
                    <svg className="h-8 w-8 text-green-500" viewBox="0 0 20 20" fill="currentColor">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                    <div className="text-left">
                      <p className="text-sm font-medium text-gray-900">{file.name}</p>
                      <p className="text-xs text-gray-500">{(file.size / 1024).toFixed(2)} KB • Click to change</p>
                    </div>
                  </div>
                )}
              </div>
            </div>
            
            {validation.file && (
              <p className="mt-2 text-sm text-red-600 font-medium">{validation.file}</p>
            )}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
          <div>
            <label htmlFor="delimiter" className="block text-sm font-medium text-gray-700 mb-1">
              Delimiter
            </label>
            <select
              id="delimiter"
              name="delimiter"
              className="block w-full rounded-md border-gray-300 shadow-sm focus:border-primary focus:ring-primary"
              value={config.delimiter}
              onChange={handleChange}
            >
              <option value=",">Comma (,)</option>
              <option value=";">Semicolon (;)</option>
              <option value="\t">Tab (\t)</option>
              <option value="|">Pipe (|)</option>
            </select>
          </div>

          <div className="flex items-center">
            <input
              type="checkbox"
              id="hasHeader"
              name="hasHeader"
              className="h-4 w-4 text-primary focus:ring-primary border-gray-300 rounded"
              checked={config.hasHeader}
              onChange={handleChange}
            />
            <label htmlFor="hasHeader" className="ml-2 block text-sm text-gray-700">
              File has header row
            </label>
          </div>
        </div>

        {!requireFile && (
          <div className="mb-6">
            <label htmlFor="fileName" className={`block text-sm font-medium ${validation.fileName ? 'text-red-600' : 'text-gray-700'} mb-1`}>
              Export File Name
            </label>
            <input
              type="text"
              id="fileName"
              name="fileName"
              className={`block w-full ${validation.fileName ? 'border-red-500 bg-red-50' : 'border-gray-300'} rounded-md shadow-sm focus:outline-none focus:ring-2 ${validation.fileName ? 'focus:ring-red-500' : 'focus:ring-primary'}`}
              placeholder="export.csv"
              value={config.fileName}
              onChange={handleChange}
            />
            {validation.fileName ? (
              <p className="mt-1 text-sm text-red-600 font-medium">{validation.fileName}</p>
            ) : (
              <p className="mt-1 text-sm text-gray-500">
                Leave empty to generate automatically.
              </p>
            )}
          </div>
        )}

        <div className="flex justify-end">
          <button
            type="submit"
            className="btn btn-primary px-6 py-2 rounded-md text-white font-medium transition-colors"
            disabled={isLoading}
          >
            {isLoading ? (
              <div className="flex items-center">
                <span className="loading loading-spinner loading-sm mr-2"></span>
                Processing...
              </div>
            ) : requireFile ? 'Upload & Process' : 'Continue'}
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
} 