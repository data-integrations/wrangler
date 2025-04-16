'use client';

import React from 'react';

type StatusType = 'idle' | 'connecting' | 'fetching' | 'ingesting' | 'completed' | 'error';

interface StatusIndicatorProps {
  status: StatusType;
  message?: string;
}

export default function StatusIndicator({ status, message }: StatusIndicatorProps) {
  const getStatusColor = () => {
    switch (status) {
      case 'idle': return 'bg-gray-100 border-gray-200';
      case 'connecting': return 'bg-blue-50 border-blue-200';
      case 'fetching': return 'bg-blue-50 border-blue-300';
      case 'ingesting': return 'bg-yellow-50 border-yellow-200';
      case 'completed': return 'bg-green-50 border-green-200';
      case 'error': return 'bg-red-50 border-red-200';
      default: return 'bg-gray-100 border-gray-200';
    }
  };

  const getTextColor = () => {
    switch (status) {
      case 'idle': return 'text-gray-700';
      case 'connecting': return 'text-blue-700';
      case 'fetching': return 'text-blue-700';
      case 'ingesting': return 'text-yellow-700';
      case 'completed': return 'text-green-700';
      case 'error': return 'text-red-700';
      default: return 'text-gray-700';
    }
  };

  const getStatusText = () => {
    switch (status) {
      case 'idle': return 'Ready';
      case 'connecting': return 'Connecting...';
      case 'fetching': return 'Fetching Data...';
      case 'ingesting': return 'Processing Data...';
      case 'completed': return 'Completed';
      case 'error': return 'Error';
      default: return 'Ready';
    }
  };

  const getStatusIcon = () => {
    switch (status) {
      case 'idle':
        return (
          <div className="p-2 bg-gray-200 rounded-full">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6 text-gray-600" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-11a1 1 0 10-2 0v3.586L7.707 9.293a1 1 0 00-1.414 1.414l3 3a1 1 0 001.414 0l3-3a1 1 0 00-1.414-1.414L11 10.586V7z" clipRule="evenodd" />
            </svg>
          </div>
        );
      case 'connecting':
        return (
          <div className="p-2 bg-blue-200 rounded-full">
            <svg className="animate-spin h-6 w-6 text-blue-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
          </div>
        );
      case 'fetching':
        return (
          <div className="p-2 bg-blue-300 rounded-full">
            <svg className="animate-spin h-6 w-6 text-blue-700" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
          </div>
        );
      case 'ingesting':
        return (
          <div className="p-2 bg-yellow-200 rounded-full">
            <svg className="animate-pulse h-6 w-6 text-yellow-700" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M3 17a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zm3.293-7.707a1 1 0 011.414 0L9 10.586V3a1 1 0 112 0v7.586l1.293-1.293a1 1 0 111.414 1.414l-3 3a1 1 0 01-1.414 0l-3-3a1 1 0 010-1.414z" clipRule="evenodd" />
            </svg>
          </div>
        );
      case 'completed':
        return (
          <div className="p-2 bg-green-200 rounded-full">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6 text-green-700" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
            </svg>
          </div>
        );
      case 'error':
        return (
          <div className="p-2 bg-red-200 rounded-full">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6 text-red-700" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
            </svg>
          </div>
        );
      default:
        return null;
    }
  };

  const getProgressBarWidth = () => {
    switch (status) {
      case 'idle': return 'w-0';
      case 'connecting': return 'w-1/4';
      case 'fetching': return 'w-1/2';
      case 'ingesting': return 'w-3/4';
      case 'completed': return 'w-full';
      case 'error': return 'w-full';
      default: return 'w-0';
    }
  };

  const getProgressBarColor = () => {
    switch (status) {
      case 'idle': return 'bg-gray-300';
      case 'connecting': return 'bg-blue-400';
      case 'fetching': return 'bg-blue-500';
      case 'ingesting': return 'bg-yellow-500';
      case 'completed': return 'bg-green-500';
      case 'error': return 'bg-red-500';
      default: return 'bg-gray-300';
    }
  };

  return (
    <div className={`${getStatusColor()} border rounded-lg p-5 mb-6 transition-all duration-300 shadow-sm hover:shadow-md`}>
      <div className="flex items-center mb-2">
        <div className="flex-shrink-0">
          {getStatusIcon()}
        </div>
        <div className="ml-4 flex-grow">
          <h3 className={`text-base font-medium ${getTextColor()}`}>{getStatusText()}</h3>
          {message && (
            <div className="mt-1 text-sm text-gray-700 max-w-prose">
              {message}
            </div>
          )}
        </div>
      </div>
      
      <div className="mt-3 w-full bg-gray-200 rounded-full h-2 overflow-hidden">
        <div 
          className={`${getProgressBarColor()} h-2 rounded-full transition-all duration-700 ease-in-out ${getProgressBarWidth()}`}
        ></div>
      </div>
    </div>
  );
} 