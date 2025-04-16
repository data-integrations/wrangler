'use client';

import { IngestionResponse } from '@/lib/api';

interface IngestionResultsProps {
  results: IngestionResponse | null;
  onReset: () => void;
}

export default function IngestionResults({
  results,
  onReset
}: IngestionResultsProps) {
  if (!results) {
    return (
      <div className="card">
        <h2 className="text-xl font-semibold mb-6">No Results Yet</h2>
        <p className="text-gray-500">No ingestion results available.</p>
        <div className="mt-6">
          <button
            type="button"
            className="btn btn-primary"
            onClick={onReset}
          >
            Start Over
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className={`card ${results.success ? 'border-green-200' : 'border-red-200'}`}>
      <div className="flex items-center mb-6">
        <div className={`w-12 h-12 rounded-full flex items-center justify-center ${
          results.success ? 'bg-green-100 text-green-600' : 'bg-red-100 text-red-600'
        }`}>
          {results.success ? (
            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
          ) : (
            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          )}
        </div>
        <h2 className="text-xl font-semibold ml-4">
          {results.success ? 'Ingestion Completed Successfully' : 'Ingestion Failed'}
        </h2>
      </div>

      <div className="mb-6 p-4 bg-gray-50 rounded-lg">
        <p className="text-gray-700">{results.message}</p>
      </div>

      {results.success && (
        <div className="mb-6">
          <div className="mb-4">
            <h3 className="text-lg font-medium">Summary</h3>
          </div>
          <div className="bg-blue-50 p-4 rounded-lg">
            <div className="flex justify-between">
              <span className="text-gray-700">Total Records Processed:</span>
              <span className="font-semibold">{results.totalRecords.toLocaleString()}</span>
            </div>
          </div>
        </div>
      )}

      {results.success && results.fileName && (
        <div className="mb-6">
          <div className="mb-2">
            <h3 className="text-lg font-medium">Download</h3>
          </div>
          <div className="bg-green-50 p-4 rounded-lg flex items-center justify-between">
            <span className="text-gray-700">File available for download:</span>
            <a
              href={results.fileName}
              className="btn btn-primary text-sm"
              download
            >
              Download File
            </a>
          </div>
        </div>
      )}

      <div className="mt-6">
        <button
          type="button"
          className="btn btn-accent"
          onClick={onReset}
        >
          Start New Ingestion
        </button>
      </div>
    </div>
  );
} 