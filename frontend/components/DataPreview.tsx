'use client';

import { useState } from 'react';

interface DataPreviewProps {
  data: string[][];
  onStartIngestion: () => void;
  onBack: () => void;
  isLoading: boolean;
}

export default function DataPreview({
  data,
  onStartIngestion,
  onBack,
  isLoading
}: DataPreviewProps) {
  const [page, setPage] = useState(1);
  const rowsPerPage = 10;
  
  // Calculate total pages
  const totalPages = Math.ceil((data.length - 1) / rowsPerPage); // Subtract header row
  
  // Get header row (first row)
  const headerRow = data.length > 0 ? data[0] : [];
  
  // Get current page's data
  const startIndex = 1 + (page - 1) * rowsPerPage; // Skip header row
  const endIndex = Math.min(startIndex + rowsPerPage, data.length);
  const currentPageData = data.slice(startIndex, endIndex);
  
  const goToPage = (newPage: number) => {
    setPage(Math.max(1, Math.min(newPage, totalPages)));
  };

  return (
    <div className="card">
      <h2 className="text-xl font-semibold mb-6">Data Preview</h2>
      
      <div className="mb-4">
        <span className="text-sm text-gray-600">
          Showing first 100 records (preview). Total records: {data.length > 1 ? data.length - 1 : 0}
        </span>
      </div>
      
      {data.length === 0 ? (
        <div className="py-8 text-center text-gray-500">
          No data available for preview.
        </div>
      ) : (
        <>
          <div className="overflow-x-auto rounded-lg border border-gray-200">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  {headerRow.map((header, index) => (
                    <th 
                      key={index} 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                    >
                      {header}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {currentPageData.map((row, rowIndex) => (
                  <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                    {row.map((cell, cellIndex) => (
                      <td key={cellIndex} className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {cell || <span className="text-gray-300">null</span>}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          
          {totalPages > 1 && (
            <div className="mt-4 flex items-center justify-between">
              <button
                className="px-3 py-1 border border-gray-300 rounded-md text-sm"
                onClick={() => goToPage(page - 1)}
                disabled={page === 1}
              >
                Previous
              </button>
              <span className="text-sm text-gray-600">
                Page {page} of {totalPages}
              </span>
              <button
                className="px-3 py-1 border border-gray-300 rounded-md text-sm"
                onClick={() => goToPage(page + 1)}
                disabled={page === totalPages}
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
      
      <div className="mt-6 flex justify-between">
        <button
          type="button"
          className="btn bg-gray-500 hover:bg-gray-600"
          onClick={onBack}
          disabled={isLoading}
        >
          Back
        </button>
        <button
          type="button"
          className="btn btn-primary"
          onClick={onStartIngestion}
          disabled={isLoading || data.length <= 1}
        >
          {isLoading ? 'Processing...' : 'Start Ingestion'}
        </button>
      </div>
    </div>
  );
} 