'use client';

import { useState } from 'react';
import { TableInfo } from '@/lib/api';

interface TableSelectionProps {
  tables: TableInfo[];
  onSubmit: (tables: TableInfo[], useJoin: boolean, joinCondition: string) => void;
  isMultiTableEnabled: boolean;
  isLoading: boolean;
}

export default function TableSelection({
  tables,
  onSubmit,
  isMultiTableEnabled,
  isLoading
}: TableSelectionProps) {
  const [selectedTables, setSelectedTables] = useState<TableInfo[]>(
    tables.map(table => ({ ...table, selected: table.selected || false }))
  );
  const [useJoin, setUseJoin] = useState<boolean>(false);
  const [joinCondition, setJoinCondition] = useState<string>('');

  const handleTableSelectionChange = (tableIndex: number, selected: boolean) => {
    const updatedTables = [...selectedTables];
    updatedTables[tableIndex].selected = selected;
    setSelectedTables(updatedTables);
  };

  const handleColumnSelectionChange = (tableIndex: number, columnIndex: number, selected: boolean) => {
    const updatedTables = [...selectedTables];
    updatedTables[tableIndex].columns[columnIndex].selected = selected;
    setSelectedTables(updatedTables);
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSubmit(selectedTables, useJoin, joinCondition);
  };

  const toggleSelectAll = (tableIndex: number, selected: boolean) => {
    const updatedTables = [...selectedTables];
    updatedTables[tableIndex].columns.forEach(col => {
      col.selected = selected;
    });
    setSelectedTables(updatedTables);
  };

  return (
    <div className="card">
      <h2 className="text-xl font-semibold mb-6">Select Tables and Columns</h2>
      <form onSubmit={handleSubmit}>
        {selectedTables.length === 0 ? (
          <div className="text-center py-8">
            <p className="text-gray-500">No tables available.</p>
          </div>
        ) : (
          <div className="space-y-6">
            {selectedTables.map((table, tableIndex) => (
              <div key={tableIndex} className="border border-gray-200 rounded-lg p-4">
                <div className="flex items-center mb-4">
                  {isMultiTableEnabled && (
                    <input
                      type="checkbox"
                      id={`table-${tableIndex}`}
                      className="h-4 w-4 text-primary focus:ring-primary border-gray-300 rounded"
                      checked={table.selected}
                      onChange={(e) => handleTableSelectionChange(tableIndex, e.target.checked)}
                    />
                  )}
                  <h3 className={`text-lg font-medium ${isMultiTableEnabled ? 'ml-2' : ''}`}>
                    {table.name}
                  </h3>
                  <div className="ml-auto">
                    <button
                      type="button"
                      className="text-sm text-primary hover:text-blue-700"
                      onClick={() => toggleSelectAll(tableIndex, true)}
                    >
                      Select All
                    </button>
                    <span className="mx-2 text-gray-300">|</span>
                    <button
                      type="button"
                      className="text-sm text-primary hover:text-blue-700"
                      onClick={() => toggleSelectAll(tableIndex, false)}
                    >
                      Clear All
                    </button>
                  </div>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
                  {table.columns.map((column, colIndex) => (
                    <div key={colIndex} className="flex items-center">
                      <input
                        type="checkbox"
                        id={`column-${tableIndex}-${colIndex}`}
                        className="h-4 w-4 text-primary focus:ring-primary border-gray-300 rounded"
                        checked={column.selected}
                        onChange={(e) => handleColumnSelectionChange(tableIndex, colIndex, e.target.checked)}
                      />
                      <label htmlFor={`column-${tableIndex}-${colIndex}`} className="ml-2 text-sm text-gray-700">
                        {column.name} <span className="text-xs text-gray-400">({column.type})</span>
                      </label>
                    </div>
                  ))}
                </div>
              </div>
            ))}

            {isMultiTableEnabled && selectedTables.filter(t => t.selected).length > 1 && (
              <div className="mt-6 p-4 border border-blue-200 bg-blue-50 rounded-lg">
                <div className="flex items-center mb-2">
                  <input
                    type="checkbox"
                    id="useJoin"
                    className="h-4 w-4 text-primary focus:ring-primary border-gray-300 rounded"
                    checked={useJoin}
                    onChange={(e) => setUseJoin(e.target.checked)}
                  />
                  <label htmlFor="useJoin" className="ml-2 text-sm font-medium text-gray-700">
                    Use JOIN for multiple tables
                  </label>
                </div>
                
                {useJoin && (
                  <div className="mt-3">
                    <label htmlFor="joinCondition" className="block text-sm font-medium text-gray-700 mb-1">
                      JOIN Condition
                    </label>
                    <input
                      type="text"
                      id="joinCondition"
                      className="form-input"
                      placeholder="e.g., table1.id = table2.table1_id"
                      value={joinCondition}
                      onChange={(e) => setJoinCondition(e.target.value)}
                    />
                    <p className="mt-1 text-xs text-gray-500">
                      Specify the JOIN condition between tables.
                    </p>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        <div className="mt-6 flex justify-end">
          <button
            type="submit"
            className="btn btn-primary"
            disabled={isLoading || selectedTables.length === 0 || !selectedTables.some(t => t.selected || !isMultiTableEnabled)}
          >
            {isLoading ? 'Processing...' : 'Preview Data'}
          </button>
        </div>
      </form>
    </div>
  );
} 