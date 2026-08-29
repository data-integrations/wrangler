import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { vi } from 'vitest';
import SchemaSelection from '../SchemaSelection';
import { toast } from 'react-toastify';
import axios from 'axios';

vi.mock('axios');

describe('SchemaSelection Component', () => {
  const mockOnTableSelect = vi.fn();
  const mockConnectionConfig = {
    host: 'localhost',
    port: 8123,
    database: 'test_db',
    user: 'test_user',
    password: 'test_pass'
  };

  const mockTables = [
    { name: 'users', rowCount: 1000 },
    { name: 'orders', rowCount: 5000 },
    { name: 'products', rowCount: 200 }
  ];

  beforeEach(() => {
    vi.clearAllMocks();
  });

  test('renders loading state initially', () => {
    render(
      <SchemaSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    expect(screen.getByText('Loading tables...')).toBeInTheDocument();
  });

  test('renders tables from ClickHouse correctly', async () => {
    axios.get.mockResolvedValueOnce({ data: mockTables });
    
    render(
      <SchemaSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    await waitFor(() => {
      mockTables.forEach(table => {
        expect(screen.getByText(table.name)).toBeInTheDocument();
        expect(screen.getByText(`${table.rowCount.toLocaleString()} rows`)).toBeInTheDocument();
      });
    });
  });

  test('renders tables from flat file correctly', async () => {
    const mockFileTables = [
      { name: 'data.csv', rowCount: 100 }
    ];
    axios.get.mockResolvedValueOnce({ data: mockFileTables });
    
    render(
      <SchemaSelection
        sourceType="flatfile"
        connectionConfig={{ fileId: 'test-file-id' }}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    await waitFor(() => {
      expect(screen.getByText('data.csv')).toBeInTheDocument();
      expect(screen.getByText('100 rows')).toBeInTheDocument();
    });
  });

  test('handles table selection correctly', async () => {
    axios.get.mockResolvedValueOnce({ data: mockTables });
    
    render(
      <SchemaSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    await waitFor(() => {
      const tableButton = screen.getByText('users');
      fireEvent.click(tableButton);
    });
    
    expect(mockOnTableSelect).toHaveBeenCalledWith('users');
  });

  test('handles API error for ClickHouse tables', async () => {
    const errorMessage = 'Failed to fetch tables';
    axios.get.mockRejectedValueOnce(new Error(errorMessage));
    
    render(
      <SchemaSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(`Error fetching tables: ${errorMessage}`);
    });
    expect(screen.getByText('No tables available')).toBeInTheDocument();
  });

  test('handles API error for flat file tables', async () => {
    const errorMessage = 'Failed to fetch file tables';
    axios.get.mockRejectedValueOnce(new Error(errorMessage));
    
    render(
      <SchemaSelection
        sourceType="flatfile"
        connectionConfig={{ fileId: 'test-file-id' }}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(`Error fetching tables: ${errorMessage}`);
    });
    expect(screen.getByText('No tables available')).toBeInTheDocument();
  });

  test('displays empty state when no tables are available', async () => {
    axios.get.mockResolvedValueOnce({ data: [] });
    
    render(
      <SchemaSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    await waitFor(() => {
      expect(screen.getByText('No tables available')).toBeInTheDocument();
    });
  });

  test('refreshes tables when connection config changes', async () => {
    const { rerender } = render(
      <SchemaSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(1);
    });
    
    const newConfig = { ...mockConnectionConfig, database: 'new_db' };
    rerender(
      <SchemaSelection
        sourceType="clickhouse"
        connectionConfig={newConfig}
        onTableSelect={mockOnTableSelect}
      />
    );
    
    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(2);
    });
  });
}); 