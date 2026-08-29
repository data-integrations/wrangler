import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { vi } from 'vitest';
import DataPreview from '../DataPreview';
import { toast } from 'react-toastify';
import axios from 'axios';

vi.mock('axios');

describe('DataPreview Component', () => {
  const mockConnectionConfig = {
    host: 'localhost',
    port: 8123,
    database: 'test_db',
    user: 'test_user',
    password: 'test_pass'
  };

  const mockPreviewData = {
    columns: ['id', 'name', 'email', 'created_at'],
    data: [
      { id: 1, name: 'John Doe', email: 'john@example.com', created_at: '2024-01-01' },
      { id: 2, name: 'Jane Smith', email: 'jane@example.com', created_at: '2024-01-02' },
      { id: 3, name: 'Bob Johnson', email: 'bob@example.com', created_at: '2024-01-03' }
    ],
    totalRows: 3
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  test('renders loading state initially', () => {
    render(
      <DataPreview
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    expect(screen.getByText('Loading preview data...')).toBeInTheDocument();
  });

  test('renders preview data from ClickHouse correctly', async () => {
    axios.get.mockResolvedValueOnce({ data: mockPreviewData });
    
    render(
      <DataPreview
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    await waitFor(() => {
      mockPreviewData.columns.forEach(column => {
        expect(screen.getByText(column)).toBeInTheDocument();
      });
      
      mockPreviewData.data.forEach(row => {
        expect(screen.getByText(row.id.toString())).toBeInTheDocument();
        expect(screen.getByText(row.name)).toBeInTheDocument();
        expect(screen.getByText(row.email)).toBeInTheDocument();
      });
    });
  });

  test('renders preview data from flat file correctly', async () => {
    axios.get.mockResolvedValueOnce({ data: mockPreviewData });
    
    render(
      <DataPreview
        sourceType="flatfile"
        connectionConfig={{ fileId: 'test-file-id' }}
        selectedTable="data.csv"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    await waitFor(() => {
      mockPreviewData.columns.forEach(column => {
        expect(screen.getByText(column)).toBeInTheDocument();
      });
      
      mockPreviewData.data.forEach(row => {
        expect(screen.getByText(row.id.toString())).toBeInTheDocument();
        expect(screen.getByText(row.name)).toBeInTheDocument();
        expect(screen.getByText(row.email)).toBeInTheDocument();
      });
    });
  });

  test('handles pagination correctly', async () => {
    const mockPaginatedData = {
      ...mockPreviewData,
      data: mockPreviewData.data.slice(0, 2),
      totalRows: 5
    };
    axios.get.mockResolvedValueOnce({ data: mockPaginatedData });
    
    render(
      <DataPreview
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    await waitFor(() => {
      expect(screen.getByText('1-2 of 5')).toBeInTheDocument();
    });
    
    const nextButton = screen.getByLabelText('Next page');
    fireEvent.click(nextButton);
    
    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(2);
    });
  });

  test('handles rows per page change', async () => {
    axios.get.mockResolvedValueOnce({ data: mockPreviewData });
    
    render(
      <DataPreview
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    await waitFor(() => {
      const rowsPerPageSelect = screen.getByLabelText('Rows per page:');
      fireEvent.change(rowsPerPageSelect, { target: { value: '25' } });
    });
    
    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(2);
    });
  });

  test('handles API error for ClickHouse preview', async () => {
    const errorMessage = 'Failed to fetch preview data';
    axios.get.mockRejectedValueOnce(new Error(errorMessage));
    
    render(
      <DataPreview
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(`Error fetching preview data: ${errorMessage}`);
    });
    expect(screen.getByText('No preview data available')).toBeInTheDocument();
  });

  test('handles API error for flat file preview', async () => {
    const errorMessage = 'Failed to fetch file preview';
    axios.get.mockRejectedValueOnce(new Error(errorMessage));
    
    render(
      <DataPreview
        sourceType="flatfile"
        connectionConfig={{ fileId: 'test-file-id' }}
        selectedTable="data.csv"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(`Error fetching preview data: ${errorMessage}`);
    });
    expect(screen.getByText('No preview data available')).toBeInTheDocument();
  });

  test('displays empty state when no preview data is available', async () => {
    axios.get.mockResolvedValueOnce({ data: { columns: [], data: [], totalRows: 0 } });
    
    render(
      <DataPreview
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    await waitFor(() => {
      expect(screen.getByText('No preview data available')).toBeInTheDocument();
    });
  });

  test('refreshes preview when selected columns change', async () => {
    const { rerender } = render(
      <DataPreview
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        selectedColumns={['id', 'name']}
      />
    );
    
    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(1);
    });
    
    rerender(
      <DataPreview
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        selectedColumns={['id', 'name', 'email']}
      />
    );
    
    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(2);
    });
  });
}); 