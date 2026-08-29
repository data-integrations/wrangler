import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { vi } from 'vitest';
import ColumnSelection from '../ColumnSelection';
import { toast } from 'react-toastify';
import axios from 'axios';

vi.mock('axios');

describe('ColumnSelection Component', () => {
  const mockOnColumnsSelect = vi.fn();
  const mockConnectionConfig = {
    host: 'localhost',
    port: 8123,
    database: 'test_db',
    user: 'test_user',
    password: 'test_pass'
  };

  const mockColumns = [
    { name: 'id', type: 'Int32', nullable: false },
    { name: 'name', type: 'String', nullable: true },
    { name: 'email', type: 'String', nullable: true },
    { name: 'created_at', type: 'DateTime', nullable: false }
  ];

  beforeEach(() => {
    vi.clearAllMocks();
  });

  test('renders loading state initially', () => {
    render(
      <ColumnSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    expect(screen.getByText('Loading columns...')).toBeInTheDocument();
  });

  test('renders columns from ClickHouse correctly', async () => {
    axios.get.mockResolvedValueOnce({ data: mockColumns });
    
    render(
      <ColumnSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      mockColumns.forEach(column => {
        expect(screen.getByText(column.name)).toBeInTheDocument();
        expect(screen.getByText(column.type)).toBeInTheDocument();
        expect(screen.getByText(column.nullable ? 'Nullable' : 'Required')).toBeInTheDocument();
      });
    });
  });

  test('renders columns from flat file correctly', async () => {
    const mockFileColumns = [
      { name: 'id', type: 'number' },
      { name: 'name', type: 'string' }
    ];
    axios.get.mockResolvedValueOnce({ data: mockFileColumns });
    
    render(
      <ColumnSelection
        sourceType="flatfile"
        connectionConfig={{ fileId: 'test-file-id' }}
        selectedTable="data.csv"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      mockFileColumns.forEach(column => {
        expect(screen.getByText(column.name)).toBeInTheDocument();
        expect(screen.getByText(column.type)).toBeInTheDocument();
      });
    });
  });

  test('handles individual column selection', async () => {
    axios.get.mockResolvedValueOnce({ data: mockColumns });
    
    render(
      <ColumnSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      const checkbox = screen.getByLabelText('id');
      fireEvent.click(checkbox);
    });
    
    expect(mockOnColumnsSelect).toHaveBeenCalledWith(['id']);
  });

  test('handles select all functionality', async () => {
    axios.get.mockResolvedValueOnce({ data: mockColumns });
    
    render(
      <ColumnSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      const selectAllCheckbox = screen.getByLabelText('Select All');
      fireEvent.click(selectAllCheckbox);
    });
    
    expect(mockOnColumnsSelect).toHaveBeenCalledWith(mockColumns.map(col => col.name));
  });

  test('handles API error for ClickHouse columns', async () => {
    const errorMessage = 'Failed to fetch columns';
    axios.get.mockRejectedValueOnce(new Error(errorMessage));
    
    render(
      <ColumnSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(`Error fetching columns: ${errorMessage}`);
    });
    expect(screen.getByText('No columns available')).toBeInTheDocument();
  });

  test('handles API error for flat file columns', async () => {
    const errorMessage = 'Failed to fetch file columns';
    axios.get.mockRejectedValueOnce(new Error(errorMessage));
    
    render(
      <ColumnSelection
        sourceType="flatfile"
        connectionConfig={{ fileId: 'test-file-id' }}
        selectedTable="data.csv"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(`Error fetching columns: ${errorMessage}`);
    });
    expect(screen.getByText('No columns available')).toBeInTheDocument();
  });

  test('displays empty state when no columns are available', async () => {
    axios.get.mockResolvedValueOnce({ data: [] });
    
    render(
      <ColumnSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      expect(screen.getByText('No columns available')).toBeInTheDocument();
    });
  });

  test('refreshes columns when table selection changes', async () => {
    const { rerender } = render(
      <ColumnSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="users"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(1);
    });
    
    rerender(
      <ColumnSelection
        sourceType="clickhouse"
        connectionConfig={mockConnectionConfig}
        selectedTable="orders"
        onColumnsSelect={mockOnColumnsSelect}
      />
    );
    
    await waitFor(() => {
      expect(axios.get).toHaveBeenCalledTimes(2);
    });
  });
}); 