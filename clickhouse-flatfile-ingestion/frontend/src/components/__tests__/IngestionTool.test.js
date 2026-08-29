import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { vi } from 'vitest';
import IngestionTool from '../IngestionTool';
import ingestionService from '../../services/ingestionService';
import { toast } from 'react-toastify';

// Mock the ingestion service
vi.mock('../../services/ingestionService');

describe('IngestionTool Component', () => {
  const mockConnectionConfig = {
    host: 'localhost',
    port: 8123,
    database: 'test_db',
    user: 'test_user',
    password: 'test_pass'
  };

  const mockTableData = {
    tables: [
      { name: 'table1', columns: ['col1', 'col2'] },
      { name: 'table2', columns: ['col3', 'col4'] }
    ]
  };

  const mockSchemaData = {
    columns: [
      { name: 'col1', type: 'String' },
      { name: 'col2', type: 'Int32' }
    ]
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  test('renders source selection step initially', () => {
    render(<IngestionTool />);
    expect(screen.getByText('Select Source')).toBeInTheDocument();
  });

  test('handles source selection correctly', async () => {
    render(<IngestionTool />);
    
    // Select ClickHouse as source
    const clickhouseOption = screen.getByText('ClickHouse Database');
    fireEvent.click(clickhouseOption);
    
    // Should move to connection form
    expect(screen.getByText('Configure Connection')).toBeInTheDocument();
  });

  test('handles connection configuration correctly', async () => {
    ingestionService.getRecordCount.mockResolvedValue(1000);
    
    render(<IngestionTool />);
    
    // Select source and fill connection form
    const clickhouseOption = screen.getByText('ClickHouse Database');
    fireEvent.click(clickhouseOption);
    
    // Fill connection form
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText('Port'), { target: { value: '8123' } });
    fireEvent.change(screen.getByLabelText('Database'), { target: { value: 'test_db' } });
    fireEvent.change(screen.getByLabelText('User'), { target: { value: 'test_user' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'test_pass' } });
    
    // Submit form
    const connectButton = screen.getByText('Connect');
    fireEvent.click(connectButton);
    
    // Should move to schema selection
    await waitFor(() => {
      expect(screen.getByText('Select Schema')).toBeInTheDocument();
    });
  });

  test('handles schema selection correctly', async () => {
    ingestionService.getSchema.mockResolvedValue(mockTableData);
    
    render(<IngestionTool />);
    
    // Complete previous steps
    const clickhouseOption = screen.getByText('ClickHouse Database');
    fireEvent.click(clickhouseOption);
    
    // Fill and submit connection form
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText('Port'), { target: { value: '8123' } });
    fireEvent.change(screen.getByLabelText('Database'), { target: { value: 'test_db' } });
    fireEvent.change(screen.getByLabelText('User'), { target: { value: 'test_user' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'test_pass' } });
    fireEvent.click(screen.getByText('Connect'));
    
    // Select table
    await waitFor(() => {
      const table1 = screen.getByText('table1');
      fireEvent.click(table1);
    });
    
    // Should move to column selection
    expect(screen.getByText('Select Columns')).toBeInTheDocument();
  });

  test('handles column selection correctly', async () => {
    ingestionService.getSchema.mockResolvedValue(mockSchemaData);
    
    render(<IngestionTool />);
    
    // Complete previous steps and select columns
    // ... (previous steps)
    
    // Select columns
    const col1Checkbox = screen.getByLabelText('col1');
    const col2Checkbox = screen.getByLabelText('col2');
    fireEvent.click(col1Checkbox);
    fireEvent.click(col2Checkbox);
    
    // Confirm selection
    fireEvent.click(screen.getByText('Confirm Selection'));
    
    // Should move to data preview
    expect(screen.getByText('Preview Data')).toBeInTheDocument();
  });

  test('handles data ingestion execution correctly', async () => {
    ingestionService.exportToFile.mockResolvedValue({ jobId: '123' });
    ingestionService.getProgress.mockResolvedValue({ progress: 100, status: 'COMPLETED' });
    
    render(<IngestionTool />);
    
    // Complete previous steps and execute ingestion
    // ... (previous steps)
    
    // Execute ingestion
    const executeButton = screen.getByText('Execute');
    fireEvent.click(executeButton);
    
    // Should show success message
    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith('Ingestion completed successfully!');
    });
  });

  test('handles connection errors correctly', async () => {
    ingestionService.getRecordCount.mockRejectedValue(new Error('Connection failed'));
    
    render(<IngestionTool />);
    
    // Complete source selection and submit invalid connection
    const clickhouseOption = screen.getByText('ClickHouse Database');
    fireEvent.click(clickhouseOption);
    
    // Submit invalid connection
    fireEvent.click(screen.getByText('Connect'));
    
    // Should show error message
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Failed to get record count: Connection failed');
    });
  });

  test('handles schema loading errors correctly', async () => {
    ingestionService.getSchema.mockRejectedValue(new Error('Failed to load schema'));
    
    render(<IngestionTool />);
    
    // Complete previous steps and try to load schema
    // ... (previous steps)
    
    // Should show error message
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Failed to get schema: Failed to load schema');
    });
  });

  test('handles ingestion execution errors correctly', async () => {
    ingestionService.exportToFile.mockRejectedValue(new Error('Ingestion failed'));
    
    render(<IngestionTool />);
    
    // Complete previous steps and execute ingestion
    // ... (previous steps)
    
    // Execute ingestion
    const executeButton = screen.getByText('Execute');
    fireEvent.click(executeButton);
    
    // Should show error message
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Failed to start ingestion: Ingestion failed');
    });
  });
}); 