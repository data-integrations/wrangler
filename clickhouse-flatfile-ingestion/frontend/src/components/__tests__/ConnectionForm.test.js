import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { vi } from 'vitest';
import ConnectionForm from '../ConnectionForm';
import { toast } from 'react-toastify';

describe('ConnectionForm Component', () => {
  const mockOnSubmit = vi.fn();
  const mockOnFileUpload = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  test('renders ClickHouse connection form when source is clickhouse', () => {
    render(<ConnectionForm sourceType="clickhouse" onSubmit={mockOnSubmit} />);
    
    expect(screen.getByLabelText('Host')).toBeInTheDocument();
    expect(screen.getByLabelText('Port')).toBeInTheDocument();
    expect(screen.getByLabelText('Database')).toBeInTheDocument();
    expect(screen.getByLabelText('User')).toBeInTheDocument();
    expect(screen.getByLabelText('Password')).toBeInTheDocument();
    expect(screen.getByText('Connect')).toBeInTheDocument();
  });

  test('renders file upload form when source is flatfile', () => {
    render(<ConnectionForm sourceType="flatfile" onFileUpload={mockOnFileUpload} />);
    
    expect(screen.getByText('Drag and drop a file here, or click to select')).toBeInTheDocument();
    expect(screen.getByText('Supported formats: CSV, JSON, Excel')).toBeInTheDocument();
  });

  test('handles file drop correctly', async () => {
    render(<ConnectionForm sourceType="flatfile" onFileUpload={mockOnFileUpload} />);
    
    const file = new File(['test data'], 'test.csv', { type: 'text/csv' });
    const dropzone = screen.getByText('Drag and drop a file here, or click to select');
    
    // Simulate file drop
    fireEvent.drop(dropzone, {
      dataTransfer: {
        files: [file]
      }
    });
    
    await waitFor(() => {
      expect(mockOnFileUpload).toHaveBeenCalledWith(file);
    });
  });

  test('handles file selection through click correctly', async () => {
    render(<ConnectionForm sourceType="flatfile" onFileUpload={mockOnFileUpload} />);
    
    const file = new File(['test data'], 'test.csv', { type: 'text/csv' });
    const input = screen.getByTestId('file-input');
    
    // Simulate file selection
    fireEvent.change(input, {
      target: {
        files: [file]
      }
    });
    
    await waitFor(() => {
      expect(mockOnFileUpload).toHaveBeenCalledWith(file);
    });
  });

  test('validates file type correctly', async () => {
    render(<ConnectionForm sourceType="flatfile" onFileUpload={mockOnFileUpload} />);
    
    const invalidFile = new File(['test data'], 'test.txt', { type: 'text/plain' });
    const dropzone = screen.getByText('Drag and drop a file here, or click to select');
    
    // Simulate invalid file drop
    fireEvent.drop(dropzone, {
      dataTransfer: {
        files: [invalidFile]
      }
    });
    
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Invalid file type. Please upload a CSV, JSON, or Excel file.');
    });
    expect(mockOnFileUpload).not.toHaveBeenCalled();
  });

  test('handles ClickHouse connection submission correctly', async () => {
    render(<ConnectionForm sourceType="clickhouse" onSubmit={mockOnSubmit} />);
    
    // Fill connection form
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText('Port'), { target: { value: '8123' } });
    fireEvent.change(screen.getByLabelText('Database'), { target: { value: 'test_db' } });
    fireEvent.change(screen.getByLabelText('User'), { target: { value: 'test_user' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'test_pass' } });
    
    // Submit form
    fireEvent.click(screen.getByText('Connect'));
    
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith({
        host: 'localhost',
        port: 8123,
        database: 'test_db',
        user: 'test_user',
        password: 'test_pass'
      });
    });
  });

  test('validates required fields in ClickHouse form', async () => {
    render(<ConnectionForm sourceType="clickhouse" onSubmit={mockOnSubmit} />);
    
    // Submit form without filling required fields
    fireEvent.click(screen.getByText('Connect'));
    
    await waitFor(() => {
      expect(screen.getByText('Host is required')).toBeInTheDocument();
      expect(screen.getByText('Port is required')).toBeInTheDocument();
      expect(screen.getByText('Database is required')).toBeInTheDocument();
      expect(screen.getByText('User is required')).toBeInTheDocument();
      expect(screen.getByText('Password is required')).toBeInTheDocument();
    });
    expect(mockOnSubmit).not.toHaveBeenCalled();
  });

  test('validates port number format', async () => {
    render(<ConnectionForm sourceType="clickhouse" onSubmit={mockOnSubmit} />);
    
    // Fill form with invalid port
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText('Port'), { target: { value: 'invalid' } });
    fireEvent.change(screen.getByLabelText('Database'), { target: { value: 'test_db' } });
    fireEvent.change(screen.getByLabelText('User'), { target: { value: 'test_user' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'test_pass' } });
    
    // Submit form
    fireEvent.click(screen.getByText('Connect'));
    
    await waitFor(() => {
      expect(screen.getByText('Port must be a valid number')).toBeInTheDocument();
    });
    expect(mockOnSubmit).not.toHaveBeenCalled();
  });

  test('handles file upload errors correctly', async () => {
    const mockError = new Error('Upload failed');
    mockOnFileUpload.mockRejectedValue(mockError);
    
    render(<ConnectionForm sourceType="flatfile" onFileUpload={mockOnFileUpload} />);
    
    const file = new File(['test data'], 'test.csv', { type: 'text/csv' });
    const dropzone = screen.getByText('Drag and drop a file here, or click to select');
    
    // Simulate file drop
    fireEvent.drop(dropzone, {
      dataTransfer: {
        files: [file]
      }
    });
    
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith('Failed to upload file: Upload failed');
    });
  });
}); 