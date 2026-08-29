import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { vi } from 'vitest';
import ProgressDisplay from '../ProgressDisplay';

describe('ProgressDisplay Component', () => {
  const mockOnExecute = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  test('renders initial state correctly', () => {
    render(
      <ProgressDisplay
        progress={0}
        isProcessing={false}
        onExecute={mockOnExecute}
      />
    );

    expect(screen.getByText('Data Ingestion Progress')).toBeInTheDocument();
    expect(screen.getByText('Ready to start ingestion')).toBeInTheDocument();
    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuenow', '0');
    expect(screen.getByRole('button')).not.toBeDisabled();
  });

  test('renders processing state correctly', () => {
    render(
      <ProgressDisplay
        progress={45}
        isProcessing={true}
        onExecute={mockOnExecute}
      />
    );

    expect(screen.getByText('Data Ingestion Progress')).toBeInTheDocument();
    expect(screen.getByText('Processing: 45%')).toBeInTheDocument();
    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuenow', '45');
    expect(screen.getByRole('button')).toBeDisabled();
  });

  test('renders completed state correctly', () => {
    render(
      <ProgressDisplay
        progress={100}
        isProcessing={false}
        onExecute={mockOnExecute}
      />
    );

    expect(screen.getByText('Data Ingestion Progress')).toBeInTheDocument();
    expect(screen.getByText('Processing complete')).toBeInTheDocument();
    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuenow', '100');
    expect(screen.getByRole('button')).not.toBeDisabled();
  });

  test('handles execute button click', () => {
    render(
      <ProgressDisplay
        progress={0}
        isProcessing={false}
        onExecute={mockOnExecute}
      />
    );

    const executeButton = screen.getByRole('button');
    fireEvent.click(executeButton);

    expect(mockOnExecute).toHaveBeenCalledTimes(1);
  });

  test('disables execute button during processing', () => {
    render(
      <ProgressDisplay
        progress={50}
        isProcessing={true}
        onExecute={mockOnExecute}
      />
    );

    const executeButton = screen.getByRole('button');
    fireEvent.click(executeButton);

    expect(mockOnExecute).not.toHaveBeenCalled();
  });

  test('updates progress display correctly', () => {
    const { rerender } = render(
      <ProgressDisplay
        progress={0}
        isProcessing={false}
        onExecute={mockOnExecute}
      />
    );

    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuenow', '0');

    rerender(
      <ProgressDisplay
        progress={75}
        isProcessing={true}
        onExecute={mockOnExecute}
      />
    );

    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuenow', '75');
    expect(screen.getByText('Processing: 75%')).toBeInTheDocument();
  });
}); 