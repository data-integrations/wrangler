import { render } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from '@mui/material/styles';
import { theme } from '../theme';

// Custom render function that includes providers
export function renderWithProviders(ui, { route = '/' } = {}) {
  window.history.pushState({}, 'Test page', route);
  
  return render(
    <BrowserRouter>
      <ThemeProvider theme={theme}>
        {ui}
      </ThemeProvider>
    </BrowserRouter>
  );
}

// Mock API response helper
export function mockApiResponse(data, status = 200) {
  return {
    data,
    status,
    statusText: status === 200 ? 'OK' : 'Error',
    headers: {},
    config: {}
  };
}

// Mock API error helper
export function mockApiError(message, status = 500) {
  const error = new Error(message);
  error.response = {
    data: { message },
    status,
    statusText: 'Error',
    headers: {},
    config: {}
  };
  return error;
}

// Mock file upload helper
export function createMockFile(name, type, content) {
  return new File([content], name, { type });
}

// Mock progress event helper
export function createProgressEvent(loaded, total) {
  return {
    loaded,
    total,
    lengthComputable: true
  };
}

// Mock WebSocket helper
export function createMockWebSocket() {
  const listeners = {};
  return {
    addEventListener: (event, callback) => {
      listeners[event] = callback;
    },
    removeEventListener: (event) => {
      delete listeners[event];
    },
    send: vi.fn(),
    close: vi.fn(),
    _trigger: (event, data) => {
      if (listeners[event]) {
        listeners[event](data);
      }
    }
  };
} 