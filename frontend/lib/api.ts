import axios from 'axios';

// Types
export interface ClickHouseConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  jwtToken: string;
  secure: boolean;
}

export interface FlatFileConfig {
  delimiter: string;
  hasHeader: boolean;
  filePath?: string;
  fileName?: string;
}

export interface TableColumn {
  name: string;
  type: string;
  selected: boolean;
}

export interface TableInfo {
  name: string;
  columns: TableColumn[];
  selected: boolean;
}

export interface IngestionRequest {
  source: 'clickhouse' | 'flatfile';
  target: 'clickhouse' | 'flatfile';
  clickHouseConfig?: ClickHouseConfig;
  flatFileConfig?: FlatFileConfig;
  tables: TableInfo[];
  joinCondition?: string;
  useJoin: boolean;
}

export interface IngestionResponse {
  success: boolean;
  message: string;
  totalRecords: number;
  fileName?: string;
}

// API client
const api = {
  // ClickHouse endpoints
  testClickHouseConnection: async (config: ClickHouseConfig) => {
    const response = await axios.post('/api/clickhouse/test-connection', config);
    return response.data;
  },
  
  getClickHouseTables: async (config: ClickHouseConfig) => {
    const response = await axios.post<TableInfo[]>('/api/clickhouse/tables', config);
    return response.data;
  },
  
  // File endpoints
  uploadFile: async (file: File, delimiter: string, hasHeader: boolean) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('delimiter', delimiter);
    formData.append('hasHeader', hasHeader.toString());
    
    const response = await axios.post('/api/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    
    return response.data;
  },
  
  // Ingestion endpoints
  ingestData: async (request: IngestionRequest) => {
    const response = await axios.post<IngestionResponse>('/api/ingestion/ingest', request);
    return response.data;
  },
  
  previewData: async (request: IngestionRequest) => {
    const response = await axios.post('/api/ingestion/preview', request);
    return response.data;
  },
  
  // Download endpoint
  getDownloadUrl: (fileName: string) => {
    return `/api/download/${fileName}`;
  },
};

export default api; 