// src/services/api.js
import { ClickHouse } from '@clickhouse/client';
import { createReadStream, createWriteStream } from 'fs';
import fs from 'fs/promises';
import csv from 'csv-parser';
import stringify from 'csv-stringify';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

class DataIngestionService {
  constructor() {
    this.clickhouse = null;
    this.fileConfig = null;
  }

  // ClickHouse Connection
  async connectClickHouse(config) {
    try {
      this.clickhouse = new ClickHouse({
        host: `${config.protocol}://${config.host}:${config.port}`,
        username: config.user,
        password: config.jwtToken,
        database: config.database,
        clickhouse_settings: {
          async_insert: 1,
          wait_for_async_insert: 0,
        },
      });

      await this.clickhouse.ping();
      return { success: true, message: 'ClickHouse connected successfully' };
    } catch (error) {
      console.error('Connection error:', error);
      return { success: false, message: error.message };
    }
  }

  // Flat File Connection
  async connectFlatFile(config) {
    try {
      const filePath = path.resolve(__dirname, '../../uploads', config.fileName);
      await fs.access(filePath);
      
      this.fileConfig = {
        path: filePath,
        delimiter: config.delimiter,
        columns: await this.inferSchema(filePath, config.delimiter)
      };

      return { 
        success: true,
        message: 'File connected successfully',
        columns: this.fileConfig.columns
      };
    } catch (error) {
      console.error('File connection error:', error);
      return { success: false, message: error.message };
    }
  }

  // Schema Inference
  async inferSchema(filePath, delimiter) {
    return new Promise((resolve, reject) => {
      const columns = [];
      const stream = createReadStream(filePath)
        .pipe(csv({ separator: delimiter }))
        .on('headers', (headers) => {
          headers.forEach(header => {
            columns.push({
              name: header,
              type: 'String',
              examples: []
            });
          });
        })
        .on('data', (row) => {
          columns.forEach(col => {
            const value = row[col.name];
            if (value !== undefined && col.examples.length < 5) {
              col.examples.push(value);
              col.type = this.detectType(value, col.type);
            }
          });
        })
        .on('end', () => resolve(columns))
        .on('error', reject);
    });
  }

  detectType(value, currentType) {
    if (currentType === 'String') {
      if (/^\d{4}-\d{2}-\d{2}/.test(value)) return 'Date';
      if (!isNaN(value)) return value.includes('.') ? 'Float64' : 'Int64';
      if (['true', 'false'].includes(value.toLowerCase())) return 'Bool';
    }
    return currentType;
  }

  // Data Operations
  async getTables() {
    try {
      const result = await this.clickhouse.query({
        query: 'SHOW TABLES',
        format: 'JSONEachRow'
      });
      return (await result.json()).map(t => t.name);
    } catch (error) {
      throw new Error(`Failed to fetch tables: ${error.message}`);
    }
  }

  async getTableSchema(tableName) {
    try {
      const result = await this.clickhouse.query({
        query: `DESCRIBE TABLE ${tableName}`,
        format: 'JSONEachRow'
      });
      return (await result.json()).map(col => ({
        name: col.name,
        type: col.type
      }));
    } catch (error) {
      throw new Error(`Schema fetch failed: ${error.message}`);
    }
  }

  async exportToFile(tableName, columns, fileType = 'csv') {
    try {
      const query = `SELECT ${columns.join(',')} FROM ${tableName}`;
      const result = await this.clickhouse.query({ query });
      const data = await result.json();

      const outputPath = path.resolve(__dirname, '../../exports', 
        `${tableName}_${Date.now()}.${fileType}`);

      await this.writeFile(outputPath, data, columns);
      return { success: true, path: outputPath, count: data.length };
    } catch (error) {
      throw new Error(`Export failed: ${error.message}`);
    }
  }

  async importToClickHouse(tableName, columns, createTable = true) {
    try {
      if (createTable) {
        await this.createTable(tableName, columns);
      }

      const insertStream = this.clickhouse.insert({
        table: tableName,
        format: 'JSONEachRow',
        values: this.generateDataStream()
      });

      let count = 0;
      for await (const row of this.readFile()) {
        insertStream.write(row);
        count++;
      }

      await insertStream.end();
      return { success: true, count };
    } catch (error) {
      throw new Error(`Import failed: ${error.message}`);
    }
  }

  // Helper Methods
  async createTable(tableName, columns) {
    const columnDefs = columns.map(col => 
      `${col.name} ${col.type}`).join(', ');
    
    await this.clickhouse.command({
      query: `CREATE TABLE IF NOT EXISTS ${tableName} (${columnDefs})
              ENGINE = MergeTree()
              ORDER BY tuple()`
    });
  }

  async writeFile(path, data, columns) {
    return new Promise((resolve, reject) => {
      const stringifier = stringify({ header: true, columns });
      const writer = createWriteStream(path);
      
      stringifier.pipe(writer);
      data.forEach(row => stringifier.write(row));
      stringifier.end();

      writer.on('finish', resolve);
      writer.on('error', reject);
    });
  }

  async *generateDataStream() {
    for await (const row of this.readFile()) {
      yield row;
    }
  }

  async *readFile() {
    try {
      const stream = createReadStream(this.fileConfig.path)
        .pipe(csv({ 
          separator: this.fileConfig.delimiter,
          strict: true
        }));

      for await (const row of stream) {
        yield row;
      }
    } catch (error) {
      throw new Error(`File read error: ${error.message}`);
    }
  }

  // Connection Management
  async closeConnection() {
    if (this.clickhouse) {
      await this.clickhouse.close();
      this.clickhouse = null;
    }
    this.fileConfig = null;
  }
}

// Singleton instance
const apiService = new DataIngestionService();

// Named exports
export const connectClickHouse = (config) => apiService.connectClickHouse(config);
export const connectFlatFile = (config) => apiService.connectFlatFile(config);

// Default export
export default apiService;