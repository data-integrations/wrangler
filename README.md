# ZeoTap Bidirectional ClickHouse & Flat File Data Ingestion Tool

This web application facilitates data ingestion between ClickHouse database and flat files. It supports bidirectional data flow, column selection, and multi-table joins.

## Features

- **Bidirectional Data Flow:** 
  - ClickHouse -> Flat File
  - Flat File -> ClickHouse
  
- **ClickHouse Integration:**
  - Connect using Host, Port, Database, User
  - JWT Token-based authentication
  - Support for both HTTP and HTTPS connections

- **Flat File Integration:**
  - Support for CSV files
  - Configurable delimiters
  - Header detection

- **Schema Discovery & Column Selection:**
  - View available tables and columns
  - Select specific columns for ingestion
  - Preserve data types

- **Multi-Table Join (Bonus Feature):**
  - Select multiple tables
  - Specify JOIN conditions
  - Combined data export

- **Additional Features:**
  - Data preview before ingestion
  - Record count reporting
  - Error handling

## Project Structure

The project is divided into two main parts:

1. **Backend (Java + Spring Boot):**
   - REST API for data ingestion operations
   - ClickHouse connectivity using JDBC
   - CSV file handling
   - Multi-table JOIN support

2. **Frontend (Next.js + React):**
   - User interface for configuring connections
   - Table/column selection interface
   - Data preview functionality
   - Progress reporting

## Setup and Installation

### Prerequisites

- Java 17+
- Node.js 16+
- npm 8+
- ClickHouse database (local or remote)

### Backend Setup

1. Navigate to the backend directory:
   ```bash
   cd backend
   ```

2. Build the project:
   ```bash
   ./mvnw clean package
   ```
   
3. Run the application:
   ```bash
   java -jar target/data-ingestion-tool-0.0.1-SNAPSHOT.jar
   ```

### Frontend Setup

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Run the development server:
   ```bash
   npm run dev
   ```

4. Access the application at http://localhost:3000

## Usage Guide

1. **Select Source and Target:**
   - Choose between ClickHouse → Flat File or Flat File → ClickHouse

2. **Configure Source:**
   - For ClickHouse: Provide connection details and JWT token
   - For Flat File: Upload a CSV file and configure delimiter settings

3. **Configure Target:**
   - For ClickHouse: Provide connection details
   - For Flat File: Specify file name and delimiter preferences

4. **Select Columns:**
   - Choose specific tables and columns to include
   - For multi-table ingestion, configure JOIN conditions

5. **Preview Data:**
   - Review a sample of the data before proceeding

6. **Start Ingestion:**
   - Begin the data transfer process
   - View progress and completion status

7. **Download Results:**
   - For Flat File targets, download the generated file
   - See total record count and processing summary

## Testing

The application can be tested with:

- ClickHouse example datasets (`uk_price_paid`, `ontime`)
- Any CSV file with proper formatting

## Security Considerations

- JWT tokens are used for secure authentication with ClickHouse
- File uploads are validated for security
- No sensitive information is logged

## License

This project is proprietary and confidential.

Copyright © 2025 ZeoTap. All rights reserved. 