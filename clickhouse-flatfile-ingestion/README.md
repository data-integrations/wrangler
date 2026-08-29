# ClickHouse Flat File Ingestion Tool

A web-based application for bidirectional data ingestion between ClickHouse database and Flat File platform.

## Features

- Bidirectional data ingestion between ClickHouse and Flat Files
- JWT-based authentication
- Schema discovery and validation
- Progress tracking for large data transfers
- Support for various file formats (CSV, JSON, etc.)
- Configurable data mapping
- Error handling and logging

## Technology Stack

### Backend
- Spring Boot
- Spring Security with JWT
- ClickHouse JDBC Driver
- Apache Commons CSV
- Jackson for JSON processing

### Frontend
- React
- Material-UI
- Axios
- React Router
- React Query

## Prerequisites

- Java 17 or higher
- Node.js 16 or higher
- ClickHouse server
- PostgreSQL (for user management)

## Environment Variables

Create a `.env` file in the backend directory with the following variables:

```properties
# Database Configuration
DB_URL=jdbc:postgresql://localhost:5432/ingestion_db
DB_USERNAME=your_db_username
DB_PASSWORD=your_db_password

# ClickHouse Configuration
CLICKHOUSE_HOST=your_clickhouse_host
CLICKHOUSE_PORT=8443
CLICKHOUSE_DATABASE=your_database
CLICKHOUSE_USER=your_username
CLICKHOUSE_PASSWORD=your_password

# JWT Configuration
JWT_SECRET=your_jwt_secret_key

# File Upload Configuration
UPLOAD_DIR=./uploads
```

## Installation

1. Clone the repository
2. Set up environment variables
3. Build and run the backend:
   ```bash
   cd backend
   ./mvnw clean install
   ./mvnw spring-boot:run
   ```
4. Build and run the frontend:
   ```bash
   cd frontend
   npm install
   npm start
   ```

## Usage

1. Access the application at `http://localhost:3000`
2. Log in with your credentials
3. Select source (ClickHouse or Flat File)
4. Configure connection parameters
5. Select tables and columns
6. Preview data
7. Start ingestion process

## Security Considerations

- All sensitive information is stored in environment variables
- JWT tokens expire after 24 hours
- Passwords are hashed using BCrypt
- SSL/TLS encryption for database connections
- Input validation and sanitization
- Rate limiting on API endpoints

## API Documentation

### Authentication
- POST /api/auth/login - Login endpoint
- POST /api/auth/refresh - Refresh token endpoint

### Ingestion
- POST /api/ingestion/export - Export data from ClickHouse to file
- POST /api/ingestion/import - Import data from file to ClickHouse
- GET /api/ingestion/progress/{jobId} - Get ingestion progress
- GET /api/ingestion/schema - Get table schema
- GET /api/ingestion/preview - Get data preview

## Error Handling

The application includes comprehensive error handling for:
- Invalid credentials
- Connection failures
- Schema mismatches
- File format errors
- Data validation errors
- Network timeouts

## Logging

- Application logs are stored in `logs/application.log`
- Log levels can be configured in `application.properties`
- Structured logging format for better analysis

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 