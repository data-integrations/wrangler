# ClickHouse Data Ingestion Application

A web-based application for bidirectional data ingestion between ClickHouse database and Flat File platform.

## Features

- Bidirectional data flow (ClickHouse to File and File to ClickHouse)
- JWT token-based authentication
- Column selection for data ingestion
- Multi-table join support
- Data preview functionality
- Progress tracking for file uploads
- Record count reporting

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- ClickHouse server (local or Docker)
- Web browser (Chrome, Firefox, or Edge recommended)

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd clickhouse-data-ingestion
```

2. Configure ClickHouse connection:
Edit `src/main/resources/application.properties`:
```properties
clickhouse.url=jdbc:clickhouse://localhost:8123/default
clickhouse.username=default
clickhouse.password=your_password
```

3. Build the application:
```bash
mvn clean install
```

## Running the Application

1. Start the application:
```bash
mvn spring-boot:run
```

2. Access the web interface:
Open your browser and navigate to `http://localhost:8080/ingestion`

## Usage

### Exporting Data from ClickHouse

1. Select "Single Table" or "Multi-Table Join" tab
2. Enter table name(s) and select columns
3. For joins, specify join conditions
4. Choose file format (CSV or JSON)
5. Click "Preview Data" to verify the data
6. Click "Export" to download the file

### Importing Data to ClickHouse

1. Select a file to upload (CSV format)
2. Enter target table name
3. Specify column names
4. Click "Import" to start the process
5. Monitor progress in the progress bar
6. View success/error messages

## Testing

1. Run unit tests:
```bash
mvn test
```

2. Test cases covered:
   - Single table export
   - File import
   - Multi-table join
   - Connection failures
   - Data preview
   - Error handling

## Configuration

### Application Properties

- Server port: `server.port=8080`
- ClickHouse connection: `clickhouse.url`, `clickhouse.username`, `clickhouse.password`
- JWT settings: `jwt.secret`, `jwt.expiration`
- File upload limits: `spring.servlet.multipart.max-file-size`, `spring.servlet.multipart.max-request-size`

### Security

- JWT authentication is enabled by default
- Configure JWT secret and expiration in `application.properties`
- Update security settings in `SecurityConfig.java`

## Troubleshooting

1. Connection Issues:
   - Verify ClickHouse server is running
   - Check connection URL and credentials
   - Ensure network connectivity

2. File Upload Issues:
   - Check file size limits
   - Verify file format (CSV)
   - Ensure proper column mapping

3. Authentication Issues:
   - Verify JWT configuration
   - Check token expiration
   - Ensure proper credentials

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 