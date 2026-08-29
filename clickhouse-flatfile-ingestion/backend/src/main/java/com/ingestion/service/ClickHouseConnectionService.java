package com.ingestion.service;

import com.ingestion.dto.ConnectionRequest;
import com.ingestion.model.ClickHouseConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class ClickHouseConnectionService {

    private final Map<String, ClickHouseDataSource> connectionPool = new ConcurrentHashMap<>();

    /**
     * Creates a connection to ClickHouse database
     *
     * @param connectionRequest Connection details
     * @return Connection ID
     */
    public String createConnection(ConnectionRequest connectionRequest) {
        String connectionId = generateConnectionId(connectionRequest);
        
        if (connectionPool.containsKey(connectionId)) {
            log.info("Connection {} already exists", connectionId);
            return connectionId;
        }

        try {
            ClickHouseProperties properties = new ClickHouseProperties();
            properties.setUser(connectionRequest.getUsername());
            properties.setPassword(connectionRequest.getPassword());
            
            String url = String.format("jdbc:clickhouse://%s:%s/%s", 
                    connectionRequest.getHost(), 
                    connectionRequest.getPort(), 
                    connectionRequest.getDatabase());
            
            ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
            
            // Test connection
            try (Connection connection = dataSource.getConnection()) {
                log.info("Successfully connected to ClickHouse database: {}", connectionRequest.getDatabase());
            }
            
            connectionPool.put(connectionId, dataSource);
            return connectionId;
        } catch (SQLException e) {
            log.error("Failed to create connection to ClickHouse: {}", e.getMessage());
            throw new RuntimeException("Failed to create connection to ClickHouse: " + e.getMessage());
        }
    }

    /**
     * Tests a connection to ClickHouse database
     *
     * @param connectionRequest Connection details
     * @return true if connection is successful, false otherwise
     */
    public boolean testConnection(ConnectionRequest connectionRequest) {
        try {
            ClickHouseProperties properties = new ClickHouseProperties();
            properties.setUser(connectionRequest.getUsername());
            properties.setPassword(connectionRequest.getPassword());
            
            String url = String.format("jdbc:clickhouse://%s:%s/%s", 
                    connectionRequest.getHost(), 
                    connectionRequest.getPort(), 
                    connectionRequest.getDatabase());
            
            ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
            
            try (Connection connection = dataSource.getConnection()) {
                log.info("Successfully tested connection to ClickHouse database: {}", connectionRequest.getDatabase());
                return true;
            }
        } catch (SQLException e) {
            log.error("Failed to test connection to ClickHouse: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Gets a connection from the pool
     *
     * @param connectionId Connection ID
     * @return Connection
     */
    public Connection getConnection(String connectionId) {
        ClickHouseDataSource dataSource = connectionPool.get(connectionId);
        if (dataSource == null) {
            throw new RuntimeException("Connection not found: " + connectionId);
        }
        
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            log.error("Failed to get connection: {}", e.getMessage());
            throw new RuntimeException("Failed to get connection: " + e.getMessage());
        }
    }

    /**
     * Gets all tables from a database
     *
     * @param connectionId Connection ID
     * @return List of table names
     */
    public List<String> getTables(String connectionId) {
        List<String> tables = new ArrayList<>();
        
        try (Connection connection = getConnection(connectionId);
             ResultSet resultSet = connection.getMetaData().getTables(null, null, "%", new String[]{"TABLE"})) {
            
            while (resultSet.next()) {
                tables.add(resultSet.getString("TABLE_NAME"));
            }
            
            return tables;
        } catch (SQLException e) {
            log.error("Failed to get tables: {}", e.getMessage());
            throw new RuntimeException("Failed to get tables: " + e.getMessage());
        }
    }

    /**
     * Gets the schema of a table
     *
     * @param connectionId Connection ID
     * @param tableName Table name
     * @return Map of column names to column types
     */
    public Map<String, String> getTableSchema(String connectionId, String tableName) {
        Map<String, String> schema = new HashMap<>();
        
        try (Connection connection = getConnection(connectionId);
             ResultSet resultSet = connection.getMetaData().getColumns(null, null, tableName, "%")) {
            
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                String columnType = resultSet.getString("TYPE_NAME");
                schema.put(columnName, columnType);
            }
            
            return schema;
        } catch (SQLException e) {
            log.error("Failed to get table schema: {}", e.getMessage());
            throw new RuntimeException("Failed to get table schema: " + e.getMessage());
        }
    }

    /**
     * Executes a query and returns the result
     *
     * @param connectionId Connection ID
     * @param query SQL query
     * @return ResultSet
     */
    public ResultSet executeQuery(String connectionId, String query) {
        try {
            Connection connection = getConnection(connectionId);
            return connection.createStatement().executeQuery(query);
        } catch (SQLException e) {
            log.error("Failed to execute query: {}", e.getMessage());
            throw new RuntimeException("Failed to execute query: " + e.getMessage());
        }
    }

    /**
     * Executes a query with parameters and returns the result
     *
     * @param connectionId Connection ID
     * @param query SQL query with parameters
     * @param parameters Query parameters
     * @return ResultSet
     */
    public ResultSet executeQuery(String connectionId, String query, Object... parameters) {
        try {
            Connection connection = getConnection(connectionId);
            java.sql.PreparedStatement preparedStatement = connection.prepareStatement(query);
            
            for (int i = 0; i < parameters.length; i++) {
                preparedStatement.setObject(i + 1, parameters[i]);
            }
            
            return preparedStatement.executeQuery();
        } catch (SQLException e) {
            log.error("Failed to execute query with parameters: {}", e.getMessage());
            throw new RuntimeException("Failed to execute query with parameters: " + e.getMessage());
        }
    }

    /**
     * Executes a batch insert
     *
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param columns Column names
     * @param values List of value arrays
     * @return Number of rows affected
     */
    public int executeBatchInsert(String connectionId, String tableName, List<String> columns, List<Object[]> values) {
        if (values.isEmpty()) {
            return 0;
        }
        
        try {
            Connection connection = getConnection(connectionId);
            connection.setAutoCommit(false);
            
            String columnList = String.join(", ", columns);
            String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));
            String query = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnList, placeholders);
            
            java.sql.PreparedStatement preparedStatement = connection.prepareStatement(query);
            
            for (Object[] row : values) {
                for (int i = 0; i < row.length; i++) {
                    preparedStatement.setObject(i + 1, row[i]);
                }
                preparedStatement.addBatch();
            }
            
            int[] results = preparedStatement.executeBatch();
            connection.commit();
            
            int totalRows = 0;
            for (int result : results) {
                totalRows += result;
            }
            
            return totalRows;
        } catch (SQLException e) {
            log.error("Failed to execute batch insert: {}", e.getMessage());
            throw new RuntimeException("Failed to execute batch insert: " + e.getMessage());
        }
    }

    /**
     * Closes a connection
     *
     * @param connectionId Connection ID
     */
    public void closeConnection(String connectionId) {
        ClickHouseDataSource dataSource = connectionPool.remove(connectionId);
        if (dataSource != null) {
            log.info("Connection {} closed", connectionId);
        }
    }

    /**
     * Generates a unique connection ID
     *
     * @param connectionRequest Connection details
     * @return Connection ID
     */
    private String generateConnectionId(ConnectionRequest connectionRequest) {
        return String.format("%s_%s_%s", 
                connectionRequest.getHost(), 
                connectionRequest.getPort(), 
                connectionRequest.getDatabase());
    }
} 