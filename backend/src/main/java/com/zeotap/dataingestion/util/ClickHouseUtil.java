package com.zeotap.dataingestion.util;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.zeotap.dataingestion.model.ClickHouseConfig;
import com.zeotap.dataingestion.model.TableColumn;
import com.zeotap.dataingestion.model.TableInfo;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
public class ClickHouseUtil {

    /**
     * Creates a ClickHouse JDBC connection with JWT token authentication
     */
    public Connection createConnection(ClickHouseConfig config) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", config.getUser());
        
        // Use JWT token for authentication if provided
        if (config.getJwtToken() != null && !config.getJwtToken().isEmpty()) {
            properties.setProperty("token", config.getJwtToken());
            properties.setProperty("auth", "Bearer");
        }

        // Build the JDBC URL
        String protocol = config.isSecure() ? "https" : "http";
        String jdbcUrl = String.format("jdbc:clickhouse:%s://%s:%d/%s", 
                protocol, config.getHost(), config.getPort(), config.getDatabase());
        
        ClickHouseDataSource dataSource = new ClickHouseDataSource(jdbcUrl, properties);
        return dataSource.getConnection();
    }
    
    /**
     * Gets a list of all tables in the ClickHouse database
     */
    public List<String> getTableNames(Connection connection) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        
        try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
            while (tables.next()) {
                tableNames.add(tables.getString("TABLE_NAME"));
            }
        }
        
        return tableNames;
    }
    
    /**
     * Gets the column information for a specific table
     */
    public TableInfo getTableInfo(Connection connection, String tableName) throws SQLException {
        List<TableColumn> columns = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        
        try (ResultSet columnsRs = metaData.getColumns(null, null, tableName, "%")) {
            while (columnsRs.next()) {
                String columnName = columnsRs.getString("COLUMN_NAME");
                String columnType = columnsRs.getString("TYPE_NAME");
                
                TableColumn column = new TableColumn(columnName, columnType, true);
                columns.add(column);
            }
        }
        
        return new TableInfo(tableName, columns, false);
    }
    
    /**
     * Gets all tables with their column information
     */
    public List<TableInfo> getAllTablesInfo(Connection connection) throws SQLException {
        List<TableInfo> tableInfos = new ArrayList<>();
        List<String> tableNames = getTableNames(connection);
        
        for (String tableName : tableNames) {
            TableInfo tableInfo = getTableInfo(connection, tableName);
            tableInfos.add(tableInfo);
        }
        
        return tableInfos;
    }
    
    /**
     * Builds a SQL query based on selected tables and columns
     */
    public String buildQuery(List<TableInfo> tables, String joinCondition, boolean useJoin) {
        StringBuilder sql = new StringBuilder("SELECT ");
        List<String> selectedColumns = new ArrayList<>();
        
        // If we're joining multiple tables, we need to prefix columns with table names
        boolean isMultiTable = tables.stream().filter(TableInfo::isSelected).count() > 1;
        
        for (TableInfo table : tables) {
            if (!table.isSelected()) continue;
            
            for (TableColumn column : table.getColumns()) {
                if (column.isSelected()) {
                    if (isMultiTable) {
                        selectedColumns.add(String.format("%s.%s", table.getName(), column.getName()));
                    } else {
                        selectedColumns.add(column.getName());
                    }
                }
            }
        }
        
        sql.append(String.join(", ", selectedColumns));
        sql.append(" FROM ");
        
        if (!useJoin || tables.stream().filter(TableInfo::isSelected).count() <= 1) {
            // Single table query
            sql.append(tables.stream()
                    .filter(TableInfo::isSelected)
                    .findFirst()
                    .map(TableInfo::getName)
                    .orElse(""));
        } else {
            // Multi-table query with join
            List<String> tableNames = tables.stream()
                    .filter(TableInfo::isSelected)
                    .map(TableInfo::getName)
                    .toList();
            
            sql.append(tableNames.get(0));
            sql.append(" JOIN ");
            sql.append(String.join(" JOIN ", tableNames.subList(1, tableNames.size())));
            
            if (joinCondition != null && !joinCondition.isEmpty()) {
                sql.append(" ON ").append(joinCondition);
            }
        }
        
        return sql.toString();
    }
    
    /**
     * Creates a table in ClickHouse based on column definitions
     */
    public void createTable(Connection connection, String tableName, List<TableColumn> columns) throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(tableName)
                .append(" (");
        
        List<String> columnDefinitions = new ArrayList<>();
        for (TableColumn column : columns) {
            columnDefinitions.add(column.getName() + " " + mapToClickHouseType(column.getType()));
        }
        
        sql.append(String.join(", ", columnDefinitions))
           .append(") ENGINE = MergeTree() ORDER BY tuple()");
        
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql.toString());
        }
    }
    
    /**
     * Maps common data types to ClickHouse types
     */
    private String mapToClickHouseType(String type) {
        // Simplistic mapping, can be expanded based on requirements
        type = type.toUpperCase();
        return switch (type) {
            case "VARCHAR", "STRING", "TEXT", "CHAR" -> "String";
            case "INT", "INTEGER", "INT32" -> "Int32";
            case "BIGINT", "INT64" -> "Int64";
            case "FLOAT", "FLOAT32" -> "Float32";
            case "DOUBLE", "FLOAT64" -> "Float64";
            case "BOOLEAN", "BOOL" -> "UInt8";
            case "DATE" -> "Date";
            case "DATETIME" -> "DateTime";
            default -> "String"; // Default to String for unknown types
        };
    }
} 