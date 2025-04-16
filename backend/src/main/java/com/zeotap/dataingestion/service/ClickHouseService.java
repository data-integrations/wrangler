package com.zeotap.dataingestion.service;

import com.zeotap.dataingestion.model.ClickHouseConfig;
import com.zeotap.dataingestion.model.TableColumn;
import com.zeotap.dataingestion.model.TableInfo;
import com.zeotap.dataingestion.util.ClickHouseUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class ClickHouseService {

    private final ClickHouseUtil clickHouseUtil;
    
    /**
     * Tests connection to the ClickHouse database
     */
    public boolean testConnection(ClickHouseConfig config) {
        try (Connection connection = clickHouseUtil.createConnection(config)) {
            return connection.isValid(5); // Test if the connection is valid with 5 second timeout
        } catch (SQLException e) {
            log.error("Failed to connect to ClickHouse", e);
            return false;
        }
    }
    
    /**
     * Gets all tables from the ClickHouse database
     */
    public List<TableInfo> getAllTables(ClickHouseConfig config) throws SQLException {
        try (Connection connection = clickHouseUtil.createConnection(config)) {
            return clickHouseUtil.getAllTablesInfo(connection);
        }
    }
    
    /**
     * Gets a preview of data from a ClickHouse table
     */
    public List<String[]> previewData(ClickHouseConfig config, List<TableInfo> tables, String joinCondition, boolean useJoin, int limit) throws SQLException {
        try (Connection connection = clickHouseUtil.createConnection(config)) {
            String sql = clickHouseUtil.buildQuery(tables, joinCondition, useJoin) + " LIMIT " + limit;
            List<String[]> result = new ArrayList<>();
            
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(sql)) {
                
                int columnCount = rs.getMetaData().getColumnCount();
                
                // Add header row with column names
                String[] header = new String[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    header[i - 1] = rs.getMetaData().getColumnName(i);
                }
                result.add(header);
                
                // Add data rows
                while (rs.next()) {
                    String[] row = new String[columnCount];
                    for (int i = 1; i <= columnCount; i++) {
                        row[i - 1] = rs.getString(i);
                    }
                    result.add(row);
                }
            }
            
            return result;
        }
    }
    
    /**
     * Executes a query and returns all results
     */
    public List<String[]> executeQuery(ClickHouseConfig config, String query) throws SQLException {
        try (Connection connection = clickHouseUtil.createConnection(config);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            
            List<String[]> result = new ArrayList<>();
            int columnCount = rs.getMetaData().getColumnCount();
            
            // Add header row with column names
            String[] header = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                header[i - 1] = rs.getMetaData().getColumnName(i);
            }
            result.add(header);
            
            // Add data rows
            while (rs.next()) {
                String[] row = new String[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    row[i - 1] = rs.getString(i);
                }
                result.add(row);
            }
            
            return result;
        }
    }
    
    /**
     * Inserts data into a ClickHouse table
     */
    public int insertData(ClickHouseConfig config, String tableName, List<TableColumn> columns, List<String[]> data) throws SQLException {
        try (Connection connection = clickHouseUtil.createConnection(config)) {
            // Check if the table exists, create if not
            clickHouseUtil.createTable(connection, tableName, columns);
            
            List<String> columnsToInsert = new ArrayList<>();
            for (TableColumn column : columns) {
                if (column.isSelected()) {
                    columnsToInsert.add(column.getName());
                }
            }
            
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ")
                    .append(tableName)
                    .append(" (")
                    .append(String.join(", ", columnsToInsert))
                    .append(") VALUES (")
                    .append("?, ".repeat(columnsToInsert.size() - 1))
                    .append("?)");
            
            String sql = sqlBuilder.toString();
            int headerOffset = 0;
            
            // If the first row is a header, skip it
            if (data.size() > 0 && data.get(0).length == columnsToInsert.size()) {
                headerOffset = 1;
            }
            
            int rowsInserted = 0;
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                // Batch insert for better performance
                for (int i = headerOffset; i < data.size(); i++) {
                    String[] row = data.get(i);
                    
                    for (int j = 0; j < columnsToInsert.size(); j++) {
                        ps.setString(j + 1, j < row.length ? row[j] : null);
                    }
                    
                    ps.addBatch();
                    rowsInserted++;
                    
                    // Execute in batches of 1000
                    if (i % 1000 == 0) {
                        ps.executeBatch();
                    }
                }
                
                ps.executeBatch(); // Execute remaining items
            }
            
            return rowsInserted;
        }
    }
} 