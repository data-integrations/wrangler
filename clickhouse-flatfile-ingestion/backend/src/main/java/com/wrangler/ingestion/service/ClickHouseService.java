package com.wrangler.ingestion.service;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.wrangler.ingestion.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class ClickHouseService {

    public DataSource createDataSource(ConnectionConfig config) {
        Properties properties = new Properties();
        properties.setProperty("user", config.getUser());
        properties.setProperty("password", config.getJwtToken());
        properties.setProperty("ssl", String.valueOf(config.isUseSsl()));
        properties.setProperty("sslmode", config.getSslMode());

        String url = String.format("jdbc:clickhouse://%s:%d/%s",
                config.getHost(), config.getPort(), config.getDatabase());
        
        return new ClickHouseDataSource(url, properties);
    }

    public List<String> getTables(DataSource dataSource) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"});
            
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        return tables;
    }

    public List<String> getColumns(DataSource dataSource, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(null, null, tableName, "%");
            
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }

    public long getRowCount(DataSource dataSource, String tableName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try (var stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT count() FROM " + tableName);
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        return 0;
    }

    public CompletableFuture<Long> exportToFile(DataSource dataSource, String tableName, 
            List<String> columns, String filePath, String delimiter, 
            ProgressCallback progressCallback) {
        return CompletableFuture.supplyAsync(() -> {
            AtomicLong rowCount = new AtomicLong(0);
            try (Connection conn = dataSource.getConnection()) {
                String columnList = String.join(", ", columns);
                String query = String.format("SELECT %s FROM %s", columnList, tableName);
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.setFetchSize(10000); // Set batch size
                    ResultSet rs = stmt.executeQuery(query);
                    
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
                        // Write header
                        writer.write(String.join(delimiter, columns));
                        writer.newLine();
                        
                        // Write data
                        while (rs.next()) {
                            List<String> row = new ArrayList<>();
                            for (String column : columns) {
                                row.add(rs.getString(column));
                            }
                            writer.write(String.join(delimiter, row));
                            writer.newLine();
                            
                            long count = rowCount.incrementAndGet();
                            if (count % 10000 == 0) {
                                progressCallback.onProgress(count);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Failed to export data to file", e);
                throw new RuntimeException("Export failed: " + e.getMessage());
            }
            return rowCount.get();
        });
    }

    public CompletableFuture<Long> importFromFile(DataSource dataSource, String tableName, 
            List<String> columns, String filePath, String delimiter, 
            ProgressCallback progressCallback) {
        return CompletableFuture.supplyAsync(() -> {
            AtomicLong rowCount = new AtomicLong(0);
            try (Connection conn = dataSource.getConnection()) {
                String columnList = String.join(", ", columns);
                String placeholders = String.join(", ", columns.stream()
                        .map(c -> "?").toList());
                
                String insertSql = String.format("INSERT INTO %s (%s) VALUES (%s)", 
                        tableName, columnList, placeholders);
                
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    conn.setAutoCommit(false);
                    
                    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                        // Skip header
                        reader.readLine();
                        
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] values = line.split(delimiter);
                            for (int i = 0; i < values.length; i++) {
                                pstmt.setString(i + 1, values[i].trim());
                            }
                            pstmt.addBatch();
                            
                            long count = rowCount.incrementAndGet();
                            if (count % 10000 == 0) {
                                pstmt.executeBatch();
                                conn.commit();
                                progressCallback.onProgress(count);
                            }
                        }
                        
                        // Execute remaining batch
                        pstmt.executeBatch();
                        conn.commit();
                    }
                }
            } catch (Exception e) {
                log.error("Failed to import data from file", e);
                throw new RuntimeException("Import failed: " + e.getMessage());
            }
            return rowCount.get();
        });
    }

    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(long count);
    }
} 