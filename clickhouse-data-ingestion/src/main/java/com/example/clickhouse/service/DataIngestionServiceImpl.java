package com.example.clickhouse.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DataIngestionServiceImpl implements DataIngestionService {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public DataIngestionServiceImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public int exportFromClickHouse(String tableName, List<String> columns, String fileFormat) {
        String columnList = String.join(", ", columns);
        String query = String.format("SELECT %s FROM %s", columnList, tableName);
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(query);

        // TODO: Implement file writing based on fileFormat
        return results.size();
    }

    @Override
    public int exportFromClickHouseWithJoin(List<String> tableNames, List<String> columns, 
                                          List<String> joinConditions, String fileFormat) {
        String columnList = String.join(", ", columns);
        String joinClause = String.join(" ", joinConditions);
        String query = String.format("SELECT %s FROM %s %s", 
            columnList, 
            String.join(", ", tableNames),
            joinClause);
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(query);

        // TODO: Implement file writing based on fileFormat
        return results.size();
    }

    @Override
    public List<Map<String, Object>> previewData(String tableName, List<String> columns, int limit) {
        String columnList = String.join(", ", columns);
        String query = String.format("SELECT %s FROM %s LIMIT %d", columnList, tableName, limit);
        return jdbcTemplate.queryForList(query);
    }

    @Override
    public List<Map<String, Object>> previewDataWithJoin(List<String> tableNames, List<String> columns, 
                                                       List<String> joinConditions, int limit) {
        String columnList = String.join(", ", columns);
        String joinClause = String.join(" ", joinConditions);
        String query = String.format("SELECT %s FROM %s %s LIMIT %d", 
            columnList, 
            String.join(", ", tableNames),
            joinClause,
            limit);
        return jdbcTemplate.queryForList(query);
    }

    @Override
    public int importToClickHouse(MultipartFile file, String tableName, List<String> columns) {
        try {
            String columnList = String.join(", ", columns);
            String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
            String insertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)", 
                tableName, columnList, placeholders);

            int count = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split(",");
                    jdbcTemplate.update(insertQuery, (Object[]) values);
                    count++;
                }
            }
            return count;
        } catch (Exception e) {
            throw new RuntimeException("Error importing data: " + e.getMessage(), e);
        }
    }
} 