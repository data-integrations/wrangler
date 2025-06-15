package com.example.clickhouse.service;

import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

public interface DataIngestionService {
    int exportFromClickHouse(String tableName, List<String> columns, String fileFormat);
    int importToClickHouse(MultipartFile file, String tableName, List<String> columns);
    
    // New methods for multi-table join and data preview
    int exportFromClickHouseWithJoin(List<String> tableNames, List<String> columns, 
                                   List<String> joinConditions, String fileFormat);
    List<Map<String, Object>> previewData(String tableName, List<String> columns, int limit);
    List<Map<String, Object>> previewDataWithJoin(List<String> tableNames, List<String> columns, 
                                                List<String> joinConditions, int limit);
} 