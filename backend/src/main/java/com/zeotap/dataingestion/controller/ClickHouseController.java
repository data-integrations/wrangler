package com.zeotap.dataingestion.controller;

import com.zeotap.dataingestion.model.ClickHouseConfig;
import com.zeotap.dataingestion.model.TableInfo;
import com.zeotap.dataingestion.service.ClickHouseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/clickhouse")
@RequiredArgsConstructor
@Slf4j
public class ClickHouseController {

    private final ClickHouseService clickHouseService;
    
    @PostMapping("/test-connection")
    public ResponseEntity<Map<String, Object>> testConnection(@RequestBody ClickHouseConfig config) {
        Map<String, Object> response = new HashMap<>();
        boolean success = clickHouseService.testConnection(config);
        
        response.put("success", success);
        if (success) {
            response.put("message", "Successfully connected to ClickHouse");
        } else {
            response.put("message", "Failed to connect to ClickHouse. Please check your configuration.");
        }
        
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/tables")
    public ResponseEntity<List<TableInfo>> getTables(@RequestBody ClickHouseConfig config) {
        try {
            List<TableInfo> tables = clickHouseService.getAllTables(config);
            return ResponseEntity.ok(tables);
        } catch (SQLException e) {
            log.error("Error fetching tables", e);
            return ResponseEntity.badRequest().build();
        }
    }
} 