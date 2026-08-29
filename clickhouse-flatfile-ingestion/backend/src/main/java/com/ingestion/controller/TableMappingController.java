package com.ingestion.controller;

import com.ingestion.dto.TableMappingRequest;
import com.ingestion.entity.TableMapping;
import com.ingestion.service.TableMappingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@RestController
@RequestMapping("/api/table-mappings")
public class TableMappingController {

    private final TableMappingService tableMappingService;

    @Autowired
    public TableMappingController(TableMappingService tableMappingService) {
        this.tableMappingService = tableMappingService;
    }

    @PostMapping
    public ResponseEntity<TableMapping> createTableMapping(@Valid @RequestBody TableMappingRequest request) {
        TableMapping tableMapping = tableMappingService.createTableMapping(request);
        return ResponseEntity.ok(tableMapping);
    }

    @GetMapping("/{tableName}")
    public ResponseEntity<TableMapping> getTableMapping(@PathVariable String tableName) {
        TableMapping tableMapping = tableMappingService.getTableMapping(tableName);
        return ResponseEntity.ok(tableMapping);
    }

    @GetMapping
    public ResponseEntity<List<TableMapping>> getAllTableMappings() {
        List<TableMapping> tableMappings = tableMappingService.getAllTableMappings();
        return ResponseEntity.ok(tableMappings);
    }

    @PutMapping("/{tableName}")
    public ResponseEntity<TableMapping> updateTableMapping(
            @PathVariable String tableName,
            @Valid @RequestBody TableMappingRequest request) {
        TableMapping tableMapping = tableMappingService.updateTableMapping(tableName, request);
        return ResponseEntity.ok(tableMapping);
    }

    @DeleteMapping("/{tableName}")
    public ResponseEntity<Void> deleteTableMapping(@PathVariable String tableName) {
        tableMappingService.deleteTableMapping(tableName);
        return ResponseEntity.ok().build();
    }
} 