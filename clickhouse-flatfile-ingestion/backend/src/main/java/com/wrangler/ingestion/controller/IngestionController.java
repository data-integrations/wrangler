package com.wrangler.ingestion.controller;

import com.wrangler.ingestion.model.ConnectionConfig;
import com.wrangler.ingestion.service.ClickHouseService;
import com.wrangler.ingestion.service.FlatFileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.sql.DataSource;
import javax.validation.Valid;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping("/api/ingestion")
@RequiredArgsConstructor
public class IngestionController {

    private final ClickHouseService clickHouseService;
    private final FlatFileService flatFileService;

    @PostMapping("/connect")
    public ResponseEntity<?> connect(@Valid @RequestBody ConnectionConfig config) {
        try {
            DataSource dataSource = clickHouseService.createDataSource(config);
            List<String> tables = clickHouseService.getTables(dataSource);
            return ResponseEntity.ok(tables);
        } catch (SQLException e) {
            log.error("Failed to connect to ClickHouse", e);
            return ResponseEntity.badRequest().body("Failed to connect: " + e.getMessage());
        }
    }

    @GetMapping("/tables/{tableName}/columns")
    public ResponseEntity<?> getColumns(@PathVariable String tableName, @Valid @RequestBody ConnectionConfig config) {
        try {
            DataSource dataSource = clickHouseService.createDataSource(config);
            List<String> columns = clickHouseService.getColumns(dataSource, tableName);
            return ResponseEntity.ok(columns);
        } catch (SQLException e) {
            log.error("Failed to get columns", e);
            return ResponseEntity.badRequest().body("Failed to get columns: " + e.getMessage());
        }
    }

    @PostMapping("/file/columns")
    public ResponseEntity<?> getFileColumns(
            @RequestParam("file") MultipartFile file,
            @RequestParam("delimiter") String delimiter) {
        try {
            List<String> columns = flatFileService.getColumns(file, delimiter);
            return ResponseEntity.ok(columns);
        } catch (IOException e) {
            log.error("Failed to read file columns", e);
            return ResponseEntity.badRequest().body("Failed to read file: " + e.getMessage());
        }
    }

    @PostMapping("/clickhouse-to-file")
    public ResponseEntity<?> clickHouseToFile(
            @Valid @RequestBody ConnectionConfig config,
            @RequestParam String tableName,
            @RequestParam List<String> columns,
            @RequestParam String filePath,
            @RequestParam String delimiter) {
        try {
            DataSource dataSource = clickHouseService.createDataSource(config);
            
            CompletableFuture<Long> future = clickHouseService.exportToFile(
                dataSource, tableName, columns, filePath, delimiter,
                count -> log.info("Exported {} rows", count)
            );
            
            Long totalRows = future.get();
            return ResponseEntity.ok().body("Export completed successfully. Total rows: " + totalRows);
        } catch (Exception e) {
            log.error("Failed to export from ClickHouse to file", e);
            return ResponseEntity.badRequest().body("Export failed: " + e.getMessage());
        }
    }

    @PostMapping("/file-to-clickhouse")
    public ResponseEntity<?> fileToClickHouse(
            @RequestParam("file") MultipartFile file,
            @RequestParam String delimiter,
            @Valid @RequestBody ConnectionConfig config,
            @RequestParam String tableName,
            @RequestParam List<String> columns) {
        try {
            // Save uploaded file temporarily
            String tempFilePath = System.getProperty("java.io.tmpdir") + "/" + file.getOriginalFilename();
            file.transferTo(new File(tempFilePath));
            
            DataSource dataSource = clickHouseService.createDataSource(config);
            
            CompletableFuture<Long> future = clickHouseService.importFromFile(
                dataSource, tableName, columns, tempFilePath, delimiter,
                count -> log.info("Imported {} rows", count)
            );
            
            Long totalRows = future.get();
            
            // Clean up temp file
            new File(tempFilePath).delete();
            
            return ResponseEntity.ok().body("Import completed successfully. Total rows: " + totalRows);
        } catch (Exception e) {
            log.error("Failed to import file to ClickHouse", e);
            return ResponseEntity.badRequest().body("Import failed: " + e.getMessage());
        }
    }
} 