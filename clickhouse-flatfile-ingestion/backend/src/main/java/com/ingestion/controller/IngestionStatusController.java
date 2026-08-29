package com.ingestion.controller;

import com.ingestion.model.IngestionStatus;
import com.ingestion.model.IngestionStatusEnum;
import com.ingestion.service.IngestionStatusService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/ingestion-status")
public class IngestionStatusController {

    @Autowired
    private IngestionStatusService ingestionStatusService;

    @PostMapping
    public ResponseEntity<IngestionStatus> createStatus(
            @RequestParam String tableName,
            @RequestParam String fileName) {
        IngestionStatus status = ingestionStatusService.createIngestionStatus(tableName, fileName);
        return ResponseEntity.ok(status);
    }

    @PutMapping("/{id}/status")
    public ResponseEntity<IngestionStatus> updateStatus(
            @PathVariable String id,
            @RequestParam IngestionStatusEnum status) {
        IngestionStatus updatedStatus = ingestionStatusService.updateStatus(id, status);
        if (updatedStatus != null) {
            return ResponseEntity.ok(updatedStatus);
        }
        return ResponseEntity.notFound().build();
    }

    @PutMapping("/{id}/progress")
    public ResponseEntity<IngestionStatus> updateProgress(
            @PathVariable String id,
            @RequestParam long processedRows,
            @RequestParam long failedRows) {
        IngestionStatus updatedStatus = ingestionStatusService.updateProgress(id, processedRows, failedRows);
        if (updatedStatus != null) {
            return ResponseEntity.ok(updatedStatus);
        }
        return ResponseEntity.notFound().build();
    }

    @PutMapping("/{id}/total-rows")
    public ResponseEntity<IngestionStatus> updateTotalRows(
            @PathVariable String id,
            @RequestParam long totalRows) {
        IngestionStatus updatedStatus = ingestionStatusService.updateTotalRows(id, totalRows);
        if (updatedStatus != null) {
            return ResponseEntity.ok(updatedStatus);
        }
        return ResponseEntity.notFound().build();
    }

    @PutMapping("/{id}/error")
    public ResponseEntity<IngestionStatus> updateErrorMessage(
            @PathVariable String id,
            @RequestParam String errorMessage) {
        IngestionStatus updatedStatus = ingestionStatusService.updateErrorMessage(id, errorMessage);
        if (updatedStatus != null) {
            return ResponseEntity.ok(updatedStatus);
        }
        return ResponseEntity.notFound().build();
    }

    @PutMapping("/{id}/retry")
    public ResponseEntity<IngestionStatus> incrementRetryCount(
            @PathVariable String id) {
        IngestionStatus updatedStatus = ingestionStatusService.incrementRetryCount(id);
        if (updatedStatus != null) {
            return ResponseEntity.ok(updatedStatus);
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping("/table/{tableName}")
    public ResponseEntity<List<IngestionStatus>> getStatusByTableName(
            @PathVariable String tableName) {
        List<IngestionStatus> statuses = ingestionStatusService.getStatusByTableName(tableName);
        return ResponseEntity.ok(statuses);
    }

    @GetMapping("/file/{fileName}")
    public ResponseEntity<List<IngestionStatus>> getStatusByFileName(
            @PathVariable String fileName) {
        List<IngestionStatus> statuses = ingestionStatusService.getStatusByFileName(fileName);
        return ResponseEntity.ok(statuses);
    }

    @GetMapping("/incomplete")
    public ResponseEntity<List<IngestionStatus>> getIncompleteIngestions() {
        List<IngestionStatus> statuses = ingestionStatusService.getIncompleteIngestions();
        return ResponseEntity.ok(statuses);
    }

    @GetMapping("/status/{status}")
    public ResponseEntity<List<IngestionStatus>> getStatusByStatus(
            @PathVariable String status) {
        List<IngestionStatus> statuses = ingestionStatusService.getStatusByStatus(status);
        return ResponseEntity.ok(statuses);
    }

    @GetMapping("/{id}")
    public ResponseEntity<IngestionStatus> getStatusById(@PathVariable String id) {
        return ingestionStatusService.getStatusById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
} 