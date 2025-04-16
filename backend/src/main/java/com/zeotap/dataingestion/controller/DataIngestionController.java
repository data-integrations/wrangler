package com.zeotap.dataingestion.controller;

import com.opencsv.exceptions.CsvValidationException;
import com.zeotap.dataingestion.model.IngestionRequest;
import com.zeotap.dataingestion.model.IngestionResponse;
import com.zeotap.dataingestion.service.DataIngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ingestion")
@RequiredArgsConstructor
@Slf4j
public class DataIngestionController {

    private final DataIngestionService dataIngestionService;
    
    @PostMapping("/ingest")
    public ResponseEntity<IngestionResponse> ingestData(@RequestBody IngestionRequest request) {
        log.info("Received ingestion request: {}", request);
        IngestionResponse response = dataIngestionService.ingestData(request);
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/preview")
    public ResponseEntity<Map<String, Object>> previewData(@RequestBody IngestionRequest request) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            List<String[]> previewData = dataIngestionService.previewData(request, 100);
            response.put("success", true);
            response.put("data", previewData);
            return ResponseEntity.ok(response);
        } catch (SQLException | IOException | CsvValidationException e) {
            log.error("Error generating preview", e);
            response.put("success", false);
            response.put("message", "Error generating preview: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
} 