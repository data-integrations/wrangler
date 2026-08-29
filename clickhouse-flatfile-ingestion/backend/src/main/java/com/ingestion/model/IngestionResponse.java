package com.ingestion.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IngestionResponse {
    
    private String status; // SUCCESS, FAILED, IN_PROGRESS
    
    private String message;
    
    private LocalDateTime startTime;
    
    private LocalDateTime endTime;
    
    private long totalRows;
    
    private long processedRows;
    
    private long failedRows;
    
    private String errorDetails;
    
    private String jobId;
    
    @Builder.Default
    private boolean isComplete = false;
} 