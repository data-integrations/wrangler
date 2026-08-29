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
public class IngestionStatus {
    
    private String jobId;
    
    private String status; // SUCCESS, FAILED, IN_PROGRESS, CANCELLED
    
    private LocalDateTime startTime;
    
    private LocalDateTime lastUpdatedTime;
    
    private long totalRows;
    
    private long processedRows;
    
    private long failedRows;
    
    private String errorMessage;
    
    private String tableName;
    
    private String fileName;
    
    @Builder.Default
    private boolean isComplete = false;
    
    @Builder.Default
    private int retryCount = 0;
} 