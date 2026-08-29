package com.ingestion.dto;

import com.ingestion.entity.IngestionStatusEnum;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class IngestionStatusDTO {
    private String id;
    private String tableName;
    private String fileName;
    private IngestionStatusEnum status;
    private Integer progress;
    private Long totalRows;
    private String errorMessage;
    private Integer retryCount;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private LocalDateTime completedAt;
} 