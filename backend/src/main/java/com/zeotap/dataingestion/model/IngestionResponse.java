package com.zeotap.dataingestion.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IngestionResponse {
    private boolean success;
    private String message;
    private long totalRecords;
    private String fileName; // For flat file ingestion
} 