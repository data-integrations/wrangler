package com.ingestion.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IngestionRequest {
    
    @NotBlank(message = "Table name is required")
    private String tableName;
    
    @NotNull(message = "ClickHouse connection details are required")
    @Valid
    private ClickHouseConnection connection;
    
    @NotNull(message = "Flat file configuration is required")
    @Valid
    private FlatFileConfig fileConfig;
    
    private String batchSize = "10000";
    
    private String maxRetries = "3";
    
    private String retryInterval = "5000"; // milliseconds
} 