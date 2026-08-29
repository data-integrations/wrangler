package com.ingestion.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Data
@NoArgsConstructor
public class IngestionRequest {

    @NotBlank(message = "Connection ID is required")
    private String connectionId;

    @NotBlank(message = "Table name is required")
    private String tableName;

    @NotBlank(message = "File name is required")
    private String fileName;

    @NotNull(message = "File configuration is required")
    @Valid
    private FlatFileConfig fileConfig;
} 