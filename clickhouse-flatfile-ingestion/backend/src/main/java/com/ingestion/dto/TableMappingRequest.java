package com.ingestion.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

@Data
@NoArgsConstructor
public class TableMappingRequest {

    @NotBlank(message = "Table name is required")
    private String tableName;

    @NotBlank(message = "Connection ID is required")
    private String connectionId;

    @NotEmpty(message = "At least one column mapping is required")
    @Valid
    private List<ColumnMapping> columnMappings;

    @Data
    @NoArgsConstructor
    public static class ColumnMapping {
        @NotBlank(message = "Source column name is required")
        private String sourceColumn;

        @NotBlank(message = "Target column name is required")
        private String targetColumn;

        private String dataType;
        private String transformation;
        private Boolean isNullable;
        private String defaultValue;
    }
} 