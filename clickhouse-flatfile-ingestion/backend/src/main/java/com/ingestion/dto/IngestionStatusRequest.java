package com.ingestion.dto;

import com.ingestion.entity.IngestionStatusEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IngestionStatusRequest {
    @NotBlank
    private String tableName;

    @NotBlank
    private String fileName;

    @NotNull
    private IngestionStatusEnum status;

    private Integer progress;
    private Long totalRows;
    private String errorMessage;
    private Integer retryCount;
} 