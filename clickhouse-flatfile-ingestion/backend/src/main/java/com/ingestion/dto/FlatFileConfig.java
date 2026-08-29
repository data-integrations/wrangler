package com.ingestion.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Data
@NoArgsConstructor
public class FlatFileConfig {

    @NotBlank(message = "File name is required")
    private String fileName;

    @NotBlank(message = "File path is required")
    private String filePath;

    @NotBlank(message = "File type is required")
    private String fileType;

    @NotBlank
    private String delimiter;

    @NotBlank
    private String encoding;

    @NotNull
    private Boolean hasHeader;

    @NotNull
    private Boolean skipEmptyLines;

    private String dateFormat;
    private String timeFormat;
    private String timestampFormat;
    private String timezone;
} 