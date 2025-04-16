package com.zeotap.dataingestion.model;

import lombok.Data;

@Data
public class FlatFileConfig {
    private String delimiter;
    private boolean hasHeader;
    private String filePath; // For uploading to ClickHouse
    private String fileName; // For exporting from ClickHouse
} 