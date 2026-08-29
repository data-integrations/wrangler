package com.ingestion.model;

import lombok.Data;

@Data
public class FlatFileConfig {
    private char delimiter;
    private char quoteCharacter;
    private char escapeCharacter;
    private boolean hasHeader;
    private boolean skipEmptyLines;
    private boolean trimValues;
} 