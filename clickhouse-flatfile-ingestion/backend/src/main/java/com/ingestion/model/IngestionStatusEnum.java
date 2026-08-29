package com.ingestion.model;

public enum IngestionStatusEnum {
    
    SUCCESS("Success"),
    FAILED("Failed"),
    IN_PROGRESS("In Progress"),
    CANCELLED("Cancelled");
    
    private final String displayName;
    
    IngestionStatusEnum(String displayName) {
        this.displayName = displayName;
    }
    
    public String getDisplayName() {
        return displayName;
    }
} 