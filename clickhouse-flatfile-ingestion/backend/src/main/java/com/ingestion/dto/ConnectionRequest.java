package com.ingestion.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import jakarta.validation.constraints.NotBlank;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionRequest {
    
    @NotBlank
    private String host;
    
    @NotBlank
    private String port;
    
    @NotBlank
    private String database;
    
    @NotBlank
    private String username;
    
    @NotBlank
    private String password;
    
    private String connectionType; // CLICKHOUSE or FLATFILE
} 