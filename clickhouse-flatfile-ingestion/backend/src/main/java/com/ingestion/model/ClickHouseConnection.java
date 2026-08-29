package com.ingestion.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Builder;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ClickHouseConnection {
    
    @NotBlank
    private String host;
    
    @NotNull
    private Integer port;
    
    @NotBlank
    private String database;
    
    @NotBlank
    private String username;
    
    @NotBlank
    private String password;
    
    @NotBlank(message = "JWT token is required")
    private String jwtToken;
    
    private boolean useHttps = false;
} 