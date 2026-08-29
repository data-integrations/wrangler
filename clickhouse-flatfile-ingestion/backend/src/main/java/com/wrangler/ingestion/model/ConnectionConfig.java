package com.wrangler.ingestion.model;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class ConnectionConfig {
    @NotBlank
    private String host;
    
    @NotNull
    private Integer port;
    
    @NotBlank
    private String database;
    
    @NotBlank
    private String user;
    
    @NotBlank
    private String jwtToken;
    
    private String sslMode = "STRICT";
    private boolean useSsl = true;
} 