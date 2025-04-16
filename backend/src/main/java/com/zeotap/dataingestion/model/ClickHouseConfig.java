package com.zeotap.dataingestion.model;

import lombok.Data;

@Data
public class ClickHouseConfig {
    private String host;
    private int port;
    private String database;
    private String user;
    private String jwtToken;
    private boolean secure; // Whether to use HTTPS or HTTP
} 