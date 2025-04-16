package com.zeotap.dataingestion.model;

import lombok.Data;

import java.util.List;

@Data
public class IngestionRequest {
    private String source; // "clickhouse" or "flatfile"
    private String target; // "clickhouse" or "flatfile"
    
    private ClickHouseConfig clickHouseConfig;
    private FlatFileConfig flatFileConfig;
    
    private List<TableInfo> tables; // Selected tables with columns
    
    // For multi-table join (bonus feature)
    private String joinCondition;
    private boolean useJoin;
} 