package com.zeotap.dataingestion.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableColumn {
    private String name;
    private String type;
    private boolean selected; // Whether the column is selected for ingestion
} 