package com.zeotap.dataingestion.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {
    private String name;
    private List<TableColumn> columns;
    private boolean selected; // For multi-table functionality
} 