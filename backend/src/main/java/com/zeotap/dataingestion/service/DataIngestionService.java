package com.zeotap.dataingestion.service;

import com.opencsv.exceptions.CsvValidationException;
import com.zeotap.dataingestion.model.ClickHouseConfig;
import com.zeotap.dataingestion.model.FlatFileConfig;
import com.zeotap.dataingestion.model.IngestionRequest;
import com.zeotap.dataingestion.model.IngestionResponse;
import com.zeotap.dataingestion.model.TableColumn;
import com.zeotap.dataingestion.model.TableInfo;
import com.zeotap.dataingestion.util.ClickHouseUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class DataIngestionService {

    private final ClickHouseService clickHouseService;
    private final FlatFileService flatFileService;
    private final ClickHouseUtil clickHouseUtil;

    /**
     * Performs data ingestion based on the request
     */
    public IngestionResponse ingestData(IngestionRequest request) {
        try {
            // Extract source and target from the request
            String source = request.getSource();
            String target = request.getTarget();
            
            if ("clickhouse".equalsIgnoreCase(source) && "flatfile".equalsIgnoreCase(target)) {
                return ingestFromClickHouseToFlatFile(request);
            } else if ("flatfile".equalsIgnoreCase(source) && "clickhouse".equalsIgnoreCase(target)) {
                return ingestFromFlatFileToClickHouse(request);
            } else {
                return IngestionResponse.builder()
                        .success(false)
                        .message("Invalid source or target. Must be 'clickhouse' or 'flatfile'.")
                        .build();
            }
        } catch (Exception e) {
            log.error("Error during data ingestion", e);
            return IngestionResponse.builder()
                    .success(false)
                    .message("Error during ingestion: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Ingest data from ClickHouse to Flat File
     */
    private IngestionResponse ingestFromClickHouseToFlatFile(IngestionRequest request) throws SQLException, IOException {
        ClickHouseConfig clickHouseConfig = request.getClickHouseConfig();
        FlatFileConfig flatFileConfig = request.getFlatFileConfig();
        List<TableInfo> tables = request.getTables();
        String joinCondition = request.getJoinCondition();
        boolean useJoin = request.isUseJoin();
        
        // Build the SQL query based on selected tables and columns
        String query = clickHouseUtil.buildQuery(tables, joinCondition, useJoin);
        
        // Execute the query
        List<String[]> data = clickHouseService.executeQuery(clickHouseConfig, query);
        
        // Write data to CSV file
        String filePath = flatFileService.writeData(data, flatFileConfig);
        String downloadLink = flatFileService.createDownloadLink(filePath);
        
        return IngestionResponse.builder()
                .success(true)
                .message("Successfully exported data to " + filePath)
                .totalRecords(data.size() - 1) // Subtract header row
                .fileName(downloadLink)
                .build();
    }
    
    /**
     * Ingest data from Flat File to ClickHouse
     */
    private IngestionResponse ingestFromFlatFileToClickHouse(IngestionRequest request) throws SQLException, IOException, CsvValidationException {
        ClickHouseConfig clickHouseConfig = request.getClickHouseConfig();
        FlatFileConfig flatFileConfig = request.getFlatFileConfig();
        List<TableInfo> tables = request.getTables();
        
        if (tables == null || tables.isEmpty() || !tables.get(0).isSelected()) {
            return IngestionResponse.builder()
                    .success(false)
                    .message("No table selected for ingestion")
                    .build();
        }
        
        TableInfo tableInfo = tables.get(0);
        String tableName = tableInfo.getName();
        List<TableColumn> columns = tableInfo.getColumns();
        
        // Get selected column indices
        List<Integer> selectedColumnIndices = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isSelected()) {
                selectedColumnIndices.add(i);
            }
        }
        
        // Read data from flat file
        List<String[]> data = flatFileService.readData(flatFileConfig.getFilePath(), flatFileConfig, selectedColumnIndices);
        
        // Filter columns to only selected ones
        List<TableColumn> selectedColumns = columns.stream()
                .filter(TableColumn::isSelected)
                .collect(Collectors.toList());
        
        // Insert data into ClickHouse
        int rowsInserted = clickHouseService.insertData(clickHouseConfig, tableName, selectedColumns, data);
        
        return IngestionResponse.builder()
                .success(true)
                .message("Successfully imported " + rowsInserted + " records to table " + tableName)
                .totalRecords(rowsInserted)
                .build();
    }
    
    /**
     * Get a preview of the data to be ingested
     */
    public List<String[]> previewData(IngestionRequest request, int limit) throws SQLException, IOException, CsvValidationException {
        String source = request.getSource();
        
        if ("clickhouse".equalsIgnoreCase(source)) {
            return clickHouseService.previewData(
                    request.getClickHouseConfig(),
                    request.getTables(),
                    request.getJoinCondition(),
                    request.isUseJoin(),
                    limit);
        } else if ("flatfile".equalsIgnoreCase(source)) {
            return flatFileService.previewData(
                    request.getFlatFileConfig().getFilePath(),
                    request.getFlatFileConfig(),
                    limit);
        } else {
            throw new IllegalArgumentException("Invalid source: " + source);
        }
    }
} 