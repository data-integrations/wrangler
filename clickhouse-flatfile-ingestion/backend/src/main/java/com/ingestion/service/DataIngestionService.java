package com.ingestion.service;

import com.ingestion.dto.IngestionRequest;
import com.ingestion.model.FlatFileConfig;
import com.ingestion.model.IngestionStatus;
import com.ingestion.model.TableMapping;
import com.ingestion.repository.TableMappingRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
public class DataIngestionService {

    @Autowired
    private ClickHouseConnectionService clickHouseService;

    @Autowired
    private FlatFileService flatFileService;

    @Autowired
    private IngestionStatusService statusService;

    @Autowired
    private TableMappingRepository tableMappingRepository;

    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    /**
     * Starts the ingestion process
     *
     * @param file File to ingest
     * @param request Ingestion request
     * @return Ingestion status ID
     */
    public String startIngestion(MultipartFile file, IngestionRequest request) {
        try {
            // Create ingestion status
            String statusId = statusService.createIngestionStatus(request.getTableName(), file.getOriginalFilename());

            // Start ingestion asynchronously
            CompletableFuture.runAsync(() -> {
                try {
                    processIngestion(file, request, statusId);
                } catch (Exception e) {
                    log.error("Ingestion failed: {}", e.getMessage());
                    statusService.updateStatus(statusId, "FAILED");
                    statusService.updateErrorMessage(statusId, e.getMessage());
                }
            }, executorService);

            return statusId;
        } catch (Exception e) {
            log.error("Failed to start ingestion: {}", e.getMessage());
            throw new RuntimeException("Failed to start ingestion: " + e.getMessage());
        }
    }

    /**
     * Processes the ingestion
     *
     * @param file File to ingest
     * @param request Ingestion request
     * @param statusId Ingestion status ID
     */
    private void processIngestion(MultipartFile file, IngestionRequest request, String statusId) {
        try {
            // Upload file
            Path filePath = flatFileService.uploadFile(file, request.getFileConfig());

            // Get table mapping
            TableMapping tableMapping = tableMappingRepository.findByTableName(request.getTableName())
                    .orElseThrow(() -> new RuntimeException("Table mapping not found: " + request.getTableName()));

            // Create flat file config
            FlatFileConfig flatFileConfig = createFlatFileConfig(request.getFileConfig());

            // Stream and process the file
            long totalRows = flatFileService.streamCSVFile(filePath, flatFileConfig, (headers, batch) -> {
                try {
                    // Transform data according to table mapping
                    List<Object[]> transformedData = transformData(batch, tableMapping);

                    // Insert data into ClickHouse
                    clickHouseService.executeBatchInsert(
                            request.getConnectionId(),
                            request.getTableName(),
                            tableMapping.getColumnMappings().keySet(),
                            transformedData
                    );

                    // Update progress
                    statusService.updateProgress(statusId, batch.size());
                } catch (Exception e) {
                    log.error("Failed to process batch: {}", e.getMessage());
                    throw new RuntimeException("Failed to process batch: " + e.getMessage());
                }
            });

            // Update total rows and mark as complete
            statusService.updateTotalRows(statusId, totalRows);
            statusService.updateStatus(statusId, "SUCCESS");
        } catch (Exception e) {
            log.error("Ingestion failed: {}", e.getMessage());
            statusService.updateStatus(statusId, "FAILED");
            statusService.updateErrorMessage(statusId, e.getMessage());
            throw new RuntimeException("Ingestion failed: " + e.getMessage());
        }
    }

    /**
     * Creates a flat file configuration
     *
     * @param fileConfig File configuration from request
     * @return FlatFileConfig
     */
    private FlatFileConfig createFlatFileConfig(com.ingestion.dto.FlatFileConfig fileConfig) {
        FlatFileConfig config = new FlatFileConfig();
        config.setDelimiter(fileConfig.getDelimiter());
        config.setQuoteChar(fileConfig.getQuoteChar());
        config.setEscapeChar(fileConfig.getEscapeChar());
        config.setLineSeparator(fileConfig.getLineSeparator());
        config.setHasHeader(fileConfig.isHasHeader());
        return config;
    }

    /**
     * Transforms data according to table mapping
     *
     * @param data Data to transform
     * @param tableMapping Table mapping
     * @return Transformed data
     */
    private List<Object[]> transformData(List<Map<String, String>> data, TableMapping tableMapping) {
        List<Object[]> transformedData = new ArrayList<>();
        Map<String, String> columnMappings = tableMapping.getColumnMappings();

        for (Map<String, String> row : data) {
            Object[] transformedRow = new Object[columnMappings.size()];
            int i = 0;
            for (Map.Entry<String, String> entry : columnMappings.entrySet()) {
                String sourceColumn = entry.getValue();
                String value = row.get(sourceColumn);
                transformedRow[i++] = value != null ? value : "";
            }
            transformedData.add(transformedRow);
        }

        return transformedData;
    }
} 