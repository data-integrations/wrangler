package com.ingestion.service;

import com.ingestion.dto.FileUploadRequest;
import com.ingestion.model.FlatFileConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class FlatFileService {

    private static final String UPLOAD_DIR = "uploads";
    private static final int BATCH_SIZE = 1000;

    /**
     * Uploads a file to the server
     *
     * @param file File to upload
     * @param request File upload request
     * @return Path to the uploaded file
     */
    public Path uploadFile(MultipartFile file, FileUploadRequest request) {
        try {
            // Create upload directory if it doesn't exist
            Path uploadPath = Paths.get(UPLOAD_DIR);
            if (!Files.exists(uploadPath)) {
                Files.createDirectories(uploadPath);
            }

            // Generate a unique filename
            String originalFilename = file.getOriginalFilename();
            String fileExtension = originalFilename.substring(originalFilename.lastIndexOf("."));
            String uniqueFilename = System.currentTimeMillis() + "_" + originalFilename;
            Path filePath = uploadPath.resolve(uniqueFilename);

            // Save the file
            Files.copy(file.getInputStream(), filePath);
            log.info("File uploaded successfully: {}", filePath);

            return filePath;
        } catch (IOException e) {
            log.error("Failed to upload file: {}", e.getMessage());
            throw new RuntimeException("Failed to upload file: " + e.getMessage());
        }
    }

    /**
     * Parses a CSV file and returns the headers and data
     *
     * @param filePath Path to the CSV file
     * @param config CSV configuration
     * @return Map containing headers and data
     */
    public Map<String, Object> parseCSVFile(Path filePath, FlatFileConfig config) {
        Map<String, Object> result = new HashMap<>();
        List<String> headers = new ArrayList<>();
        List<Map<String, String>> data = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
            // Create CSV format based on configuration
            CSVFormat csvFormat = createCSVFormat(config);

            // Parse the CSV file
            CSVParser parser = new CSVParser(reader, csvFormat);
            List<CSVRecord> records = parser.getRecords();

            if (records.isEmpty()) {
                result.put("headers", headers);
                result.put("data", data);
                return result;
            }

            // Get headers
            CSVRecord headerRecord = records.get(0);
            for (String header : headerRecord) {
                headers.add(header.trim());
            }

            // Get data
            for (int i = 1; i < records.size(); i++) {
                CSVRecord record = records.get(i);
                Map<String, String> row = new HashMap<>();
                
                for (int j = 0; j < headers.size(); j++) {
                    if (j < record.size()) {
                        row.put(headers.get(j), record.get(j).trim());
                    } else {
                        row.put(headers.get(j), "");
                    }
                }
                
                data.add(row);
            }

            result.put("headers", headers);
            result.put("data", data);
            return result;
        } catch (IOException e) {
            log.error("Failed to parse CSV file: {}", e.getMessage());
            throw new RuntimeException("Failed to parse CSV file: " + e.getMessage());
        }
    }

    /**
     * Streams a CSV file and processes it in batches
     *
     * @param filePath Path to the CSV file
     * @param config CSV configuration
     * @param processor Batch processor
     * @return Number of rows processed
     */
    public long streamCSVFile(Path filePath, FlatFileConfig config, BatchProcessor processor) {
        AtomicLong rowCount = new AtomicLong(0);
        List<Map<String, String>> batch = new ArrayList<>();
        List<String> headers = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
            // Create CSV format based on configuration
            CSVFormat csvFormat = createCSVFormat(config);

            // Parse the CSV file
            CSVParser parser = new CSVParser(reader, csvFormat);
            List<CSVRecord> records = parser.getRecords();

            if (records.isEmpty()) {
                return 0;
            }

            // Get headers
            CSVRecord headerRecord = records.get(0);
            for (String header : headerRecord) {
                headers.add(header.trim());
            }

            // Process data in batches
            for (int i = 1; i < records.size(); i++) {
                CSVRecord record = records.get(i);
                Map<String, String> row = new HashMap<>();
                
                for (int j = 0; j < headers.size(); j++) {
                    if (j < record.size()) {
                        row.put(headers.get(j), record.get(j).trim());
                    } else {
                        row.put(headers.get(j), "");
                    }
                }
                
                batch.add(row);
                rowCount.incrementAndGet();

                // Process batch if it reaches the batch size
                if (batch.size() >= BATCH_SIZE) {
                    processor.processBatch(batch);
                    batch.clear();
                }
            }

            // Process remaining rows
            if (!batch.isEmpty()) {
                processor.processBatch(batch);
            }

            return rowCount.get();
        } catch (IOException e) {
            log.error("Failed to stream CSV file: {}", e.getMessage());
            throw new RuntimeException("Failed to stream CSV file: " + e.getMessage());
        }
    }

    /**
     * Creates a CSV format based on configuration
     *
     * @param config CSV configuration
     * @return CSVFormat
     */
    private CSVFormat createCSVFormat(FlatFileConfig config) {
        CSVFormat csvFormat = CSVFormat.DEFAULT;

        // Set delimiter
        if (config.getDelimiter() != null && !config.getDelimiter().isEmpty()) {
            csvFormat = csvFormat.withDelimiter(config.getDelimiter().charAt(0));
        }

        // Set quote character
        if (config.getQuoteChar() != null && !config.getQuoteChar().isEmpty()) {
            csvFormat = csvFormat.withQuote(config.getQuoteChar().charAt(0));
        }

        // Set escape character
        if (config.getEscapeChar() != null && !config.getEscapeChar().isEmpty()) {
            csvFormat = csvFormat.withEscape(config.getEscapeChar().charAt(0));
        }

        // Set line separator
        if (config.getLineSeparator() != null && !config.getLineSeparator().isEmpty()) {
            csvFormat = csvFormat.withRecordSeparator(config.getLineSeparator());
        }

        // Set header row
        if (config.isHasHeader()) {
            csvFormat = csvFormat.withFirstRecordAsHeader();
        } else {
            csvFormat = csvFormat.withSkipHeaderRecord();
        }

        return csvFormat;
    }

    /**
     * Interface for batch processing
     */
    public interface BatchProcessor {
        void processBatch(List<Map<String, String>> batch);
    }
} 