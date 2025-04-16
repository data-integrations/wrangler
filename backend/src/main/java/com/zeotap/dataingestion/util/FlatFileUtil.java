package com.zeotap.dataingestion.util;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import com.zeotap.dataingestion.model.FlatFileConfig;
import com.zeotap.dataingestion.model.TableColumn;
import com.zeotap.dataingestion.model.TableInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class FlatFileUtil {

    @Value("${app.upload.dir}")
    private String uploadDir;

    /**
     * Saves an uploaded file to the temporary directory
     */
    public String saveUploadedFile(MultipartFile file) throws IOException {
        Path dirPath = Paths.get(uploadDir);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        
        String fileName = file.getOriginalFilename();
        Path filePath = dirPath.resolve(fileName);
        
        file.transferTo(filePath.toFile());
        return filePath.toString();
    }
    
    /**
     * Extracts schema information from a flat file
     */
    public TableInfo extractSchema(String filePath, FlatFileConfig config) throws IOException, CsvValidationException {
        char delimiter = config.getDelimiter().charAt(0);
        
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(new CSVParserBuilder().withSeparator(delimiter).build())
                .build()) {
            
            String[] header;
            if (config.isHasHeader()) {
                header = reader.readNext();
            } else {
                // If no header, read first row and generate column names
                header = reader.readNext();
                for (int i = 0; i < header.length; i++) {
                    header[i] = "column" + (i + 1);
                }
            }
            
            List<TableColumn> columns = new ArrayList<>();
            for (String columnName : header) {
                // Default to String type for all columns from flat file
                TableColumn column = new TableColumn(columnName, "String", true);
                columns.add(column);
            }
            
            return new TableInfo("imported_data", columns, true);
        }
    }
    
    /**
     * Writes data to a CSV file
     */
    public String writeToCSV(List<String[]> data, FlatFileConfig config) throws IOException {
        Path dirPath = Paths.get(uploadDir);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        
        String fileName = config.getFileName();
        if (fileName == null || fileName.isEmpty()) {
            fileName = "export_" + System.currentTimeMillis() + ".csv";
        }
        
        Path filePath = dirPath.resolve(fileName);
        
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath.toFile()), 
                config.getDelimiter().charAt(0),
                CSVWriter.DEFAULT_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {
            
            writer.writeAll(data);
        }
        
        return filePath.toString();
    }
    
    /**
     * Reads data from a CSV file
     */
    public List<String[]> readFromCSV(String filePath, FlatFileConfig config) throws IOException, CsvValidationException {
        char delimiter = config.getDelimiter().charAt(0);
        
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(new CSVParserBuilder().withSeparator(delimiter).build())
                .build()) {
            
            List<String[]> data = new ArrayList<>();
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                data.add(nextLine);
            }
            
            return data;
        }
    }
    
    /**
     * Gets the file path for a multipart file
     */
    public File getFile(MultipartFile file) throws IOException {
        return new File(saveUploadedFile(file));
    }
    
    /**
     * Creates a download link for a file
     */
    public String createDownloadLink(String filePath) {
        File file = new File(filePath);
        return "/api/download/" + file.getName();
    }
} 