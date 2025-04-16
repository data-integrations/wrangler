package com.zeotap.dataingestion.service;

import com.opencsv.exceptions.CsvValidationException;
import com.zeotap.dataingestion.model.FlatFileConfig;
import com.zeotap.dataingestion.model.TableInfo;
import com.zeotap.dataingestion.util.FlatFileUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class FlatFileService {

    private final FlatFileUtil flatFileUtil;
    
    /**
     * Saves an uploaded file and extracts its schema
     */
    public TableInfo processUploadedFile(MultipartFile file, FlatFileConfig config) throws IOException, CsvValidationException {
        String filePath = flatFileUtil.saveUploadedFile(file);
        config.setFilePath(filePath);
        return flatFileUtil.extractSchema(filePath, config);
    }
    
    /**
     * Gets a preview of data from a flat file
     */
    public List<String[]> previewData(String filePath, FlatFileConfig config, int limit) throws IOException, CsvValidationException {
        List<String[]> allData = flatFileUtil.readFromCSV(filePath, config);
        
        // Return only up to the limit
        List<String[]> preview = new ArrayList<>();
        int count = 0;
        for (String[] row : allData) {
            preview.add(row);
            count++;
            if (count >= limit) {
                break;
            }
        }
        
        return preview;
    }
    
    /**
     * Reads data from a flat file, optionally filtering by selected columns
     */
    public List<String[]> readData(String filePath, FlatFileConfig config, List<Integer> selectedColumnIndices) throws IOException, CsvValidationException {
        List<String[]> allData = flatFileUtil.readFromCSV(filePath, config);
        
        // If no column selection, return all data
        if (selectedColumnIndices == null || selectedColumnIndices.isEmpty()) {
            return allData;
        }
        
        // Filter data by selected columns
        List<String[]> filteredData = new ArrayList<>();
        for (String[] row : allData) {
            String[] filteredRow = new String[selectedColumnIndices.size()];
            for (int i = 0; i < selectedColumnIndices.size(); i++) {
                int columnIndex = selectedColumnIndices.get(i);
                if (columnIndex < row.length) {
                    filteredRow[i] = row[columnIndex];
                }
            }
            filteredData.add(filteredRow);
        }
        
        return filteredData;
    }
    
    /**
     * Writes data to a flat file
     */
    public String writeData(List<String[]> data, FlatFileConfig config) throws IOException {
        return flatFileUtil.writeToCSV(data, config);
    }
    
    /**
     * Creates a download link for the exported file
     */
    public String createDownloadLink(String filePath) {
        return flatFileUtil.createDownloadLink(filePath);
    }
} 