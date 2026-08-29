package com.wrangler.ingestion.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class FlatFileService {

    public List<String> getColumns(MultipartFile file, String delimiter) throws IOException {
        List<String> columns = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String headerLine = reader.readLine();
            if (headerLine != null) {
                String[] headers = headerLine.split(delimiter);
                for (String header : headers) {
                    columns.add(header.trim());
                }
            }
        }
        return columns;
    }

    public long getRowCount(MultipartFile file, String delimiter) throws IOException {
        long count = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            // Skip header
            reader.readLine();
            
            while (reader.readLine() != null) {
                count++;
            }
        }
        return count;
    }

    public void writeToFile(List<String> data, String filePath, String delimiter) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (String line : data) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    public List<String> readFromFile(String filePath, String delimiter) throws IOException {
        List<String> data = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                data.add(line);
            }
        }
        return data;
    }
} 