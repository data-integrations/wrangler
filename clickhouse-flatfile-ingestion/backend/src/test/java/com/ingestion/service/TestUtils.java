package com.ingestion.service;

import com.ingestion.dto.ConnectionRequest;
import com.ingestion.dto.FlatFileConfig;
import com.ingestion.dto.IngestionRequest;
import com.ingestion.dto.TableMappingRequest;
import com.ingestion.entity.TableMapping;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TestUtils {

    public static ConnectionRequest createTestConnectionRequest() {
        ConnectionRequest request = new ConnectionRequest();
        request.setHost("localhost");
        request.setPort(8123);
        request.setDatabase("testdb");
        request.setUsername("default");
        request.setPassword("");
        return request;
    }

    public static TableMappingRequest createTestTableMappingRequest() {
        TableMappingRequest request = new TableMappingRequest();
        request.setTableName("test_table");
        
        Map<String, String> schemaDefinition = new HashMap<>();
        schemaDefinition.put("id", "UInt32");
        schemaDefinition.put("name", "String");
        schemaDefinition.put("age", "UInt8");
        request.setSchemaDefinition(schemaDefinition);
        
        Map<String, String> columnMappings = new HashMap<>();
        columnMappings.put("id", "id");
        columnMappings.put("name", "name");
        columnMappings.put("age", "age");
        request.setColumnMappings(columnMappings);
        
        return request;
    }

    public static FlatFileConfig createTestFlatFileConfig() {
        FlatFileConfig config = new FlatFileConfig();
        config.setDelimiter(',');
        config.setQuoteCharacter('"');
        config.setEscapeCharacter('\\');
        config.setHasHeader(true);
        config.setSkipEmptyLines(true);
        config.setTrimValues(true);
        return config;
    }

    public static IngestionRequest createTestIngestionRequest() {
        IngestionRequest request = new IngestionRequest();
        request.setTableName("test_table");
        request.setConnectionId("test_connection");
        request.setFileConfig(createTestFlatFileConfig());
        return request;
    }

    public static MultipartFile createTestCSVFile() throws IOException {
        String content = "id,name,age\n1,John,30\n2,Jane,25\n3,Bob,40";
        return new MockMultipartFile(
            "file", 
            "test.csv", 
            "text/csv", 
            new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
        );
    }

    public static TableMapping createTestTableMapping() {
        TableMapping tableMapping = new TableMapping();
        tableMapping.setId(1L);
        tableMapping.setTableName("test_table");
        
        Map<String, String> schemaDefinition = new HashMap<>();
        schemaDefinition.put("id", "UInt32");
        schemaDefinition.put("name", "String");
        schemaDefinition.put("age", "UInt8");
        tableMapping.setSchemaDefinition(schemaDefinition);
        
        Map<String, String> columnMappings = new HashMap<>();
        columnMappings.put("id", "id");
        columnMappings.put("name", "name");
        columnMappings.put("age", "age");
        tableMapping.setColumnMappings(columnMappings);
        
        return tableMapping;
    }
} 