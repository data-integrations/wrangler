package com.ingestion.service;

import com.ingestion.dto.IngestionRequest;
import com.ingestion.entity.IngestionStatus;
import com.ingestion.entity.TableMapping;
import com.ingestion.model.FlatFileConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DataIngestionServiceTest {

    @InjectMocks
    private DataIngestionService dataIngestionService;

    @Mock
    private FlatFileService flatFileService;

    @Mock
    private ClickHouseConnectionService clickHouseConnectionService;

    @Mock
    private TableMappingService tableMappingService;

    @Mock
    private IngestionStatusService ingestionStatusService;

    private MultipartFile testFile;
    private IngestionRequest testRequest;
    private TableMapping testTableMapping;
    private Path testFilePath;

    @BeforeEach
    void setUp() throws IOException {
        testFile = TestUtils.createTestCSVFile();
        testRequest = TestUtils.createTestIngestionRequest();
        testTableMapping = TestUtils.createTestTableMapping();
        testFilePath = Path.of("test.csv");
    }

    @Test
    void testStartIngestion_Success() throws IOException {
        // Arrange
        when(flatFileService.uploadFile(any(), any())).thenReturn(testFilePath);
        when(ingestionStatusService.createStatus(anyString())).thenReturn("test-status-id");

        // Act
        String statusId = dataIngestionService.startIngestion(testFile, testRequest);

        // Assert
        assertNotNull(statusId);
        assertEquals("test-status-id", statusId);
        verify(flatFileService).uploadFile(any(), any());
        verify(ingestionStatusService).createStatus(anyString());
    }

    @Test
    void testProcessIngestion_Success() throws IOException, SQLException {
        // Arrange
        Map<String, Object> parseResult = new HashMap<>();
        parseResult.put("headers", Arrays.asList("id", "name", "age"));
        List<Map<String, String>> data = Arrays.asList(
            Map.of("id", "1", "name", "John", "age", "30"),
            Map.of("id", "2", "name", "Jane", "age", "25")
        );
        parseResult.put("data", data);

        when(tableMappingService.getTableMapping(anyString())).thenReturn(testTableMapping);
        when(flatFileService.parseCSVFile(any(), any())).thenReturn(parseResult);
        when(clickHouseConnectionService.executeBatchInsert(anyString(), anyString(), any(), any())).thenReturn(2);

        // Act
        boolean result = dataIngestionService.processIngestion(testFilePath, testRequest, "test-status-id");

        // Assert
        assertTrue(result);
        verify(tableMappingService).getTableMapping(testRequest.getTableName());
        verify(flatFileService).parseCSVFile(any(), any());
        verify(clickHouseConnectionService).executeBatchInsert(anyString(), anyString(), any(), any());
        verify(ingestionStatusService).updateStatus(anyString(), any(IngestionStatus.Status.class), anyString(), anyLong());
    }

    @Test
    void testProcessIngestion_EmptyFile() throws IOException {
        // Arrange
        Map<String, Object> parseResult = new HashMap<>();
        parseResult.put("headers", Arrays.asList("id", "name", "age"));
        parseResult.put("data", List.of());

        when(tableMappingService.getTableMapping(anyString())).thenReturn(testTableMapping);
        when(flatFileService.parseCSVFile(any(), any())).thenReturn(parseResult);

        // Act
        boolean result = dataIngestionService.processIngestion(testFilePath, testRequest, "test-status-id");

        // Assert
        assertFalse(result);
        verify(tableMappingService).getTableMapping(testRequest.getTableName());
        verify(flatFileService).parseCSVFile(any(), any());
        verify(ingestionStatusService).updateStatus(anyString(), eq(IngestionStatus.Status.FAILED), anyString(), eq(0L));
    }

    @Test
    void testProcessIngestion_InvalidData() throws IOException {
        // Arrange
        Map<String, Object> parseResult = new HashMap<>();
        parseResult.put("headers", Arrays.asList("invalid_column"));
        parseResult.put("data", List.of(Map.of("invalid_column", "value")));

        when(tableMappingService.getTableMapping(anyString())).thenReturn(testTableMapping);
        when(flatFileService.parseCSVFile(any(), any())).thenReturn(parseResult);

        // Act
        boolean result = dataIngestionService.processIngestion(testFilePath, testRequest, "test-status-id");

        // Assert
        assertFalse(result);
        verify(tableMappingService).getTableMapping(testRequest.getTableName());
        verify(flatFileService).parseCSVFile(any(), any());
        verify(ingestionStatusService).updateStatus(anyString(), eq(IngestionStatus.Status.FAILED), anyString(), eq(0L));
    }

    @Test
    void testProcessIngestion_DatabaseError() throws IOException, SQLException {
        // Arrange
        Map<String, Object> parseResult = new HashMap<>();
        parseResult.put("headers", Arrays.asList("id", "name", "age"));
        List<Map<String, String>> data = Arrays.asList(
            Map.of("id", "1", "name", "John", "age", "30")
        );
        parseResult.put("data", data);

        when(tableMappingService.getTableMapping(anyString())).thenReturn(testTableMapping);
        when(flatFileService.parseCSVFile(any(), any())).thenReturn(parseResult);
        when(clickHouseConnectionService.executeBatchInsert(anyString(), anyString(), any(), any()))
            .thenThrow(new SQLException("Database error"));

        // Act
        boolean result = dataIngestionService.processIngestion(testFilePath, testRequest, "test-status-id");

        // Assert
        assertFalse(result);
        verify(tableMappingService).getTableMapping(testRequest.getTableName());
        verify(flatFileService).parseCSVFile(any(), any());
        verify(ingestionStatusService).updateStatus(anyString(), eq(IngestionStatus.Status.FAILED), anyString(), eq(0L));
    }

    @Test
    void testCreateFlatFileConfig_Success() {
        // Arrange
        com.ingestion.dto.FlatFileConfig dtoConfig = new com.ingestion.dto.FlatFileConfig();
        dtoConfig.setDelimiter(',');
        dtoConfig.setQuoteCharacter('"');
        dtoConfig.setEscapeCharacter('\\');
        dtoConfig.setHasHeader(true);
        dtoConfig.setSkipEmptyLines(true);
        dtoConfig.setTrimValues(true);

        // Act
        FlatFileConfig result = dataIngestionService.createFlatFileConfig(dtoConfig);

        // Assert
        assertNotNull(result);
        assertEquals(dtoConfig.getDelimiter(), result.getDelimiter());
        assertEquals(dtoConfig.getQuoteCharacter(), result.getQuoteCharacter());
        assertEquals(dtoConfig.getEscapeCharacter(), result.getEscapeCharacter());
        assertEquals(dtoConfig.isHasHeader(), result.isHasHeader());
        assertEquals(dtoConfig.isSkipEmptyLines(), result.isSkipEmptyLines());
        assertEquals(dtoConfig.isTrimValues(), result.isTrimValues());
    }
} 