package com.ingestion.service;

import com.ingestion.dto.FileUploadRequest;
import com.ingestion.model.FlatFileConfig;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class FlatFileServiceTest {

    @InjectMocks
    private FlatFileService flatFileService;

    private Path tempDir;
    private MultipartFile testFile;
    private FlatFileConfig testConfig;

    @BeforeEach
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("flatfile-test");
        testFile = createTestCSVFile();
        testConfig = TestUtils.createTestFlatFileConfig();
    }

    @Test
    public void testUploadFile_Success() throws IOException {
        // Arrange
        FileUploadRequest request = new FileUploadRequest();
        request.setFileName("test.csv");

        // Act
        Path uploadedPath = flatFileService.uploadFile(testFile, request);

        // Assert
        assertTrue(Files.exists(uploadedPath));
        assertTrue(Files.isRegularFile(uploadedPath));
        assertEquals("test.csv", uploadedPath.getFileName().toString());
    }

    @Test
    public void testParseCSVFile_Success() throws IOException {
        // Arrange
        Path filePath = flatFileService.uploadFile(testFile, new FileUploadRequest());

        // Act
        Map<String, List<String>> result = flatFileService.parseCSVFile(filePath, testConfig);

        // Assert
        assertNotNull(result);
        assertTrue(result.containsKey("id"));
        assertTrue(result.containsKey("name"));
        assertTrue(result.containsKey("age"));
        assertEquals(2, result.get("id").size());
        assertEquals(2, result.get("name").size());
        assertEquals(2, result.get("age").size());
    }

    @Test
    public void testStreamCSVFile_Success() throws IOException {
        // Arrange
        Path filePath = flatFileService.uploadFile(testFile, new FileUploadRequest());
        AtomicLong rowCount = new AtomicLong(0);

        // Act
        long processedRows = flatFileService.streamCSVFile(filePath, testConfig, (batch) -> {
            rowCount.addAndGet(batch.size());
            return true;
        });

        // Assert
        assertEquals(2, processedRows);
        assertEquals(2, rowCount.get());
    }

    @Test
    public void testCreateCSVFormat_Success() {
        // Act
        CSVFormat format = flatFileService.createCSVFormat(testConfig);

        // Assert
        assertNotNull(format);
        assertEquals(testConfig.getDelimiter(), format.getDelimiter());
        assertEquals(testConfig.getQuoteCharacter(), format.getQuoteCharacter());
        assertEquals(testConfig.getEscapeCharacter(), format.getEscapeCharacter());
        assertEquals(testConfig.isHasHeader(), format.getSkipHeaderRecord());
    }

    @Test
    public void testParseCSVFile_InvalidFile() throws IOException {
        // Arrange
        MultipartFile invalidFile = new MockMultipartFile(
            "test.csv",
            "test.csv",
            "text/csv",
            "invalid,csv,data\nwithout,proper,format".getBytes()
        );
        Path filePath = flatFileService.uploadFile(invalidFile, new FileUploadRequest());

        // Act & Assert
        assertThrows(IOException.class, () -> {
            flatFileService.parseCSVFile(filePath, testConfig);
        });
    }

    @Test
    public void testStreamCSVFile_EmptyFile() throws IOException {
        // Arrange
        MultipartFile emptyFile = new MockMultipartFile(
            "empty.csv",
            "empty.csv",
            "text/csv",
            "".getBytes()
        );
        Path filePath = flatFileService.uploadFile(emptyFile, new FileUploadRequest());
        AtomicLong rowCount = new AtomicLong(0);

        // Act
        long processedRows = flatFileService.streamCSVFile(filePath, testConfig, (batch) -> {
            rowCount.addAndGet(batch.size());
            return true;
        });

        // Assert
        assertEquals(0, processedRows);
        assertEquals(0, rowCount.get());
    }

    @Test
    void testCSVWithQuotes() throws IOException {
        String content = "header1,header2\n\"value,1\",\"value,2\"\nvalue3,value4";
        Path filePath = tempDir.resolve("test.csv");
        Files.write(filePath, content.getBytes());

        Map<String, List<String>> result = flatFileService.parseCSVFile(filePath, testConfig);
        List<String> data = result.get("data");

        assertEquals(2, data.size());
        assertEquals("value,1,value,2", data.get(0));
        assertEquals("value3,value4", data.get(1));
    }

    @Test
    void testCSVWithEmptyLines() throws IOException {
        testConfig.setSkipEmptyLines(true);
        String content = "header1,header2\nvalue1,value2\n\nvalue3,value4";
        Path filePath = tempDir.resolve("test.csv");
        Files.write(filePath, content.getBytes());

        Map<String, List<String>> result = flatFileService.parseCSVFile(filePath, testConfig);
        List<String> data = result.get("data");

        assertEquals(2, data.size());
        assertEquals("value1,value2", data.get(0));
        assertEquals("value3,value4", data.get(1));
    }

    @Test
    void testCSVWithoutHeader() throws IOException {
        testConfig.setHasHeader(false);
        String content = "value1,value2\nvalue3,value4";
        Path filePath = tempDir.resolve("test.csv");
        Files.write(filePath, content.getBytes());

        Map<String, List<String>> result = flatFileService.parseCSVFile(filePath, testConfig);
        List<String> headers = result.get("headers");
        List<String> data = result.get("data");

        assertTrue(headers.isEmpty());
        assertEquals(2, data.size());
        assertEquals("value1,value2", data.get(0));
        assertEquals("value3,value4", data.get(1));
    }

    private MultipartFile createTestCSVFile() {
        String csvContent = "id,name,age\n1,John,30\n2,Jane,25";
        return new MockMultipartFile(
            "test.csv",
            "test.csv",
            "text/csv",
            csvContent.getBytes()
        );
    }
} 