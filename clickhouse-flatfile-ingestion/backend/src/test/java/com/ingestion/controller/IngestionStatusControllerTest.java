package com.ingestion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ingestion.dto.IngestionRequest;
import com.ingestion.entity.IngestionStatus;
import com.ingestion.service.DataIngestionService;
import com.ingestion.service.IngestionStatusService;
import com.ingestion.service.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(IngestionStatusController.class)
public class IngestionStatusControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private DataIngestionService dataIngestionService;

    @MockBean
    private IngestionStatusService ingestionStatusService;

    private IngestionRequest testRequest;
    private IngestionStatus testStatus;
    private MockMultipartFile testFile;

    @BeforeEach
    void setUp() throws Exception {
        testRequest = TestUtils.createTestIngestionRequest();
        testFile = new MockMultipartFile(
            "file",
            "test.csv",
            "text/csv",
            "id,name,age\n1,John,30".getBytes()
        );

        testStatus = new IngestionStatus();
        testStatus.setId("test-status-id");
        testStatus.setStatus(IngestionStatus.Status.IN_PROGRESS);
        testStatus.setMessage("Processing");
        testStatus.setStartTime(LocalDateTime.now());
        testStatus.setRowsProcessed(0L);
    }

    @Test
    void testStartIngestion_Success() throws Exception {
        // Arrange
        when(dataIngestionService.startIngestion(any(), any(IngestionRequest.class)))
            .thenReturn(testStatus.getId());

        // Act & Assert
        mockMvc.perform(multipart("/api/ingestion/start")
                .file(testFile)
                .param("tableName", testRequest.getTableName())
                .param("connectionId", testRequest.getConnectionId()))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.statusId").value(testStatus.getId()));

        verify(dataIngestionService).startIngestion(any(), any(IngestionRequest.class));
    }

    @Test
    void testGetStatus_Success() throws Exception {
        // Arrange
        when(ingestionStatusService.getStatus(testStatus.getId())).thenReturn(testStatus);

        // Act & Assert
        mockMvc.perform(get("/api/ingestion/status/{statusId}", testStatus.getId()))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value(testStatus.getId()))
            .andExpect(jsonPath("$.status").value(testStatus.getStatus().toString()))
            .andExpect(jsonPath("$.message").value(testStatus.getMessage()));

        verify(ingestionStatusService).getStatus(testStatus.getId());
    }

    @Test
    void testGetStatus_NotFound() throws Exception {
        // Arrange
        String statusId = "non-existent-id";
        when(ingestionStatusService.getStatus(statusId))
            .thenThrow(new RuntimeException("Status not found"));

        // Act & Assert
        mockMvc.perform(get("/api/ingestion/status/{statusId}", statusId))
            .andExpect(status().isNotFound());

        verify(ingestionStatusService).getStatus(statusId);
    }

    @Test
    void testGetAllStatuses_Success() throws Exception {
        // Arrange
        IngestionStatus status2 = new IngestionStatus();
        status2.setId("test-status-id-2");
        status2.setStatus(IngestionStatus.Status.COMPLETED);
        status2.setMessage("Completed");
        status2.setStartTime(LocalDateTime.now());
        status2.setEndTime(LocalDateTime.now());
        status2.setRowsProcessed(100L);

        List<IngestionStatus> statuses = Arrays.asList(testStatus, status2);
        when(ingestionStatusService.getAllStatuses()).thenReturn(statuses);

        // Act & Assert
        mockMvc.perform(get("/api/ingestion/status"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].id").value(testStatus.getId()))
            .andExpect(jsonPath("$[1].id").value(status2.getId()))
            .andExpect(jsonPath("$[0].status").value(testStatus.getStatus().toString()))
            .andExpect(jsonPath("$[1].status").value(status2.getStatus().toString()));

        verify(ingestionStatusService).getAllStatuses();
    }

    @Test
    void testDeleteStatus_Success() throws Exception {
        // Arrange
        doNothing().when(ingestionStatusService).deleteStatus(testStatus.getId());

        // Act & Assert
        mockMvc.perform(delete("/api/ingestion/status/{statusId}", testStatus.getId()))
            .andExpect(status().isNoContent());

        verify(ingestionStatusService).deleteStatus(testStatus.getId());
    }

    @Test
    void testDeleteStatus_NotFound() throws Exception {
        // Arrange
        String statusId = "non-existent-id";
        doThrow(new RuntimeException("Status not found"))
            .when(ingestionStatusService).deleteStatus(statusId);

        // Act & Assert
        mockMvc.perform(delete("/api/ingestion/status/{statusId}", statusId))
            .andExpect(status().isNotFound());

        verify(ingestionStatusService).deleteStatus(statusId);
    }

    @Test
    void testStartIngestion_InvalidRequest() throws Exception {
        // Act & Assert
        mockMvc.perform(multipart("/api/ingestion/start")
                .file(testFile))
            .andExpect(status().isBadRequest());

        verify(dataIngestionService, never()).startIngestion(any(), any(IngestionRequest.class));
    }

    @Test
    void testStartIngestion_NoFile() throws Exception {
        // Act & Assert
        mockMvc.perform(multipart("/api/ingestion/start")
                .param("tableName", testRequest.getTableName())
                .param("connectionId", testRequest.getConnectionId()))
            .andExpect(status().isBadRequest());

        verify(dataIngestionService, never()).startIngestion(any(), any(IngestionRequest.class));
    }
} 