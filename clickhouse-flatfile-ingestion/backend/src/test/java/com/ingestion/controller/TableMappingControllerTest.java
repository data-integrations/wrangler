package com.ingestion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ingestion.dto.TableMappingRequest;
import com.ingestion.entity.TableMapping;
import com.ingestion.service.TableMappingService;
import com.ingestion.service.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(TableMappingController.class)
public class TableMappingControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private TableMappingService tableMappingService;

    private TableMappingRequest testRequest;
    private TableMapping testTableMapping;

    @BeforeEach
    void setUp() {
        testRequest = TestUtils.createTestTableMappingRequest();
        testTableMapping = TestUtils.createTestTableMapping();
    }

    @Test
    void testCreateTableMapping_Success() throws Exception {
        // Arrange
        when(tableMappingService.createTableMapping(any(TableMappingRequest.class)))
            .thenReturn(testTableMapping);

        // Act & Assert
        mockMvc.perform(post("/api/table-mappings")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testRequest)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.tableName").value(testTableMapping.getTableName()))
            .andExpect(jsonPath("$.schemaDefinition").exists())
            .andExpect(jsonPath("$.columnMappings").exists());

        verify(tableMappingService).createTableMapping(any(TableMappingRequest.class));
    }

    @Test
    void testGetTableMapping_Success() throws Exception {
        // Arrange
        when(tableMappingService.getTableMapping(testTableMapping.getTableName()))
            .thenReturn(testTableMapping);

        // Act & Assert
        mockMvc.perform(get("/api/table-mappings/{tableName}", testTableMapping.getTableName()))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.tableName").value(testTableMapping.getTableName()))
            .andExpect(jsonPath("$.schemaDefinition").exists())
            .andExpect(jsonPath("$.columnMappings").exists());

        verify(tableMappingService).getTableMapping(testTableMapping.getTableName());
    }

    @Test
    void testGetTableMapping_NotFound() throws Exception {
        // Arrange
        String tableName = "non_existent_table";
        when(tableMappingService.getTableMapping(tableName))
            .thenThrow(new RuntimeException("Table mapping not found"));

        // Act & Assert
        mockMvc.perform(get("/api/table-mappings/{tableName}", tableName))
            .andExpect(status().isNotFound());

        verify(tableMappingService).getTableMapping(tableName);
    }

    @Test
    void testUpdateTableMapping_Success() throws Exception {
        // Arrange
        when(tableMappingService.updateTableMapping(eq(testTableMapping.getTableName()), any(TableMappingRequest.class)))
            .thenReturn(testTableMapping);

        // Act & Assert
        mockMvc.perform(put("/api/table-mappings/{tableName}", testTableMapping.getTableName())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testRequest)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.tableName").value(testTableMapping.getTableName()))
            .andExpect(jsonPath("$.schemaDefinition").exists())
            .andExpect(jsonPath("$.columnMappings").exists());

        verify(tableMappingService).updateTableMapping(eq(testTableMapping.getTableName()), any(TableMappingRequest.class));
    }

    @Test
    void testUpdateTableMapping_NotFound() throws Exception {
        // Arrange
        String tableName = "non_existent_table";
        when(tableMappingService.updateTableMapping(eq(tableName), any(TableMappingRequest.class)))
            .thenThrow(new RuntimeException("Table mapping not found"));

        // Act & Assert
        mockMvc.perform(put("/api/table-mappings/{tableName}", tableName)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testRequest)))
            .andExpect(status().isNotFound());

        verify(tableMappingService).updateTableMapping(eq(tableName), any(TableMappingRequest.class));
    }

    @Test
    void testDeleteTableMapping_Success() throws Exception {
        // Arrange
        doNothing().when(tableMappingService).deleteTableMapping(testTableMapping.getTableName());

        // Act & Assert
        mockMvc.perform(delete("/api/table-mappings/{tableName}", testTableMapping.getTableName()))
            .andExpect(status().isNoContent());

        verify(tableMappingService).deleteTableMapping(testTableMapping.getTableName());
    }

    @Test
    void testDeleteTableMapping_NotFound() throws Exception {
        // Arrange
        String tableName = "non_existent_table";
        doThrow(new RuntimeException("Table mapping not found"))
            .when(tableMappingService).deleteTableMapping(tableName);

        // Act & Assert
        mockMvc.perform(delete("/api/table-mappings/{tableName}", tableName))
            .andExpect(status().isNotFound());

        verify(tableMappingService).deleteTableMapping(tableName);
    }
} 