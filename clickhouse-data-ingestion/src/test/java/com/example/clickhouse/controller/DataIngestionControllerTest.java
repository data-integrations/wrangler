package com.example.clickhouse.controller;

import com.example.clickhouse.service.DataIngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(DataIngestionController.class)
public class DataIngestionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DataIngestionService dataIngestionService;

    @Test
    void testShowIngestionPage() throws Exception {
        mockMvc.perform(get("/ingestion"))
               .andExpect(status().isOk())
               .andExpect(view().name("ingestion"));
    }

    @Test
    void testSingleTableExport() throws Exception {
        when(dataIngestionService.exportFromClickHouse(any(), anyList(), any()))
            .thenReturn(2);

        mockMvc.perform(post("/ingestion/clickhouse-to-file")
                .param("tableName", "test_table")
                .param("columns", "id,name")
                .param("fileFormat", "csv"))
               .andExpect(status().isOk())
               .andExpect(view().name("ingestion"))
               .andExpect(model().attribute("message", "Successfully exported 2 records"));
    }

    @Test
    void testMultiTableJoinExport() throws Exception {
        when(dataIngestionService.exportFromClickHouseWithJoin(anyList(), anyList(), anyList(), any()))
            .thenReturn(2);

        mockMvc.perform(post("/ingestion/clickhouse-to-file/join")
                .param("tableNames", "table1,table2")
                .param("columns", "id,name")
                .param("joinConditions", "table1.id = table2.id")
                .param("fileFormat", "csv"))
               .andExpect(status().isOk())
               .andExpect(view().name("ingestion"))
               .andExpect(model().attribute("message", "Successfully exported 2 records"));
    }

    @Test
    void testFileImport() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
            "file",
            "test.csv",
            "text/csv",
            "1,Test1\n2,Test2".getBytes()
        );

        when(dataIngestionService.importToClickHouse(any(), any(), anyList()))
            .thenReturn(2);

        mockMvc.perform(multipart("/ingestion/file-to-clickhouse")
                .file(file)
                .param("tableName", "test_table")
                .param("columns", "id,name"))
               .andExpect(status().isOk())
               .andExpect(view().name("ingestion"))
               .andExpect(model().attribute("message", "Successfully imported 2 records"));
    }

    @Test
    void testDataPreview() throws Exception {
        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("name", "Test1");

        when(dataIngestionService.previewData(any(), anyList(), any()))
            .thenReturn(Collections.singletonList(record));

        mockMvc.perform(get("/ingestion/preview")
                .param("tableName", "test_table")
                .param("columns", "id,name"))
               .andExpect(status().isOk())
               .andExpect(jsonPath("$[0].id").value(1))
               .andExpect(jsonPath("$[0].name").value("Test1"));
    }

    @Test
    void testJoinDataPreview() throws Exception {
        Map<String, Object> record = new HashMap<>();
        record.put("table1.id", 1);
        record.put("table1.name", "Test1");
        record.put("table2.description", "Desc1");

        when(dataIngestionService.previewDataWithJoin(anyList(), anyList(), anyList(), any()))
            .thenReturn(Collections.singletonList(record));

        mockMvc.perform(get("/ingestion/preview/join")
                .param("tableNames", "table1,table2")
                .param("columns", "table1.id,table1.name,table2.description")
                .param("joinConditions", "table1.id = table2.id"))
               .andExpect(status().isOk())
               .andExpect(jsonPath("$[0]['table1.id']").value(1))
               .andExpect(jsonPath("$[0]['table1.name']").value("Test1"))
               .andExpect(jsonPath("$[0]['table2.description']").value("Desc1"));
    }

    @Test
    void testErrorHandling() throws Exception {
        when(dataIngestionService.exportFromClickHouse(any(), anyList(), any()))
            .thenThrow(new RuntimeException("Test error"));

        mockMvc.perform(post("/ingestion/clickhouse-to-file")
                .param("tableName", "test_table")
                .param("columns", "id,name")
                .param("fileFormat", "csv"))
               .andExpect(status().isOk())
               .andExpect(view().name("ingestion"))
               .andExpect(model().attribute("error", "Error during export: Test error"));
    }
} 