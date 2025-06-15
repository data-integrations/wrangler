package com.example.clickhouse.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ContextConfiguration(classes = TestConfig.class)
public class DataIngestionServiceTest {

    @Autowired
    private DataIngestionService dataIngestionService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setup() {
        // Create test tables if they don't exist
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS test_table1 (id Int32, name String, value Float64) ENGINE = Memory");
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS test_table2 (id Int32, description String, amount Float64) ENGINE = Memory");
        
        // Insert test data
        jdbcTemplate.execute("INSERT INTO test_table1 VALUES (1, 'Test1', 10.5), (2, 'Test2', 20.5)");
        jdbcTemplate.execute("INSERT INTO test_table2 VALUES (1, 'Desc1', 100.5), (2, 'Desc2', 200.5)");
    }

    @Test
    void testSingleTableExport() {
        // Test Case 1: Single ClickHouse table -> Flat File
        List<String> columns = Arrays.asList("id", "name", "value");
        int count = dataIngestionService.exportFromClickHouse("test_table1", columns, "csv");
        
        assertEquals(2, count, "Should export 2 records");
    }

    @Test
    void testFileImport() throws IOException {
        // Test Case 2: Flat File -> ClickHouse table
        String csvContent = "3,Test3,30.5\n4,Test4,40.5";
        MockMultipartFile file = new MockMultipartFile(
            "file",
            "test.csv",
            "text/csv",
            csvContent.getBytes()
        );

        List<String> columns = Arrays.asList("id", "name", "value");
        int count = dataIngestionService.importToClickHouse(file, "test_table1", columns);
        
        assertEquals(2, count, "Should import 2 records");
        
        // Verify data in database
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT * FROM test_table1 WHERE id IN (3, 4)");
        assertEquals(2, results.size(), "Should find 2 new records");
    }

    @Test
    void testMultiTableJoinExport() {
        // Test Case 3: Joined ClickHouse tables -> Flat File
        List<String> tableNames = Arrays.asList("test_table1", "test_table2");
        List<String> columns = Arrays.asList("test_table1.id", "test_table1.name", 
                                           "test_table2.description", "test_table2.amount");
        List<String> joinConditions = Arrays.asList("WHERE test_table1.id = test_table2.id");
        
        int count = dataIngestionService.exportFromClickHouseWithJoin(
            tableNames, columns, joinConditions, "csv");
        
        assertEquals(2, count, "Should export 2 joined records");
    }

    @Test
    void testConnectionFailure() {
        // Test Case 4: Connection/Authentication failure
        assertThrows(Exception.class, () -> {
            dataIngestionService.exportFromClickHouse("non_existent_table", 
                Arrays.asList("id"), "csv");
        });
    }

    @Test
    void testDataPreview() {
        // Test Case 5: Data preview
        List<Map<String, Object>> preview = dataIngestionService.previewData(
            "test_table1", Arrays.asList("id", "name"), 1);
        
        assertEquals(1, preview.size(), "Should return 1 record");
        assertTrue(preview.get(0).containsKey("id"), "Should contain id column");
        assertTrue(preview.get(0).containsKey("name"), "Should contain name column");
    }

    @Test
    void testJoinDataPreview() {
        // Test Case 5: Join data preview
        List<String> tableNames = Arrays.asList("test_table1", "test_table2");
        List<String> columns = Arrays.asList("test_table1.id", "test_table1.name", 
                                           "test_table2.description");
        List<String> joinConditions = Arrays.asList("WHERE test_table1.id = test_table2.id");
        
        List<Map<String, Object>> preview = dataIngestionService.previewDataWithJoin(
            tableNames, columns, joinConditions, 1);
        
        assertEquals(1, preview.size(), "Should return 1 joined record");
        assertTrue(preview.get(0).containsKey("test_table1.id"), "Should contain id column");
        assertTrue(preview.get(0).containsKey("test_table1.name"), "Should contain name column");
        assertTrue(preview.get(0).containsKey("test_table2.description"), "Should contain description column");
    }
} 