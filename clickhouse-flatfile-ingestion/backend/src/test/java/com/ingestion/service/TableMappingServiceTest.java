package com.ingestion.service;

import com.ingestion.dto.TableMappingRequest;
import com.ingestion.entity.TableMapping;
import com.ingestion.repository.TableMappingRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TableMappingServiceTest {

    @InjectMocks
    private TableMappingService tableMappingService;

    @Mock
    private TableMappingRepository tableMappingRepository;

    private TableMappingRequest testRequest;
    private TableMapping testTableMapping;

    @BeforeEach
    void setUp() {
        testRequest = TestUtils.createTestTableMappingRequest();
        testTableMapping = TestUtils.createTestTableMapping();
    }

    @Test
    void testCreateTableMapping_Success() {
        // Arrange
        when(tableMappingRepository.save(any(TableMapping.class))).thenReturn(testTableMapping);

        // Act
        TableMapping result = tableMappingService.createTableMapping(testRequest);

        // Assert
        assertNotNull(result);
        assertEquals(testTableMapping.getTableName(), result.getTableName());
        assertEquals(testTableMapping.getSchemaDefinition(), result.getSchemaDefinition());
        assertEquals(testTableMapping.getColumnMappings(), result.getColumnMappings());
        verify(tableMappingRepository).save(any(TableMapping.class));
    }

    @Test
    void testGetTableMapping_Success() {
        // Arrange
        when(tableMappingRepository.findByTableName(testTableMapping.getTableName()))
            .thenReturn(Optional.of(testTableMapping));

        // Act
        TableMapping result = tableMappingService.getTableMapping(testTableMapping.getTableName());

        // Assert
        assertNotNull(result);
        assertEquals(testTableMapping.getTableName(), result.getTableName());
        assertEquals(testTableMapping.getSchemaDefinition(), result.getSchemaDefinition());
        assertEquals(testTableMapping.getColumnMappings(), result.getColumnMappings());
        verify(tableMappingRepository).findByTableName(testTableMapping.getTableName());
    }

    @Test
    void testGetTableMapping_NotFound() {
        // Arrange
        String tableName = "non_existent_table";
        when(tableMappingRepository.findByTableName(tableName)).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(RuntimeException.class, () -> tableMappingService.getTableMapping(tableName));
        verify(tableMappingRepository).findByTableName(tableName);
    }

    @Test
    void testUpdateTableMapping_Success() {
        // Arrange
        when(tableMappingRepository.findByTableName(testTableMapping.getTableName()))
            .thenReturn(Optional.of(testTableMapping));
        when(tableMappingRepository.save(any(TableMapping.class))).thenReturn(testTableMapping);

        // Act
        TableMapping result = tableMappingService.updateTableMapping(testTableMapping.getTableName(), testRequest);

        // Assert
        assertNotNull(result);
        assertEquals(testTableMapping.getTableName(), result.getTableName());
        assertEquals(testTableMapping.getSchemaDefinition(), result.getSchemaDefinition());
        assertEquals(testTableMapping.getColumnMappings(), result.getColumnMappings());
        verify(tableMappingRepository).findByTableName(testTableMapping.getTableName());
        verify(tableMappingRepository).save(any(TableMapping.class));
    }

    @Test
    void testUpdateTableMapping_NotFound() {
        // Arrange
        String tableName = "non_existent_table";
        when(tableMappingRepository.findByTableName(tableName)).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(RuntimeException.class, () -> 
            tableMappingService.updateTableMapping(tableName, testRequest));
        verify(tableMappingRepository).findByTableName(tableName);
        verify(tableMappingRepository, never()).save(any(TableMapping.class));
    }

    @Test
    void testDeleteTableMapping_Success() {
        // Arrange
        when(tableMappingRepository.findByTableName(testTableMapping.getTableName()))
            .thenReturn(Optional.of(testTableMapping));

        // Act
        tableMappingService.deleteTableMapping(testTableMapping.getTableName());

        // Assert
        verify(tableMappingRepository).findByTableName(testTableMapping.getTableName());
        verify(tableMappingRepository).delete(testTableMapping);
    }

    @Test
    void testDeleteTableMapping_NotFound() {
        // Arrange
        String tableName = "non_existent_table";
        when(tableMappingRepository.findByTableName(tableName)).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(RuntimeException.class, () -> tableMappingService.deleteTableMapping(tableName));
        verify(tableMappingRepository).findByTableName(tableName);
        verify(tableMappingRepository, never()).delete(any(TableMapping.class));
    }
} 