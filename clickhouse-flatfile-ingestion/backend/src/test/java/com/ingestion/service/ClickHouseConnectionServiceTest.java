package com.ingestion.service;

import com.ingestion.dto.ConnectionRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ClickHouseConnectionServiceTest {

    @InjectMocks
    private ClickHouseConnectionService clickHouseConnectionService;

    @Mock
    private ClickHouseDataSource mockDataSource;

    @Mock
    private Connection mockConnection;

    @Mock
    private DatabaseMetaData mockMetaData;

    @Mock
    private ResultSet mockResultSet;

    @BeforeEach
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getTables(any(), any(), any(), any())).thenReturn(mockResultSet);
        when(mockMetaData.getColumns(any(), any(), any(), any())).thenReturn(mockResultSet);
    }

    @Test
    public void testCreateConnection_Success() throws SQLException {
        // Arrange
        ConnectionRequest request = TestUtils.createTestConnectionRequest();
        
        // Act
        String connectionId = clickHouseConnectionService.createConnection(request);
        
        // Assert
        assertNotNull(connectionId);
        assertTrue(connectionId.contains(request.getHost()));
        assertTrue(connectionId.contains(request.getDatabase()));
    }

    @Test
    public void testCreateConnection_DuplicateConnection() throws SQLException {
        // Arrange
        ConnectionRequest request = TestUtils.createTestConnectionRequest();
        
        // Act
        String connectionId1 = clickHouseConnectionService.createConnection(request);
        String connectionId2 = clickHouseConnectionService.createConnection(request);
        
        // Assert
        assertEquals(connectionId1, connectionId2);
    }

    @Test
    public void testTestConnection_Success() {
        // Arrange
        ConnectionRequest request = TestUtils.createTestConnectionRequest();
        
        // Act
        boolean result = clickHouseConnectionService.testConnection(request);
        
        // Assert
        assertTrue(result);
    }

    @Test
    public void testTestConnection_Failure() throws SQLException {
        // Arrange
        ConnectionRequest request = TestUtils.createTestConnectionRequest();
        when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection failed"));
        
        // Act
        boolean result = clickHouseConnectionService.testConnection(request);
        
        // Assert
        assertFalse(result);
    }

    @Test
    public void testGetTables_Success() throws SQLException {
        // Arrange
        String connectionId = "test_connection";
        ReflectionTestUtils.setField(clickHouseConnectionService, "connectionPool", Map.of(connectionId, mockDataSource));
        
        when(mockResultSet.next()).thenReturn(true, true, false);
        when(mockResultSet.getString("TABLE_NAME")).thenReturn("table1", "table2");
        
        // Act
        List<String> tables = clickHouseConnectionService.getTables(connectionId);
        
        // Assert
        assertEquals(2, tables.size());
        assertTrue(tables.contains("table1"));
        assertTrue(tables.contains("table2"));
    }

    @Test
    public void testGetTableSchema_Success() throws SQLException {
        // Arrange
        String connectionId = "test_connection";
        String tableName = "test_table";
        ReflectionTestUtils.setField(clickHouseConnectionService, "connectionPool", Map.of(connectionId, mockDataSource));
        
        when(mockResultSet.next()).thenReturn(true, true, false);
        when(mockResultSet.getString("COLUMN_NAME")).thenReturn("id", "name");
        when(mockResultSet.getString("TYPE_NAME")).thenReturn("UInt32", "String");
        
        // Act
        Map<String, String> schema = clickHouseConnectionService.getTableSchema(connectionId, tableName);
        
        // Assert
        assertEquals(2, schema.size());
        assertEquals("UInt32", schema.get("id"));
        assertEquals("String", schema.get("name"));
    }

    @Test
    public void testExecuteQuery_Success() throws SQLException {
        // Arrange
        String connectionId = "test_connection";
        String query = "SELECT * FROM test_table";
        ReflectionTestUtils.setField(clickHouseConnectionService, "connectionPool", Map.of(connectionId, mockDataSource));
        
        when(mockConnection.createStatement()).thenReturn(mock(java.sql.Statement.class));
        when(mockConnection.createStatement().executeQuery(query)).thenReturn(mockResultSet);
        
        // Act
        ResultSet resultSet = clickHouseConnectionService.executeQuery(connectionId, query);
        
        // Assert
        assertNotNull(resultSet);
        verify(mockConnection.createStatement()).executeQuery(query);
    }

    @Test
    public void testExecuteBatchInsert_Success() throws SQLException {
        // Arrange
        String connectionId = "test_connection";
        String tableName = "test_table";
        List<String> columns = List.of("id", "name", "age");
        List<Object[]> values = List.of(
            new Object[]{1, "John", 30},
            new Object[]{2, "Jane", 25}
        );
        
        ReflectionTestUtils.setField(clickHouseConnectionService, "connectionPool", Map.of(connectionId, mockDataSource));
        
        java.sql.PreparedStatement mockPreparedStatement = mock(java.sql.PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeBatch()).thenReturn(new int[]{1, 1});
        
        // Act
        int result = clickHouseConnectionService.executeBatchInsert(connectionId, tableName, columns, values);
        
        // Assert
        assertEquals(2, result);
        verify(mockPreparedStatement, times(2)).addBatch();
        verify(mockPreparedStatement).executeBatch();
        verify(mockConnection).commit();
    }
} 