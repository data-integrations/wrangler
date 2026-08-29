package com.ingestion.service;

import com.ingestion.entity.IngestionStatus;
import com.ingestion.repository.IngestionStatusRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IngestionStatusServiceTest {

    @InjectMocks
    private IngestionStatusService ingestionStatusService;

    @Mock
    private IngestionStatusRepository statusRepository;

    private IngestionStatus testStatus;

    @BeforeEach
    void setUp() {
        testStatus = new IngestionStatus();
        testStatus.setId("test-status-id");
        testStatus.setStatus(IngestionStatus.Status.IN_PROGRESS);
        testStatus.setMessage("Processing");
        testStatus.setStartTime(LocalDateTime.now());
        testStatus.setRowsProcessed(0L);
    }

    @Test
    void testCreateStatus_Success() {
        // Arrange
        String fileName = "test.csv";
        when(statusRepository.save(any(IngestionStatus.class))).thenReturn(testStatus);

        // Act
        String statusId = ingestionStatusService.createStatus(fileName);

        // Assert
        assertNotNull(statusId);
        assertEquals(testStatus.getId(), statusId);
        verify(statusRepository).save(any(IngestionStatus.class));
    }

    @Test
    void testGetStatus_Success() {
        // Arrange
        when(statusRepository.findById(testStatus.getId())).thenReturn(Optional.of(testStatus));

        // Act
        IngestionStatus result = ingestionStatusService.getStatus(testStatus.getId());

        // Assert
        assertNotNull(result);
        assertEquals(testStatus.getId(), result.getId());
        assertEquals(testStatus.getStatus(), result.getStatus());
        assertEquals(testStatus.getMessage(), result.getMessage());
        verify(statusRepository).findById(testStatus.getId());
    }

    @Test
    void testGetStatus_NotFound() {
        // Arrange
        String statusId = "non-existent-id";
        when(statusRepository.findById(statusId)).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(RuntimeException.class, () -> ingestionStatusService.getStatus(statusId));
        verify(statusRepository).findById(statusId);
    }

    @Test
    void testUpdateStatus_Success() {
        // Arrange
        when(statusRepository.findById(testStatus.getId())).thenReturn(Optional.of(testStatus));
        when(statusRepository.save(any(IngestionStatus.class))).thenReturn(testStatus);

        // Act
        IngestionStatus result = ingestionStatusService.updateStatus(
            testStatus.getId(),
            IngestionStatus.Status.COMPLETED,
            "Completed successfully",
            100L
        );

        // Assert
        assertNotNull(result);
        assertEquals(IngestionStatus.Status.COMPLETED, result.getStatus());
        assertEquals("Completed successfully", result.getMessage());
        assertEquals(100L, result.getRowsProcessed());
        assertNotNull(result.getEndTime());
        verify(statusRepository).findById(testStatus.getId());
        verify(statusRepository).save(any(IngestionStatus.class));
    }

    @Test
    void testUpdateStatus_NotFound() {
        // Arrange
        String statusId = "non-existent-id";
        when(statusRepository.findById(statusId)).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(RuntimeException.class, () ->
            ingestionStatusService.updateStatus(
                statusId,
                IngestionStatus.Status.COMPLETED,
                "Completed",
                100L
            ));
        verify(statusRepository).findById(statusId);
        verify(statusRepository, never()).save(any(IngestionStatus.class));
    }

    @Test
    void testGetAllStatuses_Success() {
        // Arrange
        IngestionStatus status2 = new IngestionStatus();
        status2.setId("test-status-id-2");
        status2.setStatus(IngestionStatus.Status.COMPLETED);
        
        List<IngestionStatus> statuses = Arrays.asList(testStatus, status2);
        when(statusRepository.findAll()).thenReturn(statuses);

        // Act
        List<IngestionStatus> results = ingestionStatusService.getAllStatuses();

        // Assert
        assertNotNull(results);
        assertEquals(2, results.size());
        assertEquals(testStatus.getId(), results.get(0).getId());
        assertEquals(status2.getId(), results.get(1).getId());
        verify(statusRepository).findAll();
    }

    @Test
    void testDeleteStatus_Success() {
        // Arrange
        when(statusRepository.findById(testStatus.getId())).thenReturn(Optional.of(testStatus));

        // Act
        ingestionStatusService.deleteStatus(testStatus.getId());

        // Assert
        verify(statusRepository).findById(testStatus.getId());
        verify(statusRepository).delete(testStatus);
    }

    @Test
    void testDeleteStatus_NotFound() {
        // Arrange
        String statusId = "non-existent-id";
        when(statusRepository.findById(statusId)).thenReturn(Optional.empty());

        // Act & Assert
        assertThrows(RuntimeException.class, () -> ingestionStatusService.deleteStatus(statusId));
        verify(statusRepository).findById(statusId);
        verify(statusRepository, never()).delete(any(IngestionStatus.class));
    }
} 