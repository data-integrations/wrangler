package com.ingestion.service;

import com.ingestion.entity.IngestionStatus;
import com.ingestion.entity.IngestionStatusEnum;
import com.ingestion.repository.IngestionStatusRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class IngestionStatusService {

    private final IngestionStatusRepository ingestionStatusRepository;

    @Transactional
    public IngestionStatus createIngestionStatus(String tableName, String fileName) {
        IngestionStatus status = IngestionStatus.builder()
                .tableName(tableName)
                .fileName(fileName)
                .status(IngestionStatusEnum.IN_PROGRESS)
                .progress(0)
                .retryCount(0)
                .build();
        
        return ingestionStatusRepository.save(status);
    }

    @Transactional
    public IngestionStatus updateStatus(Long id, IngestionStatusEnum status) {
        IngestionStatus ingestionStatus = ingestionStatusRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Ingestion status not found with id: " + id));
        
        ingestionStatus.setStatus(status);
        
        if (status == IngestionStatusEnum.SUCCESS || status == IngestionStatusEnum.FAILED) {
            ingestionStatus.setProgress(100);
        }
        
        return ingestionStatusRepository.save(ingestionStatus);
    }

    @Transactional
    public IngestionStatus updateProgress(Long id, Integer progress) {
        IngestionStatus ingestionStatus = ingestionStatusRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Ingestion status not found with id: " + id));
        
        ingestionStatus.setProgress(progress);
        
        return ingestionStatusRepository.save(ingestionStatus);
    }

    @Transactional
    public IngestionStatus updateTotalRows(Long id, Long totalRows) {
        IngestionStatus ingestionStatus = ingestionStatusRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Ingestion status not found with id: " + id));
        
        ingestionStatus.setTotalRows(totalRows);
        
        return ingestionStatusRepository.save(ingestionStatus);
    }

    @Transactional
    public IngestionStatus updateProcessedRows(Long id, Long processedRows) {
        IngestionStatus ingestionStatus = ingestionStatusRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Ingestion status not found with id: " + id));
        
        ingestionStatus.setProcessedRows(processedRows);
        
        if (ingestionStatus.getTotalRows() != null && ingestionStatus.getTotalRows() > 0) {
            int progress = (int) ((processedRows * 100) / ingestionStatus.getTotalRows());
            ingestionStatus.setProgress(progress);
        }
        
        return ingestionStatusRepository.save(ingestionStatus);
    }

    @Transactional
    public IngestionStatus updateErrorMessage(Long id, String errorMessage) {
        IngestionStatus ingestionStatus = ingestionStatusRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Ingestion status not found with id: " + id));
        
        ingestionStatus.setErrorMessage(errorMessage);
        ingestionStatus.setStatus(IngestionStatusEnum.FAILED);
        
        return ingestionStatusRepository.save(ingestionStatus);
    }

    @Transactional
    public IngestionStatus incrementRetryCount(Long id) {
        IngestionStatus ingestionStatus = ingestionStatusRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Ingestion status not found with id: " + id));
        
        ingestionStatus.setRetryCount(ingestionStatus.getRetryCount() + 1);
        
        return ingestionStatusRepository.save(ingestionStatus);
    }

    public List<IngestionStatus> getStatusByTableName(String tableName) {
        return ingestionStatusRepository.findByTableName(tableName);
    }

    public List<IngestionStatus> getStatusByFileName(String fileName) {
        return ingestionStatusRepository.findByFileName(fileName);
    }

    public List<IngestionStatus> getIncompleteIngestions() {
        return ingestionStatusRepository.findByCompletedAtIsNull();
    }

    public Optional<IngestionStatus> getStatusById(Long id) {
        return ingestionStatusRepository.findById(id);
    }
} 