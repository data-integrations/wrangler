package com.ingestion.mapper;

import com.ingestion.dto.IngestionStatusDTO;
import com.ingestion.entity.IngestionStatus;
import org.springframework.stereotype.Component;

@Component
public class IngestionStatusMapper {
    
    public IngestionStatusDTO toDTO(IngestionStatus entity) {
        if (entity == null) {
            return null;
        }
        
        IngestionStatusDTO dto = new IngestionStatusDTO();
        dto.setId(entity.getId());
        dto.setTableName(entity.getTableName());
        dto.setFileName(entity.getFileName());
        dto.setStatus(entity.getStatus());
        dto.setProgress(entity.getProgress());
        dto.setTotalRows(entity.getTotalRows());
        dto.setErrorMessage(entity.getErrorMessage());
        dto.setRetryCount(entity.getRetryCount());
        dto.setCreatedAt(entity.getCreatedAt());
        dto.setUpdatedAt(entity.getUpdatedAt());
        dto.setCompletedAt(entity.getCompletedAt());
        
        return dto;
    }
    
    public IngestionStatus toEntity(IngestionStatusDTO dto) {
        if (dto == null) {
            return null;
        }
        
        IngestionStatus entity = new IngestionStatus();
        entity.setId(dto.getId());
        entity.setTableName(dto.getTableName());
        entity.setFileName(dto.getFileName());
        entity.setStatus(dto.getStatus());
        entity.setProgress(dto.getProgress());
        entity.setTotalRows(dto.getTotalRows());
        entity.setErrorMessage(dto.getErrorMessage());
        entity.setRetryCount(dto.getRetryCount());
        entity.setCreatedAt(dto.getCreatedAt());
        entity.setUpdatedAt(dto.getUpdatedAt());
        entity.setCompletedAt(dto.getCompletedAt());
        
        return entity;
    }
} 