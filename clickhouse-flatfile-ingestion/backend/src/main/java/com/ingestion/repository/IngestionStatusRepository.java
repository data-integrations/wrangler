package com.ingestion.repository;

import com.ingestion.entity.IngestionStatus;
import com.ingestion.entity.IngestionStatusEnum;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface IngestionStatusRepository extends JpaRepository<IngestionStatus, Long> {
    
    List<IngestionStatus> findByTableName(String tableName);
    
    List<IngestionStatus> findByFileName(String fileName);
    
    List<IngestionStatus> findByStatus(IngestionStatusEnum status);
    
    List<IngestionStatus> findByCompletedAtIsNull();
} 