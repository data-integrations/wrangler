package com.ingestion.repository;

import com.ingestion.entity.TableMapping;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TableMappingRepository extends JpaRepository<TableMapping, Long> {
    
    Optional<TableMapping> findByTableName(String tableName);
    
    boolean existsByTableName(String tableName);
} 