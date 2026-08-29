package com.ingestion.service;

import com.ingestion.dto.TableMappingRequest;
import com.ingestion.entity.TableMapping;
import com.ingestion.repository.TableMappingRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityNotFoundException;
import java.util.List;

@Service
public class TableMappingService {

    private final TableMappingRepository tableMappingRepository;

    @Autowired
    public TableMappingService(TableMappingRepository tableMappingRepository) {
        this.tableMappingRepository = tableMappingRepository;
    }

    @Transactional
    public TableMapping createTableMapping(TableMappingRequest request) {
        if (tableMappingRepository.existsByTableName(request.getTableName())) {
            throw new IllegalArgumentException("Table mapping already exists for table: " + request.getTableName());
        }

        TableMapping tableMapping = new TableMapping();
        tableMapping.setTableName(request.getTableName());
        tableMapping.setSchemaDefinition(request.getSchemaDefinition());
        tableMapping.setColumnMappings(request.getColumnMappings());

        return tableMappingRepository.save(tableMapping);
    }

    @Transactional(readOnly = true)
    public TableMapping getTableMapping(String tableName) {
        return tableMappingRepository.findByTableName(tableName)
                .orElseThrow(() -> new EntityNotFoundException("Table mapping not found for table: " + tableName));
    }

    @Transactional(readOnly = true)
    public List<TableMapping> getAllTableMappings() {
        return tableMappingRepository.findAll();
    }

    @Transactional
    public TableMapping updateTableMapping(String tableName, TableMappingRequest request) {
        TableMapping existingMapping = getTableMapping(tableName);
        
        existingMapping.setSchemaDefinition(request.getSchemaDefinition());
        existingMapping.setColumnMappings(request.getColumnMappings());
        
        return tableMappingRepository.save(existingMapping);
    }

    @Transactional
    public void deleteTableMapping(String tableName) {
        TableMapping tableMapping = getTableMapping(tableName);
        tableMappingRepository.delete(tableMapping);
    }
} 