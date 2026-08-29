package com.ingestion.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.Map;

@Entity
@Table(name = "table_mappings")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableMapping {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "source_table", nullable = false)
    private String sourceTable;

    @Column(name = "target_table", nullable = false)
    private String targetTable;

    @ElementCollection
    @CollectionTable(name = "column_mappings", joinColumns = @JoinColumn(name = "table_mapping_id"))
    @MapKeyColumn(name = "source_column")
    @Column(name = "target_column")
    private Map<String, String> columnMappings;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
} 