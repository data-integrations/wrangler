package com.ingestion.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "ingestion_status")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IngestionStatus {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "file_name", nullable = false)
    private String fileName;
    
    @Column(name = "table_name", nullable = false)
    private String tableName;
    
    @Column(name = "status", nullable = false)
    @Enumerated(EnumType.STRING)
    private IngestionStatusType status;
    
    @Column(name = "records_processed")
    private Long recordsProcessed;
    
    @Column(name = "records_failed")
    private Long recordsFailed;
    
    @Column(name = "start_time")
    private LocalDateTime startTime;
    
    @Column(name = "end_time")
    private LocalDateTime endTime;
    
    @Column(name = "error_message")
    private String errorMessage;
    
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (status == null) {
            status = IngestionStatusType.PENDING;
        }
        if (recordsProcessed == null) {
            recordsProcessed = 0L;
        }
        if (recordsFailed == null) {
            recordsFailed = 0L;
        }
    }
    
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
} 