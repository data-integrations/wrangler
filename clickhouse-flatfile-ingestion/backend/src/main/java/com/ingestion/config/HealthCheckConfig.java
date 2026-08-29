package com.ingestion.config;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Configuration for health checks and monitoring.
 */
@Configuration
public class HealthCheckConfig {

    /**
     * Creates a health indicator for the database connection.
     */
    @Bean
    public HealthIndicator dbHealthIndicator(JdbcTemplate jdbcTemplate) {
        return () -> {
            try {
                jdbcTemplate.queryForObject("SELECT 1", Integer.class);
                return Health.up().withDetail("database", "ClickHouse").build();
            } catch (Exception e) {
                return Health.down()
                        .withDetail("database", "ClickHouse")
                        .withException(e)
                        .build();
            }
        };
    }

    /**
     * Creates a health indicator for the application.
     */
    @Bean
    public HealthIndicator applicationHealthIndicator() {
        return () -> Health.up()
                .withDetail("application", "ClickHouse Flat File Ingestion")
                .withDetail("status", "Running")
                .build();
    }
} 