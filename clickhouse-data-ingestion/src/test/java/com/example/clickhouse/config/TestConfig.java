package com.example.clickhouse.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
public class TestConfig {

    @Bean
    public DataSource clickHouseDataSource() throws SQLException {
        return new ClickHouseDataSource("jdbc:clickhouse://localhost:8123/default");
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource clickHouseDataSource) {
        return new JdbcTemplate(clickHouseDataSource);
    }
} 