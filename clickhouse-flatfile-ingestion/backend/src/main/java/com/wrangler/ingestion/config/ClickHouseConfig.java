package com.wrangler.ingestion.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
public class ClickHouseConfig {

    @Bean
    public DataSource clickHouseDataSource() {
        Properties properties = new Properties();
        properties.setProperty("user", "${CLICKHOUSE_USER}");
        properties.setProperty("password", "${CLICKHOUSE_PASSWORD}");
        properties.setProperty("ssl", "true");
        properties.setProperty("sslmode", "STRICT");
        
        return new ClickHouseDataSource("jdbc:clickhouse://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/${CLICKHOUSE_DATABASE}", properties);
    }
} 