/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */



package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;

/**
 * A directive for aggregating statistics about size and time duration columns.
 */
@PublicEvolving
public class AggregateStatsDirective implements Directive {
    private String sizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("size-column", TokenType.COLUMN_NAME);
        builder.define("time-column", TokenType.COLUMN_NAME);
        builder.define("total-size-column", TokenType.COLUMN_NAME);
        builder.define("total-time-column", TokenType.COLUMN_NAME);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) {
        sizeColumn = ((ColumnName) args.value("size-column")).value();
        timeColumn = ((ColumnName) args.value("time-column")).value();
        totalSizeColumn = ((ColumnName) args.value("total-size-column")).value();
        totalTimeColumn = ((ColumnName) args.value("total-time-column")).value();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        double totalSizeBytes = 0;
        double totalTimeSeconds = 0;

        for (Row row : rows) {
            try {
                // Parse size value
                String sizeValue = (String) row.getValue(sizeColumn);
                if (sizeValue != null) {
                    totalSizeBytes += parseSize(sizeValue);
                }

                // Parse time value
                String timeValue = (String) row.getValue(timeColumn);
                if (timeValue != null) {
                    totalTimeSeconds += parseTime(timeValue);
                }
            } catch (Exception e) {
                // Skip invalid values
                continue;
            }
        }

        // Create result row
        Row result = new Row();
        result.add(totalSizeColumn, String.format("%.2f MB", totalSizeBytes / (1024 * 1024)));
        result.add(totalTimeColumn, String.format("%.2f s", totalTimeSeconds));
        return Collections.singletonList(result);
    }

    @Override
    public void destroy() {
        // No cleanup needed
    }

    private double parseSize(String value) {
        value = value.trim().toUpperCase();
        double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
        String unit = value.replaceAll("[0-9.]", "").trim();

        switch (unit) {
            case "B":
                return number;
            case "KB":
                return number * 1024;
            case "MB":
                return number * 1024 * 1024;
            case "GB":
                return number * 1024 * 1024 * 1024;
            case "TB":
                return number * 1024 * 1024 * 1024 * 1024;
            case "PB":
                return number * 1024 * 1024 * 1024 * 1024 * 1024;
            case "KIB":
                return number * 1024;
            case "MIB":
                return number * 1024 * 1024;
            case "GIB":
                return number * 1024 * 1024 * 1024;
            case "TIB":
                return number * 1024 * 1024 * 1024 * 1024;
            case "PIB":
                return number * 1024 * 1024 * 1024 * 1024 * 1024;
            default:
                throw new IllegalArgumentException("Invalid size unit: " + unit);
        }
    }

    private double parseTime(String value) {
        value = value.trim().toLowerCase();
        double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
        String unit = value.replaceAll("[0-9.]", "").trim();

        switch (unit) {
            case "ns":
                return number / 1_000_000_000;
            case "μs":
            case "us":
                return number / 1_000_000;
            case "ms":
                return number / 1000;
            case "s":
                return number;
            case "m":
                return number * 60;
            case "h":
                return number * 3600;
            case "d":
                return number * 86400;
            default:
                throw new IllegalArgumentException("Invalid time unit: " + unit);
        }
    }
} 

