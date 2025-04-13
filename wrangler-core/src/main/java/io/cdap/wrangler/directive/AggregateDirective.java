/*
 * Copyright © 2025 Cask Data, Inc.
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
package io.cdap.wrangler.directive;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.*;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;

import java.util.List;

public class AggregateDirective implements Directive {
    private String sourceSizeColumn;
    private String sourceTimeColumn;
    private String targetSizeColumn;
    private String targetTimeColumn;
    private String sizeUnit = "B"; // Default to bytes
    private String timeUnit = "ns"; // Default to nanoseconds
    private String aggregationType = "total"; // Default to total

    private long totalSize = 0;
    private long totalTime = 0;
    private int rowCount = 0;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = new UsageDefinition.Builder("aggregate");
        builder.define("sourceSizeColumn", TokenType.COLUMN_NAME)
               .define("sourceTimeColumn", TokenType.COLUMN_NAME)
               .define("targetSizeColumn", TokenType.COLUMN_NAME)
               .define("targetTimeColumn", TokenType.COLUMN_NAME)
               .define("sizeUnit", TokenType.TEXT, "B") // Optional
               .define("timeUnit", TokenType.TEXT, "ns") // Optional
               .define("aggregationType", TokenType.TEXT, "total"); // Optional
        return builder.build();
    }

    @Override
    public void initialize(Arguments arguments) {
        try {
            sourceSizeColumn = arguments.value("sourceSizeColumn").toString();
            sourceTimeColumn = arguments.value("sourceTimeColumn").toString();
            targetSizeColumn = arguments.value("targetSizeColumn").toString();
            targetTimeColumn = arguments.value("targetTimeColumn").toString();
            sizeUnit = arguments.valueOrDefault("sizeUnit", "B").toString();
            timeUnit = arguments.valueOrDefault("timeUnit", "ns").toString();
            aggregationType = arguments.valueOrDefault("aggregationType", "total").toString();

            if (!List.of("B", "KB", "MB", "GB").contains(sizeUnit.toUpperCase())) {
                throw new DirectiveLoadException("Invalid size unit: " + sizeUnit);
            }
            if (!List.of("ns", "ms", "s", "m", "h").contains(timeUnit.toLowerCase())) {
                throw new DirectiveLoadException("Invalid time unit: " + timeUnit);
            }
            if (!List.of("total", "average").contains(aggregationType.toLowerCase())) {
                throw new DirectiveLoadException("Invalid aggregation type: " + aggregationType);
            }
        } catch (DirectiveLoadException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        for (Row row : rows) {
            Object sizeValue = row.getValue(sourceSizeColumn);
            if (sizeValue != null) {
                try {
                    long sizeInBytes = new ByteSize(sizeValue.toString()).getBytes();
                    totalSize += sizeInBytes;
                } catch (IllegalArgumentException e) {
                    throw new DirectiveExecutionException("Invalid byte size value: " + sizeValue);
                }
            }

            Object timeValue = row.getValue(sourceTimeColumn);
            if (timeValue != null) {
                try {
                    long timeInNanos = new TimeDuration(timeValue.toString()).getMilliseconds();
                    totalTime += timeInNanos;
                } catch (IllegalArgumentException e) {
                    throw new DirectiveExecutionException("Invalid time duration value: " + timeValue);
                }
            }

            rowCount++;
        }
        return rows;
    }

    @Override
    public void destroy() {
        // No cleanup required
    }

    public List<Row> finalize(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        if (rowCount == 0) {
            throw new DirectiveExecutionException("No valid rows to aggregate.");
        }

        double finalSize = convertSize(totalSize, sizeUnit);
        double finalTime = convertTime(totalTime, timeUnit);

        if ("average".equalsIgnoreCase(aggregationType)) {
            finalSize /= rowCount;
            finalTime /= rowCount;
        }

        Row result = new Row();
        result.add(targetSizeColumn, finalSize);
        result.add(targetTimeColumn, finalTime);

        return List.of(result);
    }

    private double convertSize(long sizeInBytes, String unit) {
        switch (unit.toUpperCase()) {
            case "KB":
                return sizeInBytes / 1024.0;
            case "MB":
                return sizeInBytes / (1024.0 * 1024);
            case "GB":
                return sizeInBytes / (1024.0 * 1024 * 1024);
            default:
                return sizeInBytes; // Default to bytes
        }
    }

    private double convertTime(long timeInNanos, String unit) {
        switch (unit.toLowerCase()) {
            case "ms":
                return timeInNanos / 1_000_000.0;
            case "s":
                return timeInNanos / 1_000_000_000.0;
            case "m":
                return timeInNanos / (60.0 * 1_000_000_000);
            case "h":
                return timeInNanos / (3600.0 * 1_000_000_000);
            default:
                return timeInNanos; // Default to nanoseconds
        }
    }
}