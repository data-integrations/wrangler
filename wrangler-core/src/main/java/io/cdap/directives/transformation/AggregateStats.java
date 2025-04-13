/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.directives.transformation;

import java.util.List;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

/**
 * This class implements a directive for aggregating byte size and time duration
 * statistics.
 * It allows users to define columns for size and time, aggregate them based on
 * the specified
 * aggregation type (e.g., total or average), and convert the results to the
 * desired units.
 */
@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
@Categories(categories = { "transform" })
@Description("Aggregates byte size and time duration statistics with configurable units.")
public class AggregateStats implements Directive {
    private static final String NAME = "aggregate-stats";

    private String sizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;
    private String aggregationType;
    private String sizeUnit;
    private String timeUnit;
    private boolean isLastStage = false;
    private AggregationStore aggregationStore;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);

        // Define tokens one by one
        builder.define("size-column", TokenType.COLUMN_NAME);
        builder.define("time-column", TokenType.COLUMN_NAME);
        builder.define("total-size-column", TokenType.COLUMN_NAME);
        builder.define("total-time-column", TokenType.COLUMN_NAME);
        builder.define("aggregation-type", TokenType.IDENTIFIER, true); // Optional token
        builder.define("size-unit", TokenType.IDENTIFIER, true); // Optional token
        builder.define("time-unit", TokenType.IDENTIFIER, true); // Optional token

        // Build and return the UsageDefinition object
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        // Initialize columns from arguments
        this.sizeColumn = ((ColumnName) args.value("size-column")).value();
        this.timeColumn = ((ColumnName) args.value("time-column")).value();
        this.totalSizeColumn = ((ColumnName) args.value("total-size-column")).value();
        this.totalTimeColumn = ((ColumnName) args.value("total-time-column")).value();

        // Initialize optional parameters with default values if not provided
        this.aggregationType = args.contains("aggregation-type") ? ((Identifier) args.value("aggregation-type")).value()
                : "total";
        this.sizeUnit = args.contains("size-unit") ? ((Identifier) args.value("size-unit")).value() : "bytes";
        this.timeUnit = args.contains("time-unit") ? ((Identifier) args.value("time-unit")).value() : "milliseconds";

        // Validate parameters to ensure correct values
        validateParameters();

        // Initialize aggregation store for data
        this.aggregationStore = new AggregationStore();
    }

    // Validate aggregation type, size unit, and time unit parameters
    private void validateParameters() throws DirectiveParseException {
        if (!aggregationType.equalsIgnoreCase("total") && !aggregationType.equalsIgnoreCase("average")) {
            throw new DirectiveParseException(NAME,
                    "Aggregation type must be 'total' or 'average'");
        }

        if (!isValidSizeUnit(sizeUnit)) {
            throw new DirectiveParseException(NAME,
                    "Invalid size unit. Valid units: bytes, kb, mb, gb");
        }

        if (!isValidTimeUnit(timeUnit)) {
            throw new DirectiveParseException(NAME,
                    "Invalid time unit. Valid units: nanoseconds, milliseconds, seconds, minutes, hours");
        }
    }

    // Main execution method for transforming rows
    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        // Detect if this is the last stage of execution
        if (rows == null || rows.isEmpty()) {
            isLastStage = true;
        }

        // Process each row for aggregation
        for (Row row : rows) {
            processRow(row, aggregationStore);
        }

        // If it's the last stage, return the aggregated result
        if (isLastStage) {
            return createResultRow(aggregationStore);
        }
        return rows;
    }

    // Process individual rows for aggregation
    private void processRow(Row row, AggregationStore store) throws DirectiveExecutionException {
        try {
            int sizeIdx = row.find(sizeColumn);
            int timeIdx = row.find(timeColumn);

            validateColumnsPresent(sizeIdx, timeIdx);

            Object sizeVal = row.getValue(sizeIdx);
            Object timeVal = row.getValue(timeIdx);

            validateNonNullValues(sizeVal, timeVal);

            ByteSize byteSize = new ByteSize(sizeVal.toString());
            TimeDuration timeDuration = new TimeDuration(timeVal.toString());

            store.add(byteSize.getBytes(), timeDuration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            throw new DirectiveExecutionException(NAME,
                    String.format("Invalid value format: %s", e.getMessage()), e);
        }
    }

    // Create result row after processing all input rows
    private List<Row> createResultRow(AggregationStore store) {
        Row result = new Row();
        long sizeValue = convertSize(store.getTotalBytes(), sizeUnit);
        long timeValue = convertTime(store.getTotalMilliseconds(), timeUnit);

        // If aggregation type is average, divide by the number of items
        if ("average".equalsIgnoreCase(aggregationType)) {
            int count = store.getCount();
            sizeValue = count > 0 ? sizeValue / count : 0;
            timeValue = count > 0 ? timeValue / count : 0;
        }

        result.add(totalSizeColumn, sizeValue);
        result.add(totalTimeColumn, timeValue);

        return List.of(result);
    }

    // Convert size from bytes to the specified unit
    private long convertSize(long bytes, String unit) {
        switch (unit.toLowerCase()) {
            case "kb":
                return bytes / 1024;
            case "mb":
                return bytes / (1024 * 1024);
            case "gb":
                return bytes / (1024 * 1024 * 1024);
            default:
                return bytes;
        }
    }

    // Convert time from milliseconds to the specified unit
    private long convertTime(long milliseconds, String unit) {
        switch (unit.toLowerCase()) {
            case "nanoseconds":
                return milliseconds * 1_000_000;
            case "seconds":
                return milliseconds / 1_000;
            case "minutes":
                return milliseconds / (60_000);
            case "hours":
                return milliseconds / (3_600_000);
            default:
                return milliseconds;
        }
    }

    // Validate if the given size unit is valid
    private boolean isValidSizeUnit(String unit) {
        return unit.matches("(?i)bytes|kb|mb|gb");
    }

    // Validate if the given time unit is valid
    private boolean isValidTimeUnit(String unit) {
        return unit.matches("(?i)nanoseconds|milliseconds|seconds|minutes|hours");
    }

    // Validate if the required columns are present in the row
    private void validateColumnsPresent(int sizeIdx, int timeIdx) throws DirectiveExecutionException {
        if (sizeIdx == -1) {
            throw new DirectiveExecutionException(NAME,
                    String.format("Size column '%s' not found", sizeColumn));
        }
        if (timeIdx == -1) {
            throw new DirectiveExecutionException(NAME,
                    String.format("Time column '%s' not found", timeColumn));
        }
    }

    // Validate that size and time values are not null
    private void validateNonNullValues(Object sizeVal, Object timeVal) throws DirectiveExecutionException {
        if (sizeVal == null || timeVal == null) {
            throw new DirectiveExecutionException(NAME,
                    "Null values not allowed in size/time columns");
        }
    }

    @Override
    public void destroy() {
        // No resources to clean up
    }

    // Internal class to store aggregation results
    private static class AggregationStore {
        private long totalBytes;
        private long totalMilliseconds;
        private int count;

        void add(long bytes, long milliseconds) {
            totalBytes += bytes;
            totalMilliseconds += milliseconds;
            count++;
        }

        long getTotalBytes() {
            return totalBytes;
        }

        long getTotalMilliseconds() {
            return totalMilliseconds;
        }

        int getCount() {
            return count;
        }
    }
}

