/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.directives.aggregates;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for aggregating byte sizes and time durations across multiple
 * rows.
 *
 * This directive processes ByteSize and TimeDuration tokens, accumulates them,
 * and produces summary statistics such as total size and total/average time.
 */
@Plugin(type = Directive.TYPE)
@Name(SizeTimeAggregator.NAME)
@Categories(categories = { "aggregator", "statistics" })
@Description("Aggregates byte sizes and time durations across rows, calculating totals and averages.")
public class SizeTimeAggregator implements Directive, Lineage {
    public static final String NAME = "aggregate-size-time";

    // Store keys for the transient store
    private static final String TOTAL_SIZE_KEY = "aggregate_total_size_bytes";
    private static final String TOTAL_TIME_KEY = "aggregate_total_time_ms";
    private static final String COUNT_KEY = "aggregate_count";

    // Source column names
    private String sizeColumnName;
    private String timeColumnName;

    // Target column names
    private String targetSizeColumnName;
    private String targetTimeColumnName;

    // Unit settings for output (optional)
    private String sizeUnit; // Default: bytes, Options: KB, MB, GB
    private String timeUnit; // Default: ms, Options: s, m, h
    private boolean useAverage; // Default: false (use total)

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("size-column", TokenType.COLUMN_NAME);
        builder.define("time-column", TokenType.COLUMN_NAME);
        builder.define("target-size-column", TokenType.COLUMN_NAME);
        builder.define("target-time-column", TokenType.COLUMN_NAME);
        builder.define("size-unit", TokenType.TEXT, Optional.TRUE);
        builder.define("time-unit", TokenType.TEXT, Optional.TRUE);
        builder.define("aggregate-type", TokenType.TEXT, Optional.TRUE);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumnName = ((ColumnName) args.value("size-column")).value();
        this.timeColumnName = ((ColumnName) args.value("time-column")).value();
        this.targetSizeColumnName = ((ColumnName) args.value("target-size-column")).value();
        this.targetTimeColumnName = ((ColumnName) args.value("target-time-column")).value();

        // Parse optional arguments with default values
        this.sizeUnit = args.contains("size-unit") ? ((Text) args.value("size-unit")).value().toUpperCase() : "BYTES";
        this.timeUnit = args.contains("time-unit") ? ((Text) args.value("time-unit")).value().toLowerCase() : "ms";

        // Determine aggregation type: total or average
        String aggregateType = args.contains("aggregate-type")
                ? ((Text) args.value("aggregate-type")).value().toLowerCase()
                : "total";
        this.useAverage = "average".equals(aggregateType) || "avg".equals(aggregateType);

        // Validate size unit
        if (!("BYTES".equals(sizeUnit) || "KB".equals(sizeUnit) ||
                "MB".equals(sizeUnit) || "GB".equals(sizeUnit))) {
            throw new DirectiveParseException(
                    NAME, String.format("Invalid size unit '%s'. Supported units are BYTES, KB, MB, GB", sizeUnit));
        }

        // Validate time unit
        if (!("ms".equals(timeUnit) || "s".equals(timeUnit) ||
                "m".equals(timeUnit) || "h".equals(timeUnit))) {
            throw new DirectiveParseException(
                    NAME, String.format("Invalid time unit '%s'. Supported units are ms, s, m, h", timeUnit));
        }
    }

    @Override
    public void destroy() {
        // no-op
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        // Get the transient store for aggregation
        TransientStore store = context.getTransientStore();

        // Initialize counters if they don't exist
        initializeCounters(store);

        // Process each row to accumulate values
        for (Row row : rows) {
            int sizeIdx = row.find(sizeColumnName);
            int timeIdx = row.find(timeColumnName);

            // Skip row if either column is not found
            if (sizeIdx == -1 || timeIdx == -1) {
                continue;
            }

            // Get values from columns
            Object sizeObj = row.getValue(sizeIdx);
            Object timeObj = row.getValue(timeIdx);

            // Process byte size if it's a valid type
            if (sizeObj != null) {
                long sizeBytes = 0;
                if (sizeObj instanceof ByteSize) {
                    sizeBytes = ((ByteSize) sizeObj).getBytes();
                } else if (sizeObj instanceof String) {
                    try {
                        ByteSize byteSize = new ByteSize((String) sizeObj);
                        sizeBytes = byteSize.getBytes();
                    } catch (IllegalArgumentException e) {
                        // Skip invalid format
                        continue;
                    }
                }

                // Add size to total
                if (sizeBytes > 0) {
                    long currentTotal = store.get(TOTAL_SIZE_KEY);
                    store.set(TransientVariableScope.GLOBAL, TOTAL_SIZE_KEY, currentTotal + sizeBytes);
                }
            }

            // Process time duration if it's a valid type
            if (timeObj != null) {
                long timeMs = 0;
                if (timeObj instanceof TimeDuration) {
                    timeMs = ((TimeDuration) timeObj).getMilliseconds();
                } else if (timeObj instanceof String) {
                    try {
                        TimeDuration timeDuration = new TimeDuration((String) timeObj);
                        timeMs = timeDuration.getMilliseconds();
                    } catch (IllegalArgumentException e) {
                        // Skip invalid format
                        continue;
                    }
                }

                // Add time to total
                if (timeMs > 0) {
                    long currentTotal = store.get(TOTAL_TIME_KEY);
                    store.set(TransientVariableScope.GLOBAL, TOTAL_TIME_KEY, currentTotal + timeMs);
                }
            }

            // Increment count
            long currentCount = store.get(COUNT_KEY);
            store.set(TransientVariableScope.GLOBAL, COUNT_KEY, currentCount + 1);
        }

        // Return unchanged rows during normal processing
        return rows;
    }

    /**
     * Initialize the counters in the transient store if they don't exist
     */
    private void initializeCounters(TransientStore store) {
        if (store.get(TOTAL_SIZE_KEY) == null) {
            store.set(TransientVariableScope.GLOBAL, TOTAL_SIZE_KEY, 0L);
        }
        if (store.get(TOTAL_TIME_KEY) == null) {
            store.set(TransientVariableScope.GLOBAL, TOTAL_TIME_KEY, 0L);
        }
        if (store.get(COUNT_KEY) == null) {
            store.set(TransientVariableScope.GLOBAL, COUNT_KEY, 0L);
        }
    }

    /**
     * Finalize the aggregation, creating a summary row with the aggregated values
     * 
     * This should be called after all data has been processed
     */
    public Row getAggregationResult(ExecutorContext context) {
        TransientStore store = context.getTransientStore();
        long totalSizeBytes = store.get(TOTAL_SIZE_KEY);
        long totalTimeMs = store.get(TOTAL_TIME_KEY);
        long count = store.get(COUNT_KEY);

        // Create a new result row
        Row result = new Row();

        // Calculate size based on unit
        double sizeValue;
        switch (sizeUnit) {
            case "KB":
                sizeValue = totalSizeBytes / 1024.0;
                break;
            case "MB":
                sizeValue = totalSizeBytes / (1024.0 * 1024.0);
                break;
            case "GB":
                sizeValue = totalSizeBytes / (1024.0 * 1024.0 * 1024.0);
                break;
            case "BYTES":
            default:
                sizeValue = totalSizeBytes;
                break;
        }

        // Calculate time based on unit and aggregation type
        double timeValue;
        double timeInSelectedUnit;

        // Convert to selected time unit
        switch (timeUnit) {
            case "s":
                timeInSelectedUnit = totalTimeMs / 1000.0;
                break;
            case "m":
                timeInSelectedUnit = totalTimeMs / (1000.0 * 60);
                break;
            case "h":
                timeInSelectedUnit = totalTimeMs / (1000.0 * 60 * 60);
                break;
            case "ms":
            default:
                timeInSelectedUnit = totalTimeMs;
                break;
        }

        // Apply aggregation type
        if (useAverage && count > 0) {
            timeValue = timeInSelectedUnit / count;
        } else {
            timeValue = timeInSelectedUnit;
        }

        // Add values to the result row
        result.add(targetSizeColumnName, sizeValue);
        result.add(targetTimeColumnName, timeValue);

        // Reset counters for next use
        store.set(TransientVariableScope.GLOBAL, TOTAL_SIZE_KEY, 0L);
        store.set(TransientVariableScope.GLOBAL, TOTAL_TIME_KEY, 0L);
        store.set(TransientVariableScope.GLOBAL, COUNT_KEY, 0L);

        return result;
    }

    @Override
    public Mutation lineage() {
        return Mutation.builder()
                .readable("Aggregated byte size from column '%s' and time duration from column '%s' " +
                        "into columns '%s' and '%s'",
                        sizeColumnName, timeColumnName, targetSizeColumnName, targetTimeColumnName)
                .relation(sizeColumnName, targetSizeColumnName)
                .relation(timeColumnName, targetTimeColumnName)
                .build();
    }
}