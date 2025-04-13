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

package io.cdap.directives.aggregates;

import java.util.List;

import com.google.common.collect.ImmutableList;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.EntityCountMetric;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

/**
 * A directive for aggregating byte sizes and time durations across records.
 *
 * This directive accepts:
 * - Two source column names (one for byte sizes and one for time durations)
 * - Two target column names (for total size and total/average time)
 * - Optional output units for size and time
 * - Optional aggregation type for time (sum or average)
 */
@Plugin(type = Directive.TYPE)
@Name(Aggregate.NAME)
@Categories(categories = {"aggregate"})
@Description("Aggregates byte sizes and time durations across records.")
public class Aggregate implements Directive {
    public static final String NAME = "aggregate";

    private String sizeSourceColumn;
    private String timeSourceColumn;
    private String sizeTargetColumn;
    private String timeTargetColumn;
    private String sizeUnit = "bytes";
    private String timeUnit = "milliseconds";
    private String timeAggregationType = "sum";

    private static final String SIZE_COUNTER = "total_size";
    private static final String TIME_COUNTER = "total_time";
    private static final String RECORD_COUNTER = "record_count";

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("size_source", TokenType.COLUMN_NAME);
        builder.define("time_source", TokenType.COLUMN_NAME);
        builder.define("size_target", TokenType.COLUMN_NAME);
        builder.define("time_target", TokenType.COLUMN_NAME);
        builder.define("size_unit", TokenType.TEXT, Optional.TRUE);
        builder.define("time_unit", TokenType.TEXT, Optional.TRUE);
        builder.define("time_aggregation", TokenType.TEXT, Optional.TRUE);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeSourceColumn = ((ColumnName) args.value("size_source")).value();
        this.timeSourceColumn = ((ColumnName) args.value("time_source")).value();
        this.sizeTargetColumn = ((ColumnName) args.value("size_target")).value();
        this.timeTargetColumn = ((ColumnName) args.value("time_target")).value();

        if (args.contains("size_unit")) {
            this.sizeUnit = ((Text) args.value("size_unit")).value();
        }
        if (args.contains("time_unit")) {
            this.timeUnit = ((Text) args.value("time_unit")).value();
        }
        if (args.contains("time_aggregation")) {
            String aggType = ((Text) args.value("time_aggregation")).value();
            if (!aggType.equals("sum") && !aggType.equals("average")) {
                throw new DirectiveParseException(NAME, "Time aggregation type must be either 'sum' or 'average'");
            }
            this.timeAggregationType = aggType;
        }
    }

    @Override
    public void destroy() {
        // no-op
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        // Initialize counters in transient store if not already present
        if (context != null) {
            TransientStore store = context.getTransientStore();
            if (!store.getVariables().contains(SIZE_COUNTER)) {
                store.set(TransientVariableScope.GLOBAL, SIZE_COUNTER, 0L);
            }
            if (!store.getVariables().contains(TIME_COUNTER)) {
                store.set(TransientVariableScope.GLOBAL, TIME_COUNTER, 0L);
            }
            if (!store.getVariables().contains(RECORD_COUNTER)) {
                store.set(TransientVariableScope.GLOBAL, RECORD_COUNTER, 0L);
            }
        }

        // Process each row
        for (Row row : rows) {
            try {
                // Get size value
                int sizeIdx = row.find(sizeSourceColumn);
                if (sizeIdx != -1) {
                    Object sizeVal = row.getValue(sizeIdx);
                    if (sizeVal instanceof Number) {
                        long size = ((Number) sizeVal).longValue();
                        context.getTransientStore().increment(TransientVariableScope.GLOBAL, SIZE_COUNTER, size);
                    }
                }

                // Get time value
                int timeIdx = row.find(timeSourceColumn);
                if (timeIdx != -1) {
                    Object timeVal = row.getValue(timeIdx);
                    if (timeVal instanceof Number) {
                        long time = ((Number) timeVal).longValue();
                        context.getTransientStore().increment(TransientVariableScope.GLOBAL, TIME_COUNTER, time);
                    }
                }

                // Increment record counter
                context.getTransientStore().increment(TransientVariableScope.GLOBAL, RECORD_COUNTER, 1L);

                // Add aggregated values to the row
                TransientStore store = context.getTransientStore();
                long totalSize = (Long) store.get(SIZE_COUNTER);
                long totalTime = (Long) store.get(TIME_COUNTER);
                long recordCount = (Long) store.get(RECORD_COUNTER);

                // Convert size to target unit if needed
                long convertedSize = convertSize(totalSize, sizeUnit);
                row.addOrSet(sizeTargetColumn, convertedSize);

                // Calculate time based on aggregation type
                long timeValue;
                if (timeAggregationType.equals("average")) {
                    timeValue = recordCount > 0 ? totalTime / recordCount : 0;
                } else {
                    timeValue = totalTime;
                }
                // Convert time to target unit if needed
                long convertedTime = convertTime(timeValue, timeUnit);
                row.addOrSet(timeTargetColumn, convertedTime);

            } catch (Exception e) {
                throw new DirectiveExecutionException(NAME, e.getMessage(), e);
            }
        }
        return rows;
    }

    private long convertSize(long size, String targetUnit) {
        switch (targetUnit.toLowerCase()) {
            case "kb":
                return size / 1024;
            case "mb":
                return size / (1024 * 1024);
            case "gb":
                return size / (1024 * 1024 * 1024);
            default:
                return size; // Default to bytes
        }
    }

    private long convertTime(long time, String targetUnit) {
        switch (targetUnit.toLowerCase()) {
            case "seconds":
                return time / 1000;
            case "minutes":
                return time / (1000 * 60);
            case "hours":
                return time / (1000 * 60 * 60);
            default:
                return time; // Default to milliseconds
        }
    }

    @Override
    public List<EntityCountMetric> getCountMetrics() {
        return ImmutableList.of();
    }
}