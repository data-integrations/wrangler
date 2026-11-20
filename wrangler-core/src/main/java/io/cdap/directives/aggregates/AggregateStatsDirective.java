/*
 * Copyright © 2017-2025 Cask Data, Inc.
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
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;

/**
 * A directive that aggregates byte sizes and time durations from specified source columns,
 * outputting the totals to target columns in user-specified units.
 *
 * The directive processes rows, accumulating byte sizes (e.g., "10KB", "1.5MB") and time
 * durations (e.g., "100ms", "2s") in canonical units (bytes, nanoseconds). It outputs a
 * single row with the aggregated values converted to the specified units (default: MB, seconds).
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStatsDirective.NAME)
@Categories(categories = {"aggregate"})
@Description("Aggregates byte sizes and time durations from source columns into target columns with specified units.")
public class AggregateStatsDirective implements Directive {
    public static final String NAME = "aggregate-stats";
    private String sizeCol;
    private String timeCol;
    private String targetSizeCol;
    private String targetTimeCol;
    private String sizeUnit;
    private String timeUnit;
    private static final String TOTAL_BYTES_KEY = "aggregate-stats-total-bytes";
    private static final String TOTAL_NANOS_KEY = "aggregate-stats-total-nanos";

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("source_size_col", TokenType.COLUMN_NAME);
        builder.define("source_time_col", TokenType.COLUMN_NAME);
        builder.define("target_size_col", TokenType.COLUMN_NAME);
        builder.define("target_time_col", TokenType.COLUMN_NAME);
        builder.define("size_unit", TokenType.TEXT, true); // Optional
        builder.define("time_unit", TokenType.TEXT, true); // Optional
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        sizeCol = ((ColumnName) args.value("source_size_col")).value();
        timeCol = ((ColumnName) args.value("source_time_col")).value();
        targetSizeCol = ((ColumnName) args.value("target_size_col")).value();
        targetTimeCol = ((ColumnName) args.value("target_time_col")).value();
        sizeUnit = args.contains("size_unit") ? ((Text) args.value("size_unit")).value() : "MB";
        timeUnit = args.contains("time_unit") ? ((Text) args.value("time_unit")).value() : "seconds";
    }

    @Override
    public void destroy() {
        // No cleanup needed
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        TransientStore store = context.getTransientStore();

        for (Row row : rows) {
            // Process byte size
            Object sizeVal = row.getValue(sizeCol);
            if (sizeVal instanceof String) {
                try {
                    long bytes = new ByteSize((String) sizeVal).value();
                    Long currentBytes = store.get(TOTAL_BYTES_KEY);
                    store.set(TransientVariableScope.GLOBAL, TOTAL_BYTES_KEY,
                              (currentBytes != null ? currentBytes : 0L) + bytes);
                } catch (IllegalArgumentException e) {
                    // Skip invalid byte sizes
                }
            }

            // Process time duration
            Object timeVal = row.getValue(timeCol);
            if (timeVal instanceof String) {
                try {
                    long nanos = new TimeDuration((String) timeVal).value();
                    Long currentNanos = store.get(TOTAL_NANOS_KEY);
                    store.set(TransientVariableScope.GLOBAL, TOTAL_NANOS_KEY,
                              (currentNanos != null ? currentNanos : 0L) + nanos);
                } catch (IllegalArgumentException e) {
                    // Skip invalid time durations
                }
            }
        }

        // Return a single row with aggregated results
        Row result = new Row();
        Long totalBytes = store.get(TOTAL_BYTES_KEY);
        double sizeOutput = totalBytes != null ? totalBytes : 0L;
        switch (sizeUnit.toUpperCase()) {
            case "MB":
                sizeOutput /= (1024.0 * 1024.0);
                break;
            case "GB":
                sizeOutput /= (1024.0 * 1024.0 * 1024.0);
                break;
            case "TB":
                sizeOutput /= (1024.0 * 1024.0 * 1024.0 * 1024.0);
                break;
            case "KB":
                sizeOutput /= 1024.0;
                break;
            case "B":
            default:
                break;
        }
        result.add(targetSizeCol, sizeOutput);

        Long totalNanos = store.get(TOTAL_NANOS_KEY);
        double timeOutput = totalNanos != null ? totalNanos : 0L;
        switch (timeUnit.toUpperCase()) {
            case "SECONDS":
                timeOutput /= 1_000_000_000.0;
                break;
            case "MINUTES":
                timeOutput /= (60.0 * 1_000_000_000.0);
                break;
            case "HOURS":
                timeOutput /= (3600.0 * 1_000_000_000.0);
                break;
            case "MS":
                timeOutput /= 1_000_000.0;
                break;
            case "NS":
            default:
                break;
        }
        result.add(targetTimeCol, timeOutput);

        return Collections.singletonList(result);
    }

    @Override
    public List<EntityCountMetric> getCountMetrics() {
        // No metrics defined for this directive
        return ImmutableList.of();
    }
}