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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.parser.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Directive that aggregates byte size and time duration, outputting a single row in specified units.
 */
@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
@Description("Aggregates byte size and time duration into a single row with specified units and aggregation type.")
public class AggregateStats implements Directive {
    private String byteSizeCol;
    private String timeDurationCol;
    private String targetSizeCol;
    private String targetTimeCol;
    private String sizeUnit;
    private String timeUnit;
    private String aggType;
    private TransientStore store;
    private long rowCount;

    private static final String BYTE_TOTAL_KEY = "aggregate_stats_byte_total";
    private static final String TIME_TOTAL_KEY = "aggregate_stats_time_total";
    private static final String ROW_COUNT_KEY = "aggregate_stats_row_count";
    private static final long KB = 1024L;
    private static final long MB = KB * 1024L;
    private static final long GB = MB * 1024L;
    private static final long TB = GB * 1024L;
    private static final long PB = TB * 1024L;
    private static final long NS = 1L;
    private static final long US = NS * 1000L;
    private static final long MS = US * 1000L;
    private static final long S = MS * 1000L;
    private static final long MIN = S * 60L;
    private static final long H = MIN * 60L;
    private static final long D = H * 24L;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("byteSizeCol", TokenType.COLUMN_NAME);
        builder.define("timeDurationCol", TokenType.COLUMN_NAME);
        builder.define("targetSizeCol", TokenType.COLUMN_NAME);
        builder.define("targetTimeCol", TokenType.COLUMN_NAME);
        builder.define("sizeUnit", TokenType.TEXT, true); // Optional
        builder.define("timeUnit", TokenType.TEXT, true); // Optional
        builder.define("aggType", TokenType.TEXT, true);  // Optional
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) {
        byteSizeCol = ((ColumnName) args.value("byteSizeCol")).value();
        timeDurationCol = ((ColumnName) args.value("timeDurationCol")).value();
        targetSizeCol = ((ColumnName) args.value("targetSizeCol")).value();
        targetTimeCol = ((ColumnName) args.value("targetTimeCol")).value();
        sizeUnit = args.contains("sizeUnit") ? ((Text) args.value("sizeUnit")).value().toLowerCase() : "mb";
        timeUnit = args.contains("timeUnit") ? ((Text) args.value("timeUnit")).value().toLowerCase() : "s";
        aggType = args.contains("aggType") ? ((Text) args.value("aggType")).value().toLowerCase() : "total";
        if (!aggType.equals("total") && !aggType.equals("average")) {
            throw new IllegalArgumentException("Invalid aggregation type: " + aggType + "; must be 'total' or 'average'");
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        store = context.getTransientStore();
        if (store.get(BYTE_TOTAL_KEY) == null) {
            store.set(TransientVariableScope.GLOBAL, BYTE_TOTAL_KEY, 0L);
            store.set(TransientVariableScope.GLOBAL, TIME_TOTAL_KEY, 0L);
            store.set(TransientVariableScope.GLOBAL, ROW_COUNT_KEY, 0L);
        }

        for (Row row : rows) {
            Object byteValue = row.getValue(byteSizeCol);
            if (byteValue != null) {
                try {
                    long bytes = new ByteSize(byteValue.toString()).getBytes();
                    long currentTotal = (Long) store.get(BYTE_TOTAL_KEY);
                    store.set(TransientVariableScope.GLOBAL, BYTE_TOTAL_KEY, currentTotal + bytes);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid byte size format in column " + byteSizeCol + ": " + byteValue, e);
                }
            }
            Object timeValue = row.getValue(timeDurationCol);
            if (timeValue != null) {
                try {
                    long nanos = new TimeDuration(timeValue.toString()).getNanos();
                    long currentTotal = (Long) store.get(TIME_TOTAL_KEY);
                    store.set(TransientVariableScope.GLOBAL, TIME_TOTAL_KEY, currentTotal + nanos);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid time duration format in column " + timeDurationCol + ": " + timeValue, e);
                }
            }
            rowCount = (Long) store.get(ROW_COUNT_KEY) + 1;
            store.set(TransientVariableScope.GLOBAL, ROW_COUNT_KEY, rowCount);
        }
        // Return empty list during execution; output in destroy
        return Collections.emptyList();
    }

    @Override
    public void destroy() {
        if (store == null) {
            return;
        }
        Long byteTotal = (Long) store.get(BYTE_TOTAL_KEY);
        Long timeTotal = (Long) store.get(TIME_TOTAL_KEY);
        Long count = (Long) store.get(ROW_COUNT_KEY);
        List<Row> output = new ArrayList<>();
        if (byteTotal != null && timeTotal != null && count != null && count > 0) {
            BigDecimal sizeValue = new BigDecimal(byteTotal);
            BigDecimal timeValue = new BigDecimal(timeTotal);
            if (aggType.equals("average")) {
                sizeValue = sizeValue.divide(new BigDecimal(count), 10, BigDecimal.ROUND_HALF_UP);
                timeValue = timeValue.divide(new BigDecimal(count), 10, BigDecimal.ROUND_HALF_UP);
            }
            switch (sizeUnit) {
                case "b":
                    break; // No conversion
                case "kb":
                    sizeValue = sizeValue.divide(new BigDecimal(KB), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "mb":
                    sizeValue = sizeValue.divide(new BigDecimal(MB), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "gb":
                    sizeValue = sizeValue.divide(new BigDecimal(GB), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "tb":
                    sizeValue = sizeValue.divide(new BigDecimal(TB), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "pb":
                    sizeValue = sizeValue.divide(new BigDecimal(PB), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                default:
                    throw new IllegalStateException("Unknown size unit: " + sizeUnit);
            }
            switch (timeUnit) {
                case "ns":
                    break; // No conversion
                case "us":
                    timeValue = timeValue.divide(new BigDecimal(US), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "ms":
                    timeValue = timeValue.divide(new BigDecimal(MS), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "s":
                    timeValue = timeValue.divide(new BigDecimal(S), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "min":
                    timeValue = timeValue.divide(new BigDecimal(MIN), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "h":
                    timeValue = timeValue.divide(new BigDecimal(H), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                case "d":
                    timeValue = timeValue.divide(new BigDecimal(D), 10, BigDecimal.ROUND_HALF_UP);
                    break;
                default:
                    throw new IllegalStateException("Unknown time unit: " + timeUnit);
            }
            Row result = new Row();
            result.add(targetSizeCol, sizeValue.doubleValue());
            result.add(targetTimeCol, timeValue.doubleValue());
            output.add(result);
        }
        // Clear store to prevent state leakage
        store.reset(TransientVariableScope.GLOBAL);
        // Store final result
        if (!output.isEmpty()) {
            store.set(TransientVariableScope.GLOBAL, "aggregate_stats_output_row", output.get(0));
        }
    }
}