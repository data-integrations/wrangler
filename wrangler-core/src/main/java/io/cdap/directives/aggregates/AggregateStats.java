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
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * A directive that aggregates byte sizes and time durations from specified
 * columns.
 * It calculates total size and total/average time based on the input columns.
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Categories(categories = { "aggregate" })
@Description("Aggregates byte sizes and time durations from specified columns.")
public class AggregateStats implements Directive {
    public static final String NAME = "aggregate-stats";

    private String sizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;
    private String outputSizeUnit;
    private String outputTimeUnit;
    private boolean calculateAverage;

    private long totalBytes;
    private long totalNanoseconds;
    private int rowCount;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("size-column", TokenType.COLUMN_NAME);
        builder.define("time-column", TokenType.COLUMN_NAME);
        builder.define("total-size-column", TokenType.COLUMN_NAME);
        builder.define("total-time-column", TokenType.COLUMN_NAME);
        builder.define("output-size-unit", TokenType.IDENTIFIER, true);
        builder.define("output-time-unit", TokenType.IDENTIFIER, true);
        builder.define("calculate-average", TokenType.BOOLEAN, true);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumn = ((ColumnName) args.value("size-column")).value();
        this.timeColumn = ((ColumnName) args.value("time-column")).value();
        this.totalSizeColumn = ((ColumnName) args.value("total-size-column")).value();
        this.totalTimeColumn = ((ColumnName) args.value("total-time-column")).value();

        if (args.contains("output-size-unit")) {
            this.outputSizeUnit = ((Identifier) args.value("output-size-unit")).value();
        } else {
            this.outputSizeUnit = "MB";
        }

        if (args.contains("output-time-unit")) {
            this.outputTimeUnit = ((Identifier) args.value("output-time-unit")).value();
        } else {
            this.outputTimeUnit = "s";
        }

        if (args.contains("calculate-average")) {
            this.calculateAverage = (boolean) args.value("calculate-average").value();
        } else {
            this.calculateAverage = false;
        }

        this.totalBytes = 0;
        this.totalNanoseconds = 0;
        this.rowCount = 0;
    }

    @Override
    public void destroy() {
        // no-op
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        // Process each row to accumulate totals
        for (Row row : rows) {
            int sizeIdx = row.find(sizeColumn);
            int timeIdx = row.find(timeColumn);

            if (sizeIdx == -1) {
                throw new DirectiveExecutionException(NAME,
                        String.format("Column '%s' not found in row", sizeColumn));
            }
            if (timeIdx == -1) {
                throw new DirectiveExecutionException(NAME,
                        String.format("Column '%s' not found in row", timeColumn));
            }

            Object sizeValue = row.getValue(sizeIdx);
            Object timeValue = row.getValue(timeIdx);

            if (sizeValue instanceof String) {
                ByteSize byteSize = new ByteSize((String) sizeValue);
                totalBytes += byteSize.getBytes();
            } else {
                throw new DirectiveExecutionException(NAME,
                        String.format("Column '%s' must contain byte size values", sizeColumn));
            }

            if (timeValue instanceof String) {
                TimeDuration timeDuration = new TimeDuration((String) timeValue);
                totalNanoseconds += timeDuration.getNanoseconds();
            } else {
                throw new DirectiveExecutionException(NAME,
                        String.format("Column '%s' must contain time duration values", timeColumn));
            }

            rowCount++;
        }

        // Create a single row with the aggregated results
        Row result = new Row();

        // Convert total bytes to requested unit
        double totalSize;
        switch (outputSizeUnit.toLowerCase()) {
            case "b":
                totalSize = totalBytes;
                break;
            case "kb":
                totalSize = totalBytes / 1024.0;
                break;
            case "mb":
                totalSize = totalBytes / (1024.0 * 1024.0);
                break;
            case "gb":
                totalSize = totalBytes / (1024.0 * 1024.0 * 1024.0);
                break;
            case "tb":
                totalSize = totalBytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
                break;
            default:
                throw new DirectiveExecutionException(NAME,
                        String.format("Unsupported size unit: %s", outputSizeUnit));
        }

        // Convert total nanoseconds to requested unit
        double totalTime;
        switch (outputTimeUnit.toLowerCase()) {
            case "ns":
                totalTime = totalNanoseconds;
                break;
            case "us":
                totalTime = totalNanoseconds / 1000.0;
                break;
            case "ms":
                totalTime = totalNanoseconds / (1000.0 * 1000.0);
                break;
            case "s":
                totalTime = totalNanoseconds / (1000.0 * 1000.0 * 1000.0);
                break;
            case "m":
                totalTime = totalNanoseconds / (60.0 * 1000.0 * 1000.0 * 1000.0);
                break;
            case "h":
                totalTime = totalNanoseconds / (60.0 * 60.0 * 1000.0 * 1000.0 * 1000.0);
                break;
            default:
                throw new DirectiveExecutionException(NAME,
                        String.format("Unsupported time unit: %s", outputTimeUnit));
        }

        // If calculating average, divide by row count
        if (calculateAverage) {
            totalTime = totalTime / rowCount;
        }

        result.add(totalSizeColumn, totalSize);
        result.add(totalTimeColumn, totalTime);

        return ImmutableList.of(result);
    }

    @Override
    public List<EntityCountMetric> getCountMetrics() {
        return null;
    }
}

