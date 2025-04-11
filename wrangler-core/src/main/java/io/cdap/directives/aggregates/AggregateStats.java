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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.annotations.Categories;

import java.util.List;

/**
 * A directive for aggregating byte sizes and time durations.
 *
 * This directive takes four arguments:
 * 1. Source column containing byte sizes
 * 2. Source column containing time durations
 * 3. Target column for total size
 * 4. Target column for total time
 */
@Categories(categories = { "aggregate" })
public class AggregateStats implements Directive {
    private ColumnName sourceSizeColumn;
    private ColumnName sourceTimeColumn;
    private ColumnName targetSizeColumn;
    private ColumnName targetTimeColumn;
    private long totalSize;
    private long totalTime;
    private int count;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("source-size-column", TokenType.COLUMN_NAME, "Source column containing byte sizes");
        builder.define("source-time-column", TokenType.COLUMN_NAME, "Source column containing time durations");
        builder.define("target-size-column", TokenType.COLUMN_NAME, "Target column for total size");
        builder.define("target-time-column", TokenType.COLUMN_NAME, "Target column for total time");
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        ColumnName column = args.value("source-size-column");
        if (column == null) {
            throw new DirectiveParseException(
                    "aggregate-stats requires a source size column");
        }
        sourceSizeColumn = column;

        column = args.value("source-time-column");
        if (column == null) {
            throw new DirectiveParseException(
                    "aggregate-stats requires a source time column");
        }
        sourceTimeColumn = column;

        column = args.value("target-size-column");
        if (column == null) {
            throw new DirectiveParseException(
                    "aggregate-stats requires a target size column");
        }
        targetSizeColumn = column;

        column = args.value("target-time-column");
        if (column == null) {
            throw new DirectiveParseException(
                    "aggregate-stats requires a target time column");
        }
        targetTimeColumn = column;

        totalSize = 0;
        totalTime = 0;
        count = 0;
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        for (Row row : rows) {
            Object sizeObj = row.getValue(sourceSizeColumn.value());
            Object timeObj = row.getValue(sourceTimeColumn.value());

            if (sizeObj != null && !sizeObj.toString().isEmpty()) {
                try {
                    ByteSize size = new ByteSize(sizeObj.toString());
                    totalSize += size.getBytes();
                } catch (Exception e) {
                    context.getMetrics().count("aggregate-stats.errors", 1);
                }
            }

            if (timeObj != null && !timeObj.toString().isEmpty()) {
                try {
                    TimeDuration duration = new TimeDuration(timeObj.toString());
                    totalTime += duration.getMilliseconds();
                } catch (Exception e) {
                    context.getMetrics().count("aggregate-stats.errors", 1);
                }
            }

            count++;
        }

        // Create a single row with the aggregated values
        Row result = new Row();
        // Convert total size to MB
        long sizeInMB = totalSize / (1024 * 1024);
        result.add(targetSizeColumn.value(), sizeInMB + "MB");
        // Format time in milliseconds
        result.add(targetTimeColumn.value(), totalTime + "ms");
        result.add("count", count);

        return List.of(result);
    }

    @Override
    public void destroy() {
        // Nothing to clean up
    }
}