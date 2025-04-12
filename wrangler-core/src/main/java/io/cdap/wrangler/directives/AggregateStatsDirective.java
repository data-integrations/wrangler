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
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for aggregating byte sizes and time durations across rows.
 */
@Categories(categories = { "transform" })
public class AggregateStatsDirective implements Directive {
    private ColumnName sizeColumn;
    private ColumnName timeColumn;
    private ColumnName totalSizeColumn;
    private ColumnName totalTimeColumn;
    private long totalBytes = 0;
    private long totalNanoseconds = 0;
    private int rowCount = 0;

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
        sizeColumn = args.value("size-column");
        timeColumn = args.value("time-column");
        totalSizeColumn = args.value("total-size-column");
        totalTimeColumn = args.value("total-time-column");
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        for (Row row : rows) {
            // Get size value and convert to bytes
            Object sizeValue = row.getValue(sizeColumn.value());
            if (sizeValue != null) {
                String sizeStr = sizeValue.toString();
                try {
                    ByteSize byteSize = new ByteSize(sizeStr);
                    totalBytes += byteSize.getBytes();
                } catch (IllegalArgumentException e) {
                    // Skip invalid byte size values
                }
            }

            // Get time value and convert to nanoseconds
            Object timeValue = row.getValue(timeColumn.value());
            if (timeValue != null) {
                String timeStr = timeValue.toString();
                try {
                    TimeDuration timeDuration = new TimeDuration(timeStr);
                    totalNanoseconds += timeDuration.getNanoseconds();
                } catch (IllegalArgumentException e) {
                    // Skip invalid time duration values
                }
            }

            rowCount++;
        }

        // Create a new row with the aggregated values
        Row result = new Row();
        result.add(totalSizeColumn.value(), String.format("%.2f MB", totalBytes / (1024.0 * 1024)));
        result.add(totalTimeColumn.value(), String.format("%.2f s", totalNanoseconds / (1000.0 * 1000 * 1000)));

        List<Row> results = new ArrayList<>();
        results.add(result);
        return results;
    }

    @Override
    public void destroy() {
        // Clean up any resources if needed
    }
} 