// File: wrangler-core/src/main/java/io/cdap/wrangler/steps/AggregateStatsDirective.java

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

package io.cdap.wrangler.steps;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;

import java.util.List;
import java.util.ArrayList;

/**
 * Directive for aggregating byte sizes and time durations across rows.
 */
public class AggregateStatsDirective implements Directive {
    public static final String NAME = "aggregate-stats";
    private String sizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("size_column", TokenType.COLUMN_NAME);
        builder.define("time_column", TokenType.COLUMN_NAME);
        builder.define("total_size_column", TokenType.COLUMN_NAME);
        builder.define("total_time_column", TokenType.COLUMN_NAME);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumn = ((ColumnName) args.value("size_column")).value();
        this.timeColumn = ((ColumnName) args.value("time_column")).value();
        this.totalSizeColumn = ((ColumnName) args.value("total_size_column")).value();
        this.totalTimeColumn = ((ColumnName) args.value("total_time_column")).value();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) 
            throws DirectiveExecutionException {
        try {
            long totalBytes = 0;
            long totalNanos = 0;
            int validRows = 0;

            // Aggregate values from all rows
            for (Row row : rows) {
                Object sizeObj = row.getValue(sizeColumn);
                Object timeObj = row.getValue(timeColumn);

                if (sizeObj instanceof ByteSize) {
                    totalBytes += ((ByteSize) sizeObj).getBytes();
                }

                if (timeObj instanceof TimeDuration) {
                    totalNanos += ((TimeDuration) timeObj).getNanos();
                }

                validRows++;
            }

            // Create result row with aggregated values
            Row resultRow = new Row();
            
            // Convert to MB and seconds
            double totalMB = totalBytes / (1024.0 * 1024.0);
            double totalSeconds = totalNanos / 1_000_000_000.0;

            // Set the values in the result row
            resultRow.setValue(totalSizeColumn, totalMB);
            resultRow.setValue(totalTimeColumn, totalSeconds);

            // Add average values
            if (validRows > 0) {
                resultRow.setValue(totalSizeColumn + "_avg", totalMB / validRows);
                resultRow.setValue(totalTimeColumn + "_avg", totalSeconds / validRows);
            }

            return List.of(resultRow);

        } catch (Exception e) {
            throw new DirectiveExecutionException(this.getClass().getName(), 
                "Error executing aggregate-stats directive: " + e.getMessage(), e);
        }
    }
}