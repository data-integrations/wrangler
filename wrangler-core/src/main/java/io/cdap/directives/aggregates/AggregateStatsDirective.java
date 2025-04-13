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
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;

/**
 * A directive for aggregating byte sizes and time durations.
 * Example usage: aggregate-stats :data_size :response_time total_mb total_sec
 */
@Plugin(type = "directive")
@Name("aggregate-stats")
@Description("Aggregates byte sizes and time durations from specified columns")
public class AggregateStatsDirective implements Directive {
    private String sizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;
    private double totalSize;
    private double totalDuration;

    /**
     * Defines how the directive should be used
     */
    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("sizeColumn", TokenType.COLUMN_NAME);
        builder.define("timeColumn", TokenType.COLUMN_NAME);
        builder.define("totalSizeColumn", TokenType.COLUMN_NAME);
        builder.define("totalTimeColumn", TokenType.COLUMN_NAME);
        return builder.build();
    }

    /**
     * Initialize the directive with arguments
     */
    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumn = args.value("sizeColumn").toString().replaceFirst("^:", "");
        this.timeColumn = args.value("timeColumn").toString().replaceFirst("^:", "");
        this.totalSizeColumn = args.value("totalSizeColumn").toString();
        this.totalTimeColumn = args.value("totalTimeColumn").toString();
    }

    /**
     * Execute the directive
     */
    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        totalSize = 0.0;
        totalDuration = 0.0;

        for (Row row : rows) {
            Object sizeObj = row.getValue(sizeColumn);
            Object timeObj = row.getValue(timeColumn);

            if (sizeObj == null) {
                throw new DirectiveExecutionException("Column '" + sizeColumn + "' not found");
            }
            if (timeObj == null) {
                throw new DirectiveExecutionException("Column '" + timeColumn + "' not found");
            }

            try {
                if (sizeObj instanceof ByteSize) {
                    totalSize += ((ByteSize) sizeObj).getBytes() / (1024.0 * 1024.0);
                } else {
                    ByteSize size = new ByteSize(sizeObj.toString());
                    totalSize += size.getBytes() / (1024.0 * 1024.0);
                }
            } catch (Exception e) {
                throw new DirectiveExecutionException("Invalid byte size format: " + sizeObj);
            }

            try {
                if (timeObj instanceof TimeDuration) {
                    totalDuration += ((TimeDuration) timeObj).getNanoseconds() / 1_000_000_000.0;
                } else {
                    TimeDuration duration = new TimeDuration(timeObj.toString());
                    totalDuration += duration.getNanoseconds() / 1_000_000_000.0;
                }
            } catch (Exception e) {
                throw new DirectiveExecutionException("Invalid time duration format: " + timeObj);
            }
        }

        Row result = new Row();
        result.add(totalSizeColumn, totalSize);
        result.add(totalTimeColumn, totalDuration);
        return Collections.singletonList(result);
    }

    @Override
    public void destroy() {
        // Reset totals
        totalSize = 0.0;
        totalDuration = 0.0;
    }
} 