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

package io.cdap.wrangler.statistics;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ReportErrorAndProceed;
import io.cdap.wrangler.api.DirectiveExecutionException;

import java.util.List;

/**
 * A directive for aggregating byte size and time duration statistics.
 */
@Categories(categories = { "aggregate" })
public class AggregateStats implements Directive {
    public static final String NAME = "aggregate-stats";
    private String sizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;
    private String sizeUnit = "MB";
    private String timeUnit = "s";
    private String aggregationType = "total";

    private long totalBytes = 0;
    private long totalNanoseconds = 0;
    private int rowCount = 0;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("size-column", TokenType.COLUMN_NAME, "Source column containing byte sizes");
        builder.define("time-column", TokenType.COLUMN_NAME, "Source column containing time durations");
        builder.define("total-size-column", TokenType.TEXT, "Target column for total size");
        builder.define("total-time-column", TokenType.TEXT, "Target column for total time");
        builder.define("size-unit", TokenType.TEXT, "Output unit for size (B, KB, MB, GB, TB, PB)", false);
        builder.define("time-unit", TokenType.TEXT, "Output unit for time (ns, us, ms, s, m, h, d)", false);
        builder.define("aggregation-type", TokenType.TEXT, "Type of aggregation (total, average)", false);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumn = ((ColumnName) args.value("size-column")).value();
        this.timeColumn = ((ColumnName) args.value("time-column")).value();
        this.totalSizeColumn = ((Text) args.value("total-size-column")).value();
        this.totalTimeColumn = ((Text) args.value("total-time-column")).value();

        if (args.contains("size-unit")) {
            this.sizeUnit = ((Text) args.value("size-unit")).value();
        }
        if (args.contains("time-unit")) {
            this.timeUnit = ((Text) args.value("time-unit")).value();
        }
        if (args.contains("aggregation-type")) {
            this.aggregationType = ((Text) args.value("aggregation-type")).value();
        }
    }

    @Override
    public void destroy() {
        // No-op
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context)
            throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed {
        for (Row row : rows) {
            Object sizeValue = row.getValue(sizeColumn);
            Object timeValue = row.getValue(timeColumn);

            if (sizeValue instanceof ByteSize) {
                totalBytes += ((ByteSize) sizeValue).getBytes();
            } else if (sizeValue instanceof String) {
                try {
                    totalBytes += new ByteSize((String) sizeValue).getBytes();
                } catch (IllegalArgumentException e) {
                    throw new ErrorRowException(
                            String.format("Invalid byte size format in column '%s': %s", sizeColumn, sizeValue), 1);
                }
            }

            if (timeValue instanceof TimeDuration) {
                totalNanoseconds += ((TimeDuration) timeValue).getNanoseconds();
            } else if (timeValue instanceof String) {
                try {
                    totalNanoseconds += new TimeDuration((String) timeValue).getNanoseconds();
                } catch (IllegalArgumentException e) {
                    throw new ErrorRowException(
                            String.format("Invalid time duration format in column '%s': %s", timeColumn, timeValue), 1);
                }
            }

            rowCount++;
        }

        // Create a single row with the aggregated results
        Row result = new Row();
        try {
            result.add(totalSizeColumn, convertBytes(totalBytes, sizeUnit));
            result.add(totalTimeColumn, convertNanoseconds(totalNanoseconds, timeUnit));
        } catch (IllegalArgumentException e) {
            throw new DirectiveExecutionException(e.getMessage());
        }
        return List.of(result);
    }

    private double convertBytes(long bytes, String unit) {
        double value = bytes;
        switch (unit.toUpperCase()) {
            case "B":
                break;
            case "KB":
                value /= 1024;
                break;
            case "MB":
                value /= (1024 * 1024);
                break;
            case "GB":
                value /= (1024 * 1024 * 1024);
                break;
            case "TB":
                value /= (1024L * 1024 * 1024 * 1024);
                break;
            case "PB":
                value /= (1024L * 1024 * 1024 * 1024 * 1024);
                break;
            default:
                throw new IllegalArgumentException("Invalid byte size unit: " + unit);
        }
        return aggregationType.equals("average") ? value / rowCount : value;
    }

    private double convertNanoseconds(long nanoseconds, String unit) {
        double value = nanoseconds;
        switch (unit.toLowerCase()) {
            case "ns":
                break;
            case "us":
            case "µs":
                value /= 1000;
                break;
            case "ms":
                value /= (1000 * 1000);
                break;
            case "s":
                value /= (1000 * 1000 * 1000);
                break;
            case "m":
                value /= (60L * 1000 * 1000 * 1000);
                break;
            case "h":
                value /= (60L * 60 * 1000 * 1000 * 1000);
                break;
            case "d":
                value /= (24L * 60 * 60 * 1000 * 1000 * 1000);
                break;
            default:
                throw new IllegalArgumentException("Invalid time unit: " + unit);
        }
        return aggregationType.equals("average") ? value / rowCount : value;
    }
}
