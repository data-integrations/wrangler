/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 */
package io.cdap.directives.aggregates;

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
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;


/**
 * Directive to aggregate byte size and time duration fields.
 */
@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
@Categories(categories = {"aggregates"})
@Description("Aggregates total byte size and total or average time duration across rows.")
public class AggregateStats implements Directive {
    private static final String TOTAL = "total";
    private static final String AVERAGE = "average";

    private String sizeColumn;
    private String timeColumn;
    private String sizeTargetColumn;
    private String timeTargetColumn;
    private String outputSizeUnit = "B";
    private String outputTimeUnit = "ms";
    private String aggregationType = TOTAL;

    private long totalBytes = 0;
    private long totalTime = 0;
    private int rowCount = 0;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("size_column", TokenType.COLUMN_NAME);
        builder.define("time_column", TokenType.COLUMN_NAME);
        builder.define("target_size_column", TokenType.COLUMN_NAME);
        builder.define("target_time_column", TokenType.COLUMN_NAME);
        builder.define("size_unit", TokenType.TEXT, true);
        builder.define("time_unit", TokenType.TEXT, true);
        builder.define("aggregation_type", TokenType.TEXT, true);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumn = ((ColumnName) args.value("size_column")).value();
        this.timeColumn = ((ColumnName) args.value("time_column")).value();
        this.sizeTargetColumn = ((ColumnName) args.value("target_size_column")).value();
        this.timeTargetColumn = ((ColumnName) args.value("target_time_column")).value();

        if (args.contains("size_unit")) {
            this.outputSizeUnit = ((Text) args.value("size_unit")).value();
        }
        if (args.contains("time_unit")) {
            this.outputTimeUnit = ((Text) args.value("time_unit")).value();
        }
        if (args.contains("aggregation_type")) {
            this.aggregationType = ((Text) args.value("aggregation_type")).value().toLowerCase();
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {

        List<Row> result = new ArrayList<>();

        for (Row row : rows) {
            Object byteVal = row.getValue(sizeColumn);
            Object timeVal = row.getValue(timeColumn);

            // If values are strings, try to parse them into ByteSize and TimeDuration objects.
            if (byteVal instanceof String) {
                try {
                    byteVal = new ByteSize((String) byteVal);
                } catch (IllegalArgumentException e) {
                    throw new DirectiveExecutionException("Failed to parse ByteSize from string: " + byteVal, e);
                }
            }

            if (timeVal instanceof String) {
                try {
                    timeVal = new TimeDuration((String) timeVal);
                } catch (IllegalArgumentException e) {
                    throw new DirectiveExecutionException("Failed to parse TimeDuration from string: " + timeVal, e);
                }
            }


            if (byteVal instanceof ByteSize && timeVal instanceof TimeDuration) {
                totalBytes += ((ByteSize) byteVal).getBytes();
                totalTime += ((TimeDuration) timeVal).getMilliseconds();
                rowCount++;
            } else {
                throw new DirectiveExecutionException(
                        String.format("Expected ByteSize and TimeDuration types, but got %s and %s",
                                byteVal.getClass().getSimpleName(),
                                timeVal.getClass().getSimpleName()));
            }
//            System.out.println("After Row"+rowCount+": "+row+" Byte Size: "+byteVal+" Byte Time: "+timeVal);
        }

        long finalBytes = convertBytes(totalBytes, outputSizeUnit);
        long finalTime =
                convertTime(aggregationType.equals(AVERAGE) ? totalTime / rowCount : totalTime, outputTimeUnit);
        Row output = new Row();
        output.add(sizeTargetColumn, finalBytes);
        output.add(timeTargetColumn, finalTime);
        result.add(output);

        return result;
    }



    @Override
    public void destroy() {
        // no-op
    }

    @Override
    public List<EntityCountMetric> getCountMetrics() {
        return null;
    }

    private long convertBytes(long bytes, String unit) {
        switch (unit.toUpperCase()) {
            case "KB":
                return bytes / 1024;
            case "MB":
                return bytes / (1024 * 1024);
            case "GB":
                return bytes / (1024 * 1024 * 1024);
            default:
                return bytes; // default to bytes
        }
    }

    private long convertTime(long ms, String unit) {
        switch (unit.toLowerCase()) {
            case "seconds":
                return ms / 1000;
            case "minutes":
                return ms / (1000 * 60);
            default:
                return ms; // default to ms
        }
    }
}
