
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

import io.cdap.directives.parser.ByteSizeParser;
import io.cdap.directives.parser.TimeDurationParser;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.ColumnName;
import java.util.List;
import java.util.ArrayList;

/**
 * Aggregates size and duration columns and appends the aggregated, formatted values to the output row.
 */
public class Aggregates implements Directive {

    private String sizeCol;
    private String timeCol;
    private String outputSizeCol;
    private String outputTimeCol;
    private String aggregationType;
    private String outputSizeUnit;
    private String outputTimeUnit;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-size-duration");
        builder.define("sizeCol", TokenType.COLUMN_NAME);
        builder.define("timeCol", TokenType.COLUMN_NAME);
        builder.define("outputSizeCol", TokenType.COLUMN_NAME);
        builder.define("outputTimeCol", TokenType.COLUMN_NAME);
        builder.define("aggregationType", TokenType.LITERAL, "total"); // optional: total or average
        builder.define("outputSizeUnit", TokenType.LITERAL, "MB");     // optional: B, KB, MB, GB
        builder.define("outputTimeUnit", TokenType.LITERAL, "minutes");// optional: milliseconds, seconds, minutes, etc.
        return builder.build();
    }

    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
        sizeCol = ((ColumnName) arguments.value("sizeCol")).value();
        timeCol = ((ColumnName) arguments.value("timeCol")).value();
        outputSizeCol = ((ColumnName) arguments.value("outputSizeCol")).value();
        outputTimeCol = ((ColumnName) arguments.value("outputTimeCol")).value();
        aggregationType = arguments.value("aggregationType").value().toString();
        outputSizeUnit = arguments.value("outputSizeUnit").value().toString();
        outputTimeUnit = arguments.value("outputTimeUnit").value().toString();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        TransientStore store = context.getTransientStore();

        long totalBytes = 0;
        long totalMillis = 0;

        for (Row row : rows) {
            Object sizeObj = row.getValue(sizeCol);
            Object timeObj = row.getValue(timeCol);

            if (sizeObj instanceof String) {
                try {
                    totalBytes += ByteSizeParser.parse((String) sizeObj);
                } catch (Exception e) {
                    throw new DirectiveExecutionException("Failed to parse size: " + sizeObj, e);
                }
            }

            if (timeObj instanceof String) {
                try {
                    totalMillis += TimeDurationParser.parse((String) timeObj);
                } catch (Exception e) {
                    throw new DirectiveExecutionException("Failed to parse time: " + timeObj, e);
                }
            }
        }

        long count = rows.size();
        double sizeValue = aggregationType.equalsIgnoreCase("average") && count > 0
                ? totalBytes / (double) count : totalBytes;

        double timeValue = aggregationType.equalsIgnoreCase("average") && count > 0
                ? totalMillis / (double) count : totalMillis;

        String sizeStr = ByteSizeParser.format((long) sizeValue, outputSizeUnit);
        String timeStr = TimeDurationParser.format((long) timeValue, outputTimeUnit);

        Row result = new Row();
        result.add(outputSizeCol, sizeStr);
        result.add(outputTimeCol, timeStr);

        List<Row> output = new ArrayList<>();
        output.add(result);
        return output;
    }

    @Override
    public void destroy() {
        // No cleanup needed
    }
}