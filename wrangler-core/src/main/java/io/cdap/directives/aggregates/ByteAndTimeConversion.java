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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.annotations.Usage;

import java.util.List;
import java.util.ArrayList;

@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
// @Usage("aggregate-stats :byteCol :timeCol :outByteCol :outTimeCol
// [aggregationType] byteUnit timeUnit")
@Description("Aggregates byte size and time duration columns across all rows into a single summary row.")

public class ByteAndTimeConversion implements Directive {

    private String byteColumn;
    private String timeColumn;
    private String targetByteColumn;
    private String targetTimeColumn;
    private String aggregationType;
    private String outputSizeUnit;
    private String outputTimeUnit;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("byteColumn", TokenType.COLUMN_NAME);
        builder.define("timeColumn", TokenType.COLUMN_NAME);
        builder.define("targetByteColumn", TokenType.COLUMN_NAME);
        builder.define("targetTimeColumn", TokenType.COLUMN_NAME);
        // builder.define("aggregationType", TokenType.TEXT, "total");
        // builder.define("outputSizeUnit", TokenType.TEXT, "MB");
        // builder.define("outputTimeUnit", TokenType.TEXT, "minutes");

        return builder.build();
    }

    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
        byteColumn = ((ColumnName) arguments.value("byteColumn")).value();
        timeColumn = ((ColumnName) arguments.value("timeColumn")).value();
        targetByteColumn = ((ColumnName) arguments.value("targetByteColumn")).value();
        targetTimeColumn = ((ColumnName) arguments.value("targetTimeColumn")).value();
        // aggregationType = arguments.value("aggregationType").value().toString();
        // outputSizeUnit = arguments.value("outputSizeUnit").value().toString();
        // outputTimeUnit = arguments.value("outputTimeUnit").value().toString();
        aggregationType = "total";
        outputSizeUnit = "MB";
        outputTimeUnit = "minutes";
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        TransientStore store = context.getTransientStore();

        long totalBytes = 0;
        long totalMillis = 0;

        for (Row row : rows) {
            Object sizeObj = row.getValue(byteColumn);
            Object timeObj = row.getValue(timeColumn);

            if (sizeObj instanceof String) {
                try {
                    totalBytes += Byteparser((String) sizeObj);
                } catch (Exception e) {
                    throw new DirectiveExecutionException("Failed to parse size: " + sizeObj, e);
                }
            }

            if (timeObj instanceof String) {
                try {
                    totalMillis += TimeParser((String) timeObj);
                } catch (Exception e) {
                    throw new DirectiveExecutionException("Failed to parse time: " + timeObj, e);
                }
            }
        }

        long count = rows.size();
        double sizeValue = aggregationType.equalsIgnoreCase("average") && count > 0
                ? totalBytes / (double) count
                : totalBytes;

        double timeValue = aggregationType.equalsIgnoreCase("average") && count > 0
                ? totalMillis / (double) count
                : totalMillis;

        String sizeStr = Byteformat((long) sizeValue, outputSizeUnit);
        String timeStr = Timeformat((long) timeValue, outputTimeUnit);

        Row result = new Row();
        result.add(targetByteColumn, sizeStr);
        result.add(targetTimeColumn, timeStr);

        List<Row> output = new ArrayList<>();
        output.add(result);
        return output;
    }

    @Override
    public void destroy() {
        // No cleanup needed
    }

    private long Byteparser(String input) {
        input = input.trim().toUpperCase();
        if (input.endsWith("KB"))
            return Long.parseLong(input.replace("KB", "").trim()) * 1024;
        if (input.endsWith("MB"))
            return Long.parseLong(input.replace("MB", "").trim()) * 1024 * 1024;
        if (input.endsWith("GB"))
            return Long.parseLong(input.replace("GB", "").trim()) * 1024 * 1024 * 1024;
        if (input.endsWith("B"))
            return Long.parseLong(input.replace("B", "").trim());
        throw new IllegalArgumentException("Invalid byte size format: " + input);
    }

    private String Byteformat(long bytes, String unit) {
        switch (unit.toUpperCase()) {
            case "B":
                return bytes + "B";
            case "KB":
                return (bytes / 1024.0) + "KB";
            case "MB":
                return (bytes / (1024.0 * 1024)) + "MB";
            case "GB":
                return (bytes / (1024.0 * 1024 * 1024)) + "GB";
            default:
                throw new IllegalArgumentException("Unsupported output size unit: " + unit);
        }
    }

    private long TimeParser(String input) {
        input = input.trim().toLowerCase();

        if (input.endsWith("ms")) {
            return Long.parseLong(input.replace("ms", "").trim());
        } else if (input.endsWith("s")) {
            return Long.parseLong(input.replace("s", "").trim()) * 1000;
        } else if (input.endsWith("m")) {
            return Long.parseLong(input.replace("m", "").trim()) * 60 * 1000;
        } else if (input.endsWith("h")) {
            return Long.parseLong(input.replace("h", "").trim()) * 60 * 60 * 1000;
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + input);
        }
    }

    private String Timeformat(long millis, String unit) {
        switch (unit.toLowerCase()) {
            case "milliseconds":
            case "ms":
                return millis + "ms";
            case "seconds":
            case "s":
                return (millis / 1000.0) + "s";
            case "minutes":
            case "m":
                return (millis / (60 * 1000.0)) + "min";
            case "hours":
            case "h":
                return (millis / (60 * 60 * 1000.0)) + "h";
            default:
                throw new IllegalArgumentException("Unsupported output time unit: " + unit);
        }
    }

}
