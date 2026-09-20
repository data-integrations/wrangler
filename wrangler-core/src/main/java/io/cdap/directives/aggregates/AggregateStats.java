/*
 * Copyright © 2025 Garv Tayal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cdap.directives.aggregates;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.ByteSize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Description("Aggregates size and time columns with statistical calculations.")
public class AggregateStats implements Directive {

    public static final String NAME = "aggregate-stats";

    private String sourceSizeCol;
    private String sourceTimeCol;
    private String targetSizeCol;
    private String targetTimeCol;
    private String sizeUnit = "mb";
    private String timeUnit = "s";
    private String aggType = "total";

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("sourceSizeCol", TokenType.COLUMN_NAME);
        builder.define("sourceTimeCol", TokenType.COLUMN_NAME);
        builder.define("targetSizeCol", TokenType.COLUMN_NAME);
        builder.define("targetTimeCol", TokenType.COLUMN_NAME);
        builder.define("sizeUnit", TokenType.TEXT);
        builder.define("timeUnit", TokenType.TEXT);
        builder.define("aggType", TokenType.TEXT);
        return builder.build();
    }

    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
        this.sourceSizeCol = arguments.value("sourceSizeCol");
        this.sourceTimeCol = arguments.value("sourceTimeCol");
        this.targetSizeCol = arguments.value("targetSizeCol");
        this.targetTimeCol = arguments.value("targetTimeCol");

        if (arguments.contains("sizeUnit")) {
            this.sizeUnit = arguments.value("sizeUnit").toString().toLowerCase();
        }
        if (arguments.contains("timeUnit")) {
            this.timeUnit = arguments.value("timeUnit").toString().toLowerCase();
        }
        if (arguments.contains("aggType")) {
            this.aggType = arguments.value("aggType").toString().toLowerCase();
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        List<Long> sizeBytesList = new ArrayList<>();
        List<Long> timeNanosList = new ArrayList<>();

        for (Row row : rows) {
            Object sizeValue = row.getValue(sourceSizeCol);
            Object timeValue = row.getValue(sourceTimeCol);

            if (sizeValue != null) {
                try {
                    ByteSize byteSize = new ByteSize(sizeValue.toString());
                    sizeBytesList.add(byteSize.getBytes());
                } catch (IllegalArgumentException e) {
                    throw new DirectiveExecutionException("Invalid byte size format: " + sizeValue, e);
                }
            }

            if (timeValue != null) {
                try {
                    long nanos = parseToNanos(timeValue.toString());
                    timeNanosList.add(nanos);
                } catch (IllegalArgumentException e) {
                    throw new DirectiveExecutionException("Invalid time duration format: " + timeValue, e);
                }
            }
        }

        Row resultRow = createResultRow(sizeBytesList, timeNanosList);
        return Collections.singletonList(resultRow);
    }

    private Row createResultRow(List<Long> sizeList, List<Long> timeList) {
        double finalSize;
        double finalTime;

        switch (aggType) {
            case "average":
                finalSize = sizeList.stream().mapToLong(Long::longValue).average().orElse(0.0);
                finalTime = timeList.stream().mapToLong(Long::longValue).average().orElse(0.0);
                break;
            case "min":
                finalSize = sizeList.stream().mapToLong(Long::longValue).min().orElse(0L);
                finalTime = timeList.stream().mapToLong(Long::longValue).min().orElse(0L);
                break;
            case "max":
                finalSize = sizeList.stream().mapToLong(Long::longValue).max().orElse(0L);
                finalTime = timeList.stream().mapToLong(Long::longValue).max().orElse(0L);
                break;
            case "median":
                finalSize = calculateMedian(sizeList);
                finalTime = calculateMedian(timeList);
                break;
            case "variance":
                finalSize = calculateVariance(sizeList);
                finalTime = calculateVariance(timeList);
                break;
            case "stddev":
                finalSize = calculateStandardDeviation(sizeList);
                finalTime = calculateStandardDeviation(timeList);
                break;
            default: // total
                finalSize = sizeList.stream().mapToLong(Long::longValue).sum();
                finalTime = timeList.stream().mapToLong(Long::longValue).sum();
        }

        finalSize = convertFromBytes(finalSize, sizeUnit);
        finalTime = convertFromNanos(finalTime, timeUnit);

        Row resultRow = new Row();
        resultRow.add(targetSizeCol, finalSize);
        resultRow.add(targetTimeCol, finalTime);
        return resultRow;
    }

    private double calculateMedian(List<Long> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        List<Long> sortedValues = new ArrayList<>(values);
        Collections.sort(sortedValues);
        int size = sortedValues.size();
        if (size % 2 == 0) {
            return (sortedValues.get(size / 2 - 1) + sortedValues.get(size / 2)) / 2.0;
        } else {
            return sortedValues.get(size / 2);
        }
    }

    private double calculateVariance(List<Long> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        double mean = values.stream().mapToLong(Long::longValue).average().orElse(0.0);
        return values.stream()
                     .mapToDouble(value -> Math.pow(value - mean, 2))
                     .average()
                     .orElse(0.0);
    }

    private double calculateStandardDeviation(List<Long> values) {
        return Math.sqrt(calculateVariance(values));
    }

    private double convertFromBytes(double value, String unit) {
        switch (unit) {
            case "kb":
                return value / 1024;
            case "mb":
                return value / (1024 * 1024);
            case "gb":
                return value / (1024 * 1024 * 1024);
            case "tb":
                return value / (1024L * 1024 * 1024 * 1024);
            default:
                return value; 
        }
    }

    private double convertFromNanos(double value, String unit) {
        switch (unit) {
            case "ms":
                return value / 1_000_000;
            case "s":
                return value / 1_000_000_000;
            case "m":
                return value / (60 * 1_000_000_000L);
            case "h":
                return value / (60 * 60 * 1_000_000_000L);
            default:
                return value; // Assuming nanoseconds
        }
    }

    private long parseToNanos(String duration) throws DirectiveExecutionException {
        duration = duration.trim().toLowerCase();
        try {
            if (duration.endsWith("ns")) {
                return Long.parseLong(duration.replace("ns", ""));
            } else if (duration.endsWith("us")) {
                return Long.parseLong(duration.replace("us", "")) * 1_000;
            } else if (duration.endsWith("ms")) {
                return Long.parseLong(duration.replace("ms", "")) * 1_000_000;
            } else if (duration.endsWith("s")) {
                return Long.parseLong(duration.replace("s", "")) * 1_000_000_000;
            } else if (duration.endsWith("m")) {
                return Long.parseLong(duration.replace("m", "")) * 60 * 1_000_000_000L;
            } else if (duration.endsWith("h")) {
                return Long.parseLong(duration.replace("h", "")) * 60 * 60 * 1_000_000_000L;
            } else {
                throw new DirectiveExecutionException("Unrecognized time unit in: " + duration);
            }
        } catch (NumberFormatException e) {
            throw new DirectiveExecutionException("Invalid time value format: " + duration, e);
        }
    }

    @Override
    public void destroy() {
        // Cleanup resources if needed
    }
}