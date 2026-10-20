package io.cdap.directives.aggregates;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Plugin(type = Directive.TYPE)
@Name(AggregateDirective.NAME)
@Categories(categories = {"transform"})
@Description("Performs aggregation (e.g., sum, average) on a specified column.")
public class AggregateDirective implements Directive {
    private static final Logger log = LoggerFactory.getLogger(AggregateDirective.class);
    public static final String NAME = "aggregate-stats";
    String sourceSizeColumn;
    String sourceTimeColumn;
    String targetSizeColumn;
    String targetTimeColumn;
    String outputSizeUnit;
    String outputTimeUnit;
    private static Double totalSizeMb = 0.0;
    private static Double totalTimeSec = 0.0;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("sourceSizeColumn", TokenType.COLUMN_NAME, "Source column containing byte sizes.", false);
        builder.define("sourceTimeColumn", TokenType.COLUMN_NAME, "Source column containing time durations.", false);
        builder.define("targetSizeColumn", TokenType.IDENTIFIER, "Target column name for total size.", false);
        builder.define("targetTimeColumn", TokenType.IDENTIFIER, "Target column name for total or average time.", false);
        builder.define("outputSizeUnit", TokenType.TEXT, "Output unit for size (e.g., 'MB', 'GB').", true);
        builder.define("outputTimeUnit", TokenType.TEXT, "Output unit for time (e.g., 'seconds', 'minutes').", true);
        builder.define("aggregationType", TokenType.TEXT, "Aggregation type for time (e.g., 'total', 'average').", true);
        System.out.println("AggregateDirective define method called. " + builder.toString());
        return builder.build();
    }

    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
        try {
            Token sourceSizeToken = arguments.value("sourceSizeColumn", null);
            Token sourceTimeToken = arguments.value("sourceTimeColumn", null);
            Token targetSizeToken = arguments.value("targetSizeColumn", null);
            Token targetTimeToken = arguments.value("targetTimeColumn", null);

            if (sourceSizeToken == null || sourceTimeToken == null || targetSizeToken == null || targetTimeToken == null) {
                throw new DirectiveParseException("Missing required arguments for AggregateDirective.");
            }

            this.sourceSizeColumn = sourceSizeToken.value().toString();
            this.sourceTimeColumn = sourceTimeToken.value().toString();
            this.targetSizeColumn = targetSizeToken.value().toString();
            this.targetTimeColumn = targetTimeToken.value().toString();

            this.outputSizeUnit = arguments.contains("outputSizeUnit")
                    ? arguments.value(outputSizeUnit, "mb").value().toString().toLowerCase()
                    : "mb";  // default to mb

            this.outputTimeUnit = arguments.contains("outputTimeUnit")
                    ? arguments.value(outputTimeUnit, "s").value().toString().toLowerCase()
                    : "seconds";  // default to seconds

        } catch (Exception e) {
            throw new DirectiveParseException("Failed to parse initialization arguments for AggregateDirective.", e);
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        double totalSizeMb = 0.0;
        double totalTimeSec = 0.0;

        for (Row row : rows) {
            Object sizeObj = row.getValue(sourceSizeColumn);
            Object timeObj = row.getValue(sourceTimeColumn);

            try {
                if (sizeObj != null) {
                    String size = sizeObj.toString();
                    long sizeInBytes = ByteSize.parse(size).getBytes();
                    totalSizeMb += sizeInBytes / (1024.0 * 1024.0); // Convert to MB
                }

                if (timeObj != null) {
                    String time = timeObj.toString();
                    long timeInMillis = TimeDuration.parse(time).getMilliseconds();
                    totalTimeSec += timeInMillis / 1000.0; // Convert to seconds
                }
            } catch (Exception e) {
                System.err.println("Error processing row: " + row);
                throw new DirectiveExecutionException("Failed to process row: " + row, e);
            }
        }

        // Return only the final aggregated result
        Row result = new Row();
        result.add(targetSizeColumn, totalSizeMb);
        result.add(targetTimeColumn, totalTimeSec);

        System.out.println("Final Result Row: " + result);
        return Collections.singletonList(result);
    }

    private double convertSize(String size, String unit) throws DirectiveExecutionException {
        if (unit == null || unit.isEmpty()) {
            throw new DirectiveExecutionException("Unit cannot be null or empty.");
        }

        unit = unit.toLowerCase();
        double value;

        size = size.toLowerCase(); // Normalize size to lowercase
        if (size.endsWith("mb")) {
            value = Double.parseDouble(size.replace("mb", "").trim());
        } else if (size.endsWith("kb")) {
            value = Double.parseDouble(size.replace("kb", "").trim()) / 1024.0;
        } else if (size.endsWith("gb")) {
            value = Double.parseDouble(size.replace("gb", "").trim()) * 1024.0;
        } else if (size.endsWith("b")) {
            value = Double.parseDouble(size.replace("b", "").trim()) / (1024.0 * 1024.0);
        } else {
            throw new DirectiveExecutionException("Unsupported size format: " + size);
        }

        switch (unit) {
            case "mb":
                return value;
            case "kb":
                return value * 1024.0;
            case "gb":
                return value / 1024.0;
            default:
                throw new DirectiveExecutionException("Unsupported unit: " + unit);
        }
    }

    private double convertTime(String time, String outputTimeUnit) throws DirectiveExecutionException {
        double value;
        String unit;

        try {
            // Extract numeric value and unit from the input time string
            value = Double.parseDouble(time.replaceAll("[^0-9.]", ""));
            unit = time.replaceAll("[0-9.]", "").trim().toLowerCase(); // Normalize unit to lowercase
        } catch (NumberFormatException e) {
            throw new DirectiveExecutionException("Invalid time format: " + time, e);
        }

        // Convert the input time to seconds
        double timeInSeconds;
        switch (unit) {
            case "s": // seconds
            case "seconds":
                timeInSeconds = value;
                break;
            case "ms": // milliseconds
                timeInSeconds = value / 1000;
                break;
            case "m": // minutes
                timeInSeconds = value * 60;
                break;
            case "h": // hours
                timeInSeconds = value * 3600;
                break;
            default:
                throw new DirectiveExecutionException("Unsupported unit: " + unit);
        }

        // Convert the time in seconds to the desired output unit
        switch (outputTimeUnit.trim().toLowerCase()) { // Normalize output unit to lowercase
            case "s": // seconds
            case "seconds":
                return timeInSeconds;
            case "minutes":
                return timeInSeconds / 60;
            case "hours":
                return timeInSeconds / 3600;
            default:
                throw new DirectiveExecutionException("Unsupported output time unit: " + outputTimeUnit);
        }
    }

    @Override
    public void destroy() {
        log.info("Destroying AggregateDirective resources.");
    }
}