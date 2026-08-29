package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import java.util.List;

@PublicEvolving
public class AggregateDirective implements Directive {
    private String sourceSizeColumn;
    private String sourceTimeColumn;
    private String targetSizeColumn;
    private String targetTimeColumn;
    private String sizeUnit = "bytes"; // Default unit
    private String timeUnit = "seconds"; // Default unit
    private String aggregationType = "total"; // Default aggregation type

    private long totalSize = 0;
    private long totalTime = 0;
    private int rowCount = 0;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate");
        builder.define("sourceSizeColumn", TokenType.COLUMN_NAME);
        builder.define("sourceTimeColumn", TokenType.COLUMN_NAME);
            builder.define("targetTimeColumn", TokenType.COLUMN_NAME);
            builder.define("sizeUnit", TokenType.TEXT);
            builder.define("timeUnit", TokenType.TEXT);
            builder.define("aggregationType", TokenType.TEXT);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        sourceSizeColumn = ((ColumnName) args.value("sourceSizeColumn")).value();
        sourceTimeColumn = ((ColumnName) args.value("sourceTimeColumn")).value();
        targetSizeColumn = ((ColumnName) args.value("targetSizeColumn")).value();
        targetTimeColumn = ((ColumnName) args.value("targetTimeColumn")).value();

        if (args.contains("sizeUnit")) {
            sizeUnit = args.value("sizeUnit").value().toString().toLowerCase();
        }
        if (args.contains("timeUnit")) {
            timeUnit = args.value("timeUnit").value().toString().toLowerCase();
        }
        if (args.contains("aggregationType")) {
            aggregationType = args.value("aggregationType").value().toString().toLowerCase();
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        for (Row row : rows) {
            // Read byte size and time duration
            Object sizeValue = row.getValue(sourceSizeColumn);
            Object timeValue = row.getValue(sourceTimeColumn);

            if (sizeValue != null && sizeValue instanceof Long) {
                totalSize += (Long) sizeValue; // Assume size is already in bytes
            }

            if (timeValue != null && timeValue instanceof Long) {
                totalTime += (Long) timeValue; // Assume time is already in nanoseconds
            }

            rowCount++;
        }
        return rows; // Return rows as-is for now
    }

    @Override
    public void destroy() {
        // No cleanup required
    }

    public List<Row> finalize(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        // Perform unit conversions
        double finalSize = convertSize(totalSize, sizeUnit);
        double finalTime = convertTime(totalTime, timeUnit);

        if (aggregationType.equals("average")) {
            finalSize /= rowCount;
            finalTime /= rowCount;
        }

        // Create a new row with the aggregate values
        Row aggregateRow = new Row();
        aggregateRow.add(targetSizeColumn, finalSize);
        aggregateRow.add(targetTimeColumn, finalTime);

        return List.of(aggregateRow);
    }

    private double convertSize(long size, String unit) {
        switch (unit) {
            case "kb":
                return size / 1024.0;
            case "mb":
                return size / (1024.0 * 1024);
            case "gb":
                return size / (1024.0 * 1024 * 1024);
            default:
                return size; // Default is bytes
        }
    }

    private double convertTime(long time, String unit) {
        switch (unit) {
            case "milliseconds":
                return time / 1_000_000.0;
            case "seconds":
                return time / 1_000_000_000.0;
            case "minutes":
                return time / (60.0 * 1_000_000_000);
            default:
                return time; // Default is nanoseconds
        }
    }

    public void initialize(TokenGroup args) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'initialize'");
    }

    // Removed duplicate initialize method

    // Removed duplicate initialize method

}