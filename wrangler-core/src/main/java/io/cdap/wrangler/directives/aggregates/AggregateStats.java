package io.cdap.wrangler.directives.aggregates;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.Arguments;

import java.util.Collections;
import java.util.List;

/**
 * A directive that aggregates byte sizes and time durations from multiple rows.
 */
@Categories(categories = {"aggregate"})
public class AggregateStats implements Directive {
    public static final String NAME = "aggregate-stats";
    private String sizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;
    private String outputSizeUnit = "MB";
    private String outputTimeUnit = "s";
    private String aggregationType = "total";

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("size-column", TokenType.COLUMN_NAME);
        builder.define("time-column", TokenType.COLUMN_NAME);
        builder.define("total-size-column", TokenType.COLUMN_NAME);
        builder.define("total-time-column", TokenType.COLUMN_NAME);
        builder.define("size-unit", TokenType.TEXT, false);
        builder.define("time-unit", TokenType.TEXT, false);
        builder.define("aggregation-type", TokenType.TEXT, false);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) {
        List<Token> tokens = args.getTokens();
        if (tokens.size() < 4) {
            throw new IllegalArgumentException("aggregate-stats requires at least 4 arguments");
        }

        sizeColumn = ((ColumnName) tokens.get(0)).value();
        timeColumn = ((ColumnName) tokens.get(1)).value();
        totalSizeColumn = ((ColumnName) tokens.get(2)).value();
        totalTimeColumn = ((ColumnName) tokens.get(3)).value();

        if (tokens.size() > 4) {
            outputSizeUnit = ((Text) tokens.get(4)).value();
        }
        if (tokens.size() > 5) {
            outputTimeUnit = ((Text) tokens.get(5)).value();
        }
        if (tokens.size() > 6) {
            aggregationType = ((Text) tokens.get(6)).value();
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        long totalBytes = 0;
        long totalNanos = 0;
        int rowCount = 0;

        for (Row row : rows) {
            Object sizeValue = row.getValue(sizeColumn);
            Object timeValue = row.getValue(timeColumn);

            if (sizeValue instanceof ByteSize) {
                totalBytes += ((ByteSize) sizeValue).getBytes();
            }

            if (timeValue instanceof TimeDuration) {
                totalNanos += ((TimeDuration) timeValue).getNanoseconds();
            }

            rowCount++;
        }

        Row result = new Row();
        result.add(totalSizeColumn, convertBytes(totalBytes, outputSizeUnit));
        result.add(totalTimeColumn, convertNanos(totalNanos, outputTimeUnit, aggregationType, rowCount));

        return Collections.singletonList(result);
    }

    private double convertBytes(long bytes, String unit) {
        switch (unit.toUpperCase()) {
            case "B":
                return bytes;
            case "KB":
                return bytes / 1024.0;
            case "MB":
                return bytes / (1024.0 * 1024);
            case "GB":
                return bytes / (1024.0 * 1024 * 1024);
            case "TB":
                return bytes / (1024.0 * 1024 * 1024 * 1024);
            case "PB":
                return bytes / (1024.0 * 1024 * 1024 * 1024 * 1024);
            default:
                throw new IllegalArgumentException("Unsupported byte size unit: " + unit);
        }
    }

    private double convertNanos(long nanos, String unit, String type, int count) {
        double value;
        switch (unit.toLowerCase()) {
            case "ns":
                value = nanos;
                break;
            case "us":
                value = nanos / 1000.0;
                break;
            case "ms":
                value = nanos / (1000.0 * 1000);
                break;
            case "s":
                value = nanos / (1000.0 * 1000 * 1000);
                break;
            case "m":
                value = nanos / (60.0 * 1000 * 1000 * 1000);
                break;
            case "h":
                value = nanos / (60.0 * 60 * 1000 * 1000 * 1000);
                break;
            case "d":
                value = nanos / (24.0 * 60 * 60 * 1000 * 1000 * 1000);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }

        if ("average".equalsIgnoreCase(type)) {
            return value / count;
        }
        return value;
    }

    @Override
    public void destroy() {
        // No cleanup needed
    }
} 