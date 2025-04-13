package io.cdap.wrangler.executor;


import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.UsageDefinition;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.TokenTypes;
import io.cdap.wrangler.api.ByteSize;
import io.cdap.wrangler.api.TimeDuration;

import java.util.ArrayList;
import java.util.List;

public class AggregateSizeDuration implements Directive {
    private String sizeColumn;
    private String durationColumn;
    private String targetSizeColumn;
    private String targetDurationColumn;
    private String aggregationType;
    private String outputSizeUnit;
    private String outputTimeUnit;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregateSizeDuration")
            .define("sizeColumn", TokenType.COLUMN_NAME)
            .define("durationColumn", TokenType.COLUMN_NAME)
            .define("targetSizeColumn", TokenType.COLUMN_NAME)
            .define("targetDurationColumn", TokenType.COLUMN_NAME)
            .define("aggregationType", TokenType.TEXT)
            .define("outputSizeUnit", TokenType.TEXT)
            .define("outputTimeUnit", TokenType.TEXT)
            .build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumn = args.value("sizeColumn");
        this.durationColumn = args.value("durationColumn");
        this.targetSizeColumn = args.value("targetSizeColumn");
        this.targetDurationColumn = args.value("targetDurationColumn");
        this.aggregationType = args.contains("aggregationType") ? args.value("aggregationType") : "sum";
        this.outputSizeUnit = args.contains("outputSizeUnit") ? args.value("outputSizeUnit") : "bytes";
        this.outputTimeUnit = args.contains("outputTimeUnit") ? args.value("outputTimeUnit") : "seconds";
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        try {
            long totalSize = 0;
            long totalDuration = 0;
            int rowCount = 0;

            for (Row row : rows) {
                Object sizeValue = row.getValue(sizeColumn);
                Object durationValue = row.getValue(durationColumn);

                long sizeInBytes = convertSizeToBytes(sizeValue);
                long durationInNanoSeconds = convertDurationToNanoSeconds(durationValue);

                totalSize += sizeInBytes;
                totalDuration += durationInNanoSeconds;
                rowCount++;
            }

            if ("average".equalsIgnoreCase(aggregationType) && rowCount > 0) {
                totalSize /= rowCount;
                totalDuration /= rowCount;
            }

            double finalSize = convertSizeFromBytes(totalSize);
            double finalDuration = convertDurationFromNanoSeconds(totalDuration);

            Row resultRow = new Row();
            resultRow.add(targetSizeColumn, finalSize);
            resultRow.add(targetDurationColumn, finalDuration);

            List<Row> result = new ArrayList<>();
            result.add(resultRow);
            return result;

        } catch (Exception e) {
            throw new DirectiveExecutionException("Error executing aggregation", e);
        }
    }

    @Override
    public void destroy() {
        // No cleanup needed
    }

    private long convertSizeToBytes(Object sizeValue) {
        if (sizeValue == null) return 0L;
        try {
            return new ByteSize(sizeValue.toString().trim()).getBytes();
        } catch (Exception e) {
            throw new DirectiveExecutionException("Failed to convert byte size: " + sizeValue, e);
        }
    }

    private long convertDurationToNanoSeconds(Object durationValue) {
        if (durationValue == null) return 0L;
        try {
            return new TimeDuration(durationValue.toString().trim()).getNanoSeconds();
        } catch (Exception e) {
            throw new DirectiveExecutionException("Failed to convert duration: " + durationValue, e);
        }
    }

    private double convertSizeFromBytes(long bytes) {
        switch (outputSizeUnit.toLowerCase()) {
            case "kb": return bytes / 1024.0;
            case "mb": return bytes / (1024.0 * 1024);
            case "gb": return bytes / (1024.0 * 1024 * 1024);
            default: return (double) bytes;
        }
    }

    private double convertDurationFromNanoSeconds(long nanos) {
        switch (outputTimeUnit.toLowerCase()) {
            case "ms": return nanos / 1_000_000.0;
            case "s": return nanos / 1_000_000_000.0;
            case "m": return nanos / 60_000_000_000.0;
            default: return nanos / 1_000_000_000.0;
        }
    }
}
