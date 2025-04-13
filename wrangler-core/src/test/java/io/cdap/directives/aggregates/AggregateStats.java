package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

@PublicEvolving
public class AggregateStats implements Directive {
    private static final String TOTAL_BYTES_KEY = "total_bytes";
    private static final String TOTAL_NANOS_KEY = "total_nanos";
    private static final String ROW_COUNT_KEY = "row_count";
   
    // Configuration parameters
    private String sourceSizeCol;
    private String sourceTimeCol;
    private String targetSizeCol;
    private String targetTimeCol;
    private String outputSizeUnit = "MB";  // Default
    private String outputTimeUnit = "s";   // Default

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
            .withDescription("Aggregates byte sizes and time durations across rows")
            .withArguments(
                new ColumnName("source_size_column"),
                new ColumnName("source_time_column"),
                new ColumnName("target_size_column"),
                new ColumnName("target_time_column")
            )
            .withOptionalArguments(
                new Text("output_size_unit", TokenType.TEXT),
                new Text("output_time_unit", TokenType.TEXT)
            )
            .build();
    }

    @Override
    public void initialize(List<Directive> directives) {
        // No initialization needed for other directives in this case
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        // Initialize or get existing totals from store
        TransientStore store = context.getTransientStore();
        long totalBytes = store.get(TOTAL_BYTES_KEY, 0L);
        long totalNanos = store.get(TOTAL_NANOS_KEY, 0L);
        int rowCount = store.get(ROW_COUNT_KEY, 0);

        for (Row row : rows) {
            // Process byte size
            Object sizeValue = row.getValue(sourceSizeCol);
            if (sizeValue != null) {
                ByteSize bs = new ByteSize(sizeValue.toString());
                totalBytes += bs.getBytes();
            }

            // Process time duration
            Object timeValue = row.getValue(sourceTimeCol);
            if (timeValue != null) {
                TimeDuration td = new TimeDuration(timeValue.toString());
                totalNanos += td.getNanos();
            }

            rowCount++;
        }

        // Update store with new totals
        store.set(TOTAL_BYTES_KEY, totalBytes);
        store.set(TOTAL_NANOS_KEY, totalNanos);
        store.set(ROW_COUNT_KEY, rowCount);

        // If final batch, return aggregated results
        if (context.isStageCompleted()) {
            return generateResult(totalBytes, totalNanos, rowCount);
        }

        return new ArrayList<>(); // Return empty list for intermediate batches
    }

    private List<Row> generateResult(long totalBytes, long totalNanos, int rowCount) {
        Row result = new Row();
       
        // Convert to output units
        double outputSize = convertBytes(totalBytes, outputSizeUnit);
        double outputTime = convertNanos(totalNanos, outputTimeUnit);

        result.add(targetSizeCol, outputSize);
        result.add(targetTimeCol, outputTime);
       
        // Optional: Add count metric
        result.add("row_count", rowCount);

        return Collections.singletonList(result);
    }

    private double convertBytes(long bytes, String unit) {
        switch (unit.toUpperCase()) {
            case "B":   return bytes;
            case "KB":  return bytes / 1024.0;
            case "MB":  return bytes / (1024.0 * 1024);
            case "GB":  return bytes / (1024.0 * 1024 * 1024);
            case "TB":  return bytes / (1024.0 * 1024 * 1024 * 1024);
            default: throw new IllegalArgumentException("Unsupported size unit: " + unit);
        }
    }

    private double convertNanos(long nanos, String unit) {
        switch (unit.toLowerCase()) {
            case "ns": return nanos;
            case "ms": return nanos / 1_000_000.0;
            case "s":  return nanos / 1_000_000_000.0;
            case "m":  return nanos / (60_000_000_000.0);
            case "h":  return nanos / (3_600_000_000_000.0);
            default: throw new IllegalArgumentException("Unsupported time unit: " + unit);
        }
    }

    @Override
    public void destroy() {
        // Clean up resources if needed
    }
}
