package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.*;
import io.cdap.wrangler.executor.Context;

public class AggregateStatsDirective implements Directive {
    private String sizeColumn;
    private String timeColumn;
    private String outputSizeColumn;
    private String outputTimeColumn;
    private boolean calculateAverage;

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumn = args.value("size_column", TokenType.COLUMN_NAME).value();
        this.timeColumn = args.value("time_column", TokenType.COLUMN_NAME).value();
        this.outputSizeColumn = args.value("output_size", TokenType.COLUMN_NAME).value();
        this.outputTimeColumn = args.value("output_time", TokenType.COLUMN_NAME).value();
        
        // Optional argument
        this.calculateAverage = args.contains("avg") && 
                               args.value("avg", TokenType.BOOLEAN).value();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        // Initialize accumulators in context store
        if (!context.has("total_bytes")) {
            context.set("total_bytes", 0L);
            context.set("total_nanos", 0L);
            context.set("row_count", 0);
        }

        // Process each row
        for (Row row : rows) {
            long bytes = parseSizeValue(row.getValue(sizeColumn));
            long nanos = parseTimeValue(row.getValue(timeColumn));
            
            context.set("total_bytes", (Long)context.get("total_bytes") + bytes);
            context.set("total_nanos", (Long)context.get("total_nanos") + nanos);
            context.set("row_count", (Integer)context.get("row_count") + 1);
        }

        // Return empty list during execution (aggregation happens in finalize)
        return Collections.emptyList();
    }

    @Override
    public void destroy() {
        // Cleanup if needed
    }

    @Override
    public List<Row> finalize(ExecutorContext context) throws DirectiveExecutionException {
        long totalBytes = (Long) context.get("total_bytes");
        long totalNanos = (Long) context.get("total_nanos");
        int rowCount = (Integer) context.get("row_count");

        // Calculate final values
        double finalSize = calculateAverage ? totalBytes / (1024.0 * 1024 * rowCount) 
                                          : totalBytes / (1024.0 * 1024);
        double finalTime = calculateAverage ? totalNanos / (1_000_000_000.0 * rowCount)
                                          : totalNanos / 1_000_000_000.0;

        // Create result row
        Row result = new Row();
        result.add(outputSizeColumn, finalSize);
        result.add(outputTimeColumn, finalTime);
        
        return Collections.singletonList(result);
    }

    private long parseSizeValue(Object value) {
        if (value instanceof String) {
            return new ByteSize((String)value).getBytes();
        }
        throw new DirectiveExecutionException("Invalid byte size value: " + value);
    }

    private long parseTimeValue(Object value) {
        if (value instanceof String) {
            return new TimeDuration((String)value).getNanos();
        }
        throw new DirectiveExecutionException("Invalid time duration value: " + value);
    }
}