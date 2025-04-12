package io.cdap.wrangler.steps.transformation;

import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.SyntaxError;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * A directive for aggregating byte size and time duration values.
 */
public class AggregateStats {
    
    private final String sizeColumn;
    private final String timeColumn;
    private final String sizeOutputColumn;
    private final String timeOutputColumn;
    private final String sizeUnit;
    private final String timeUnit;
    private final String operation;
    
    /**
     * Constructor for the AggregateStats directive.
     * 
     * @param sizeColumn Column containing byte size values
     * @param timeColumn Column containing time duration values
     * @param sizeOutputColumn Output column for aggregated size
     * @param timeOutputColumn Output column for aggregated time
     * @param sizeUnit Output unit for size (B, KB, MB, GB, TB, PB)
     * @param timeUnit Output unit for time (ns, µs, ms, s, m, h, d)
     * @param operation Aggregation operation (total, average)
     */
    public AggregateStats(String sizeColumn, String timeColumn, 
                         String sizeOutputColumn, String timeOutputColumn,
                         Optional<String> sizeUnit, Optional<String> timeUnit,
                         Optional<String> operation) {
        this.sizeColumn = sizeColumn;
        this.timeColumn = timeColumn;
        this.sizeOutputColumn = sizeOutputColumn;
        this.timeOutputColumn = timeOutputColumn;
        this.sizeUnit = sizeUnit.isPresent() ? sizeUnit.get() : "B";
        this.timeUnit = timeUnit.isPresent() ? timeUnit.get() : "ns";
        this.operation = operation.isPresent() ? operation.get().toLowerCase() : "total";
    }
    
    /**
     * Defines the usage of the directive.
     * 
     * @return The usage definition
     */
    public static UsageDefinition usage() {
        return UsageDefinition.builder("aggregate-stats")
            .define("size_column", TokenType.COLUMN)
            .define("time_column", TokenType.COLUMN)
            .define("size_output", TokenType.TEXT)
            .define("time_output", TokenType.TEXT)
            .define("size_unit", TokenType.TEXT, Optional.of("B"))
            .define("time_unit", TokenType.TEXT, Optional.of("ns"))
            .define("operation", TokenType.TEXT, Optional.of("total"))
            .build();
    }
    
    /**
     * Executes the aggregation directive on the rows.
     * 
     * @param rows The input rows
     * @param context The execution context
     * @return The processed rows
     * @throws DirectiveExecutionException If an error occurs during execution
     */
    public List<Row> execute(List<Row> rows, ExecutorContext context) 
        throws DirectiveExecutionException {
        
        double totalBytes = 0;
        double totalNanos = 0;
        int rowCount = 0;
        
        // Process each row
        for (Row row : rows) {
            if (row.find(sizeColumn) != -1 && row.find(timeColumn) != -1) {
                String sizeStr = (String) row.getValue(sizeColumn);
                String timeStr = (String) row.getValue(timeColumn);
                
                try {
                    ByteSize byteSize = new ByteSize(sizeStr);
                    TimeDuration timeDuration = new TimeDuration(timeStr);
                    
                    totalBytes += (double) byteSize.value();
                    totalNanos += (double) timeDuration.value();
                    rowCount++;
                } catch (SyntaxError e) {
                    throw new DirectiveExecutionException(
                        "Error parsing value: " + e.getMessage());
                }
            }
        }
        
        if (rowCount == 0) {
            throw new DirectiveExecutionException("No valid rows found for aggregation");
        }
        
        // Calculate the result based on the operation
        double resultBytes = "average".equals(operation) ? totalBytes / rowCount : totalBytes;
        double resultNanos = "average".equals(operation) ? totalNanos / rowCount : totalNanos;
        
        // Convert to requested units
        double sizeConverted;
        double timeConverted;
        
        try {
            ByteSize dummySize = new ByteSize("1B");
            TimeDuration dummyTime = new TimeDuration("1ns");
            
            sizeConverted = dummySize.convertFromBytes(resultBytes, sizeUnit);
            timeConverted = dummyTime.convertFromNanos(resultNanos, timeUnit);
        } catch (SyntaxError e) {
            throw new DirectiveExecutionException(
                "Error converting to specified units: " + e.getMessage());
        }
        
        // Add the results to each row
        for (Row row : rows) {
            row.add(sizeOutputColumn, sizeConverted);
            row.add(timeOutputColumn, timeConverted);
        }
        
        return rows;
    }
}
