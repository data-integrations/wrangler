package io.cdap.wrangler.steps.transformation;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import avro.shaded.com.google.common.base.Optional;

public class AggregateStats implements Directive {
    public static final String NAME = "aggregate-stats";
    private String sizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;
    private String sizeUnit;
    private String timeUnit;
    
    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("source_size_column", TokenType.COLUMN_NAME);
        builder.define("source_time_column", TokenType.COLUMN_NAME);
        builder.define("target_size_column", TokenType.COLUMN_NAME);
        builder.define("target_time_column", TokenType.COLUMN_NAME);
        builder.define("size_unit", TokenType.STRING);
        builder.define("time_unit", TokenType.STRING);
        return builder.build();
    }
    
    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
        this.sizeColumn = ((ColumnName) arguments.value("source_size_column")).value();
        this.timeColumn = ((ColumnName) arguments.value("source_time_column")).value();
        this.totalSizeColumn = ((ColumnName) arguments.value("target_size_column")).value();
        this.totalTimeColumn = ((ColumnName) arguments.value("target_time_column")).value();
        
        if (arguments.contains("size_unit")) {
            this.sizeUnit = ((Text) arguments.value("size_unit")).value();
        } else {
            this.sizeUnit = "mb"; // Default to MB
        }
        
        if (arguments.contains("time_unit")) {
            this.timeUnit = ((Text) arguments.value("time_unit")).value();
        } else {
            this.timeUnit = "s"; // Default to seconds
        }
    }
    
    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        // Get the transient store from context
        Map<String, Object> store = (Map<String, Object>) context.getTransientStore();
        
        // Initialize accumulators if not present
        if (!store.containsKey("totalBytes")) {
            store.put("totalBytes", 0L);
            store.put("totalNanoseconds", 0L);
            store.put("rowCount", 0);
        }
        
        // Get current accumulator values
        long totalBytes = (long) store.get("totalBytes");
        long totalNanoseconds = (long) store.get("totalNanoseconds");
        int rowCount = (int) store.get("rowCount");
        
        // Process each row and update accumulators
        for (Row row : rows) {
            try {
                // Process byte size
                Object sizeObj = row.getValue(sizeColumn);
                if (sizeObj != null) {
                    ByteSize byteSize = new ByteSize(sizeObj.toString());
                    totalBytes += byteSize.getBytes();
                }
                
                // Process time duration
                Object timeObj = row.getValue(timeColumn);
                if (timeObj != null) {
                    TimeDuration timeDuration = new TimeDuration(timeObj.toString());
                    totalNanoseconds += timeDuration.getMilliseconds();
                }
                
                rowCount++;
            } catch (Exception e) {
                throw new DirectiveExecutionException("Error processing row: " + e.getMessage());
            }
        }
        
        // Update store
        store.put("totalBytes", totalBytes);
        store.put("totalNanoseconds", totalNanoseconds);
        store.put("rowCount", rowCount);
        
        // On last batch, create result row
        if (context.getTransientStore().containsKey("lastBatch") || rows.isEmpty()) {
            Row resultRow = new Row();
            
            // Convert to requested units
            double convertedSize = 0;
            switch (sizeUnit.toLowerCase()) {
                case "b":
                    convertedSize = totalBytes;
                    break;
                case "kb":
                    convertedSize = totalBytes / 1024.0;
                    break;
                case "mb":
                    convertedSize = totalBytes / (1024.0 * 1024.0);
                    break;
                case "gb":
                    convertedSize = totalBytes / (1024.0 * 1024.0 * 1024.0);
                    break;
                case "tb":
                    convertedSize = totalBytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
                    break;
                case "pb":
                    convertedSize = totalBytes / (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
                    break;
                default:
                    convertedSize = totalBytes / (1024.0 * 1024.0); // Default to MB
            }
            
            double convertedTime = 0;
            switch (timeUnit.toLowerCase()) {
                case "ms":
                    convertedTime = totalNanoseconds / 1_000_000.0;
                    break;
                case "s":
                    convertedTime = totalNanoseconds / 1_000_000_000.0;
                    break;
                case "m":
                    convertedTime = totalNanoseconds / (60.0 * 1_000_000_000.0);
                    break;
                case "h":
                    convertedTime = totalNanoseconds / (60.0 * 60.0 * 1_000_000_000.0);
                    break;
                case "d":
                    convertedTime = totalNanoseconds / (24.0 * 60.0 * 60.0 * 1_000_000_000.0);
                    break;
                default:
                    convertedTime = totalNanoseconds / 1_000_000_000.0; // Default to seconds
            }
            
            resultRow.addOrSet(totalSizeColumn, convertedSize);
            resultRow.addOrSet(totalTimeColumn, convertedTime);
            
            return Collections.singletonList(resultRow);
        }
        
        // For intermediate batches, return the original rows
        return rows;
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'destroy'");
    }
}