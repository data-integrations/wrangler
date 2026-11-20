package io.cdap.directives;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatsDirective implements Directive {
    private String sizeColumn;
    private String timeColumn;
    private String outputSize;
    private String outputTime;
    
    private long totalBytes = 0;
    private long totalNanos = 0;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
            .with("size_column", TokenType.COLUMN_NAME, "Column containing byte sizes")
            .with("time_column", TokenType.COLUMN_NAME, "Column containing time durations")
            .with("output_size", TokenType.COLUMN_NAME, "Output column for total size")
            .with("output_time", TokenType.COLUMN_NAME, "Output column for total time")
            .build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.sizeColumn = ((Text) args.value("size_column")).value();
        this.timeColumn = ((Text) args.value("time_column")).value();
        this.outputSize = ((Text) args.value("output_size")).value();
        this.outputTime = ((Text) args.value("output_time")).value();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        for (Row row : rows) {
            try {
                String sizeValue = row.getValue(sizeColumn).toString();
                ByteSize byteSize = new ByteSize(sizeValue);
                totalBytes += byteSize.getBytes();
                
                String timeValue = row.getValue(timeColumn).toString();
                TimeDuration timeDuration = new TimeDuration(timeValue);
                totalNanos += timeDuration.getNanoSeconds();
            } catch (Exception e) {
                throw new DirectiveExecutionException(
                    String.format("Error processing row %s: %s", row, e.getMessage()), e);
            }
        }
        return rows;
    }

    @Override
    public List<Row> finalize(ExecutorContext context) throws DirectiveExecutionException {
        Row result = new Row();
        result.add(outputSize, totalBytes);
        result.add(outputTime, totalNanos);
        
        List<Row> results = new ArrayList<>(1);
        results.add(result);
        return results;
    }

    @Override
    public void destroy() {
        // Reset the state variables
        totalBytes = 0;
        totalNanos = 0;
    }
}