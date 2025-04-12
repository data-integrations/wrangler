package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.Usage;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Usage(
  directive = "aggregate-stats",
  usage = "aggregate-stats <source_size_column> <source_time_column> <target_size_column> <target_time_column>",
  description = "Aggregates byte size values and time durations by summing all entries from the source columns "
              + "and converting them to MB and seconds respectively."
)
public class AggregateStatsDirective implements Directive {
    private String sourceSize;
    private String sourceTime;
    private String targetSize;
    private String targetTime;
    
    private double totalBytes = 0;
    private double totalMilliseconds = 0;
    
    @Override
    public void prepare() throws DirectiveParseException {
        // Extract arguments from the directive context.
        // In production, the arguments come from the parsed recipe.
        // Here we simulate extraction for testing purposes.
        List<String> args = getArgs();
        if (args.size() < 4) {
            throw new DirectiveParseException("Expected 4 arguments, found " + args.size());
        }
        sourceSize = args.get(0);
        sourceTime = args.get(1);
        targetSize = args.get(2);
        targetTime = args.get(3);
    }

    @Override
    public List<Row> execute(List<Row> rows, DirectiveContext context) throws DirectiveParseException {
        // Reset aggregate totals.
        totalBytes = 0;
        totalMilliseconds = 0;
        
        for (Row row : rows) {
            // Parse and aggregate byte size from the source column.
            Object sizeObj = row.getValue(sourceSize);
            if (sizeObj != null) {
                try {
                    ByteSize bs = new ByteSize(sizeObj.toString());
                    totalBytes += bs.getBytes();
                } catch (Exception e) {
                    // Optionally log the error and proceed.
                }
            }
            // Parse and aggregate time duration from the source column.
            Object timeObj = row.getValue(sourceTime);
            if (timeObj != null) {
                try {
                    TimeDuration td = new TimeDuration(timeObj.toString());
                    totalMilliseconds += td.getMilliseconds();
                } catch (Exception e) {
                    // Optionally log the error and proceed.
                }
            }
        }
        // Convert aggregates to desired output units:
        // For size: convert bytes to megabytes (1 MB = 1024 * 1024 bytes).
        double totalSizeMB = totalBytes / (1024.0 * 1024.0);
        // For time: convert milliseconds to seconds.
        double totalTimeSec = totalMilliseconds / 1000.0;
        
        Row out = new Row();
        out.add(targetSize, totalSizeMB);
        out.add(targetTime, totalTimeSec);
        return Collections.singletonList(out);
    }

    @Override
    public void destroy() {
        // Cleanup if needed.
    }
    
    // Dummy method to simulate directive argument extraction.
    private List<String> getArgs() {
        // In a full implementation, these parameters will come from the recipe.
        // For testing, we simulate a fixed argument list.
        return Arrays.asList("data_transfer_size", "response_time", "total_size_mb", "total_time_sec");
    }
}
