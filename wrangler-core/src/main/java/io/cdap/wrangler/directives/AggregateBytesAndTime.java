package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.ReportErrorAndProceed;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.parser.*;
import io.cdap.wrangler.api.parser.TokenType;

import java.util.List;

@PublicEvolving
public class AggregateBytesAndTime implements Directive {

    private String sourceSizeColumn;
    private String sourceTimeColumn;
    private String targetSizeColumn;
    private String targetTimeColumn;
    private String sizeUnit = "B"; // Default to bytes
    private String timeUnit = "ms"; // Default to milliseconds
    private String aggregationType = "total"; // Default to total

    private long totalBytes;
    private long totalMilliseconds;
    private int rowCount;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-bytes-time")
                .define("sourceSizeColumn", TokenType.COLUMN, "Source column for byte sizes.")
                .define("sourceTimeColumn", TokenType.COLUMN, "Source column for time durations.")
                .define("targetSizeColumn", TokenType.COLUMN, "Target column for total size.")
                .define("targetTimeColumn", TokenType.COLUMN, "Target column for total or average time.")
                .defineOptional("sizeUnit", TokenType.STRING, "Output size unit (e.g., B, KB, MB, GB).")
                .defineOptional("timeUnit", TokenType.STRING, "Output time unit (e.g., ms, s, m, h).")
                .defineOptional("aggregationType", TokenType.STRING, "Aggregation type (total, average).")
                .build();
    }

    @Override
    public void initialize(Arguments args) {
        sourceSizeColumn = ((Column) args.value("sourceSizeColumn")).name();
        sourceTimeColumn = ((Column) args.value("sourceTimeColumn")).name();
        targetSizeColumn = ((Column) args.value("targetSizeColumn")).name();
        targetTimeColumn = ((Column) args.value("targetTimeColumn")).name();
        if (args.has("sizeUnit")) sizeUnit = ((String) args.value("sizeUnit")).toUpperCase();
        if (args.has("timeUnit")) timeUnit = ((String) args.value("timeUnit")).toLowerCase();
        if (args.has("aggregationType")) aggregationType = ((String) args.value("aggregationType")).toLowerCase();

        // Initialize accumulators
        totalBytes = 0;
        totalMilliseconds = 0;
        rowCount = 0;
    }

    @Override
    public void destroy() {
        // Cleanup if necessary
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        for (Row row : rows) {
            // Extract size and time values
            Object sizeValue = row.getValue(sourceSizeColumn);
            Object timeValue = row.getValue(sourceTimeColumn);

            // Add size to total (convert to bytes)
            if (sizeValue instanceof ByteSize) {
                totalBytes += ((ByteSize) sizeValue).getBytes();
            }

            // Add time to total (convert to milliseconds)
            if (timeValue instanceof TimeDuration) {
                totalMilliseconds += ((TimeDuration) timeValue).getMilliseconds();
            }

            rowCount++;
        }
        return rows; // Pass rows downstream; finalization will aggregate results
    }

    @Override
    public List<Row> finalize(List<Row> rows) throws Exception {
        Row resultRow = new Row();

        // Calculate size aggregate
        long finalSize = totalBytes;
        switch (sizeUnit) {
            case "KB": finalSize /= 1024; break;
            case "MB": finalSize /= 1024 * 1024; break;
            case "GB": finalSize /= 1024 * 1024 * 1024; break;
            default: break; // Default is bytes
        }

        // Calculate time aggregate
        long finalTime = totalMilliseconds;
        switch (timeUnit) {
            case "s": finalTime /= 1000; break;
            case "m": finalTime /= 1000 * 60; break;
            case "h": finalTime /= 1000 * 60 * 60; break;
            default: break; // Default is milliseconds
        }

        // If aggregation type is average, divide by row count
        if ("average".equalsIgnoreCase(aggregationType)) {
            finalSize /= rowCount;
            finalTime /= rowCount;
        }

        // Add results to a new row
        resultRow.add(targetSizeColumn, finalSize);
        resultRow.add(targetTimeColumn, finalTime);

        // Return a single-row list
        return List.of(resultRow);
    }

    @Override
    public MutationType mutationType() {
        return MutationType.ROW;
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'initialize'");
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context)
            throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execute'");
    }
}
