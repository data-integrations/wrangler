package io.cdap.wrangler.api.parser;

import java.util.List;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ByteSize;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.ReportErrorAndProceed;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TimeDuration;

/**
 * Directive to aggregate byte sizes and time durations.
 */
public class AggregateStats implements Directive {
    private String sourceSizeColumn;
    private String sourceTimeColumn;
    private Integer targetSizeColumn;
    private Integer targetTimeColumn;

    /**
     * Constructor for AggregateStats directive.
     *
     * @param sourceSizeColumn  the source column containing byte sizes
     * @param sourceTimeColumn   the source column containing time durations
     * @param targetSizeColumn   the target column for total size
     * @param targetTimeColumn   the target column for total time
     */
    public AggregateStats(String sourceSizeColumn, String sourceTimeColumn, Integer targetSizeColumn, Integer targetTimeColumn) {
        this.sourceSizeColumn = sourceSizeColumn;
        this.sourceTimeColumn = sourceTimeColumn;
        this.targetSizeColumn = targetSizeColumn;
        this.targetTimeColumn = targetTimeColumn;
    }

    public void execute(List<Row> rows) {
        long totalSize = 0;
        long totalTime = 0;

        for (Row row : rows) {
            try {
                // Parse byte size and time duration from the respective columns
                Object sizeValue = row.getValue(sourceSizeColumn);
                Object timeValue = row.getValue(sourceTimeColumn);

                if (sizeValue == null || timeValue == null) {
                    continue; // Skip rows with missing values
                }

                ByteSize byteSize = new ByteSize(sizeValue.toString());
                TimeDuration timeDuration = new TimeDuration(timeValue.toString());

                // Aggregate the values
                totalSize += byteSize.getBytes();
                totalTime += timeDuration.getNanoseconds();
            } catch (Exception e) {
                // Log or handle invalid data gracefully
                System.err.println("Error processing row: " + e.getMessage());
            }
        }

        // Create a new row with the aggregated values
        Row resultRow = new Row();
        resultRow.setValue(targetSizeColumn, totalSize);
        resultRow.setValue(targetTimeColumn, totalTime);

        // Add the resultRow to the rows list or handle it as needed
        rows.add(resultRow);
    }

    @Override
    public UsageDefinition define() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}