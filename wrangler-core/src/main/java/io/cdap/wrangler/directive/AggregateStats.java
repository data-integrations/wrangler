package io.cdap.wrangler.directive;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Public;
import io.cdap.wrangler.api.executor.ExecutorContext;

import java.util.List;

@Public
public class AggregateStats implements Directive {
    private String sizeColumn;
    private String timeColumn;
    private String targetSizeColumn;
    private String targetTimeColumn;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
            .define("sizeColumn", TokenType.COLUMN_NAME)
            .define("timeColumn", TokenType.COLUMN_NAME)
            .define("targetSizeColumn", TokenType.COLUMN_NAME)
            .define("targetTimeColumn", TokenType.COLUMN_NAME)
            .build();
    }

    @Override
    public void initialize(Arguments arguments) {
        sizeColumn = arguments.value("sizeColumn");
        timeColumn = arguments.value("timeColumn");
        targetSizeColumn = arguments.value("targetSizeColumn");
        targetTimeColumn = arguments.value("targetTimeColumn");
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        long totalBytes = 0;
        long totalNanoseconds = 0;

        for (Row row : rows) {
            ByteSize size = (ByteSize) row.getValue(sizeColumn);
            TimeDuration time = (TimeDuration) row.getValue(timeColumn);

            totalBytes += size.getBytes();
            totalNanoseconds += time.getNanoseconds();
        }

        Row result = new Row();
        result.add(targetSizeColumn, totalBytes / (1024.0 * 1024)); // Convert to MB
        result.add(targetTimeColumn, totalNanoseconds / 1_000_000_000.0); // Convert to seconds

        return List.of(result);
    }
}
