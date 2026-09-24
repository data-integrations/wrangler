package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.executor.ExecutionContext;
import io.cdap.wrangler.api.executor.Executor;
import io.cdap.wrangler.api.parser.*;

import java.util.List;

/**
 * Directive for aggregating byte sizes and time durations.
 */
@PublicEvolving
public class AggregateDirective implements Executor {
    private String byteSizeColumn;
    private String timeDurationColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;
    private String sizeUnit = "B";
    private String timeUnit = "ms";
    private long totalBytes = 0;
    private long totalTime = 0;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate")
            .define("byteSizeColumn", TokenType.COLUMN_NAME)
            .define("timeDurationColumn", TokenType.COLUMN_NAME)
            .define("totalSizeColumn", TokenType.COLUMN_NAME)
            .define("totalTimeColumn", TokenType.COLUMN_NAME)
            .optional("sizeUnit", TokenType.TEXT)
            .optional("timeUnit", TokenType.TEXT)
            .build();
    }

    @Override
    public void initialize(ExecutionContext context) {
        List<Token> arguments = context.getArguments();
        byteSizeColumn = ((ColumnName) arguments.get(0)).value();
        timeDurationColumn = ((ColumnName) arguments.get(1)).value();
        totalSizeColumn = ((ColumnName) arguments.get(2)).value();
        totalTimeColumn = ((ColumnName) arguments.get(3)).value();
        if (arguments.size() > 4) {
            sizeUnit = ((Text) arguments.get(4)).value();
        }
        if (arguments.size() > 5) {
            timeUnit = ((Text) arguments.get(5)).value();
        }
    }

    @Override
    public void destroy() {
        // No resources to release
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutionContext context) {
        for (Row row : rows) {
            long byteSize = ((ByteSize) row.getValue(byteSizeColumn)).getBytes();
            long timeDuration = ((TimeDuration) row.getValue(timeDurationColumn)).getMilliseconds();
            totalBytes += byteSize;
            totalTime += timeDuration;
        }
        return rows;
    }

    @Override
    public void finalize(ExecutionContext context) {
        // Convert totals to specified units
        if (sizeUnit.equalsIgnoreCase("KB")) {
            totalBytes /= 1024;
        } else if (sizeUnit.equalsIgnoreCase("MB")) {
            totalBytes /= (1024 * 1024);
        } else if (sizeUnit.equalsIgnoreCase("GB")) {
            totalBytes /= (1024 * 1024 * 1024);
        }

        if (timeUnit.equalsIgnoreCase("s")) {
            totalTime /= 1000;
        } else if (timeUnit.equalsIgnoreCase("m")) {
            totalTime /= (1000 * 60);
        } else if (timeUnit.equalsIgnoreCase("h")) {
            totalTime /= (1000 * 60 * 60);
        }

        // Create a new row with the aggregated values
        Row aggregatedRow = new Row();
        aggregatedRow.add(totalSizeColumn, totalBytes);
        aggregatedRow.add(totalTimeColumn, totalTime);
        context.write(aggregatedRow);
    }
}
