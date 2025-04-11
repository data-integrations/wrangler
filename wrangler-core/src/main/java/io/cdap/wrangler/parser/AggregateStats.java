package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;

import java.util.Collections;
import java.util.List;

/**
 * Custom directive to aggregate byte size and time duration columns.
 */
public class AggregateStats implements Directive, Aggregate {
    private String byteSizeColumn;
    private String timeColumn;
    private String outputSizeColumn;
    private String outputTimeColumn;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
                .define("byteSizeColumn", TokenType.COLUMN_NAME)
                .define("timeColumn", TokenType.COLUMN_NAME)
                .define("outputSizeColumn", TokenType.IDENTIFIER)
                .define("outputTimeColumn", TokenType.IDENTIFIER)
                .build();
    }

    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
        byteSizeColumn = ((ColumnName) arguments.value("byteSizeColumn")).value();
        timeColumn = ((ColumnName) arguments.value("timeColumn")).value();
        outputSizeColumn = ((Identifier) arguments.value("outputSizeColumn")).value();
        outputTimeColumn = ((Identifier) arguments.value("outputTimeColumn")).value();
    }

    @Override
    public void aggregate(Store store, Row row) throws DirectiveExecutionException {
        Object byteValue = row.getValue(byteSizeColumn);
        Object timeValue = row.getValue(timeColumn);

        long bytes = Long.parseLong(byteValue.toString());
        long millis = Long.parseLong(timeValue.toString());

        store.set("total_bytes", store.getOrDefault("total_bytes", 0L) + bytes);
        store.set("total_time", store.getOrDefault("total_time", 0L) + millis);
        store.set("count", store.getOrDefault("count", 0L) + 1);
    }

    @Override
    public List<Row> emit(Store store) {
        long totalBytes = store.getOrDefault("total_bytes", 0L);
        long totalTime = store.getOrDefault("total_time", 0L);

        double totalMB = totalBytes / (1024.0 * 1024);
        double totalSec = totalTime / 1000.0;

        Row result = new Row();
        result.add(outputSizeColumn, totalMB);
        result.add(outputTimeColumn, totalSec);

        return Collections.singletonList(result);
    }
}
