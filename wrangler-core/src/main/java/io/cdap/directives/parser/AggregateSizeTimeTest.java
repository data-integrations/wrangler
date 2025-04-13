package io.cdap.wrangler.directive;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.*;
import org.reflections.Store;

import java.util.Collections;
import java.util.List;

public class AggregateSizeAndTime implements Directive {
    public static final String NAME = "aggregate-size-time";

    private String sizeColumn;
    private String timeColumn;
    private String outputSizeColumn;
    private String outputTimeColumn;
    private String sizeUnit = "B";
    private String timeUnit = "ms";
    private String aggregationType = "total";

    private static final String TOTAL_SIZE_KEY = "agg.total.bytes";
    private static final String TOTAL_TIME_KEY = "agg.total.duration";
    private static final String COUNT_KEY = "agg.count";

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder(NAME)
                .setDescription("Aggregates byte size and time duration columns across rows")
                .addRequiredArg("size_column", TokenType.COLUMN_NAME)
                .addRequiredArg("time_column", TokenType.COLUMN_NAME)
                .addRequiredArg("output_size_column", TokenType.COLUMN_NAME)
                .addRequiredArg("output_time_column", TokenType.COLUMN_NAME)
                .addOptionalArg("output_size_unit", TokenType.BYTE_SIZE)
                .addOptionalArg("output_time_unit", TokenType.TIME_DURATION)
                .build();
    }

    @Override
    public void initialize(Arguments args) {
        sizeColumn = ((ColumnName) args.value("size_column")).value();
        timeColumn = ((ColumnName) args.value("time_column")).value();
        outputSizeColumn = ((ColumnName) args.value("output_size_column")).value();
        outputTimeColumn = ((ColumnName) args.value("output_time_column")).value();

        if (args.contains("output_size_unit")) {
            sizeUnit = args.value("output_size_unit").toString().toUpperCase();
        }
        if (args.contains("output_time_unit")) {
            timeUnit = args.value("output_time_unit").toString().toLowerCase();
        }
        if (args.contains("aggregation_type")) {
            aggregationType = args.value("aggregation_type").toString().toLowerCase();
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed {
        return List.of();
    }

    @Override
    public void destroy() {

    }

    @Override
    public List<Row> execute(Row row, ExecutorContext context) {
        Store store = context.getStore();

        Object sizeObj = row.getValue(sizeColumn);
        Object timeObj = row.getValue(timeColumn);

        long byteValue = sizeObj instanceof String ? new ByteSize((String) sizeObj).getBytes() : ((Number) sizeObj).longValue();
        long timeValue = timeObj instanceof String ? new TimeDuration((String) timeObj).getMilliseconds() : ((Number) timeObj).longValue();

        store.increment(TOTAL_SIZE_KEY, byteValue);
        store.increment(TOTAL_TIME_KEY, timeValue);
        store.increment(COUNT_KEY, 1);

        return Collections.emptyList();
    }

    @Override
    public List<Row> finalize(ExecutorContext context) {
        Store store = context.getStore();
        long totalBytes = store.getAsLong(TOTAL_SIZE_KEY);
        long totalDuration = store.getAsLong(TOTAL_TIME_KEY);
        long count = store.getAsLong(COUNT_KEY);

        double sizeResult = convertSize(totalBytes, sizeUnit);
        double timeResult = convertTime(totalDuration, timeUnit);

        if (aggregationType.equals("average") && count > 0) {
            sizeResult /= count;
            timeResult /= count;
        }

        Row result = new Row();
        result.add(outputSizeColumn, sizeResult);
        result.add(outputTimeColumn, timeResult);

        return Collections.singletonList(result);
    }

    private double convertSize(long bytes, String unit) {
        switch (unit) {
            case "KB": return bytes / 1024.0;
            case "MB": return bytes / (1024.0 * 1024);
            case "GB": return bytes / (1024.0 * 1024 * 1024);
            default: return bytes;
        }
    }

    private double convertTime(long millis, String unit) {
        switch (unit) {
            case "s":
            case "sec":
            case "seconds": return millis / 1000.0;
            case "m":
            case "min":
            case "minutes": return millis / (60.0 * 1000);
            default: return millis;
        }
    }
}