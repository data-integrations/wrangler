package io.cdap.directives.aggregates;

import java.util.List;
import java.util.HashMap;
import java.util.Map;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.LazyNumber;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

public class Aggregation implements Directive {

    private String byteSizeColumn;
    private String timeColumn;
    private String totalSizeColumn;
    private String totalTimeColumn;
    private String outputSizeUnit;
    private String outputTimeUnit;
    private String aggregationType;

    private Map<String, Long> store;
    private long rowCount;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("directive");
        builder.define("byteSizeColumn", TokenType.COLUMN_NAME);
        builder.define("timeColumn", TokenType.COLUMN_NAME);
        builder.define("totalSizeColumn", TokenType.COLUMN_NAME);
        builder.define("totalTimeColumn", TokenType.COLUMN_NAME);
        builder.define("outputSizeUnit", TokenType.TEXT, Optional.TRUE);
        builder.define("outputTimeUnit", TokenType.TEXT, Optional.TRUE);
        builder.define("aggregationType", TokenType.TEXT, Optional.TRUE);

        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.byteSizeColumn = args.value("byteSizeColumn");
        this.timeColumn = args.value("timeColumn");
        this.totalSizeColumn = args.value("totalSizeColumn");
        this.totalTimeColumn = args.value("totalTimeColumn");

        if (args.contains("outputSizeUnit")) {
            this.outputSizeUnit = args.value("outputSizeUnit");
        }

        if (args.contains("outputTimeUnit")) {
            this.outputTimeUnit = args.value("outputTimeUnit");
        }

        if (args.contains("aggregationType")) {
            this.aggregationType = args.value("aggregationType");
        }

        store = new HashMap<>();
        store.put(byteSizeColumn, 0L);
        store.put(timeColumn, 0L);
        rowCount = 0;
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context)
        throws DirectiveExecutionException, ErrorRowException {
        for (Row row : rows) {
            long byteSize = (long) row.getValue(byteSizeColumn);
            long timeDuration = (long) row.getValue(timeColumn);

            long currentTotalBytes = store.get(byteSizeColumn);
            long currentTotalTime = store.get(timeColumn);

            context.getProperties().put(String.valueOf(store.get(byteSizeColumn)), String.valueOf(currentTotalBytes + byteSize));
            context.getProperties().put(String.valueOf(store.get(timeColumn)), String.valueOf(currentTotalTime + timeDuration));

            rowCount++;
        }

        return rows;
    }

    @Override
    public void destroy() {
    }

    public void finalize(ExecutorContext context) throws DirectiveExecutionException {
        long totalBytes = store.get(byteSizeColumn);
        long totalTime = store.get(timeColumn);

        if (outputSizeUnit != null) {
            totalBytes = convertBytesToUnit(totalBytes, outputSizeUnit);
        }

        if (outputTimeUnit != null) {
            totalTime = convertTimeToUnit(totalTime, outputTimeUnit);
        }

        context.getProperties().put(totalSizeColumn, new Numeric(new LazyNumber(totalBytes)).toString());
        context.getProperties().put(totalTimeColumn, new Numeric(new LazyNumber(totalTime)).toString());

        if (aggregationType != null && aggregationType.equals("average")) {
            if (rowCount > 0) {
                totalBytes /= rowCount;
                totalTime /= rowCount;
            }
        }
    }

    private long convertBytesToUnit(long totalBytes, String unit) {
        switch (unit.toUpperCase()) {
            case "MB":
                return totalBytes / (1024 * 1024);
            case "GB":
                return totalBytes / (1024 * 1024 * 1024);
            default:
                return totalBytes;
        }
    }

    private long convertTimeToUnit(long totalTime, String unit) {
        switch (unit.toUpperCase()) {
            case "SECONDS":
                return totalTime / 1_000_000_000L;
            case "MINUTES":
                return totalTime / 60_000_000_000L;
            default:
                return totalTime;
        }
    }
}
