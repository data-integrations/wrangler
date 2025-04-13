package io.cdap.directives.aggregates;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * Directive for aggregating byte size and time duration values across a batch of records.
 */
@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
@Description("Aggregates statistics for byte sizes and time durations")
public class AggregateStats implements Directive {
    public static final String DIRECTIVE_NAME = "aggregate-stats";
    private String column;
    private String storeKeyPrefix;

    private String type = "bytes";   // default
    private String mode = "total";   // default
    private String unit = null;
    private String outputColumn = null;


    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(DIRECTIVE_NAME);
        builder.define("column", TokenType.COLUMN_NAME);
        builder.define("type", TokenType.TEXT);        // bytes or duration
        builder.define("mode", TokenType.TEXT);        // total or average
        builder.define("unit", TokenType.TEXT);        // e.g., KB, seconds
        builder.define("into", TokenType.TEXT);        // output column name
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        this.column = ((ColumnName) args.value("column")).value();
        this.storeKeyPrefix = "stats." + this.column;

        this.type = ((Text) args.value("type")).value().toLowerCase();
        this.mode = ((Text) args.value("mode")).value().toLowerCase();
        this.unit = args.contains("unit") ? ((Text) args.value("unit")).value().toLowerCase() : null;
        this.outputColumn = args.contains("into") ? ((Text) args.value("into")).value() : null;
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed {
        TransientStore store = context.getTransientStore();

        // Initialize or get existing values
        Long totalSize = store.get(storeKeyPrefix + ".size");
        Long totalTime = store.get(storeKeyPrefix + ".time");
        Integer count = store.get(storeKeyPrefix + ".count");

        if (totalSize == null) {
            store.set(TransientVariableScope.GLOBAL, storeKeyPrefix + ".size", 0L);
            totalSize = 0L;
        }
        if (totalTime == null) {
            store.set(TransientVariableScope.GLOBAL, storeKeyPrefix + ".time", 0L);
            totalTime = 0L;
        }
        if (count == null) {
            store.set(TransientVariableScope.GLOBAL, storeKeyPrefix + ".count", 0);
            count = 0;
        }

        for (Row row : rows) {
            Object value = row.getValue(column);
            if (value != null) {
                try {
                    if (value instanceof ByteSize) {
                        store.increment(TransientVariableScope.GLOBAL, storeKeyPrefix + ".size",
                                ((ByteSize) value).getBytes());
                    } else if (value instanceof TimeDuration) {
                        store.increment(TransientVariableScope.GLOBAL, storeKeyPrefix + ".time",
                                ((TimeDuration) value).getNanoseconds());
                    }
                    store.increment(TransientVariableScope.GLOBAL, storeKeyPrefix + ".count", 1);
                } catch (Exception e) {
                    throw new DirectiveExecutionException(
                            String.format("Failed to process value '%s' in column '%s': %s",
                                    value, column, e.getMessage()));
                }
            }
        }

        // If this is the end of a partition, process results
        if (context.isEndPartition()) {
            Long finalSize = store.get(storeKeyPrefix + ".size");
            Long finalTime = store.get(storeKeyPrefix + ".time");
            Integer finalCount = store.get(storeKeyPrefix + ".count");

            Row resultRow = new Row();

            Object finalValue;
            if ("average".equals(mode) && finalCount > 0) {
                finalValue = "bytes".equals(type)
                        ? finalSize / finalCount
                        : finalTime / finalCount;
            } else {
                finalValue = "bytes".equals(type)
                        ? finalSize
                        : finalTime;
            }

            // Unit conversion
            if (unit != null) {
                if ("bytes".equals(type)) {
                    switch (unit) {
                        case "kb": finalValue = ((Long) finalValue) / 1024.0; break;
                        case "mb": finalValue = ((Long) finalValue) / (1024.0 * 1024); break;
                    }
                } else if ("duration".equals(type)) {
                    switch (unit) {
                        case "seconds": finalValue = ((Long) finalValue) / 1_000_000_000.0; break;
                        case "ms":      finalValue = ((Long) finalValue) / 1_000_000.0; break;
                    }
                }
            }

            // Determine column name
            String outputCol = outputColumn != null ? outputColumn :
                    mode + "_" + type + (unit != null ? "_in_" + unit : "");

            resultRow.add(outputCol, finalValue);
            resultRow.add("count", finalCount);

            return List.of(resultRow);
        }

        return rows;
    }

    @Override
    public void destroy() {
        // no-op
    }
}
