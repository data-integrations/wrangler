package io.cdap.wrangler.executor;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;

public class AggregateStatsDirective implements Directive {
    private String sizeColumn;
    private String timeColumn;
    private static final String TOTAL_BYTES_KEY = "total_bytes";
    private static final String TOTAL_NANOS_KEY = "total_nanos";

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("size_column", TokenType.COLUMN_NAME);
        builder.define("time_column", TokenType.COLUMN_NAME);
        return builder.build();
    }

    @Override
    public void initialize(Arguments args) {
        sizeColumn = args.value("size_column");
        timeColumn = args.value("time_column");
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        TransientStore store = context.getTransientStore();
        
        for (Row row : rows) {
            ByteSize size = (ByteSize) row.getValue(sizeColumn);
            TimeDuration time = (TimeDuration) row.getValue(timeColumn);
            
            store.increment(TransientVariableScope.GLOBAL, TOTAL_BYTES_KEY, size.getBytes());
            store.increment(TransientVariableScope.GLOBAL, TOTAL_NANOS_KEY, time.getNanoseconds());
        }
        
        return rows;
    }

    @Override
    public void destroy() {
        // No cleanup needed
    }
}