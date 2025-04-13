package io.cdap.wrangler.core.directive;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Directive;
import io.cdap.wrangler.api.executor.DirectiveContext;
import io.cdap.wrangler.api.executor.DirectiveExecutor;

@Directive(name = "aggregate-stats", description = "Aggregates byte sizes and time durations.")
public class AggregateStats implements DirectiveExecutor {
    // ...fields for source/target columns and aggregation logic...

    @Override
    public void initialize(DirectiveContext context) {
        // ...initialize source/target columns...
    }

    @Override
    public void execute(Row row, ExecutorContext context) {
        // ...read byte size and time duration, accumulate totals...
    }

    @Override
    public void destroy() {
        // ...finalize and return aggregated results...
    }
}
