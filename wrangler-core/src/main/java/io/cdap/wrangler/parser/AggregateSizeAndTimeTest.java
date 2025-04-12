package io.cdap.wrangler.directive;

import io.cdap.wrangler.api.executor.ExecutorContext;
import io.cdap.wrangler.api.row.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class AggregateSizeAndTimeTest {

    @Test
    public void testAggregationTotalMBSeconds() throws Exception {
        AggregateSizeAndTime directive = new AggregateSizeAndTime();
        directive.initialize(new MockArguments(
                "bytes", "duration", "total_size", "total_time", "MB", "seconds", "total"
        ));

        ExecutorContext context = new MockExecutorContext();

        List<Row> rows = Arrays.asList(
                new Row("bytes", "1024KB").add("duration", "1s"),
                new Row("bytes", "2048KB").add("duration", "2s")
        );

        for (Row row : rows) {
            directive.execute(row, context);
        }

        List<Row> results = directive.finalize(context);
        Row result = results.get(0);

        // 1024KB = 1MB, 2048KB = 2MB → total = 3MB
        assertEquals(3.0, (double) result.getValue("total_size"), 0.01);
        assertEquals(3.0, (double) result.getValue("total_time"), 0.01);
    }

    @Test
    public void testAggregationAverageKBMilliseconds() throws Exception {
        AggregateSizeAndTime directive = new AggregateSizeAndTime();
        directive.initialize(new MockArguments(
                "bytes", "duration", "avg_size", "avg_time", "KB", "ms", "average"
        ));

        ExecutorContext context = new MockExecutorContext();

        List<Row> rows = Arrays.asList(
                new Row("bytes", "1MB").add("duration", "500ms"),
                new Row("bytes", "1MB").add("duration", "1500ms")
        );

        for (Row row : rows) {
            directive.execute(row, context);
        }

        List<Row> results = directive.finalize(context);
        Row result = results.get(0);

        // Total = 2048KB, average = 1024KB
        assertEquals(1024.0, (double) result.getValue("avg_size"), 0.01);
        assertEquals(1000.0, (double) result.getValue("avg_time"), 0.01);
    }
}