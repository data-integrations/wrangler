package io.cdap.directives.aggregates;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

    @Test
    public void testBasicTotalAggregation() throws Exception {
        String[] recipe = {
            "aggregate-stats :bytes :duration :total_bytes_b :total_time_ns"
        };

        List<Row> rows = Arrays.asList(
            new Row("bytes", "1KB").add("duration", "1s"), // 1024 B, 1_000_000_000 ns
            new Row("bytes", "10B").add("duration", "500ms"), // 10 B, 500_000_000 ns
            new Row("bytes", "1MB").add("duration", "0.1s") // 1048576 B, 100_000_000 ns
        );
        // Expected: Total Bytes = 1024 + 10 + 1048576 = 1049610
        // Expected: Total Nanos = 1_000_000_000 + 500_000_000 + 100_000_000 = 1_600_000_000

        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(1049610.0, (Double) results.get(0).getValue("total_bytes_b"), 0.001);
        Assert.assertEquals(1_600_000_000.0, (Double) results.get(0).getValue("total_time_ns"), 0.001);
    }

     @Test
    public void testAggregationWithUnitsAndAverage() throws Exception {
        String[] recipe = {
            "aggregate-stats :bytes :duration :total_bytes_mb :avg_time_s size_unit='MB' time_unit='s' time_agg='average'"
        };

        List<Row> rows = Arrays.asList(
            new Row("bytes", "1024KB").add("duration", "1s"), // 1 MB, 1_000_000_000 ns
            new Row("bytes", "512KB").add("duration", "2000ms"), // 0.5 MB, 2_000_000_000 ns
            new Row("bytes", "2MB").add("duration", "0.5s") // 2 MB, 500_000_000 ns
        );
        // Expected: Total Bytes = 1 + 0.5 + 2 = 3.5 MB

        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(3.5, (Double) results.get(0).getValue("total_bytes_mb"), 0.001);
        Assert.assertEquals(1.16666, (Double) results.get(0).getValue("avg_time_s"), 0.001);
     }


     @Test
    public void testAggregationWithNullsAndInvalid() throws Exception {
        String[] recipe = {
            "aggregate-stats :bytes :duration :total_bytes_kb :total_time_ms size_unit='KB' time_unit='ms'"
        };

        List<Row> rows = Arrays.asList(
            new Row("bytes", "1KB").add("duration", "1s"),       // 1024 B, 1000 ms
            new Row("bytes", null).add("duration", "500ms"),      // 0 B, 500 ms
            new Row("bytes", "2KB").add("duration", null),       // 2048 B, 0 ms
            new Row("bytes", "bad data").add("duration", "100ms"), // 0 B, 100 ms (byte skipped)
            new Row("bytes", "0.5KB").add("duration", "invalid")  // 512 B, 0 ms (time skipped)
        );
        // Expected Total Bytes = 1024 + 0 + 2048 + 0 + 512 = 3584 B
        // Expected Output Size (KB) = 3584 / 1024.0 = 3.5 KB
        // Expected Output Time (ms) = 1_600_000_000 / 1_000_000.0 = 1600.0 ms

        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(3.5, (Double) results.get(0).getValue("total_bytes_kb"), 0.001);
        Assert.assertEquals(1600.0, (Double) results.get(0).getValue("total_time_ms"), 0.001);
    }

     @Test
    public void testEmptyInput() throws Exception {
         String[] recipe = {
            "aggregate-stats :bytes :duration :total_bytes_b :total_time_ns"
        };
         List<Row> rows = Arrays.asList(); // Empty input
         List<Row> results = TestingRig.execute(recipe, rows);
         Assert.assertEquals(0, results.size()); 
    }

}