package io.cdap.wrangler.executor;


import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;


import java.util.Arrays;
import java.util.List;

public class AggregateStatsDirectiveTest {

    @Test
    public void testAggregateStatsDirectiveTotal() throws Exception {
        List<Row> rows = Arrays.asList(
            new Row("data_transfer_size", "1KB").add("response_time", "100ms"),
            new Row("data_transfer_size", "2KB").add("response_time", "150ms"),
            new Row("data_transfer_size", "512B").add("response_time", "50ms")
        );

        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
        };

        List<Row> results = TestingRig.executeDirectives(recipe, rows);

        double expectedTotalSizeInMB = (1024 + 2048 + 512) / (1024.0 * 1024.0); // 3.5KB to MB
        double expectedTotalTimeInSec = (100 + 150 + 50) / 1000.0; // 300ms to seconds

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(expectedTotalSizeInMB,
            (double) results.get(0).getValue("total_size_mb"), 0.0001);
        Assert.assertEquals(expectedTotalTimeInSec,
            (double) results.get(0).getValue("total_time_sec"), 0.0001);
    }
}
