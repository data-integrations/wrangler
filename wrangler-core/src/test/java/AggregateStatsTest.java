package io.cdap.wrangler.steps;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.test.TestingRig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

    @Test
    public void testAggregateStats() {
        List<Row> input = Arrays.asList(
            new Row("data_transfer_size", "10KB").add("response_time", "150ms"),
            new Row("data_transfer_size", "1MB").add("response_time", "2.5s")
        );

        String[] recipe = {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        List<Row> result = TestingRig.execute(recipe, input);
        Assert.assertEquals(1, result.size());

        Row output = result.get(0);

        double expectedSizeMB = (10 * 1024 + 1 * 1024 * 1024) / (1024.0 * 1024);
        double expectedTimeSec = (150 + 2500) / 1000.0;

        Assert.assertEquals(expectedSizeMB, (double) output.getValue("total_size_mb"), 0.001);
        Assert.assertEquals(expectedTimeSec, (double) output.getValue("total_time_sec"), 0.001);
    }
}
