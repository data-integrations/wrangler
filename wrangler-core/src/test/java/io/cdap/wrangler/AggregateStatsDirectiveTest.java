package io.cdap.wrangler;

import io.cdap.wrangler.api.Row;
import io.cdap.directives.AggregateStatsDirective;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsDirectiveTest {

    @Test
    public void testAggregateStats() throws Exception {
        List<Row> rows = Arrays.asList(
                new Row().add("data_transfer_size", "10KB").add("response_time", "150ms"),
                new Row().add("data_transfer_size", "20KB").add("response_time", "250ms")
        );

        AggregateStatsDirective directive = new AggregateStatsDirective();
        directive.initialize(Arrays.asList(
                "data_transfer_size", "response_time", "total_size_mb", "total_time_sec"
        ));

        List<Row> result = directive.execute(rows, null);
        Row row = result.get(0);

        double expectedSizeMB = (30 * 1024) / (1024.0 * 1024.0); // 30KB in MB
        double expectedTimeSec = 400 / 1000.0; // 400ms in sec

        Assert.assertEquals(expectedSizeMB, (Double) row.getValue("total_size_mb"), 0.001);
        Assert.assertEquals(expectedTimeSec, (Double) row.getValue("total_time_sec"), 0.001);
    }
}
