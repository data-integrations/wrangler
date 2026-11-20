import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.AggregateStats;

public class AggregateStatsTest {
    @Test
    public void testAggregation() {
        // Setup test data
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("10KB", "150ms"));
        rows.add(new Row("20MB", "2s"));

        // Create an instance of AggregateStats
        AggregateStats aggregateStats = new AggregateStats("10KB", "150ms", 10, 20);
        aggregateStats.execute(rows);

        // Assert expected results
        Assert.assertEquals(20971520, rows.get(0).getValue("totalSize")); // 20MB + 10KB in bytes
        Assert.assertEquals(2150000000L, rows.get(0).getValue("totalTime")); // 2s + 150ms in nanoseconds
    }
}