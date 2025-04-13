import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.test.TestingRig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

  @Test
  public void testAggregateStatsSum() throws Exception {
    List<Row> inputRows = Arrays.asList(
      new Row("data_transfer_size", "1MB").add("response_time", "500ms"),
      new Row("data_transfer_size", "2MB").add("response_time", "1500ms"),
      new Row("data_transfer_size", "512KB").add("response_time", "2000ms")
    );

    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, inputRows);

    // Assert a single aggregated output row
    Assert.assertEquals(1, results.size());

    Row result = results.get(0);

    // Expected: Total Size in MB
    // 1MB + 2MB + 0.5MB = 3.5MB (using 1024-based conversion)
    double expectedTotalSizeMB = 1 + 2 + 512.0 / 1024;

    // Expected: Total Time in seconds
    // 0.5 + 1.5 + 2.0 = 4.0 seconds
    double expectedTotalTimeSec = (500 + 1500 + 2000) / 1000.0;

    Assert.assertEquals(expectedTotalSizeMB, (double) result.getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedTotalTimeSec, (double) result.getValue("total_time_sec"), 0.001);
  }
}
