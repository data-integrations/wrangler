package io.cdap.wrangler.steps.aggregate;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.test.TestingRig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsDirectiveTest {

  @Test
  public void testTotalAggregation() throws Exception {
    List<Row> inputRows = Arrays.asList(
      new Row("data_transfer_size", "1MB").add("response_time", "2s"),
      new Row("data_transfer_size", "2MB").add("response_time", "3s"),
      new Row("data_transfer_size", "512KB").add("response_time", "1.5s")
    );

    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec 'MB' 's'"
    };

    List<Row> results = TestingRig.execute(recipe, inputRows);

    // 1MB + 2MB + 0.5MB = 3.5MB
    double expectedTotalSizeMB = 3.5;
    // 2s + 3s + 1.5s = 6.5s
    double expectedTotalTimeSec = 6.5;

    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    Assert.assertEquals(expectedTotalSizeMB,
                        (Double) result.getValue("total_size_mb"),
                        0.001);
    Assert.assertEquals(expectedTotalTimeSec,
                        (Double) result.getValue("total_time_sec"),
                        0.001);
  }

  @Test
  public void testAverageAggregation() throws Exception {
    List<Row> inputRows = Arrays.asList(
      new Row("data_transfer_size", "1MB").add("response_time", "2s"),
      new Row("data_transfer_size", "3MB").add("response_time", "4s")
    );

    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec 'MB' 's' true"
    };

    List<Row> results = TestingRig.execute(recipe, inputRows);

    // avg size: (1 + 3) / 2 = 2MB
    double expectedAvgSize = 2.0;
    // avg time: (2 + 4) / 2 = 3s
    double expectedAvgTime = 3.0;

    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    Assert.assertEquals(expectedAvgSize,
                        (Double) result.getValue("total_size_mb"),
                        0.001);
    Assert.assertEquals(expectedAvgTime,
                        (Double) result.getValue("total_time_sec"),
                        0.001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSizeUnit() throws Exception {
    List<Row> input = List.of(new Row("data_transfer_size", "5PB").add("response_time", "1s"));

    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec 'MB' 's'"
    };

    TestingRig.execute(recipe, input); // should throw
  }
}
