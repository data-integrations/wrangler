package io.cdap.wrangler.parser;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

  @Test
  public void testAggregateStats() throws Exception {
    List<Row> input = Arrays.asList(
      new Row("data_transfer_size", "1MB").add("response_time", "500ms"),
      new Row("data_transfer_size", "2MB").add("response_time", "1500ms"),
      new Row("data_transfer_size", "512KB").add("response_time", "2s")
    );

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, input);

    Assert.assertEquals(1, results.size());

    double expectedMB = (1 * 1024 * 1024 + 2 * 1024 * 1024 + 512 * 1024) / (1024.0 * 1024.0);
    double expectedSeconds = (500 + 1500 + 2000) / 1000.0;

    Assert.assertEquals(expectedMB, (Double) results.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedSeconds, (Double) results.get(0).getValue("total_time_sec"), 0.001);
  }
}
