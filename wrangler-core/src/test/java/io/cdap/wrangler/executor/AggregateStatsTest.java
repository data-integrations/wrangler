package io.cdap.wrangler.executor;

import io.cdap.wrangler.api.Row;
// import java.io.cdap.wrangler.directives.aggregates.AggregateStats;
// "C:\zeotap\wrangler\wrangler-core\src\main\java\io\cdap\directives\aggregates\AggregateStats.java"
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {
  @Test
  public void testAggregation() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("data_transfer_size", "1MB").add("response_time", "100ms"),
      new Row("data_transfer_size", "2MB").add("response_time", "200ms"),
      new Row("data_transfer_size", "0.5MB").add("response_time", "50ms")
    );

    AggregateStats directive = new AggregateStats();
    directive.initialize(new TestArguments(
      "size-column", "data_transfer_size",
      "time-column", "response_time",
      "output-size-column", "total_size_mb",
      "output-time-column", "total_time_sec"
    ));

    directive.execute(rows, null);
    List<Row> results = directive.finalize();

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(3.5, (Double) results.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(0.350, (Double) results.get(0).getValue("total_time_sec"), 0.001);
  }
}