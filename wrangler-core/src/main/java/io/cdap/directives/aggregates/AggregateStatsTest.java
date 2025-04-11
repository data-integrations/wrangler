package io.cdap.directives.aggregate;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.parser.ColumnName;
import org.junit.Test;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

  @Test
  public void testAggregateStatsManualExecution() throws Exception {
    // Create the directive
    AggregateStats directive = new AggregateStats();

    // Manually create the Arguments object
    Arguments args = new Arguments() {
      @Override
      public <T> T value(String name) {
        switch (name) {
          case "sizeCol": return (T) new ColumnName("transfer_size");
          case "timeCol": return (T) new ColumnName("response_time");
          case "outputSizeCol": return (T) new ColumnName("total_size_mb");
          case "outputTimeCol": return (T) new ColumnName("total_time_sec");
        }
        return null;
      }

      @Override
      public boolean contains(String name) {
        return true;
      }
    };

    directive.initialize(args);

    List<Row> inputRows = Arrays.asList(
      new Row("transfer_size", "10MB").add("response_time", "1500ms"),
      new Row("transfer_size", "512KB").add("response_time", "500ms")
    );

    List<Row> output = directive.execute(inputRows, new DummyContext());

    // Expected
    double expectedMb = (10 * 1024 * 1024 + 512 * 1024) / (1024.0 * 1024);
    double expectedSec = (1500 + 500) / 1000.0;

    Assert.assertEquals(1, output.size());
    Row result = output.get(0);

    Assert.assertEquals(expectedMb, (double) result.getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedSec, (double) result.getValue("total_time_sec"), 0.001);
  }

  // Dummy ExecutorContext (you can enhance this if needed)
  private static class DummyContext implements ExecutorContext {}
}
