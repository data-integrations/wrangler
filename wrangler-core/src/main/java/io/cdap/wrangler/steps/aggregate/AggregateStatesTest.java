package io.cdap.wrangler.steps.aggregate;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Store;
import io.cdap.wrangler.internal.DefaultExecutorContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {
  @Test
  public void testAggregateTotalBytesAndTime() throws Exception {
    AggregateStats directive = new AggregateStats();
    directive.initialize(new MockArguments(
      ":size", ":duration", ":total_size", ":total_time", "MB", "s", false));

    Row row1 = new Row().add(":size", new ByteSize("1MB")).add(":duration", new TimeDuration("1s"));
    Row row2 = new Row().add(":size", new ByteSize("2MB")).add(":duration", new TimeDuration("3s"));
    List<Row> rows = Arrays.asList(row1, row2);

    ExecutorContext context = new DefaultExecutorContext();
    directive.execute(rows, context);

    List<Row> result = directive.finalize(context).rows();
    Row output = result.get(0);

    Assert.assertEquals(3.0, output.getValue(":total_size"));  // 3 MB
    Assert.assertEquals(4.0, output.getValue(":total_time"));  // 4 s
  }
}
