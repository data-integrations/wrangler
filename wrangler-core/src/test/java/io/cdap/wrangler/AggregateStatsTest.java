package io.cdap.wrangler;

import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {
  @Test
  public void testAggregate() throws Exception {
    Row r1 = new Row("size", "5MB", "time", "2s");
    Row r2 = new Row("size", "3MB", "time", "3s");

    AggregateStats directive = new AggregateStats();
    directive.initialize(null, new String[] {"size", "time", "totalSize", "totalTime"});
    List<Row> output = directive.execute(Arrays.asList(r1, r2));

    Assert.assertEquals(1, output.size());
    Row out = output.get(0);
    Assert.assertEquals(8.0, (double) out.getValue("totalSize"), 0.001);
    Assert.assertEquals(5.0, (double) out.getValue("totalTime"), 0.001);
  }
}