package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;

public class AggregateStatsDirectiveTest {

  @Test
  public void testAggregateStatsDirective() throws DirectiveParseException {
    AggregateStatsDirective directive = new AggregateStatsDirective();
    directive.prepare();
    
    // Create sample rows with byte size and time duration values.
    Row row1 = new Row();
    row1.add("data_transfer_size", "1024KB"); // 1 MB when 1024 KB = 1 MB
    row1.add("response_time", "5s");            // 5 seconds
    
    Row row2 = new Row();
    row2.add("data_transfer_size", "2048KB"); // 2 MB
    row2.add("response_time", "3s");            // 3 seconds
    
    List<Row> results = directive.execute(Arrays.asList(row1, row2), null);
    Assert.assertEquals(1, results.size());
    Row output = results.get(0);
    
    // Expected results: Total size = (1 MB + 2 MB) = 3 MB, Total time = (5s + 3s) = 8 s.
    Assert.assertEquals(3.0, (Double) output.getValue("total_size_mb"), 0.001);
    Assert.assertEquals(8.0, (Double) output.getValue("total_time_sec"), 0.001);
  }
}
