package io.cdap.wrangler.directive;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AggregateStatsTest {
    @Test
    public void testAggregateStats() {
        Row row1 = new Row().add("size", new ByteSize("10MB")).add("time", new TimeDuration("5s"));
        Row row2 = new Row().add("size", new ByteSize("20MB")).add("time", new TimeDuration("10s"));

        AggregateStats directive = new AggregateStats();
        directive.initialize(new Arguments("size", "time", "total_size_mb", "total_time_sec"));

        List<Row> result = directive.execute(List.of(row1, row2), null);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals(30.0, result.get(0).getValue("total_size_mb"), 0.001);
        Assert.assertEquals(15.0, result.get(0).getValue("total_time_sec"), 0.001);
    }
}
