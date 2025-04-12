package io.cdap.directives.aggregates;

import java.util.ArrayList;
import java.util.List;

import io.cdap.cdap.internal.capability.autoinstall.Spec;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AggregationTests {

    private Aggregation directive;

    @Before
    public void setUp() {
        directive = new Aggregation();
    }

    @Test
    public void testAggregationSum() {
        try {
            List<Row> rows = new ArrayList<>();
            rows.add(new Row("data_transfer_size", 5000));
            rows.add(new Row("response_time", 1000000000));

            directive.initialize((Arguments) new Spec.Action.Argument("byteSizeColumn", "data_transfer_size", false));
            directive.initialize((Arguments) new Spec.Action.Argument("timeColumn", "response_time", false));
            directive.initialize((Arguments) new Spec.Action.Argument("totalSizeColumn", "total_size_mb", false));
            directive.initialize((Arguments) new Spec.Action.Argument("totalTimeColumn", "total_time_sec", false));
            directive.initialize((Arguments) new Spec.Action.Argument("aggregationType", "total", false));

            List<Row> result = directive.execute(rows, null);
            directive.finalize(null);

            Assert.assertEquals(String.valueOf(0.00476837158203125), result.get(0).getValue("total_size_mb"), 0.001);
            Assert.assertEquals(String.valueOf(1.0), result.get(0).getValue("total_time_sec"), 0.001);
        } catch (DirectiveExecutionException | ErrorRowException e) {
            Assert.fail("Execution failed: " + e.getMessage());
        } catch (DirectiveParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAggregationAverage() {
        try {
            List<Row> rows = new ArrayList<>();
            rows.add(new Row("data_transfer_size", 2000));
            rows.add(new Row("response_time", 500000000));

            directive.initialize((Arguments) new Spec.Action.Argument("byteSizeColumn", "data_transfer_size", false));
            directive.initialize((Arguments) new Spec.Action.Argument("timeColumn", "response_time", false));
            directive.initialize((Arguments) new Spec.Action.Argument("totalSizeColumn", "total_size_mb", false));
            directive.initialize((Arguments) new Spec.Action.Argument("totalTimeColumn", "total_time_sec", false));
            directive.initialize((Arguments) new Spec.Action.Argument("aggregationType", "total", false));

            List<Row> result = directive.execute(rows, null);
            directive.finalize(null);

            Assert.assertEquals(String.valueOf(0.0019073486328125), result.get(0).getValue("total_size_mb"), 0.001);
            Assert.assertEquals(String.valueOf(0.75), result.get(0).getValue("total_time_sec"), 0.001);
        } catch (DirectiveExecutionException | ErrorRowException e) {
            Assert.fail("Execution failed: " + e.getMessage());
        } catch (DirectiveParseException e) {
            throw new RuntimeException(e);
        }
    }
}

