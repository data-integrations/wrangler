package io.cdap.wrangler;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.utils.TestingRig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit test for AggregateStats directive.
 */
public class AggregateStatsTest {

    @Test
    public void testAggregationTotal() throws Exception {
        List<Row> inputRows = Arrays.asList(
            new Row("data_transfer_size", "1MB").add("response_time", "500ms"),
            new Row("data_transfer_size", "2MB").add("response_time", "1.5s"),
            new Row("data_transfer_size", "512KB").add("response_time", "2s")
        );

        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        List<Row> results = TestingRig.execute(recipe, inputRows);

        // There should be only one output row from aggregate
        Assert.assertEquals(1, results.size());

        Row result = results.get(0);

        double totalMB = 1 + 2 + 0.5; // 3.5 MB
        double totalSec = 0.5 + 1.5 + 2; // 4.0 sec

        double actualMB = (Double) result.getValue("total_size_mb");
        double actualSec = (Double) result.getValue("total_time_sec");

        Assert.assertEquals(totalMB, actualMB, 0.001);
        Assert.assertEquals(totalSec, actualSec, 0.001);
    }
}