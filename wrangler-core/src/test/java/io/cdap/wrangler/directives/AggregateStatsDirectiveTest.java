package io.cdap.wrangler.directives;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatsDirectiveTest {

    @Test
    public void testAggregateStatsTotal() throws Exception {
        // Step 1: Prepare Input Data
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("data_transfer_size", "10KB").add("response_time", "500ms"));
        rows.add(new Row().add("data_transfer_size", "1MB").add("response_time", "2s"));
        rows.add(new Row().add("data_transfer_size", "500KB").add("response_time", "1.5s"));

        // Step 2: Define the Recipe
        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        // Step 3: Execute the Recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Step 4: Assert the Results
        Assert.assertEquals(1, results.size());

        Row resultRow = results.get(0);

        // Calculate expected values
        double expectedTotalSizeInMB = (10 * 1024 + 1 * 1024 * 1024 + 500 * 1024) / (1024.0 * 1024.0); // Convert to MB
        double expectedTotalTimeInSeconds = (500 / 1000.0) + 2 + 1.5; // Convert to seconds

        Assert.assertEquals(expectedTotalSizeInMB, (Double) resultRow.getValue("total_size_mb"), 0.001);
        Assert.assertEquals(expectedTotalTimeInSeconds, (Double) resultRow.getValue("total_time_sec"), 0.001);
    }

    @Test
    public void testAggregateStatsAverage() throws Exception {
        // Step 1: Prepare Input Data
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("data_transfer_size", "10KB").add("response_time", "500ms"));
        rows.add(new Row().add("data_transfer_size", "1MB").add("response_time", "2s"));
        rows.add(new Row().add("data_transfer_size", "500KB").add("response_time", "1.5s"));

        // Step 2: Define the Recipe
        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time avg_size_mb avg_time_sec"
        };

        // Step 3: Execute the Recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Step 4: Assert the Results
        Assert.assertEquals(1, results.size());

        Row resultRow = results.get(0);

        // Calculate expected values
        double totalSizeInBytes = 10 * 1024 + 1 * 1024 * 1024 + 500 * 1024;
        double expectedAvgSizeInMB = (totalSizeInBytes / rows.size()) / (1024.0 * 1024.0); // Convert to MB
        double totalTimeInSeconds = (500 / 1000.0) + 2 + 1.5;
        double expectedAvgTimeInSeconds = totalTimeInSeconds / rows.size();

        Assert.assertEquals(expectedAvgSizeInMB, (Double) resultRow.getValue("avg_size_mb"), 0.001);
        Assert.assertEquals(expectedAvgTimeInSeconds, (Double) resultRow.getValue("avg_time_sec"), 0.001);
    }
}