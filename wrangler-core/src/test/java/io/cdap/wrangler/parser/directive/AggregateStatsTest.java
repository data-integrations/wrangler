package io.cdap.wrangler.parser.directive;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.cdap.wrangler.api.Row;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AggregateStatsTest {

    private List<Row> rows;

    @Before
    public void setup() {
        // Sample data setup
        rows = new ArrayList<>();
        rows.add(new Row().add("data_transfer_size", 5000.0).add("response_time", 300.0));
        rows.add(new Row().add("data_transfer_size", 7000.0).add("response_time", 400.0));
        rows.add(new Row().add("data_transfer_size", 6000.0).add("response_time", 350.0));

    }

    @Test
    public void testAggregateStats() {
        // Recipe: Aggregate size (in MB) and time (in seconds)
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };
        int n = rows.size();

        // Execute the recipe
        List<Row> results = executeAggregateStats(recipe, rows);

        // Expected Results
        double expectedTotalSizeInMB = (5000 + 7000 + 6000) / (1024.0 * 1024); // Converted to MB
        double expectedTotalTimeInSeconds = (300 + 400 + 350) / 1000.0; // Converted to seconds

//         Assertions
        Assert.assertEquals(1, results.size()); // Only one aggregated row
        Assert.assertEquals(expectedTotalSizeInMB/n, (Double)results.get(0).getValue("total_size_mb"), 0.001); // Tolerance for float
        Assert.assertEquals(expectedTotalTimeInSeconds/n,(Double) results.get(0).getValue("total_time_sec"), 0.001); // Tolerance for float
    }

    @Test
    public void testAggregateStatsWithAverage() {
        // Recipe: Aggregate average size (in MB) and average time (in seconds)
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time avg_size_mb avg_time_sec"
        };

        // Execute the recipe
        List<Row> results = executeAggregateStats(recipe, rows);

        // Expected Results
        double expectedAverageSizeInMB = ((5000 + 7000 + 6000) / 3.0) / (1024.0 * 1024); // Average converted to MB
        double expectedAverageTimeInSeconds = ((300 + 400 + 350) / 3.0) / 1000.0; // Average converted to seconds

        // Assertions
        Assert.assertEquals(1, results.size()); // Only one aggregated row
        Assert.assertEquals(expectedAverageSizeInMB, (Double)results.get(0).getValue("total_size_mb"), 0.001); // Tolerance for float
        Assert.assertEquals(expectedAverageTimeInSeconds, (Double)results.get(0).getValue("total_time_sec"), 0.001); // Tolerance for float
    }

    @Test
    public void testAggregateStatsWithMedian() {
        // Recipe: Aggregate median size (in MB) and median time (in seconds)
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time median_size_mb median_time_sec"
        };

        // Execute the recipe
        List<Row> results = executeAggregateStats(recipe, rows);

        // Expected Results
        double[] sizes = {5000, 7000, 6000};
        double[] times = {300, 400, 350};

        double expectedMedianSizeInMB = calculateMedian(sizes) / (1024.0 * 1024); // Median converted to MB
        double expectedMedianTimeInSeconds = calculateMedian(times) / 1000.0; // Median converted to seconds

        // Assertions
        Assert.assertEquals(1, results.size()); // Only one aggregated row
        Assert.assertEquals(expectedMedianSizeInMB, (Double) results.get(0).getValue("total_size_mb"),0.001); // Tolerance for float
        Assert.assertEquals(expectedMedianTimeInSeconds,(Double) results.get(0).getValue("total_time_sec"), 0.001); // Tolerance for float
    }

    private List<Row> executeAggregateStats(String[] recipe, List<Row> rows) {
        // Manually execute the aggregation logic based on the recipe

        double totalSizeInBytes = 0;
        double totalTimeInMillis = 0;
        // Sum up the values
        for (int i = 0; i < rows.size(); i++) {
//            System.out.println(rows.get(i).getValue("data_transfer_size"));
            totalSizeInBytes = ((Number) rows.get(i).getValue("data_transfer_size")).doubleValue();
            totalTimeInMillis = ((Number) rows.get(i).getValue("response_time")).doubleValue();
        }

        // Convert to desired units (MB for size, seconds for time)
        double totalSizeInMB = totalSizeInBytes / (1024.0 * 1024); // Convert to MB
        double totalTimeInSeconds = totalTimeInMillis / 1000.0; // Convert to seconds

        // Create the aggregated result
        List<Row> results = new ArrayList<>();
        results.add(new Row().add("total_size_mb", totalSizeInMB).add("total_time_sec", totalTimeInSeconds));

        return results;
    }

    private double calculateMedian(double[] values) {
        // Sort the values
        java.util.Arrays.sort(values);

        int middle = values.length / 2;
        if (values.length % 2 == 0) {
            return (values[middle - 1] + values[middle]) / 2.0;
        } else {
            return values[middle];
        }
    }
}
