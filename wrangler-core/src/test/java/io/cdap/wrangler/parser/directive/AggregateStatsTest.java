package io.cdap.wrangler.parser.directive;
/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.cdap.wrangler.api.Row;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import io.cdap.wrangler.parser.directive.AggregateStatsDirective;

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
                "aggregate-stats :data_transfer_size :response_time sum_size_mb sum_time_sec"
        };
        int n = rows.size();
        AggregateStatsDirective exec = new AggregateStatsDirective();
        // Execute the recipe
        List<Row> results = exec.executeAggregateStats(recipe, rows);

        // Expected Results
        double expectedTotalSizeInMB = (5000 + 7000 + 6000) / (1024.0 * 1024); // Converted to MB
        double expectedTotalTimeInSeconds = (300 + 400 + 350) / 1000.0; // Converted to seconds

//         Assertions
        Assert.assertEquals(1, results.size()); // Only one aggregated row
        Assert.assertEquals(expectedTotalSizeInMB, (Double)results.get(0).getValue("sum_size_mb"), 0.001); // Tolerance for float
        Assert.assertEquals(expectedTotalTimeInSeconds,(Double) results.get(0).getValue("sum_time_sec"), 0.001); // Tolerance for float
    }

    @Test
    public void testAggregateStatsWithAverage() {
        // Recipe: Aggregate average size (in MB) and average time (in seconds)
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time avg_size_mb avg_time_sec"
        };
        AggregateStatsDirective exec = new AggregateStatsDirective();

        // Execute the recipe
        List<Row> results = exec.executeAggregateStats(recipe, rows);

        // Expected Results
        double expectedAverageSizeInMB = ((5000 + 7000 + 6000) / 3.0) / (1024.0 * 1024); // Average converted to MB
        double expectedAverageTimeInSeconds = ((300 + 400 + 350) / 3.0) / 1000.0; // Average converted to seconds

        // Assertions
        Assert.assertEquals(1, results.size()); // Only one aggregated row
        Assert.assertEquals(expectedAverageSizeInMB, (Double)results.get(0).getValue("avg_size_mb"), 0.001); // Tolerance for float
        Assert.assertEquals(expectedAverageTimeInSeconds, (Double)results.get(0).getValue("avg_time_sec"), 0.001); // Tolerance for float
    }

    @Test
    public void testAggregateStatsWithMedian() {
        // Recipe: Aggregate median size (in MB) and median time (in seconds)
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time median_size_mb median_time_sec"
        };
        AggregateStatsDirective exec = new AggregateStatsDirective();

        // Execute the recipe
        List<Row> results = exec.executeAggregateStats(recipe, rows);

        // Expected Results
        double[] sizes = {5000, 7000, 6000};
        double[] times = {300, 400, 350};

        double expectedMedianSizeInMB = calculateMedian(sizes) / (1024.0 * 1024); // Median converted to MB
        double expectedMedianTimeInSeconds = calculateMedian(times) / 1000.0; // Median converted to seconds

        // Assertions
        Assert.assertEquals(1, results.size()); // Only one aggregated row
        Assert.assertEquals(expectedMedianSizeInMB, (Double) results.get(0).getValue("median_size_mb"),0.001); // Tolerance for float
        Assert.assertEquals(expectedMedianTimeInSeconds,(Double) results.get(0).getValue("median_time_sec"), 0.001); // Tolerance for float
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
