/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.wrangler.directives.aggregation;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for aggregate-stats directive with ByteSize and TimeDuration values.
 */
public class AggregateStatsTest {

    @Test
    public void testByteSizeAndTimeDurationAggregation() throws Exception {
        // Create sample data with columns for data transfer size and response time
        List<Row> rows = new ArrayList<>();

        // Row 1: 5MB transfer, 2s response
        Row row1 = new Row();
        row1.add("data_transfer_size", "5MB");
        row1.add("response_time", "2s");
        rows.add(row1);

        // Row 2: 10KB transfer, 500ms response
        Row row2 = new Row();
        row2.add("data_transfer_size", "10KB");
        row2.add("response_time", "500ms");
        rows.add(row2);

        // Row 3: 2.5MB transfer, 1.5m response
        Row row3 = new Row();
        row3.add("data_transfer_size", "2.5MB");
        row3.add("response_time", "1.5m");
        rows.add(row3);

        // Define the recipe to aggregate the data
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        // Execute the recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify the results
        Assert.assertEquals(1, results.size());

        // Calculate expected values
        // Size calculation: 5MB + 10KB + 2.5MB in MB
        // 5MB = 5 * 1024 * 1024 bytes
        // 10KB = 10 * 1024 bytes
        // 2.5MB = 2.5 * 1024 * 1024 bytes
        // Total bytes = 5 * 1024 * 1024 + 10 * 1024 + 2.5 * 1024 * 1024
        // Total MB = Total bytes / (1024 * 1024)
        double expectedTotalSizeInMB = 5 + (10.0 * 1024) / (1024 * 1024) + 2.5;

        // Time calculation: 2s + 500ms + 1.5m in seconds
        // 2s = 2 seconds
        // 500ms = 0.5 seconds
        // 1.5m = 1.5 * 60 seconds = 90 seconds
        // Total seconds = 2 + 0.5 + 90 = 92.5
        double expectedTotalTimeInSeconds = 2 + 0.5 + (1.5 * 60);

        // Assert with small tolerance for floating point comparisons
        Assert.assertEquals(expectedTotalSizeInMB,
                (Double) results.get(0).getValue("total_size_mb"), 0.001);
        Assert.assertEquals(expectedTotalTimeInSeconds,
                (Double) results.get(0).getValue("total_time_sec"), 0.001);
    }

    @Test
    public void testByteSizeAndTimeDurationAverages() throws Exception {
        // Create sample data with columns for data transfer size and response time
        List<Row> rows = new ArrayList<>();

        // Row 1: 5MB transfer, 2s response
        Row row1 = new Row();
        row1.add("data_transfer_size", "5MB");
        row1.add("response_time", "2s");
        rows.add(row1);

        // Row 2: 10KB transfer, 500ms response
        Row row2 = new Row();
        row2.add("data_transfer_size", "10KB");
        row2.add("response_time", "500ms");
        rows.add(row2);

        // Row 3: 2.5MB transfer, 1.5m response
        Row row3 = new Row();
        row3.add("data_transfer_size", "2.5MB");
        row3.add("response_time", "1.5m");
        rows.add(row3);

        // Define the recipe to aggregate the data with average
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time avg_size_mb avg_time_sec average"
        };

        // Execute the recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify the results
        Assert.assertEquals(1, results.size());

        // Calculate expected values
        // Size calculation: (5MB + 10KB + 2.5MB) / 3 in MB
        double totalSizeInMB = 5 + (10.0 * 1024) / (1024 * 1024) + 2.5;
        double expectedAvgSizeInMB = totalSizeInMB / 3;

        // Time calculation: (2s + 500ms + 1.5m) / 3 in seconds
        double totalTimeInSeconds = 2 + 0.5 + (1.5 * 60);
        double expectedAvgTimeInSeconds = totalTimeInSeconds / 3;

        // Assert with small tolerance for floating point comparisons
        Assert.assertEquals(expectedAvgSizeInMB,
                (Double) results.get(0).getValue("avg_size_mb"), 0.001);
        Assert.assertEquals(expectedAvgTimeInSeconds,
                (Double) results.get(0).getValue("avg_time_sec"), 0.001);
    }

    @Test
    public void testMultipleUnitOutputs() throws Exception {
        // Create sample data with columns for data transfer size and response time
        List<Row> rows = new ArrayList<>();

        // Create 5 rows with various sizes and times
        Row row1 = new Row();
        row1.add("data_transfer_size", "1GB");
        row1.add("response_time", "3m");
        rows.add(row1);

        Row row2 = new Row();
        row2.add("data_transfer_size", "500MB");
        row2.add("response_time", "45s");
        rows.add(row2);

        Row row3 = new Row();
        row3.add("data_transfer_size", "250MB");
        row3.add("response_time", "20s");
        rows.add(row3);

        Row row4 = new Row();
        row4.add("data_transfer_size", "100KB");
        row4.add("response_time", "150ms");
        rows.add(row4);

        Row row5 = new Row();
        row5.add("data_transfer_size", "2MB");
        row5.add("response_time", "1s");
        rows.add(row5);

        // Define the recipe to aggregate the data with multiple output units
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time total_size_gb total_time_min"
        };

        // Execute the recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify the results
        Assert.assertEquals(1, results.size());

        // Calculate expected values
        // Size calculation: 1GB + 500MB + 250MB + 100KB + 2MB in GB
        // 1GB = 1 GB
        // 500MB = 500/1024 GB
        // 250MB = 250/1024 GB
        // 100KB = 100/(1024*1024) GB
        // 2MB = 2/1024 GB
        // Total GB = 1 + 500/1024 + 250/1024 + 100/(1024*1024) + 2/1024
        double expectedTotalSizeInGB = 1 + 500.0 / 1024 + 250.0 / 1024 + 100.0 / (1024 * 1024) + 2.0 / 1024;

        // Time calculation: 3m + 45s + 20s + 150ms + 1s in minutes
        // 3m = 3 minutes
        // 45s = 45/60 minutes
        // 20s = 20/60 minutes
        // 150ms = 150/(1000*60) minutes
        // 1s = 1/60 minutes
        // Total minutes = 3 + 45/60 + 20/60 + 150/(1000*60) + 1/60
        double expectedTotalTimeInMin = 3 + 45.0 / 60 + 20.0 / 60 + 150.0 / (1000 * 60) + 1.0 / 60;

        // Assert with small tolerance for floating point comparisons
        Assert.assertEquals(expectedTotalSizeInGB,
                (Double) results.get(0).getValue("total_size_gb"), 0.001);
        Assert.assertEquals(expectedTotalTimeInMin,
                (Double) results.get(0).getValue("total_time_min"), 0.001);
    }

    @Test
    public void testEmptyDataSet() throws Exception {
        // Create an empty data set
        List<Row> rows = new ArrayList<>();

        // Define the recipe to aggregate the data
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        // Execute the recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify the results - should have one row with zeros
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(0.0, (Double) results.get(0).getValue("total_size_mb"), 0.001);
        Assert.assertEquals(0.0, (Double) results.get(0).getValue("total_time_sec"), 0.001);
    }

    @Test
    public void testMedianStatistics() throws Exception {
        // Create sample data with columns for data transfer size and response time
        List<Row> rows = new ArrayList<>();

        // Create multiple rows with different values to allow for meaningful median
        // calculation
        // Row 1: 100KB transfer, 100ms response
        Row row1 = new Row();
        row1.add("data_transfer_size", "100KB");
        row1.add("response_time", "100ms");
        rows.add(row1);

        // Row 2: 200KB transfer, 200ms response
        Row row2 = new Row();
        row2.add("data_transfer_size", "200KB");
        row2.add("response_time", "200ms");
        rows.add(row2);

        // Row 3: 300KB transfer, 300ms response
        Row row3 = new Row();
        row3.add("data_transfer_size", "300KB");
        row3.add("response_time", "300ms");
        rows.add(row3);

        // Row 4: 400KB transfer, 400ms response
        Row row4 = new Row();
        row4.add("data_transfer_size", "400KB");
        row4.add("response_time", "400ms");
        rows.add(row4);

        // Row 5: 500KB transfer, 500ms response
        Row row5 = new Row();
        row5.add("data_transfer_size", "500KB");
        row5.add("response_time", "500ms");
        rows.add(row5);

        // Define the recipe to aggregate the data with median
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time median_size_kb median_time_ms median"
        };

        // Execute the recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify the results
        Assert.assertEquals(1, results.size());

        // Median for an odd number of values is the middle value
        // For size: [100, 200, 300, 400, 500] -> median is 300
        // For time: [100, 200, 300, 400, 500] -> median is 300
        Assert.assertEquals(300.0, (Double) results.get(0).getValue("median_size_kb"), 0.001);
        Assert.assertEquals(300.0, (Double) results.get(0).getValue("median_time_ms"), 0.001);
    }

    @Test
    public void testPercentileP95Statistics() throws Exception {
        // Create sample data with columns for data transfer size and response time
        List<Row> rows = new ArrayList<>();

        // Create 20 rows with increasing values
        for (int i = 1; i <= 20; i++) {
            Row row = new Row();
            row.add("data_transfer_size", i + "MB");
            row.add("response_time", i + "s");
            rows.add(row);
        }

        // Define the recipe to aggregate the data with p95 percentile
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time p95_size_mb p95_time_sec p95"
        };

        // Execute the recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify the results
        Assert.assertEquals(1, results.size());

        // P95 for 20 values means the 19th value (index 0-based)
        // For size: [1, 2, 3, ..., 20] -> p95 is 19
        // For time: [1, 2, 3, ..., 20] -> p95 is 19
        Assert.assertEquals(19.0, (Double) results.get(0).getValue("p95_size_mb"), 0.001);
        Assert.assertEquals(19.0, (Double) results.get(0).getValue("p95_time_sec"), 0.001);
    }

    @Test
    public void testPercentileP99Statistics() throws Exception {
        // Create sample data with columns for data transfer size and response time
        List<Row> rows = new ArrayList<>();

        // Create 100 rows with increasing values
        for (int i = 1; i <= 100; i++) {
            Row row = new Row();
            row.add("data_transfer_size", i + "MB");
            row.add("response_time", i + "s");
            rows.add(row);
        }

        // Define the recipe to aggregate the data with p99 percentile
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time p99_size_mb p99_time_sec p99"
        };

        // Execute the recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify the results
        Assert.assertEquals(1, results.size());

        // P99 for 100 values means the 99th value (index 0-based)
        // For size: [1, 2, 3, ..., 100] -> p99 is 99
        // For time: [1, 2, 3, ..., 100] -> p99 is 99
        Assert.assertEquals(99.0, (Double) results.get(0).getValue("p99_size_mb"), 0.001);
        Assert.assertEquals(99.0, (Double) results.get(0).getValue("p99_time_sec"), 0.001);
    }

    @Test
    public void testMultipleStatisticsComparison() throws Exception {
        // Create sample data with a skewed distribution
        List<Row> rows = new ArrayList<>();

        // Create rows with mostly small values but a few large outliers
        // First, 90 small values (1-10MB)
        for (int i = 1; i <= 90; i++) {
            Row row = new Row();
            int value = (i % 10) + 1; // Values 1-10MB repeated
            row.add("data_transfer_size", value + "MB");
            row.add("response_time", value + "s");
            rows.add(row);
        }

        // Then, 10 large values (50-500MB) representing outliers
        for (int i = 1; i <= 10; i++) {
            Row row = new Row();
            int value = i * 50; // Values 50, 100, 150, ..., 500
            row.add("data_transfer_size", value + "MB");
            row.add("response_time", value + "s");
            rows.add(row);
        }

        // Test total aggregation
        List<Row> totalResults = TestingRig.execute(new String[] {
                "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        }, rows);

        // Test average aggregation
        List<Row> avgResults = TestingRig.execute(new String[] {
                "aggregate-stats :data_transfer_size :response_time avg_size_mb avg_time_sec average"
        }, rows);

        // Test median aggregation
        List<Row> medianResults = TestingRig.execute(new String[] {
                "aggregate-stats :data_transfer_size :response_time median_size_mb median_time_sec median"
        }, rows);

        // Test p95 aggregation
        List<Row> p95Results = TestingRig.execute(new String[] {
                "aggregate-stats :data_transfer_size :response_time p95_size_mb p95_time_sec p95"
        }, rows);

        // Calculate expected values
        // Total: Sum of all values
        double totalSize = 0;
        for (int i = 1; i <= 90; i++) {
            totalSize += (i % 10) + 1;
        }
        for (int i = 1; i <= 10; i++) {
            totalSize += i * 50;
        }

        // Average: Total / 100
        double avgSize = totalSize / 100;

        // Median: Middle value (or average of two middle values)
        // Since we have 90 values in range 1-10 and 10 values 50-500
        // The median will be in the first group around 5-6MB
        double medianSize = 5.5; // Approximate median for illustration

        // P95: 95th value
        // This will be in the second group of larger values
        double p95Size = 250; // Approximately the 5th value in the large group

        // Verify all results
        Assert.assertEquals(1, totalResults.size());
        Assert.assertEquals(1, avgResults.size());
        Assert.assertEquals(1, medianResults.size());
        Assert.assertEquals(1, p95Results.size());

        // Total size should be the sum of all sizes
        Assert.assertEquals(totalSize, (Double) totalResults.get(0).getValue("total_size_mb"), 1.0);

        // Average size
        Assert.assertEquals(avgSize, (Double) avgResults.get(0).getValue("avg_size_mb"), 1.0);

        // Median size - less affected by outliers
        Assert.assertEquals(medianSize, (Double) medianResults.get(0).getValue("median_size_mb"), 1.0);

        // P95 size - at the high end of the distribution
        Assert.assertEquals(p95Size, (Double) p95Results.get(0).getValue("p95_size_mb"), 50.0);

        // The p95 should be significantly higher than median, showing effect of
        // outliers
        double p95Value = (Double) p95Results.get(0).getValue("p95_size_mb");
        double medianValue = (Double) medianResults.get(0).getValue("median_size_mb");
        Assert.assertTrue("P95 should be significantly higher than median", p95Value > 10 * medianValue);
    }
}
// End of AggregateStatsTest class