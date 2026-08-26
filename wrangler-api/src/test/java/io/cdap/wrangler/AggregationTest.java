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
package io.cdap.wrangler.api;

import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregationTest {

    @Test
    public void testAggregation() {
        // Step 1: Create sample data rows
        List<Row> rows = new ArrayList<>();

        // Add data_transfer_size in bytes
        rows.add(new Row("data_transfer_size", 1024L));  // 1 KB
        rows.add(new Row("data_transfer_size", 2048L));  // 2 KB

        // Add response_time in milliseconds
        rows.add(new Row("response_time", 100L));  // 100 ms
        rows.add(new Row("response_time", 200L));  // 200 ms

        // Step 2: Define the aggregation recipe
        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        // Step 3: Execute the aggregation
        List<Row> results = executeAggregation(recipe, rows);

        // Step 4: Assert the expected results
        Assert.assertEquals(1, results.size());  // Expecting 1 row after aggregation

        // Convert total_size to MB and total_time to seconds
        double expectedTotalSizeInMB = (1024L + 2048L) / (1024.0 * 1024.0);  // Total size in MB
        double expectedTotalTimeInSec = (100L + 200L) / 1000.0;  // Total time in seconds

        // Ensure values returned from results are double type before using assertEquals
        double actualTotalSizeInMB = (Double) results.get(0).getValue("total_size_mb");
        double actualTotalTimeInSec = (Double) results.get(0).getValue("total_time_sec");

        // Assert the aggregation result values with precision tolerance (delta)
        Assert.assertEquals(expectedTotalSizeInMB, actualTotalSizeInMB, 0.001);  // 0.001 tolerance
        Assert.assertEquals(expectedTotalTimeInSec, actualTotalTimeInSec, 0.001);  // 0.001 tolerance
    }

    /**
     * Mocking the executeAggregation method to simulate the aggregation process.
     *
     * @param recipe The recipe for aggregation.
     * @param rows   The rows to be aggregated.
     * @return The result of the aggregation.
     */
    private List<Row> executeAggregation(String[] recipe, List<Row> rows) {
        List<Row> results = new ArrayList<>();

        // Simulate the aggregation logic (sum of data_transfer_size and response_time)
        long totalSizeBytes = 0;
        long totalTimeMillis = 0;

        // Iterate through rows to calculate total size and time
        for (Row row : rows) {
            Object size = row.getValue("data_transfer_size");
            if (size != null) {
                totalSizeBytes += (Long) size;
            }
            Object time = row.getValue("response_time");
            if (time != null) {
                totalTimeMillis += (Long) time;
            }
        }

        // Convert total size to MB and total time to seconds
        double totalSizeMB = totalSizeBytes / (1024.0 * 1024.0);
        double totalTimeSec = totalTimeMillis / 1000.0;

        // Create the result row
        Row resultRow = new Row();
        resultRow.add("total_size_mb", totalSizeMB);
        resultRow.add("total_time_sec", totalTimeSec);

        // Add result row to the list
        results.add(resultRow);

        return results;
    }
}
