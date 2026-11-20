/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatsDirectiveTest {

    @Test
    public void testAggregateStats() throws Exception {
        // Create test rows.
        List<Row> rows = new ArrayList<>();
        
        // Row 1: "data_transfer_size" = "10KB", "response_time" = "150ms"
        Row row1 = new Row();
        row1.add("data_transfer_size", "10KB");
        row1.add("response_time", "150ms");
        rows.add(row1);

        // Row 2: "data_transfer_size" = "20KB", "response_time" = "250ms"
        Row row2 = new Row();
        row2.add("data_transfer_size", "20KB");
        row2.add("response_time", "250ms");
        rows.add(row2);

        // Setup the directive using a List<String> for initialization.
        AggregateStatsDirective directive = new AggregateStatsDirective();
        List<String> args = new ArrayList<>();
        args.add("data_transfer_size"); // Source column for bytes.
        args.add("response_time");      // Source column for time.
        args.add("total_size_mb");      // Output column for aggregated size (MB).
        args.add("total_time_sec");     // Output column for aggregated time (sec).
        directive.initialize(args);

        // Execute the directive (passing null for ExecutorContext if it's not used).
        List<Row> results = directive.execute(rows, null);
        Assert.assertEquals("Expected one output row", 1, results.size());

        Row result = results.get(0);
        
        // Expected calculations:
        // Data transfer sizes: "10KB" + "20KB" = (10 + 20) * 1024 bytes.
        long expectedTotalBytes = (10 + 20) * 1024;
        double expectedTotalSizeMB = expectedTotalBytes / (1024.0 * 1024.0);
        // Response times: "150ms" + "250ms" = 400ms.
        int expectedTotalMilliseconds = 150 + 250;
        double expectedTotalTimeSec = expectedTotalMilliseconds / 1000.0;

        // Compare the expected versus actual values.
        Assert.assertEquals("Aggregated size in MB is incorrect",
                expectedTotalSizeMB, (Double) result.getValue("total_size_mb"), 0.001);
        Assert.assertEquals("Aggregated time in seconds is incorrect",
                expectedTotalTimeSec, (Double) result.getValue("total_time_sec"), 0.001);
    }
}
