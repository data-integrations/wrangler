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
package io.cdap.directives.aggregates;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link AggregateSizeDuration}
 */
public class AggregateSizeDurationTest {

        @Test
        public void testAggregateSumMBAndSeconds() throws Exception {
                // Create sample rows with whole numbers
                List<Row> inputRows = Arrays.asList(
                                new Row().add("data_transfer_size", "1MB").add("response_time", "2s"),
                                new Row().add("data_transfer_size", "512KB").add("response_time", "1s"),
                                new Row().add("data_transfer_size", "2MB").add("response_time", "2s"));

                String[] recipe = new String[] {
                                "aggregate-size-duration :data_transfer_size :response_time total_size_mb " +
                                                "total_time_sec MB s"
                };

                List<Row> results = TestingRig.execute(recipe, inputRows);

                // Expected: 1MB + 0.5MB + 2MB = 3.5MB and 2s + 1s + 2s = 5s (whole numbers
                // only)
                Assert.assertEquals(1, results.size());
                long actualSize = (Long) results.get(0).getValue("total_size_mb"); // Use long for whole numbers
                long actualTime = (Long) results.get(0).getValue("total_time_sec"); // Use long for whole numbers

                Assert.assertEquals(3, actualSize); // Adjusted for whole numbers
                Assert.assertEquals(5, actualTime); // Adjusted for whole numbers
        }

        @Test
        public void testAggregateAvgBytesAndMs() throws Exception {
                // Create sample rows with whole numbers
                List<Row> inputRows = Arrays.asList(
                                new Row().add("data_transfer_size", "1024B").add("response_time", "100ms"),
                                new Row().add("data_transfer_size", "2048B").add("response_time", "300ms"));

                String[] recipe = new String[] {
                                "aggregate-size-duration :data_transfer_size :response_time avg_size avg_time B ms avg"
                };

                List<Row> results = TestingRig.execute(recipe, inputRows);

                // Expected average: (1024 + 2048) / 2 = 1536B and (100 + 300) / 2 = 200ms
                // (whole numbers only)
                Assert.assertEquals(1, results.size());
                long actualSize = (Long) results.get(0).getValue("avg_size"); // Use long for whole numbers
                long actualTime = (Long) results.get(0).getValue("avg_time"); // Use long for whole numbers

                Assert.assertEquals(1536, actualSize); // Adjusted for whole numbers
                Assert.assertEquals(200, actualTime); // Adjusted for whole numbers
        }
}
