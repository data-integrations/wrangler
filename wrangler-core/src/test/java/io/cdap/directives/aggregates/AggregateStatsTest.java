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
 
 public class AggregateStatsTest {
 
     @Test
     public void testAggregateStatsTotalInBytesAndMs() throws Exception {
         // Single input row with data transfer size and response time
         List<Row> rows = Arrays.asList(
                 new Row("data_transfer_size", "10KB").add("response_time", "100ms")
         );
 
         // Recipe to apply the aggregate-stats directive
         String[] recipe = new String[] {
                 "aggregate-stats :data_transfer_size :response_time :total_size_bytes :total_time_ms"
         };
 
         // Execute the directive with the input row
         List<Row> result = TestingRig.execute(recipe, rows);
 
         // Calculate the expected totals for size and time (for a single row)
         double expectedTotalBytes = 10 * 1024; // 10KB = 10240 bytes
         double expectedMs = 100.0; // 100ms in milliseconds
 
         // Variables to accumulate the totals from the result row
         double totalBytes = 0.0;
         double totalMs = 0.0;
 
         // Iterate through the result and accumulate the values
         for (Row row : result) {
             Object sizeVal = row.getValue("total_size_bytes");
             Object timeVal = row.getValue("total_time_ms");
 
             // Ensure the values are of type Number and accumulate
             if (sizeVal instanceof Number && timeVal instanceof Number) {
                 totalBytes += ((Number) sizeVal).doubleValue();
                 totalMs += ((Number) timeVal).doubleValue();
             } else {
                 Assert.fail("Output row missing expected numeric total fields.");
             }
         }
 
         // Assert that the number of rows in the result is the same as the input rows (single row test)
         Assert.assertEquals(rows.size(), result.size());
 
         // Assert that the accumulated totals match the expected totals
         Assert.assertEquals(expectedTotalBytes, totalBytes, 0.001);
         Assert.assertEquals(expectedMs, totalMs, 0.001);
     }
 
 }
 
