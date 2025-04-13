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
  package io.cdap.wrangler;
  import io.cdap.wrangler.TestingRig;
  import io.cdap.wrangler.api.Row;
  import org.junit.Assert;
  import org.junit.Test;
  
  import java.util.ArrayList;
  import java.util.List;
  
  public class AggregateStatsTest {
  
      @Test
      public void testAggregateStatsTotalMode() throws Exception {
          List<Row> rows = new ArrayList<>();
          rows.add(new Row("data_transfer_size", "1MB").add("response_time", "1s"));
          rows.add(new Row("data_transfer_size", "2MB").add("response_time", "2s"));
          rows.add(new Row("data_transfer_size", "512KB").add("response_time", "500ms"));
  
          String[] recipe = new String[] {
                  "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
          };
  
          List<Row> results = TestingRig.execute(recipe, rows);
  
          Assert.assertEquals(1, results.size());
  
          double expectedTotalSizeMB = (1 + 2 + 0.5); // 3.5MB
          double expectedTotalTimeSeconds = (1 + 2 + 0.5); // 3.5s
  
          Row result = results.get(0);
          double actualSizeMB = Double.parseDouble(result.getValue("total_size_mb").toString().replace(" MB", ""));
          double actualTimeSec = Double.parseDouble(result.getValue("total_time_sec").toString().replace(" s", ""));
  
          Assert.assertEquals(expectedTotalSizeMB, actualSizeMB, 0.001);
          Assert.assertEquals(expectedTotalTimeSeconds, actualTimeSec, 0.001);
      }
  
      @Test
      public void testAggregateStatsAverageMode() throws Exception {
          List<Row> rows = new ArrayList<>();
          rows.add(new Row("data_transfer_size", "1MB").add("response_time", "1s"));
          rows.add(new Row("data_transfer_size", "3MB").add("response_time", "3s"));
  
          String[] recipe = new String[] {
                  "aggregate-stats :data_transfer_size :response_time avg_size_mb avg_time_sec average"
          };
  
          List<Row> results = TestingRig.execute(recipe, rows);
  
          Assert.assertEquals(1, results.size());
  
          double expectedAvgSizeMB = (1 + 3) / 2.0; // 2.0MB
          double expectedAvgTimeSec = (1 + 3) / 2.0; // 2.0s
  
          Row result = results.get(0);
          double actualAvgSizeMB = Double.parseDouble(result.getValue("avg_size_mb").toString().replace(" MB", ""));
          double actualAvgTimeSec = Double.parseDouble(result.getValue("avg_time_sec").toString().replace(" s", ""));
  
          Assert.assertEquals(expectedAvgSizeMB, actualAvgSizeMB, 0.001);
          Assert.assertEquals(expectedAvgTimeSec, actualAvgTimeSec, 0.001);
      }
  
      // Add more tests here for p95, p99, median, invalid formats, etc.
  
  }